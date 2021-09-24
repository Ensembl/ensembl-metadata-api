import sqlalchemy as db
from sqlalchemy.orm import Session
import pymysql

from ensembl.production.metadata.config import MetadataConfig

pymysql.install_as_MySQLdb()
config = MetadataConfig()


def load_database(uri):
    try:
        engine = db.create_engine(uri)
    except AttributeError as err:
        raise ValueError(f'Could not connect to database {uri}: {err}.') from err

    try:
        connection = engine.connect()
    except db.exc.OperationalError as err:
        raise ValueError(f'Could not connect to database {uri}: {err}.') from err

    connection.close()
    return engine


def check_parameter(param):
    if param is not None and not isinstance(param, list):
        param = [param]
    return param


class BaseAdaptor:
    def __init__(self, metadata_uri=None):
        # This is sqlalchemy's metadata, not Ensembl's!
        self.md = db.MetaData()

        if metadata_uri is None:
            metadata_uri = config.METADATA_URI
        self.metadata_db = load_database(metadata_uri)
        self.metadata_db_session = Session(self.metadata_db, future=True)


class ReleaseAdaptor(BaseAdaptor):
    def fetch_releases(self,
                       release_id=None,
                       release_version=None,
                       current_only=True,
                       release_type=None,
                       site_name=None
                       ):
        release_id = check_parameter(release_id)
        release_version = check_parameter(release_version)
        release_type = check_parameter(release_type)
        site_name = check_parameter(site_name)

        # Reflect existing tables, letting sqlalchemy load linked tables where possible.
        release = db.Table('ensembl_release', self.md, autoload_with=self.metadata_db)
        site = self.md.tables['ensembl_site']

        release_select = db.select(
                release.c.release_id,
                release.c.version.label('release_version'),
                db.cast(release.c.release_date, db.String),
                release.c.label.label('release_label'),
                release.c.is_current,
                release.c.release_type,
                site.c.name.label('site_name'),
                site.c.label.label('site_label'),
                site.c.uri.label('site_uri')
            ).select_from(release)

        # These options are in order of decreasing specificity,
        # and thus the ones later in the list can be redundant.
        if release_id is not None:
            release_select = release_select.filter(release.c.release_id.in_(release_id))
        elif release_version is not None:
            release_select = release_select.filter(release.c.version.in_(release_version))
        elif current_only:
            release_select = release_select.filter_by(is_current=1)

        if release_type is not None:
            release_select = release_select.filter(release.c.release_type.in_(release_type))

        release_select = release_select.join(site)
        if site_name is not None:
            release_select = release_select.filter(site.c.name.in_(site_name))

        return self.metadata_db_session.execute(release_select).all()

    def fetch_releases_for_genome(self, genome_uuid, site_name=None):
        genome = db.Table('genome', self.md, autoload_with=self.metadata_db)
        genome_release = db.Table('genome_release', self.md, autoload_with=self.metadata_db)

        release_id_select = db.select(
                genome_release.c.release_id
            ).select_from(genome).filter_by(
                genome_uuid=genome_uuid
            ).join(genome_release)

        release_ids = [rid for (rid,) in self.metadata_db_session.execute(release_id_select)]

        return self.fetch_releases(release_id=release_ids, site_name=site_name)

    def fetch_releases_for_dataset(self, dataset_uuid, site_name=None):
        dataset = db.Table('dataset', self.md, autoload_with=self.metadata_db)
        genome_dataset = db.Table('genome_dataset', self.md, autoload_with=self.metadata_db)

        release_id_select = db.select(
                genome_dataset.c.release_id
            ).select_from(dataset).filter_by(
                dataset_uuid=dataset_uuid
            ).join(genome_dataset)

        release_ids = [rid for (rid,) in self.metadata_db_session.execute(release_id_select)]

        return self.fetch_releases(release_id=release_ids, site_name=site_name)


class GenomeAdaptor(BaseAdaptor):
    taxon_names = {}

    def __init__(self, metadata_uri=None, taxonomy_uri=None):
        super().__init__(metadata_uri)

        if taxonomy_uri is None:
            taxonomy_uri = config.TAXONOMY_URI
        self.taxonomy_db = load_database(taxonomy_uri)
        self.taxonomy_db_session = Session(self.taxonomy_db, future=True)

        # Cache the taxon names; data is in a separate db,
        # which is tricky to fetch elegantly and efficiently otherwise.
        taxonomy_ids = self.fetch_taxonomy_ids()
        self.taxon_names = self.fetch_taxonomy_names(taxonomy_ids)

    def fetch_taxonomy_ids(self):
        organism = db.Table('organism', self.md, autoload_with=self.metadata_db)
        taxonomy_id_select = db.select(organism.c.taxonomy_id.distinct())
        taxonomy_ids = [tid for (tid,) in self.metadata_db.execute(taxonomy_id_select)]
        return taxonomy_ids

    def fetch_taxonomy_names(self, taxonomy_id):
        ncbi_taxa_name = db.Table('ncbi_taxa_name', self.md, autoload_with=self.taxonomy_db)

        taxons = {}
        for tid in taxonomy_id:
            names = {
                'scientific_name': None,
                'synonym': []
            }
            taxons[tid] = names

        sci_name_select = db.select(
            ncbi_taxa_name.c.taxon_id,
            ncbi_taxa_name.c.name
        ).filter(
            ncbi_taxa_name.c.taxon_id.in_(taxonomy_id),
            ncbi_taxa_name.c.name_class == 'scientific name'
        )
        for x in self.taxonomy_db.execute(sci_name_select):
            taxons[x.taxon_id]['scientific_name'] = x.name

        synonym_class = [
            'common name',
            'equivalent name',
            'genbank common name',
            'genbank synonym',
            'synonym'
        ]
        synonyms_select = db.select(
            ncbi_taxa_name.c.taxon_id,
            ncbi_taxa_name.c.name
        ).filter(
            ncbi_taxa_name.c.taxon_id.in_(taxonomy_id),
            ncbi_taxa_name.c.name_class.in_(synonym_class)
        )
        for x in self.taxonomy_db.execute(synonyms_select):
            taxons[x.taxon_id]['synonym'].append(x.name)

        return taxons

    def fetch_genomes(self,
                      genome_id=None, genome_uuid=None,
                      assembly_accession=None,
                      ensembl_name=None, taxonomy_id=None,
                      unreleased_only=False,
                      site_name=None, release_type=None, release_version=None, current_only=True
                      ):
        genome_id = check_parameter(genome_id)
        genome_uuid = check_parameter(genome_uuid)
        assembly_accession = check_parameter(assembly_accession)
        ensembl_name = check_parameter(ensembl_name)
        taxonomy_id = check_parameter(taxonomy_id)

        genome = db.Table('genome', self.md, autoload_with=self.metadata_db)
        assembly = self.md.tables['assembly']
        organism = self.md.tables['organism']

        genome_select = db.select(
                genome.c.genome_id,
                genome.c.genome_uuid,
                organism.c.ensembl_name,
                organism.c.url_name,
                organism.c.display_name,
                organism.c.strain,
                organism.c.taxonomy_id,
                assembly.c.accession.label('assembly_accession'),
                assembly.c.name.label('assembly_name'),
                assembly.c.ucsc_name.label('assembly_ucsc_name'),
                assembly.c.level.label('assembly_level')
            ).select_from(genome).join(assembly).join(organism)

        if unreleased_only:
            genome_release = db.Table('genome_release', self.md, autoload_with=self.metadata_db)

            genome_select = genome_select.outerjoin(genome_release).filter_by(genome_id=None)

        elif site_name is not None:
            genome_release = db.Table('genome_release', self.md, autoload_with=self.metadata_db)
            release = self.md.tables['ensembl_release']
            site = self.md.tables['ensembl_site']

            genome_select = genome_select.join(
                                genome_release).join(
                                release).join(
                                site).filter_by(name=site_name)

            if release_type is not None:
                genome_select = genome_select.filter(release.c.release_type == release_type)

            if current_only:
                genome_select = genome_select.filter(genome_release.c.is_current == 1)

            if release_version is not None:
                genome_select = genome_select.filter(release.c.version <= release_version)

        # These options are in order of decreasing specificity,
        # and thus the ones later in the list can be redundant.
        if genome_id is not None:
            genome_select = genome_select.filter(genome.c.genome_id.in_(genome_id))
        elif genome_uuid is not None:
            genome_select = genome_select.filter(genome.c.genome_uuid.in_(genome_uuid))
        elif assembly_accession is not None:
            genome_select = genome_select.filter(assembly.c.accession.in_(assembly_accession))
        elif ensembl_name is not None:
            genome_select = genome_select.filter(organism.c.ensembl_name.in_(ensembl_name))
        elif taxonomy_id is not None:
            genome_select = genome_select.filter(organism.c.taxonomy_id.in_(taxonomy_id))

        for result in self.metadata_db_session.execute(genome_select):
            taxon_names = self.taxon_names[result.taxonomy_id]
            result_dict = dict(result)
            result_dict.update(taxon_names)
            yield result_dict

    def fetch_genomes_by_genome_uuid(self,
                                     genome_uuid,
                                     unreleased_only=False,
                                     site_name=None, release_type=None,
                                     release_version=None, current_only=True
                                     ):
        return self.fetch_genomes(genome_uuid=genome_uuid,
                                  unreleased_only=unreleased_only,
                                  site_name=site_name,
                                  release_type=release_type,
                                  release_version=release_version,
                                  current_only=current_only)

    def fetch_genomes_by_assembly_accession(self,
                                            assembly_accession,
                                            unreleased_only=False,
                                            site_name=None, release_type=None,
                                            release_version=None, current_only=True
                                            ):
        return self.fetch_genomes(assembly_accession=assembly_accession,
                                  unreleased_only=unreleased_only,
                                  site_name=site_name,
                                  release_type=release_type,
                                  release_version=release_version,
                                  current_only=current_only)

    def fetch_genomes_by_ensembl_name(self,
                                      ensembl_name,
                                      unreleased_only=False,
                                      site_name=None, release_type=None,
                                      release_version=None, current_only=True
                                      ):
        return self.fetch_genomes(ensembl_name=ensembl_name,
                                  unreleased_only=unreleased_only,
                                  site_name=site_name,
                                  release_type=release_type,
                                  release_version=release_version,
                                  current_only=current_only)

    def fetch_genomes_by_taxonomy_id(self,
                                     taxonomy_id,
                                     unreleased_only=False,
                                     site_name=None, release_type=None,
                                     release_version=None, current_only=True
                                     ):
        return self.fetch_genomes(taxonomy_id=taxonomy_id,
                                  unreleased_only=unreleased_only,
                                  site_name=site_name,
                                  release_type=release_type,
                                  release_version=release_version,
                                  current_only=current_only)

    def fetch_genomes_by_scientific_name(self,
                                         scientific_name,
                                         unreleased_only=False,
                                         site_name=None, release_type=None,
                                         release_version=None, current_only=True
                                         ):
        taxonomy_ids = [t_id for t_id in self.taxon_names
                        if self.taxon_names[t_id]['scientific_name'] == scientific_name]

        return self.fetch_genomes_by_taxonomy_id(taxonomy_ids,
                                                 unreleased_only=unreleased_only,
                                                 site_name=site_name,
                                                 release_type=release_type,
                                                 release_version=release_version,
                                                 current_only=current_only)

    def fetch_genomes_by_synonym(self,
                                 synonym,
                                 unreleased_only=False,
                                 site_name=None, release_type=None,
                                 release_version=None, current_only=True
                                 ):
        taxonomy_ids = []
        for taxon_id in self.taxon_names:
            if synonym.casefold() in [x.casefold() for x in self.taxon_names[taxon_id]['synonym']]:
                taxonomy_ids.append(taxon_id)

        return self.fetch_genomes_by_taxonomy_id(taxonomy_ids,
                                                 unreleased_only=unreleased_only,
                                                 site_name=site_name,
                                                 release_type=release_type,
                                                 release_version=release_version,
                                                 current_only=current_only)

    def fetch_sequences(self,
                        genome_id=None, genome_uuid=None,
                        assembly_accession=None,
                        chromosomal_only=False
                        ):
        genome_id = check_parameter(genome_id)
        genome_uuid = check_parameter(genome_uuid)
        assembly_accession = check_parameter(assembly_accession)

        genome = db.Table('genome', self.md, autoload_with=self.metadata_db)
        assembly = self.md.tables['assembly']
        assembly_sequence = db.Table('assembly_sequence', self.md, autoload_with=self.metadata_db)

        seq_select = db.select(
                assembly_sequence.c.accession,
                assembly_sequence.c.name,
                assembly_sequence.c.sequence_location,
                assembly_sequence.c.length,
                assembly_sequence.c.chromosomal,
                assembly_sequence.c.sequence_checksum,
                assembly_sequence.c.ga4gh_identifier
            ).select_from(genome).filter_by(
                genome_uuid=genome_uuid
            ).join(assembly).join(assembly_sequence)
        if chromosomal_only == 1:
            seq_select = seq_select.filter_by(chromosomal=True)

        # These options are in order of decreasing specificity,
        # and thus the ones later in the list can be redundant.
        if len(genome_id) > 0:
            seq_select = seq_select.filter(genome.c.genome_id.in_(genome_id))
        elif len(genome_uuid) > 0:
            seq_select = seq_select.filter(genome.c.genome_uuid.in_(genome_uuid))
        elif len(assembly_accession) > 0:
            seq_select = seq_select.filter(assembly.c.accession.in_(assembly_accession))

        for result in session.execute(seq_select):
            yield dict(result)

    def fetch_sequences_by_genome_uuid(self, genome_uuid, chromosomal_only=False):
        return self.fetch_sequences(genome_uuid=genome_uuid,
                                    chromosomal_only=chromosomal_only)

    def fetch_sequences_by_assembly_accession(self, assembly_accession, chromosomal_only=False):
        return self.fetch_sequences(assembly_accession=assembly_accession,
                                    chromosomal_only=chromosomal_only)
