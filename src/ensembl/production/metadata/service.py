import itertools
from concurrent import futures
import grpc
import logging
import sqlalchemy as db
from sqlalchemy import or_
from sqlalchemy.orm import Session

from ensembl.production.metadata import ensembl_metadata_pb2_grpc
from ensembl.production.metadata import ensembl_metadata_pb2

from ensembl.production.metadata.config import MetadataConfig as cfg


def load_database(uri=None):
    if uri is None:
        if cfg.metadata_pass is None:
            uri = f"mysql+pymysql://{cfg.metadata_user}@{cfg.metadata_host}:{cfg.metadata_port}/ensembl_metadata_2020"
        else:
            uri = f"mysql+pymysql://{cfg.metadata_user}:{cfg.metadata_pass}@{cfg.metadata_host}:{cfg.metadata_port}/ensembl_metadata_2020"

        if cfg.taxon_pass is None:
            taxonomy_uri = f"mysql+pymysql://{cfg.taxon_user}@{cfg.taxon_host}:{cfg.taxon_port}/ncbi_taxonomy"
        else:
            taxonomy_uri = f"mysql+pymysql://{cfg.taxon_user}:{cfg.taxon_pass}@{cfg.taxon_host}:{cfg.taxon_port}/ncbi_taxonomy"

    try:
        engine = db.create_engine(uri, pool_size=cfg.pool_size, max_overflow=cfg.max_overflow)
        taxonomy_engine = db.create_engine(taxonomy_uri, pool_size=cfg.pool_size, max_overflow=cfg.max_overflow)
    except AttributeError:
        raise ValueError(f'Could not connect to database. Check metadata_uri env variable.')

    try:
        connection = engine.connect()
        taxonomy_connection = taxonomy_engine.connect()
    except db.exc.OperationalError as err:
        raise ValueError(f'Could not connect to database {uri}: {err}.') from err

    connection.close()
    taxonomy_connection.close()
    return engine, taxonomy_engine


def get_karyotype_information(metadata_db, genome_uuid):
    if genome_uuid is None:
        return create_genome()

    md = db.MetaData()
    session = Session(metadata_db, future=True)

    # Reflect existing tables, letting sqlalchemy load linked tables where possible.
    genome = db.Table('genome', md, autoload_with=metadata_db)
    assembly = db.Table('assembly', md, autoload_with=metadata_db)
    assembly_sequence = db.Table('assembly_sequence', md, autoload_with=metadata_db)
    karyotype_info = db.select([
            assembly.c.level,
            assembly_sequence.c.chromosomal,
            assembly_sequence.c.sequence_location,
    ]).where(genome.c.genome_uuid == genome_uuid) \
        .where(genome.c.assembly_id == assembly_sequence.c.assembly_id)\
        .where(assembly_sequence.c.assembly_id == assembly.c.assembly_id)

    karyotype_results = session.execute(karyotype_info).all()
    if len(karyotype_results) == 1:
        karyotype_data = dict(karyotype_results[0])
        karyotype_data['genome_uuid'] = genome_uuid
        return create_karyotype(karyotype_data)
    else:
        return create_karyotype()


def get_top_level_statistics(metadata_db, organism_id):
    if organism_id is None:
        return create_assembly()
    md = db.MetaData()
    session = Session(metadata_db, future=True)

    # Reflect existing tables, letting sqlalchemy load linked tables where possible.
    genome = db.Table('genome', md, autoload_with=metadata_db)
    genome_dataset = db.Table('genome_dataset', md, autoload_with=metadata_db)
    dataset_attribute = db.Table('dataset_attribute', md, autoload_with=metadata_db)
    attribute = db.Table('attribute', md, autoload_with=metadata_db)

    stats_info = db.select([
            dataset_attribute.c.type,
            dataset_attribute.c.value,
            attribute.c.name,
            attribute.c.label
    ]).select_from(genome).select_from(genome_dataset).select_from(dataset_attribute) \
        .where(genome.c.organism_id == organism_id) \
        .where(genome.c.genome_id == genome_dataset.c.genome_id) \
        .where(genome_dataset.c.dataset_id == dataset_attribute.c.dataset_id) \
        .where(dataset_attribute.c.attribute_id == attribute.c.attribute_id)

    stats_results = session.execute(stats_info).all()
    statistics = []
    if len(stats_results) > 0:
        for stat_type, stat_value, name, label in stats_results:
            statistics.append({
                'name': name,
                'label': label,
                'statistic_type': stat_type,
                'statistic_value': stat_value
            })
        return create_top_level_statistics({
            'organism_id': organism_id,
            'statistics': statistics
        })
    else:
        return create_top_level_statistics()


def get_assembly_information(metadata_db, assembly_id):
    if assembly_id is None:
        return create_assembly()
    md = db.MetaData()
    session = Session(metadata_db, future=True)

    # Reflect existing tables, letting sqlalchemy load linked tables where possible.
    assembly = db.Table('assembly', md, autoload_with=metadata_db)
    assembly_sequence = db.Table('assembly_sequence', md, autoload_with=metadata_db)
    assembly_info = db.select([
            assembly.c.accession,
            assembly.c.level,
            assembly.c.name,
            assembly_sequence.c.chromosomal,
            assembly_sequence.c.length,
            assembly_sequence.c.sequence_location,
            assembly_sequence.c.sequence_checksum,
            assembly_sequence.c.ga4gh_identifier
    ]).where(assembly.c.assembly_id == assembly_sequence.c.assembly_id)

    assembly_results = session.execute(assembly_info).all()
    assembly_results = dict(assembly_results[0])
    assembly_results['assembly_id'] = assembly_id

    if len(assembly_results) > 0:
        return create_assembly(assembly_results)
    else:
        return create_assembly()


def get_species_information(metadata_db, taxonomy_db, genome_uuid):
    if genome_uuid is None:
        return create_genome()

    md = db.MetaData()
    session = Session(metadata_db, future=True)

    genome = db.Table('genome', md, autoload_with=metadata_db)
    organism = md.tables['organism']

    genome_select = db.select(
            organism.c.ensembl_name,
            organism.c.display_name,
            organism.c.taxonomy_id,
            organism.c.scientific_name,
            organism.c.strain,
            organism.c.scientific_parlance_name
        ).select_from(genome).filter_by(
            genome_uuid=genome_uuid
        ).join(organism)

    species_results = session.execute(genome_select).all()
    if len(species_results) == 1:
        species_data = dict(species_results[0])
        species_data['genome_uuid'] = genome_uuid
        td = db.MetaData()
        session = Session(taxonomy_db, future=True)
        tax_name = db.Table('ncbi_taxa_name', td, autoload_with=taxonomy_db)
        tax_names = db.select([tax_name.c.name, tax_name.c.name_class]).where(
            tax_name.c.taxon_id == species_data['taxonomy_id'])
        taxo_results = session.execute(tax_names).all()
        common_names = []
        # Get the common name and alternative names
        species_data['ncbi_common_name'] = None
        if len(taxo_results) > 0:
            for item in taxo_results:
                if item[1] is not None and item[0] is not None:
                    if item[1] == 'genbank common name':
                        species_data['ncbi_common_name'] = item[0]
                    if item[1] == 'common name':
                        common_names.append(item[1])
            species_data['alternative_names'] = common_names
            if len(common_names) > 0:
                species_data['common_name'] = common_names[0]
            else:
                species_data['common_name'] = None
        return create_species(species_data)
    else:
        return create_species()


def get_sub_species_info(metadata_db, organism_id):
    if organism_id is None:
        return create_sub_species()

    md = db.MetaData()
    session = Session(metadata_db, future=True)

    # Reflect existing tables, letting sqlalchemy load linked tables where possible.
    organism_group_member = db.Table('organism_group_member', md, autoload_with=metadata_db)
    organism_group = db.Table('organism_group', md, autoload_with=metadata_db)

    sub_species_select = db.select(
            organism_group.c.type,
            organism_group.c.name,
        ).select_from(organism_group_member).filter_by(
            organism_id=organism_id
        ).join(organism_group)

    sub_species_results = session.execute(sub_species_select).fetchall()
    species_name = []
    species_type = []
    if len(sub_species_results) > 0:
        for key, value in sub_species_results:
            species_type.append(key)
            species_name.append(value)

        return create_sub_species({
            'organism_id': organism_id,
            'species_type': species_type,
            'species_name': species_name
        })
    else:
        return create_sub_species()


def get_grouping_info(metadata_db, organism_id):
    if organism_id is None:
        return create_grouping()

    md = db.MetaData()
    session = Session(metadata_db, future=True)

    # Reflect existing tables, letting sqlalchemy load linked tables where possible.
    organism_group_member = db.Table('organism_group_member', md, autoload_with=metadata_db)
    organism_group = db.Table('organism_group', md, autoload_with=metadata_db)

    grouping_select = db.select(
            organism_group.c.type,
            organism_group.c.name,
        ).select_from(organism_group_member).filter_by(
            organism_id=organism_id
        ).join(organism_group)

    grouping_results = session.execute(grouping_select).all()

    species_name = []
    species_type = []
    if len(grouping_results) > 0:
        for key, value in grouping_results:
            species_type.append(key)
            species_name.append(value)

        return create_grouping({
            'organism_id': organism_id,
            'species_type': species_type,
            'species_name': species_name
        })
    else:
        return create_grouping()


def get_genome_by_uuid(metadata_db, genome_uuid, release_version):
    if genome_uuid is None:
        return create_genome()

    sqlalchemy_md = db.MetaData()
    session = Session(metadata_db, future=True)

    # Reflect existing tables, letting sqlalchemy load linked tables where possible.
    genome = db.Table('genome', sqlalchemy_md, autoload_with=metadata_db)
    genome_release = db.Table('genome_release', sqlalchemy_md, autoload_with=metadata_db)
    release = sqlalchemy_md.tables['ensembl_release']
    assembly = sqlalchemy_md.tables['assembly']
    organism = sqlalchemy_md.tables['organism']

    genome_query = get_genome_query(genome, genome_release, release, assembly, organism).select_from(genome)\
        .join(assembly).join(organism).outerjoin(genome_release).outerjoin(release)\
        .where(genome.c.genome_uuid == genome_uuid)

    if release_version == 0:
        genome_query = genome_query.where(release.c.is_current == 1)
    else:
        genome_query = genome_query.where(release.c.version == release_version)
    genome_results = session.execute(genome_query).all()
    if len(genome_results) == 1:
        return create_genome(genome_results[0])
    return create_genome()


def get_genomes_by_keyword_iterator(metadata_db, keyword, release_version):
    if not keyword:
        return
    sqlalchemy_md = db.MetaData()
    session = Session(metadata_db, future=True)

    # Reflect existing tables, letting sqlalchemy load linked tables where possible.
    genome = db.Table('genome', sqlalchemy_md, autoload_with=metadata_db)
    genome_release = db.Table('genome_release', sqlalchemy_md, autoload_with=metadata_db)
    release = sqlalchemy_md.tables['ensembl_release']
    assembly = sqlalchemy_md.tables['assembly']
    organism = sqlalchemy_md.tables['organism']

    genome_query = get_genome_query(genome, genome_release, release, assembly, organism).select_from(genome)\
        .outerjoin(assembly)\
        .outerjoin(organism)\
        .outerjoin(genome_release)\
        .outerjoin(release) \
        .where(or_(assembly.c.tolid == keyword,
                   assembly.c.accession == keyword,
                   assembly.c.name == keyword,
                   organism.c.scientific_name == keyword,
                   organism.c.scientific_parlance_name == keyword,
                   organism.c.species_taxonomy_id == keyword))
    if release_version == 0:
        genome_query = genome_query.where(release.c.is_current == 1)
    else:
        genome_query = genome_query.where(release.c.version <= release_version)
    genome_results = session.execute(genome_query).all()

    most_recent_genomes = []
    for _, genome_release_group in itertools.groupby(genome_results, lambda r: r["assembly_accession"]):
        most_recent_genome = sorted(genome_release_group, key=lambda g: g["release_version"], reverse=True)[0]
        most_recent_genomes.append(most_recent_genome)

    for genome_row in most_recent_genomes:
        yield create_genome(genome_row)


def get_genome_by_name(metadata_db, ensembl_name, site_name, release_version):
    if ensembl_name is None and site_name is None:
        return create_genome()

    # This is sqlalchemy's metadata, not Ensembl's!
    md = db.MetaData()
    session = Session(metadata_db, future=True)

    # Reflect existing tables, letting sqlalchemy load linked tables where possible.
    genome = db.Table('genome', md, autoload_with=metadata_db)
    genome_release = db.Table('genome_release', md, autoload_with=metadata_db)
    assembly = md.tables['assembly']
    organism = md.tables['organism']
    release = md.tables['ensembl_release']
    site = md.tables['ensembl_site']

    genome_select = get_genome_query(genome, genome_release, release, assembly, organism).select_from(genome).join(
        assembly).join(organism).filter_by(ensembl_name=ensembl_name)

    if release_version == 0:
        genome_select = genome_select.join(genome_release).join(release).filter_by(
            is_current=True)
    else:
        genome_select = genome_select.join(genome_release).join(release).filter_by(
            version=release_version)

    genome_select = genome_select.join(site).filter_by(
        name=site_name)

    genome_results = session.execute(genome_select).all()

    if len(genome_results) == 1:
        return create_genome(genome_results[0])
    else:
        return create_genome()


def get_genome_query(genome, genome_release, release, assembly, organism):
    return db.select(
        genome.c.genome_uuid,
        genome.c.created,
        organism.c.ensembl_name,
        organism.c.url_name,
        organism.c.display_name,
        organism.c.taxonomy_id,
        organism.c.scientific_name,
        organism.c.strain,
        organism.c.scientific_parlance_name,
        assembly.c.accession.label('assembly_accession'),
        assembly.c.name.label('assembly_name'),
        assembly.c.ucsc_name.label('assembly_ucsc_name'),
        assembly.c.level.label('assembly_level'),
        release.c.version.label('release_version'),
        release.c.release_date,
        release.c.label.label('release_label'),
        release.c.is_current
    ).where(genome_release.c.is_current == 1)


def genome_sequence_iterator(metadata_db, genome_uuid, chromosomal_only):
    if genome_uuid is None:
        return

    # This is sqlalchemy's metadata, not Ensembl's!
    md = db.MetaData()
    session = Session(metadata_db, future=True)

    # Reflect existing tables, letting sqlalchemy load linked tables where possible.
    genome = db.Table('genome', md, autoload_with=metadata_db)
    assembly = md.tables['assembly']
    assembly_sequence = db.Table('assembly_sequence', md, autoload_with=metadata_db)

    seq_select = db.select(
            assembly_sequence.c.accession,
            assembly_sequence.c.name,
            assembly_sequence.c.sequence_location,
            assembly_sequence.c.length,
            assembly_sequence.c.chromosomal
        ).select_from(genome).filter_by(
            genome_uuid=genome_uuid
        ).join(assembly).join(assembly_sequence)
    if chromosomal_only == 1:
        seq_select = seq_select.filter_by(chromosomal=True)

    for result in session.execute(seq_select):
        yield create_genome_sequence(dict(result))


def release_iterator(metadata_db, site_name, release_version, current_only):
    # This is sqlalchemy's metadata, not Ensembl's!
    md = db.MetaData()
    session = Session(metadata_db, future=True)

    # Reflect existing tables, letting sqlalchemy load linked tables where possible.
    release = db.Table('ensembl_release', md, autoload_with=metadata_db)
    site = md.tables['ensembl_site']

    release_select = db.select(
            release.c.version.label('release_version'),
            db.cast(release.c.release_date, db.String),
            release.c.label.label('release_label'),
            release.c.is_current,
            site.c.name.label('site_name'),
            site.c.label.label('site_label'),
            site.c.uri.label('site_uri')
        ).select_from(release)
    if len(release_version) > 0:
        release_select = release_select.filter(release.c.version.in_(release_version))
    if current_only == 1:
        release_select = release_select.filter_by(is_current=1)

    release_select = release_select.join(site)
    if len(site_name) > 0:
        release_select = release_select.filter(site.c.name.in_(site_name))

    release_results = session.execute(release_select).all()

    for result in release_results:
        yield create_release(dict(result))


def release_by_uuid_iterator(metadata_db, genome_uuid):
    if genome_uuid is None:
        return

    # This is sqlalchemy's metadata, not Ensembl's!
    md = db.MetaData()
    session = Session(metadata_db, future=True)

    # Reflect existing tables, letting sqlalchemy load linked tables where possible.
    genome = db.Table('genome', md, autoload_with=metadata_db)
    genome_release = db.Table('genome_release', md, autoload_with=metadata_db)
    release = md.tables['ensembl_release']
    site = md.tables['ensembl_site']

    release_select = db.select(
            release.c.version.label('release_version'),
            db.cast(release.c.release_date, db.String),
            release.c.label.label('release_label'),
            release.c.is_current,
            site.c.name.label('site_name'),
            site.c.label.label('site_label'),
            site.c.uri.label('site_uri')
        ).select_from(genome).filter_by(
            genome_uuid=genome_uuid
        ).join(genome_release).join(release).join(site)

    release_results = session.execute(release_select).all()

    for result in release_results:
        yield create_release(dict(result))


def get_dataset_by_genome_id(metadata_db, genome_uuid, requested_dataset_type):
    # This is sqlalchemy's metadata, not Ensembl's!
    md = db.MetaData()
    session = Session(metadata_db, future=True)

    # Reflect existing tables, letting sqlalchemy load linked tables where possible.
    genome = db.Table('genome', md, autoload_with=metadata_db)
    genome_dataset = db.Table('genome_dataset', md, autoload_with=metadata_db)
    dataset = db.Table('dataset', md, autoload_with=metadata_db)
    dataset_type = db.Table('dataset_type', md, autoload_with=metadata_db)
    attribute = db.Table('attribute', md, autoload_with=metadata_db)
    dataset_attribute = db.Table('dataset_attribute', md, autoload_with=metadata_db)
    ensembl_release = db.Table('ensembl_release', md, autoload_with=metadata_db)

    dataset_select = db.select(
                               dataset.c.dataset_uuid,
                               dataset.c.name.label('dataset_name'),
                               attribute.c.name,
                               dataset_attribute.c.type,
                               dataset.c.version.label('dataset_version'),
                               dataset.c.label.label('dataset_label'),
                               ensembl_release.c.version,
                               dataset_attribute.c.value
                               ) \
        .select_from(genome).filter_by(genome_uuid=genome_uuid) \
        .join(genome_dataset) \
        .join(ensembl_release) \
        .join(dataset) \
        .join(dataset_attribute) \
        .join(attribute) \
        .join(dataset_type) \
        .where(dataset_type.c.name == requested_dataset_type) \
        .order_by(dataset_type.c.name, dataset.c.name) \
        .distinct()

    dataset_results = session.execute(dataset_select).all()
    return create_dataset_infos(genome_uuid, requested_dataset_type, dataset_results)


def create_species(data=None):
    if data is None:
        return ensembl_metadata_pb2.Species()
    species = ensembl_metadata_pb2.Species(
        genome_uuid=data['genome_uuid'],
        common_name=data['common_name'],
        ncbi_common_name=data['ncbi_common_name'],
        scientific_name=data['scientific_name'],
        alternative_names=data['alternative_names'],
        taxon_id=data['taxonomy_id'],
        scientific_parlance_name=data['scientific_parlance_name']
    )
    return species
    # return json_format.MessageToJson(species)


def create_top_level_statistics(data=None):
    if data is None:
        return ensembl_metadata_pb2.TopLevelStatistics()
    species = ensembl_metadata_pb2.TopLevelStatistics(
        organism_id=data['organism_id'],
        statistics=data['statistics'],
    )
    return species


def create_karyotype(data=None):
    if data is None:
        return ensembl_metadata_pb2.Karyotype()

    karyotype = ensembl_metadata_pb2.Karyotype(
        genome_uuid=data['genome_uuid'],
        code=data['code'],
        chromosomal=data['chromosomal'],
        location=data['location']
    )
    return karyotype


def create_grouping(data=None):
    if data is None:
        return ensembl_metadata_pb2.Grouping()
    grouping = ensembl_metadata_pb2.Grouping(
        organism_id=data['organism_id'],
        species_name=data['species_name'],
        species_type=data['species_type'],
    )
    return grouping


def create_sub_species(data=None):
    if data is None:
        return ensembl_metadata_pb2.SubSpecies()
    sub_species = ensembl_metadata_pb2.SubSpecies(
        organism_id=data['organism_id'],
        species_name=data['species_name'],
        species_type=data['species_type'],
    )
    return sub_species


def create_assembly(data=None):
    if data is None:
        return ensembl_metadata_pb2.AssemblyInfo()
    assembly = ensembl_metadata_pb2.AssemblyInfo(
        assembly_id=data['assembly_id'],
        accession=data['accession'],
        level=data['level'],
        name=data['name'],
        chromosomal=data['chromosomal'],
        length=data['length'],
        sequence_location=data['sequence_location'],
        sequence_checksum=data['sequence_checksum'],
        ga4gh_identifier=data['ga4gh_identifier'],
    )
    return assembly


def create_genome(data=None):
    if data is None:
        return ensembl_metadata_pb2.Genome()

    assembly = ensembl_metadata_pb2.Assembly(
        accession=data['assembly_accession'],
        name=data['assembly_name'],
        ucsc_name=data['assembly_ucsc_name'],
        level=data['assembly_level'],
    )

    taxon = ensembl_metadata_pb2.Taxon(
        taxonomy_id=data['taxonomy_id'],
        scientific_name=data['scientific_name'],
        strain=data['strain'],
    )
    # TODO: fetch common_name(s) from ncbi_taxonomy database

    organism = ensembl_metadata_pb2.Organism(
        display_name=data['display_name'],
        strain=data['strain'],
        scientific_name=data['scientific_name'],
        url_name=data['url_name'],
        ensembl_name=data['ensembl_name'],
        scientific_parlance_name=data['scientific_parlance_name']
    )

    release = ensembl_metadata_pb2.Release(
        release_version=data['release_version'],
        release_date=str(data['release_date']),
        release_label=data['release_label'],
        is_current=data['is_current']
    )

    genome = ensembl_metadata_pb2.Genome(
        genome_uuid=data['genome_uuid'],
        created=str(data['created']),
        assembly=assembly,
        taxon=taxon,
        organism=organism,
        release=release
    )
    return genome


def create_genome_sequence(data=None):
    if data is None:
        return ensembl_metadata_pb2.GenomeSequence()

    genome_sequence = ensembl_metadata_pb2.GenomeSequence()

    # The following relies on keys matching exactly the message attributes.
    for k, v in data.items():
        if v is not None:
            setattr(genome_sequence, k, v)

    return genome_sequence


def create_release(data=None):
    if data is None:
        return ensembl_metadata_pb2.Release()

    release = ensembl_metadata_pb2.Release()

    # The following relies on keys matching exactly the message attributes.
    for k, v in data.items():
        if v is not None:
            setattr(release, k, v)

    return release


def create_dataset_info(data=None):
    if data is None:
        return ensembl_metadata_pb2.DatasetInfos.DatasetInfo()
    return ensembl_metadata_pb2.DatasetInfos.DatasetInfo(**dict(data))


def create_dataset_infos(genome_uuid, requested_dataset_type, data=None):
    if data is None or data == []:
        return ensembl_metadata_pb2.DatasetInfos()
    dataset_infos = [create_dataset_info(result) for result in data]
    return ensembl_metadata_pb2.DatasetInfos(
        genome_uuid=genome_uuid,
        dataset_type=requested_dataset_type,
        dataset_infos=dataset_infos
    )


class EnsemblMetadataServicer(ensembl_metadata_pb2_grpc.EnsemblMetadataServicer):
    def __init__(self):
        self.db, self.taxo_db = load_database()

    def GetSpeciesInformation(self, request, context):
        return get_species_information(self.db, self.taxo_db, request.genome_uuid)

    def GetAssemblyInformation(self, request, context):
        return get_assembly_information(self.db, request.assembly_id)

    def GetSubSpeciesInformation(self, request, context):
        return get_sub_species_info(self.db, request.organism_id)

    def GetGroupingInformation(self, request, context):
        return get_sub_species_info(self.db, request.organism_id)

    def GetKaryotypeInformation(self, request, context):
        return get_sub_species_info(self.db, request.genome_uuid)

    def GetTopLevelStatistics(self, request, context):
        return get_top_level_statistics(self.db, request.organism_id)

    def GetGenomeByUUID(self, request, context):
        return get_genome_by_uuid(self.db,
                                  request.genome_uuid,
                                  request.release_version
                                  )

    def GetGenomesByKeyword(self, request, context):
        return get_genomes_by_keyword_iterator(self.db, request.keyword, request.release_version)

    def GetGenomeByName(self, request, context):
        return get_genome_by_name(self.db,
                                  request.ensembl_name,
                                  request.site_name,
                                  request.release_version
                                  )

    def GetRelease(self, request, context):
        return release_iterator(self.db,
                                request.site_name,
                                request.release_version,
                                request.current_only
                                )

    def GetReleaseByUUID(self, request, context):
        return release_by_uuid_iterator(self.db,
                                        request.genome_uuid
                                        )

    def GetGenomeSequence(self, request, context):
        return genome_sequence_iterator(self.db,
                                        request.genome_uuid,
                                        request.chromosomal_only
                                        )

    def GetDatasetInformation(self, request, context):
        return get_dataset_by_genome_id(self.db, request.genome_uuid, request.dataset_type)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    ensembl_metadata_pb2_grpc.add_EnsemblMetadataServicer_to_server(
        EnsemblMetadataServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    serve()
