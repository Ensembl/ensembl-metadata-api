# See the NOTICE file distributed with this work for additional information
#   regarding copyright ownership.
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#       http://www.apache.org/licenses/LICENSE-2.0
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
import sqlalchemy as db
from ensembl.production.metadata.config import get_metadata_uri, get_taxonomy_uri
from ensembl.database.dbconnection import DBConnection
from ensembl.ncbi_taxonomy.models import NCBITaxaName, NCBITaxaNode
from ensembl.production.metadata.models import *


def check_parameter(param):
    if param is not None and not isinstance(param, list):
        param = [param]
    return param


class BaseAdaptor:
    def __init__(self, metadata_uri=None):
        if metadata_uri is None:
            metadata_uri = get_metadata_uri()
        self.metadata_db = DBConnection(metadata_uri)


class ReleaseAdaptor(BaseAdaptor):


    def fetch_releases(
            self,
            release_id=None,
            release_version=None,
            current_only=True,
            release_type=None,
            site_name=None,
    ):
        release_id = check_parameter(release_id)
        release_version = check_parameter(release_version)
        release_type = check_parameter(release_type)
        site_name = check_parameter(site_name)

        release_select = db.select(
            EnsemblRelease, EnsemblSite
        ).join(EnsemblRelease.ensembl_site)

        # WHERE ensembl_release.release_id = :release_id_1
        if release_id is not None:
            release_select = release_select.filter(
                EnsemblRelease.release_id.in_(release_id)
            )
        # WHERE ensembl_release.version = :version_1
        elif release_version is not None:
            release_select = release_select.filter(
                EnsemblRelease.version.in_(release_version)
            )
        # WHERE ensembl_release.is_current =:is_current_1
        elif current_only:
            release_select = release_select.filter(
                EnsemblRelease.is_current == 1
            )

        # WHERE ensembl_release.release_type = :release_type_1
        if release_type is not None:
            release_select = release_select.filter(
                EnsemblRelease.release_type.in_(release_type)
            )

        # WHERE ensembl_site.name = :name_1
        if site_name is not None:
            release_select = release_select.filter(
                EnsemblSite.name.in_(site_name)
            )
        with self.metadata_db.session_scope() as session:
            session.expire_on_commit = False
            return session.execute(release_select).all()

    def fetch_releases_for_genome(self, genome_uuid, site_name=None):

        # SELECT genome_release.release_id
        # FROM genome_release
        # JOIN genome ON genome.genome_id = genome_release.genome_id
        # WHERE genome.genome_uuid =:genome_uuid_1
        release_id_select = db.select(
            GenomeRelease.release_id
        ).filter(
            Genome.genome_uuid == genome_uuid
        ).join(
            GenomeRelease.genome
        )

        release_ids = []
        with self.metadata_db.session_scope() as session:
            release_objects = session.execute(release_id_select).all()
            for rid in release_objects:
                release_ids.append(rid[0])
            release_ids = list(dict.fromkeys(release_ids))
        return self.fetch_releases(release_id=release_ids, site_name=site_name)

    def fetch_releases_for_dataset(self, dataset_uuid, site_name=None):

        # SELECT genome_release.release_id
        # FROM genome_dataset
        # JOIN dataset ON dataset.dataset_id = genome_dataset.dataset_id
        # WHERE dataset.dataset_uuid = :dataset_uuid_1
        release_id_select = db.select(
            GenomeDataset.release_id
        ).filter(
            Dataset.dataset_uuid == dataset_uuid
        ).join(
            GenomeDataset.dataset
        )

        release_ids = []
        with self.metadata_db.session_scope() as session:
            release_objects = session.execute(release_id_select).all()
            for rid in release_objects:
                release_ids.append(rid[0])
            release_ids = list(dict.fromkeys(release_ids))
        return self.fetch_releases(release_id=release_ids, site_name=site_name)


class NewReleaseAdaptor(BaseAdaptor):

    def __init__(self, metadata_uri=None):
        super().__init__(metadata_uri)
        # Get current release ID from ensembl_release
        with self.metadata_db.session_scope() as session:
            self.current_release_id = (session.execute(db.select(EnsemblRelease.release_id).filter(EnsemblRelease.is_current == 1)).one()[0])
        if self.current_release_id == "":
            raise Exception("Current release not found")
     #   print (self.current_release_id)

        # Get last release ID from ensembl_release
        with self.metadata_db.session_scope() as session:
            ############### Refactor this once done. It is messy.
            current_version = int(session.execute(db.select(EnsemblRelease.version).filter(EnsemblRelease.release_id == self.current_release_id)).one()[0])
            past_versions = session.execute(db.select(EnsemblRelease.version).filter(EnsemblRelease.version < current_version)).all()
            sorted_versions = []
            # Do I have to account for 1.12 and 1.2
            for version in past_versions:
                sorted_versions.append(float(version[0]))
            sorted_versions.sort()
            self.previous_release_id = (session.execute(db.select(EnsemblRelease.release_id).filter(EnsemblRelease.version == sorted_versions[-1])).one()[0])
            if self.previous_release_id == "":
                raise Exception("Previous release not found")

    #     new_genomes (list of new genomes in the new release)
    def fetch_new_genomes(self):
        with self.metadata_db.session_scope() as session:
            genome_selector=db.select(
            EnsemblRelease, EnsemblSite
        ).join(EnsemblRelease.ensembl_site)
            old_genomes = session.execute(db.select(EnsemblRelease.version).filter(EnsemblRelease.version < current_version)).all()
        new_genomes = []
        novel_old_genomes = []
        novel_new_genomes = []

        return session.execute(release_select).all()


class GenomeAdaptor(BaseAdaptor):
    def __init__(self, metadata_uri=None, taxonomy_uri=None):
        super().__init__(metadata_uri)

        if taxonomy_uri is None:
            taxonomy_uri = get_taxonomy_uri()
        self.taxonomy_db = DBConnection(taxonomy_uri)

    def fetch_taxonomy_names(self, taxonomy_ids):

        taxons = {}
        for tid in taxonomy_ids:
            names = {"scientific_name": None, "synonym": []}
            taxons[tid] = names

        for taxon in taxons:
            sci_name_select = db.select(
                NCBITaxaName.name
            ).filter(
                NCBITaxaName.taxon_id == taxon,
                NCBITaxaName.name_class == "scientific name",
            )
            synonym_class = [
                "common name",
                "equivalent name",
                "genbank common name",
                "genbank synonym",
                "synonym",
            ]

            synonyms_select = db.select(
                NCBITaxaName.name
            ).filter(
                NCBITaxaName.taxon_id == taxon,
                NCBITaxaName.name_class.in_(synonym_class),
            )

            with self.taxonomy_db.session_scope() as session:
                sci_name = session.execute(sci_name_select).one()
                taxons[taxon]["scientific_name"] = sci_name[0]
                synonyms = session.execute(synonyms_select).all()
                for synonym in synonyms:
                    taxons[taxon]["synonym"].append(synonym[0])
        return taxons

    def fetch_taxonomy_ids(self, taxonomy_names):
        taxids = []
        taxonomy_names = check_parameter(taxonomy_names)
        for taxon in taxonomy_names:
            taxa_name_select = db.select(
                NCBITaxaName.taxon_id
            ).filter(
                NCBITaxaName.name == taxon
            )
            with self.taxonomy_db.session_scope() as session:
                taxid = session.execute(taxa_name_select).one()
                taxids.append(taxid[0])
        return taxids

    def fetch_genomes(
            self,
            genome_id=None,
            genome_uuid=None,
            assembly_accession=None,
            ensembl_name=None,
            taxonomy_id=None,
            group=None,
            unreleased_only=False,
            site_name=None,
            release_type=None,
            release_version=None,
            current_only=True,
    ):
        genome_id = check_parameter(genome_id)
        genome_uuid = check_parameter(genome_uuid)
        assembly_accession = check_parameter(assembly_accession)
        ensembl_name = check_parameter(ensembl_name)
        taxonomy_id = check_parameter(taxonomy_id)
        group = check_parameter(group)

        genome_select = db.select(
          Genome, Organism, Assembly
        ).join(Genome.assembly).join(Genome.organism)
        
        if group :
          genome_select = db.select(
              Genome, Organism, Assembly, OrganismGroupMember, OrganismGroup
          ).join(Genome.assembly).join(Genome.organism) \
            .join(Organism.organism_group_members) \
            .join(OrganismGroupMember.organism_group) \
            .filter(OrganismGroup.type == 'division').filter(OrganismGroup.name.in_(group))
            
        if unreleased_only:
            genome_select = genome_select.outerjoin(Genome.genome_releases).filter(
                GenomeRelease.genome_id == None
            )
        elif site_name is not None:
            genome_select = genome_select.join(
                Genome.genome_releases).join(
                GenomeRelease.ensembl_release).join(
                EnsemblRelease.ensembl_site).filter(EnsemblSite.name == site_name)

            if release_type is not None:
                genome_select = genome_select.filter(EnsemblRelease.release_type == release_type)

            if current_only:
                genome_select = genome_select.filter(GenomeRelease.is_current == 1)

            if release_version is not None:
                genome_select = genome_select.filter(EnsemblRelease.version <= release_version)

        # These options are in order of decreasing specificity,
        # and thus the ones later in the list can be redundant.
        if genome_id is not None:
            genome_select = genome_select.filter(Genome.genome_id == genome_id)

        elif genome_uuid is not None:
            genome_select = genome_select.filter(Genome.genome_uuid == genome_uuid)

        elif assembly_accession is not None:
            genome_select = genome_select.filter(Assembly.accession == assembly_accession)

        elif ensembl_name is not None:
            genome_select = genome_select.filter(Organism.ensembl_name == ensembl_name)

        elif taxonomy_id is not None:
            genome_select = genome_select.filter(Organism.taxonomy_id == taxonomy_id)

        with self.metadata_db.session_scope() as session:
            session.expire_on_commit = False
            return session.execute(genome_select.order_by("ensembl_name")).all()

    def fetch_genomes_by_genome_uuid(
            self,
            genome_uuid,
            unreleased_only=False,
            site_name=None,
            release_type=None,
            release_version=None,
            current_only=True,
    ):
        return self.fetch_genomes(
            genome_uuid=genome_uuid,
            unreleased_only=unreleased_only,
            site_name=site_name,
            release_type=release_type,
            release_version=release_version,
            current_only=current_only,
        )

    def fetch_genomes_by_assembly_accession(
            self,
            assembly_accession,
            unreleased_only=False,
            site_name=None,
            release_type=None,
            release_version=None,
            current_only=True,
    ):
        return self.fetch_genomes(
            assembly_accession=assembly_accession,
            unreleased_only=unreleased_only,
            site_name=site_name,
            release_type=release_type,
            release_version=release_version,
            current_only=current_only,
        )

    def fetch_genomes_by_ensembl_name(
            self,
            ensembl_name,
            unreleased_only=False,
            site_name=None,
            release_type=None,
            release_version=None,
            current_only=True,
    ):
        return self.fetch_genomes(
            ensembl_name=ensembl_name,
            unreleased_only=unreleased_only,
            site_name=site_name,
            release_type=release_type,
            release_version=release_version,
            current_only=current_only,
        )

    def fetch_genomes_by_taxonomy_id(
            self,
            taxonomy_id,
            unreleased_only=False,
            site_name=None,
            release_type=None,
            release_version=None,
            current_only=True,
    ):
        return self.fetch_genomes(
            taxonomy_id=taxonomy_id,
            unreleased_only=unreleased_only,
            site_name=site_name,
            release_type=release_type,
            release_version=release_version,
            current_only=current_only,
        )

    def fetch_genomes_by_scientific_name(
            self,
            scientific_name,
            unreleased_only=False,
            site_name=None,
            release_type=None,
            release_version=None,
            current_only=True,
    ):
        taxonomy_ids = self.fetch_taxonomy_ids(scientific_name)

        return self.fetch_genomes_by_taxonomy_id(
            taxonomy_ids,
            unreleased_only=unreleased_only,
            site_name=site_name,
            release_type=release_type,
            release_version=release_version,
            current_only=current_only,
        )

    def fetch_sequences(
            self,
            genome_id=None,
            genome_uuid=None,
            assembly_accession=None,
            chromosomal_only=False,
    ):
        genome_id = check_parameter(genome_id)
        genome_uuid = check_parameter(genome_uuid)
        assembly_accession = check_parameter(assembly_accession)

        seq_select = db.select(AssemblySequence, )

        if chromosomal_only:
            seq_select = seq_select.filter(AssemblySequence.chromosomal == 1)

        # These options are in order of decreasing specificity,
        # and thus the ones later in the list can be redundant.
        if genome_id is not None:
            seq_select = seq_select.join(AssemblySequence.assembly).join(Assembly.genomes).filter(
                Genome.genome_id == genome_id
            )

        elif genome_uuid is not None:
            seq_select = seq_select.join(AssemblySequence.assembly).join(Assembly.genomes).filter(
                Genome.genome_uuid == genome_uuid
            )

        elif assembly_accession is not None:
            seq_select = seq_select.filter(Assembly.accession == assembly_accession)

        with self.metadata_db.session_scope() as session:
            session.expire_on_commit = False
            return session.execute(seq_select).all()

    def fetch_sequences_by_genome_uuid(self, genome_uuid, chromosomal_only=False):
        return self.fetch_sequences(
            genome_uuid=genome_uuid, chromosomal_only=chromosomal_only
        )

    def fetch_sequences_by_assembly_accession(
            self, assembly_accession, chromosomal_only=False
    ):
        return self.fetch_sequences(
            assembly_accession=assembly_accession, chromosomal_only=chromosomal_only
        )
                