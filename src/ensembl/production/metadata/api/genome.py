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
from sqlalchemy.engine import make_url

from ensembl.database import DBConnection
from ensembl.ncbi_taxonomy.models import NCBITaxaName

from ensembl.production.metadata.api.base import BaseAdaptor, check_parameter
from ensembl.production.metadata.api.models import Genome, Organism, Assembly, OrganismGroup, OrganismGroupMember, \
    GenomeRelease, EnsemblRelease, EnsemblSite, AssemblySequence, GenomeDataset, Dataset, DatasetType, DatasetSource
import logging

logger = logging.getLogger(__name__)


class GenomeAdaptor(BaseAdaptor):
    def __init__(self, metadata_uri, taxonomy_uri=None):
        super().__init__(metadata_uri)
        self.taxonomy_db = DBConnection(taxonomy_uri)

    def fetch_taxonomy_names(self, taxonomy_ids):
        if taxonomy_ids.isdigit():
            taxonomy_ids = check_parameter(taxonomy_ids)
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
                logger.debug(taxa_name_select)
                taxid = session.execute(taxa_name_select).one()
                taxids.append(taxid[0])
        return taxids

    def fetch_genomes(self, genome_id=None, genome_uuid=None, assembly_accession=None, ensembl_name=None,
                      taxonomy_id=None, group=None, group_type=None, unreleased_only=False, site_name=None,
                      release_type=None, release_version=None, current_only=True):
        genome_id = check_parameter(genome_id)
        genome_uuid = check_parameter(genome_uuid)
        assembly_accession = check_parameter(assembly_accession)
        ensembl_name = check_parameter(ensembl_name)
        taxonomy_id = check_parameter(taxonomy_id)
        group = check_parameter(group)
        group_type = check_parameter(group_type)

        genome_select = db.select(
            Genome, Organism, Assembly
        ).join(Genome.assembly).join(Genome.organism)

        if group:
            group_type = group_type if group_type else ['Division']
            genome_select = db.select(
                Genome, Organism, Assembly, OrganismGroup
            ).join(Genome.assembly).join(Genome.organism) \
                .join(Organism.organism_group_members) \
                .join(OrganismGroupMember.organism_group) \
                .filter(OrganismGroup.type.in_(group_type)).filter(OrganismGroup.name.in_(group))

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
            genome_select = genome_select.filter(Genome.genome_id.in_(genome_id))

        elif genome_uuid is not None:
            genome_select = genome_select.filter(Genome.genome_uuid.in_(genome_uuid) )

        elif assembly_accession is not None:
            genome_select = genome_select.filter(Assembly.accession.in_(assembly_accession))

        elif ensembl_name is not None:
            genome_select = genome_select.filter(Organism.ensembl_name.in_(ensembl_name))

        elif taxonomy_id is not None:
            genome_select = genome_select.filter(Organism.taxonomy_id.in_(taxonomy_id))

        with self.metadata_db.session_scope() as session:
            session.expire_on_commit = False
            return session.execute(genome_select.order_by("ensembl_name")).all()

    def fetch_genomes_by_genome_uuid(self, genome_uuid, unreleased_only=False, site_name=None, release_type=None,
                                     release_version=None, current_only=True):
        return self.fetch_genomes(
            genome_uuid=genome_uuid,
            unreleased_only=unreleased_only,
            site_name=site_name,
            release_type=release_type,
            release_version=release_version,
            current_only=current_only,
        )

    def fetch_genomes_by_assembly_accession(self, assembly_accession, unreleased_only=False, site_name=None,
                                            release_type=None, release_version=None, current_only=True):
        return self.fetch_genomes(
            assembly_accession=assembly_accession,
            unreleased_only=unreleased_only,
            site_name=site_name,
            release_type=release_type,
            release_version=release_version,
            current_only=current_only,
        )

    def fetch_genomes_by_ensembl_name(self, ensembl_name, unreleased_only=False, site_name=None, release_type=None,
                                      release_version=None, current_only=True):
        return self.fetch_genomes(
            ensembl_name=ensembl_name,
            unreleased_only=unreleased_only,
            site_name=site_name,
            release_type=release_type,
            release_version=release_version,
            current_only=current_only,
        )

    def fetch_genomes_by_taxonomy_id(self, taxonomy_id, unreleased_only=False, site_name=None, release_type=None,
                                     release_version=None, current_only=True):
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

    def fetch_sequences(self, genome_id=None, genome_uuid=None, assembly_accession=None, chromosomal_only=False):
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

    def fetch_genome_datasets(self, genome_id=None, genome_uuid=None, unreleased_datasets=False, dataset_uuid=None,
                              dataset_name=None, dataset_source=None):
        try:
            genome_select = db.select(
                Genome,
                GenomeDataset,
                Dataset,
                DatasetType,
                DatasetSource
            ).select_from(Genome) \
                .join(GenomeDataset, Genome.genome_id == GenomeDataset.genome_id) \
                .join(Dataset, GenomeDataset.dataset_id == Dataset.dataset_id) \
                .join(DatasetType, Dataset.dataset_type_id == DatasetType.dataset_type_id) \
                .join(DatasetSource, Dataset.dataset_source_id == DatasetSource.dataset_source_id)

            # set default group topic as 'assembly' to fetch unique datasource
            if not dataset_name:
                dataset_name = "assembly"

            genome_id = check_parameter(genome_id)
            genome_uuid = check_parameter(genome_uuid)
            dataset_uuid = check_parameter(dataset_uuid)
            dataset_name = check_parameter(dataset_name)
            dataset_source = check_parameter(dataset_source)

            if genome_id is not None:
                genome_select = genome_select.filter(Genome.genome_id.in_(genome_id))

            if genome_uuid is not None:
                genome_select = genome_select.filter(Genome.genome_uuid.in_(genome_uuid))

            if dataset_uuid is not None:
                genome_select = genome_select.filter(Dataset.dataset_uuid.in_(dataset_uuid))

            if unreleased_datasets:
                genome_select = genome_select.filter(GenomeDataset.release_id.is_(None)) \
                    .filter(GenomeDataset.is_current == 0)
            if dataset_name is not None:
                genome_select = genome_select.filter(DatasetType.name.in_(dataset_name))

            if dataset_source is not None:
                genome_select = genome_select.filter(DatasetSource.name.in_(dataset_source))
            logger.debug(genome_select)
            with self.metadata_db.session_scope() as session:
                session.expire_on_commit = False
                return session.execute(genome_select).all()

        except Exception as e:
            raise ValueError(str(e))

    def fetch_genomes_info(
            self,
            genome_id=None,
            genome_uuid=None,
            unreleased_genomes=False,
            ensembl_name=None,
            group=None,
            group_type=None,
            unreleased_datasets=False,
            dataset_name=None,
            dataset_source=None
    ):
        try:
            genome_id = check_parameter(genome_id)
            genome_uuid = check_parameter(genome_uuid)
            ensembl_name = check_parameter(ensembl_name)
            group = check_parameter(group)
            group_type = check_parameter(group_type)
            dataset_name = check_parameter(dataset_name)
            dataset_source = check_parameter(dataset_source)

            if group is None:
                group_type = group_type if group_type else ['Division']
                with self.metadata_db.session_scope() as session:
                    session.expire_on_commit = False
                    group = [org_type[0] for org_type in session.execute(
                        db.select(OrganismGroup.name).filter(OrganismGroup.type.in_(group_type))).all()]

            # get genome, assembly and organism information
            genomes = self.fetch_genomes(
                genome_id=genome_id,
                genome_uuid=genome_uuid,
                unreleased_only=unreleased_genomes,
                ensembl_name=ensembl_name,
                group=group,
                group_type=group_type,
            )

            for genome in genomes:
                dataset = self.fetch_genome_datasets(
                    genome_uuid=genome[0].genome_uuid,
                    unreleased_datasets=unreleased_datasets,
                    dataset_name=dataset_name,
                    dataset_source=dataset_source
                )
                yield [{'genome': genome, 'datasets': dataset}]
        except Exception as e:
            raise ValueError(str(e))
