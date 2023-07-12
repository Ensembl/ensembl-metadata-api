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
    GenomeRelease, EnsemblRelease, EnsemblSite, AssemblySequence, GenomeDataset, Dataset, DatasetType, DatasetSource, \
    Attribute, DatasetAttribute
import logging

logger = logging.getLogger(__name__)


class GenomeAdaptor(BaseAdaptor):
    def __init__(self, metadata_uri, taxonomy_uri=None):
        super().__init__(metadata_uri)
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
                logger.debug(taxa_name_select)
                taxid = session.execute(taxa_name_select).one()
                taxids.append(taxid[0])
        return taxids

    def fetch_genomes(self, genome_id=None, genome_uuid=None, assembly_accession=None, assembly_name=None,
                      ensembl_name=None, taxonomy_id=None, group=None, group_type=None, unreleased_only=False,
                      site_name=None, release_type=None, release_version=None, current_only=True):
        """
        Fetches genome information based on the specified parameters.

        Args:
            genome_id (Union[int, List[int]]): The ID(s) of the genome(s) to fetch.
            genome_uuid (Union[str, List[str]]): The UUID(s) of the genome(s) to fetch.
            assembly_accession (Union[str, List[str]]): The assenbly accession of the assembly(s) to fetch.
            assembly_name (Union[str, List[str]]): The name(s) of the assembly(s) to fetch.
            ensembl_name (Union[str, List[str]]): The Ensembl name(s) of the organism(s) to fetch.
            taxonomy_id (Union[int, List[int]]): The taxonomy ID(s) of the organism(s) to fetch.
            group (Union[str, List[str]]): The name(s) of the organism group(s) to filter by.
            group_type (Union[str, List[str]]): The type(s) of the organism group(s) to filter by.
            unreleased_only (bool): Whether to fetch only genomes that have not been released.
            site_name (str): The name of the Ensembl site to filter by.
            release_type (str): The type of the Ensembl release to filter by.
            release_version (int): The maximum version of the Ensembl release to filter by.
            current_only (bool): Whether to fetch only current genomes.

        Returns:
            List[Tuple[Genome, Organism, Assembly, EnsemblRelease]]: A list of tuples containing the fetched genome information.
            Each tuple contains the following elements:
                - Genome: An instance of the Genome class.
                - Organism: An instance of the Organism class.
                - Assembly: An instance of the Assembly class.
                - EnsemblRelease: An instance of the EnsemblRelease class.

        Notes:
            - The parameters are not mutually exclusive, meaning more than one of them can be provided at a time.
            - The function uses a database session to execute the query and returns the results as a list of tuples.
            - The results are ordered by the Ensembl name.

        Example usage:
            genome_info = fetch_genomes(genome_id=12345)
        """
        # Parameter validation
        genome_id = check_parameter(genome_id)
        genome_uuid = check_parameter(genome_uuid)
        assembly_accession = check_parameter(assembly_accession)
        assembly_name = check_parameter(assembly_name)
        ensembl_name = check_parameter(ensembl_name)
        taxonomy_id = check_parameter(taxonomy_id)
        group = check_parameter(group)
        group_type = check_parameter(group_type)

        # Construct the initial database query
        genome_select = db.select(
            Genome, Organism, Assembly, EnsemblRelease, OrganismGroupMember, OrganismGroup
        ).select_from(Genome) \
            .join(Organism, Organism.organism_id == Genome.organism_id) \
            .join(Assembly, Assembly.assembly_id == Genome.assembly_id) \
            .join(GenomeRelease, Genome.genome_id == GenomeRelease.genome_id) \
            .join(EnsemblRelease, GenomeRelease.release_id == EnsemblRelease.release_id) \
            .join(EnsemblSite, EnsemblSite.site_id == EnsemblRelease.site_id) \
            .join(OrganismGroupMember, OrganismGroupMember.organism_id == Organism.organism_id) \
            .join(OrganismGroup, OrganismGroup.organism_group_id == OrganismGroupMember.organism_group_id) \

        # Apply group filtering if group parameter is provided
        if group:
            group_type = group_type if group_type else ['Division']
            genome_select = genome_select.filter(OrganismGroup.type.in_(group_type)).filter(OrganismGroup.name.in_(group))

        # Apply additional filters based on the provided parameters
        if unreleased_only:
            genome_select = genome_select.outerjoin(Genome.genome_releases).filter(
                GenomeRelease.genome_id == None
            )
        if site_name is not None:
            genome_select = genome_select.filter(EnsemblSite.name == site_name)

            if release_type is not None:
                genome_select = genome_select.filter(EnsemblRelease.release_type == release_type)

            if current_only:
                genome_select = genome_select.filter(GenomeRelease.is_current == 1)

            if release_version != 0.0:
                genome_select = genome_select.filter(EnsemblRelease.version <= release_version)

        # These options are in order of decreasing specificity,
        # and thus the ones later in the list can be redundant.
        if genome_id is not None:
            genome_select = genome_select.filter(Genome.genome_id.in_(genome_id))

        if genome_uuid is not None:
            genome_select = genome_select.filter(Genome.genome_uuid.in_(genome_uuid))

        if assembly_accession is not None:
            genome_select = genome_select.filter(Assembly.accession.in_(assembly_accession))

        if assembly_name is not None:
            genome_select = genome_select.filter(Assembly.name.in_(assembly_name))

        if ensembl_name is not None:
            genome_select = genome_select.filter(Organism.ensembl_name.in_(ensembl_name))

        if taxonomy_id is not None:
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

    def fetch_sequences(self, genome_id=None, genome_uuid=None, assembly_uuid=None,
                        assembly_accession=None, chromosomal_only=False):
        """
        Fetches sequences based on the provided parameters.

        Args:
            genome_id (int or None): Genome ID to filter by.
            genome_uuid (str or None): Genome UUID to filter by.
            assembly_uuid (Union[str, List[str]]): The assembly_uuid of the assembly(s) to fetch.
            assembly_accession (str or None): Assembly accession to filter by.
            chromosomal_only (bool): Flag indicating whether to fetch only chromosomal sequences.

        Returns:
            list: A list of fetched sequences.
        """
        genome_id = check_parameter(genome_id)
        genome_uuid = check_parameter(genome_uuid)
        assembly_uuid = check_parameter(assembly_uuid)
        assembly_accession = check_parameter(assembly_accession)

        seq_select = db.select(
            Genome, Assembly, AssemblySequence
        ).select_from(Genome) \
            .join(Assembly, Assembly.assembly_id == Genome.assembly_id) \
            .join(AssemblySequence, AssemblySequence.assembly_id == Assembly.assembly_id)

        if chromosomal_only:
            seq_select = seq_select.filter(AssemblySequence.chromosomal == 1)

        # These options are in order of decreasing specificity,
        # and thus the ones later in the list can be redundant.
        if genome_id is not None:
            seq_select = seq_select.filter(Genome.genome_id == genome_id)

        if genome_uuid is not None:
            seq_select = seq_select.filter(Genome.genome_uuid == genome_uuid)

        if assembly_uuid is not None:
            seq_select = seq_select.filter(Assembly.assembly_uuid.in_(assembly_uuid))

        if assembly_accession is not None:
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

    def fetch_genome_datasets(self, genome_id=None, genome_uuid=None, organism_uuid=None, unreleased_datasets=False,
                              dataset_uuid=None, dataset_name=None, dataset_source=None, dataset_type=None,
                              release_version=None):
        """
        Fetches genome datasets based on the provided parameters.

        Args:
            genome_id (int or list or None): Genome ID(s) to filter by.
            genome_uuid (str or list or None): Genome UUID(s) to filter by.
            organism_uuid (str or list or None): Organism UUID(s) to filter by.
            unreleased_datasets (bool): Flag indicating whether to fetch only unreleased datasets.
            dataset_uuid (str or list or None): Dataset UUID(s) to filter by.
            dataset_name (str or None): Dataset name to filter by, default is 'assembly'.
            dataset_source (str or None): Dataset source to filter by.
            dataset_type (str or None): Dataset type to filter by.
            release_version (float or None): EnsemblRelease version to filter by.

        Returns:
            List[Tuple[
                    Genome, GenomeDataset, Dataset, DatasetType,
                    DatasetSource, EnsemblRelease, DatasetAttribute, Attribute
                ]]: A list of tuples containing the fetched genome information.
            Each tuple contains the following elements:
                - Genome: An instance of the Genome class.
                - Organism: An instance of the Organism class.
                - GenomeDataset: An instance of the GenomeDataset class.
                - Dataset: An instance of the Dataset class.
                - DatasetType: An instance of the DatasetType class.
                - DatasetSource: An instance of the DatasetSource class.
                - EnsemblRelease: An instance of the EnsemblRelease class.
                - DatasetAttribute: An instance of the DatasetAttribute class.
                - Attribute: An instance of the Attribute class.

        Raises:
            ValueError: If an exception occurs during the fetch process.

        """
        try:
            genome_select = db.select(
                Genome,
                Organism,
                GenomeDataset,
                Dataset,
                DatasetType,
                DatasetSource,
                EnsemblRelease,
                DatasetAttribute,
                Attribute
            ).select_from(Genome) \
                .join(Organism, Organism.organism_id == Genome.organism_id) \
                .join(GenomeDataset, Genome.genome_id == GenomeDataset.genome_id) \
                .join(Dataset, GenomeDataset.dataset_id == Dataset.dataset_id) \
                .join(DatasetType, Dataset.dataset_type_id == DatasetType.dataset_type_id) \
                .join(DatasetSource, Dataset.dataset_source_id == DatasetSource.dataset_source_id) \
                .join(EnsemblRelease, GenomeDataset.release_id == EnsemblRelease.release_id) \
                .join(DatasetAttribute, DatasetAttribute.dataset_id == Dataset.dataset_id) \
                .join(Attribute, Attribute.attribute_id == DatasetAttribute.attribute_id)

            # set default group topic as 'assembly' to fetch unique datasource
            if not dataset_name:
                dataset_name = "assembly"

            genome_id = check_parameter(genome_id)
            genome_uuid = check_parameter(genome_uuid)
            organism_uuid = check_parameter(organism_uuid)
            dataset_uuid = check_parameter(dataset_uuid)
            dataset_name = check_parameter(dataset_name)
            dataset_source = check_parameter(dataset_source)
            dataset_type = check_parameter(dataset_type)

            if genome_id is not None:
                genome_select = genome_select.filter(Genome.genome_id.in_(genome_id))

            if genome_uuid is not None:
                genome_select = genome_select.filter(Genome.genome_uuid.in_(genome_uuid))

            if organism_uuid is not None:
                genome_select = genome_select.filter(Organism.organism_uuid.in_(organism_uuid))

            if dataset_uuid is not None:
                genome_select = genome_select.filter(Dataset.dataset_uuid.in_(dataset_uuid))

            if unreleased_datasets:
                genome_select = genome_select.filter(GenomeDataset.release_id.is_(None)) \
                    .filter(GenomeDataset.is_current == 0)

            if dataset_name is not None and "all" not in dataset_name:
                genome_select = genome_select.filter(DatasetType.name.in_(dataset_name))

            if dataset_source is not None:
                genome_select = genome_select.filter(DatasetSource.name.in_(dataset_source))

            if dataset_type is not None:
                genome_select = genome_select.filter(DatasetType.name.in_(dataset_type))

            if release_version:
                genome_select = genome_select.filter(EnsemblRelease.version <= release_version)

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
