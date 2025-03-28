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
from __future__ import annotations

import logging
from typing import List, Tuple, NamedTuple

import sqlalchemy as db
from ensembl.utils.database import DBConnection
from ensembl.ncbi_taxonomy.models import NCBITaxaName
from sqlalchemy.orm import aliased

from ensembl.production.metadata.api.models import Genome, Organism, Assembly, OrganismGroup, OrganismGroupMember, \
    GenomeRelease, EnsemblRelease, EnsemblSite, AssemblySequence, GenomeDataset, Dataset, DatasetType, DatasetSource, \
    ReleaseStatus, DatasetStatus
from ensembl.production.metadata.api.adaptors.base import BaseAdaptor, check_parameter, cfg

logger = logging.getLogger(__name__)


class DatasetAttributeItem(NamedTuple):
    name: str
    value: str
    type: str
    label: str


class GenomeDatasetItem(NamedTuple):
    dataset: Dataset
    dataset_type: DatasetType
    dataset_source: DatasetSource
    release: GenomeDataset
    attributes: [DatasetAttributeItem]


class GenomeDatasetsListItem(NamedTuple):
    genome: Genome
    release: EnsemblRelease
    datasets: [GenomeDatasetItem]


class GenomeAdaptor(BaseAdaptor):
    def __init__(self, metadata_uri: str, taxonomy_uri: str):
        super().__init__(metadata_uri)
        self.taxonomy_db = DBConnection(taxonomy_uri, pool_size=cfg.pool_size, pool_recycle=cfg.pool_recycle)

    def fetch_taxonomy_names(self, taxonomy_ids, synonyms=None):

        if synonyms is None:
            synonyms = []
        taxonomy_ids = check_parameter(taxonomy_ids)
        synonyms = [
            "common name",
            "equivalent name",
            "genbank synonym",
            "synonym",
        ] if len(check_parameter(synonyms)) == 0 else synonyms
        required_class_name = ["genbank common name", "scientific name"]
        taxons = {}
        with self.taxonomy_db.session_scope() as session:
            for tid in taxonomy_ids:
                taxons[tid] = {"scientific_name": None, "genbank_common_name": None, "synonym": []}

                taxonomyname_query = db.select(
                    NCBITaxaName.name,
                    NCBITaxaName.name_class,
                ).filter(
                    NCBITaxaName.taxon_id == tid,
                    NCBITaxaName.name_class.in_(required_class_name + synonyms),
                )

                for taxon_name in session.execute(taxonomyname_query).all():
                    if taxon_name[1] in synonyms:
                        taxons[tid]['synonym'].append(taxon_name[0])
                    if taxon_name[1] in required_class_name:
                        taxon_format_name = "_".join(taxon_name[1].split(' '))
                        taxons[tid][taxon_format_name] = taxon_name[0]
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

    def fetch_genomes_by_assembly_name_genebuild(self,
                                                 production_name: str,
                                                 assembly: str,
                                                 genebuild: str = None,
                                                 use_default: bool = False,
                                                 release_version: float = None):
        """
        Fetch genomes according to assembly and genebuild version.
        Args:
            assembly: assembly identifier (either assembly_default or just name depending on use_default param)
            genebuild: genebuild date as from compara_db format YYYY-MM
            use_default: whether to use Assembly.assembly_default or Assembly.name to filter genomes
            release_version: whether to return data up to this release version

        Returns:
            List[Genome]

        """
        genome_select = db.select(Genome) \
            .join(Organism, Organism.organism_id == Genome.organism_id) \
            .join(Assembly, Assembly.assembly_id == Genome.assembly_id) \
            .join(GenomeDataset).join(Dataset)
        assembly_field = Assembly.assembly_default if use_default else Assembly.name
        genome_select = genome_select.filter(assembly_field == assembly).filter(
            Genome.production_name == production_name)
        if genebuild:
            genome_select = genome_select.filter(Genome.genebuild_date == genebuild)
        logger.debug(f"Allow Unreleased {cfg.allow_unreleased}")
        genome_select = genome_select \
            .join(GenomeRelease) \
            .join(EnsemblRelease) \
            .join(EnsemblSite)
        if not cfg.allow_unreleased:
            genome_select = genome_select.filter(EnsemblRelease.status == ReleaseStatus.RELEASED)
        else:
            genome_select = genome_select.filter(GenomeDataset.is_current == 1)
        if release_version is not None and release_version > 0:
            # if release is specified
            genome_select = genome_select.filter(EnsemblRelease.version <= release_version)
        with self.metadata_db.session_scope() as session:
            session.expire_on_commit = False
            return session.execute(genome_select).all()

    def fetch_genomes(self, genome_id=None, genome_uuid=None, genome_tag=None, organism_uuid=None, assembly_uuid=None,
                      assembly_accession=None, assembly_name=None, use_default_assembly=False, biosample_id=None,
                      production_name=None, taxonomy_id=None, group=None, unreleased_only=False, site_name=None,
                      release_type=None, release_version=None, current_only=False):
        """
        Fetches genome information based on the specified parameters.

        Args:
            genome_id (Union[int, List[int]]): The ID(s) of the genome(s) to fetch.
            genome_uuid str|None: The UUID of the genome to fetch.
            genome_tag (Union[str, List[str]]): genome_tag value is either in Assembly.url_name or told_id.
            organism_uuid (Union[str, List[str]]): The UUID(s) of the organism(s) to fetch.
            assembly_uuid (Union[str, List[str]]): The UUID(s) of the assembly(s) to fetch.
            assembly_accession (Union[str, List[str]]): The assenbly accession of the assembly(s) to fetch.
            assembly_name (Union[str, List[str]]): The name(s) of the assembly(s) to fetch.
            use_default_assembly (bool): Whether to use default assembly name or not.
            biosample_id (Union[str, List[str]]): The Ensembl name(s) of the organism(s) to fetch.
            production_name (Union[str, List[str]]): The production name(s) of the organism(s) to fetch.
            taxonomy_id (Union[int, List[int]]): The taxonomy ID(s) of the organism(s) to fetch.
            group (Union[str, List[str]]): The name(s) of the organism group(s) to filter by.
            unreleased_only (bool): Fetch only unreleased genomes (default: False). allow_unreleased is used by gRPC
                                     to fetch both released and unreleased genomes, while unreleased_only
                                     is used in production pipelines (fetches only unreleased genomes)
            site_name (str): The name of the Ensembl site to filter by.
            release_type (str): The dataset_type of the Ensembl release to filter by.
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
        genome_tag = check_parameter(genome_tag)
        organism_uuid = check_parameter(organism_uuid)
        assembly_uuid = check_parameter(assembly_uuid)
        assembly_accession = check_parameter(assembly_accession)
        assembly_name = check_parameter(assembly_name)
        biosample_id = check_parameter(biosample_id)
        production_name = check_parameter(production_name)
        taxonomy_id = check_parameter(taxonomy_id)
        group = check_parameter(group)

        # Construct the initial database query
        genome_select = db.select(
            Genome, Organism, Assembly
        ).select_from(Genome) \
            .join(Organism, Organism.organism_id == Genome.organism_id) \
            .join(Assembly, Assembly.assembly_id == Genome.assembly_id)

        # Apply group filtering if group parameter is provided
        if group:
            genome_select = db.select(
                Genome, Organism, Assembly, OrganismGroup, OrganismGroupMember
            ).join(Genome.assembly).join(Genome.organism) \
                .join(Organism.organism_group_members) \
                .join(OrganismGroupMember.organism_group) \
                .filter(OrganismGroup.name.in_(group) | OrganismGroup.code.in_(group))

        # Apply additional filters based on the provided parameters
        if genome_id is not None:
            genome_select = genome_select.filter(Genome.genome_id.in_(genome_id))

        if genome_uuid is not None:
            genome_select = genome_select.filter(Genome.genome_uuid == genome_uuid)

        if genome_tag is not None:
            genome_select = genome_select.filter(
                db.or_(
                    Assembly.url_name.in_(genome_tag),
                    Assembly.tol_id.in_(genome_tag)
                )
            )

        if organism_uuid is not None:
            genome_select = genome_select.filter(Organism.organism_uuid.in_(organism_uuid))

        if assembly_uuid is not None:
            genome_select = genome_select.filter(Assembly.assembly_uuid.in_(assembly_uuid))

        if assembly_accession is not None:
            genome_select = genome_select.filter(Assembly.accession.in_(assembly_accession))

        if assembly_name is not None:
            # case() function is used to conditionally select between columns, sql equivalent is:
            # CASE
            #     WHEN :use_default_assembly = 1 THEN assembly.assembly_default
            #     ELSE assembly.name
            # END
            conditional_column = db.case(
                # literal is used to prevent evaluating use_default_assembly to a boolean (True or False)
                (db.literal(use_default_assembly) == 1, Assembly.assembly_default),
                else_=Assembly.name
            )
            lowered_assemblies = [name.lower() for name in assembly_name]
            genome_select = genome_select.filter(db.func.lower(conditional_column).in_(lowered_assemblies))

        if biosample_id is not None:
            genome_select = genome_select.filter(Organism.biosample_id.in_(biosample_id))

        if production_name is not None:
            genome_select = genome_select.filter(Genome.production_name.in_(production_name))

        if taxonomy_id is not None:
            genome_select = genome_select.filter(Organism.taxonomy_id.in_(taxonomy_id))
        logger.debug(f"ALLOW_UNRELEASED is set to {cfg.allow_unreleased}...")
        genome_select = genome_select.add_columns(EnsemblRelease, EnsemblSite) \
            .join(GenomeRelease) \
            .join(EnsemblRelease) \
            .join(EnsemblSite)
        if current_only or not cfg.allow_unreleased:
            genome_select = genome_select.filter(GenomeRelease.is_current == 1)
        if not cfg.allow_unreleased:
            # TODO See whether GRPC is supposed to return "non current" genome for a genome_release
            genome_select = genome_select.filter(EnsemblRelease.status == ReleaseStatus.RELEASED)
        elif unreleased_only:
            # fetch only unreleased ones
            genome_select = genome_select.filter(EnsemblRelease.status != ReleaseStatus.RELEASED)

        if release_version is not None and release_version > 0:
            # if release is specified
            genome_select = genome_select.filter(EnsemblRelease.version <= release_version)

        if site_name is not None:
            genome_select = genome_select.add_columns(EnsemblSite).filter(EnsemblSite.name == site_name)

        if release_type is not None:
            genome_select = genome_select.filter(EnsemblRelease.release_type == release_type)
        logger.debug(f"fetch_genome: {genome_select} / {release_version}")
        with self.metadata_db.session_scope() as session:
            session.expire_on_commit = False
            return session.execute(genome_select.order_by("production_name", EnsemblRelease.version.desc())).all()

    def fetch_genomes_by_genome_uuid(self, genome_uuid, site_name=None, release_type=None, release_version=None,
                                     current_only=True):
        return self.fetch_genomes(genome_uuid=genome_uuid, site_name=site_name, release_type=release_type,
                                  release_version=release_version, current_only=current_only)

    def fetch_genomes_by_assembly_accession(self, assembly_accession, allow_unreleased=False, site_name=None,
                                            release_type=None, release_version=None, current_only=True):
        return self.fetch_genomes(assembly_accession=assembly_accession, site_name=site_name, release_type=release_type,
                                  release_version=release_version, current_only=current_only)

    def fetch_genomes_by_ensembl_name(self, ensembl_name, site_name=None, release_type=None,
                                      release_version=None, current_only=True):
        return self.fetch_genomes(biosample_id=ensembl_name, site_name=site_name, release_type=release_type,
                                  release_version=release_version, current_only=current_only)

    def fetch_genomes_by_taxonomy_id(self, taxonomy_id, site_name=None, release_type=None,
                                     release_version=None, current_only=True):
        return self.fetch_genomes(taxonomy_id=taxonomy_id, site_name=site_name, release_type=release_type,
                                  release_version=release_version, current_only=current_only)

    def fetch_genomes_by_scientific_name(
            self,
            scientific_name,
            allow_unreleased=False,
            site_name=None,
            release_type=None,
            release_version=None,
            current_only=True,
    ):
        taxonomy_ids = self.fetch_taxonomy_ids(scientific_name)

        return self.fetch_genomes_by_taxonomy_id(
            taxonomy_ids,
            site_name=site_name,
            release_type=release_type,
            release_version=release_version,
            current_only=current_only,
        )

    def fetch_genome_by_specific_keyword(self,
                                         tolid, assembly_accession_id, assembly_name, ensembl_name,
                                         common_name, scientific_name, scientific_parlance_name,
                                         species_taxonomy_id, release_version=None
                                         ):
        """
        Fetches genomes based on a specific keyword and release version.

        Args:
            tolid (str or None): TOLID to filter genomes by.
            assembly_accession_id (str or None): Assembly accession ID to filter genomes by.
            assembly_name (str or None): Assembly name to filter genomes by.
            ensembl_name (str or None): Ensembl name to filter genomes by.
            common_name (str or None): Common name to filter genomes by.
            scientific_name (str or None): Scientific name to filter genomes by.
            scientific_parlance_name (str or None): Scientific parlance name to filter genomes by.
            species_taxonomy_id (str or None): Species taxonomy ID to filter genomes by.
            release_version (int or None, optional): Release version to filter by. If set to 0 or None, fetches only current genomes. Defaults to None.

        Returns:
            list: A list of fetched genomes matching the keyword and release version.
        """

        genome_query = db.select(Genome, Assembly, Organism, EnsemblRelease).select_from(Genome) \
            .join(Organism, Genome.organism_id == Organism.organism_id) \
            .join(Assembly, Genome.assembly_id == Assembly.assembly_id)
        genome_query = genome_query.add_columns(EnsemblSite) \
            .join(GenomeRelease, GenomeRelease.genome_id == Genome.genome_id) \
            .join(EnsemblRelease, EnsemblRelease.release_id == GenomeRelease.release_id) \
            .join(EnsemblSite,
                  EnsemblRelease.site_id == EnsemblSite.site_id & EnsemblSite.site_id == cfg.ensembl_site_id)
        if not cfg.allow_unreleased:
            logger.debug("NOT Allowed Unreleased")
            genome_query = genome_query.filter(EnsemblRelease.status == ReleaseStatus.RELEASED)
        if release_version is not None and release_version > 0:
            genome_query = genome_query.where(EnsemblRelease.version <= release_version)

        provided_fields = [
            tolid,
            assembly_accession_id,
            assembly_name,
            ensembl_name,
            common_name,
            scientific_name,
            scientific_parlance_name,
            species_taxonomy_id
        ]

        # Count how many fields are provided
        provided_count = sum(1 for field in provided_fields if field)
        # Default behaviour: return an empty list if more than one is provided
        if provided_count != 1:
            return []

        # Check which field is provided and execute the query accordingly
        if tolid:
            logger.debug(f"tolid: {tolid}")
            genome_query = genome_query.where(
                db.func.lower(Assembly.tol_id) == tolid.lower()
            )
        elif assembly_accession_id:
            logger.debug(f"assembly_accession_id: {assembly_accession_id}")
            genome_query = genome_query.where(
                db.func.lower(Assembly.accession) == assembly_accession_id.lower()
            )
        elif assembly_name:
            logger.debug(f"assembly_name: {assembly_name}")
            genome_query = genome_query.where(
                db.func.lower(Assembly.name) == assembly_name.lower()
            )
        elif ensembl_name:
            logger.debug(f"ensembl_name: {ensembl_name}")
            genome_query = genome_query.where(
                db.func.lower(Assembly.ensembl_name) == ensembl_name.lower()
            )
        elif common_name:
            logger.debug(f"common_name: {common_name}")
            genome_query = genome_query.where(
                db.func.lower(Organism.common_name) == common_name.lower()
            )
        elif scientific_name:
            logger.debug(f"scientific_name: {scientific_name}")
            genome_query = genome_query.where(
                db.func.lower(Organism.scientific_name) == scientific_name.lower()
            )
        elif scientific_parlance_name:
            logger.debug(f"scientific_parlance_name: {scientific_parlance_name}")
            genome_query = genome_query.where(
                db.func.lower(Organism.scientific_parlance_name) == scientific_parlance_name.lower()
            )
        elif species_taxonomy_id:
            logger.debug(f"species_taxonomy_id: {species_taxonomy_id}")
            genome_query = genome_query.where(
                db.func.lower(Organism.species_taxonomy_id) == species_taxonomy_id.lower()
            )
        else:
            return []

        logger.debug(f"bySpecificKeyword: {genome_query} {release_version}")
        with self.metadata_db.session_scope() as session:
            session.expire_on_commit = False
            return session.execute(genome_query).all()


    def fetch_genome_by_release_version(self, release_version):
        """
        Fetches genomes based on a specific release version.

        Args:            
            release_version (Float): Release version to filter by.

        Returns:
            list: A list of fetched genomes matching the release version.
        """

        genome_query = db.select(Genome, Assembly, Organism, EnsemblRelease).select_from(Genome) \
            .join(Organism, Genome.organism_id == Organism.organism_id) \
            .join(Assembly, Genome.assembly_id == Assembly.assembly_id)
        genome_query = genome_query.add_columns(EnsemblSite) \
            .join(GenomeRelease, GenomeRelease.genome_id == Genome.genome_id) \
            .join(EnsemblRelease, EnsemblRelease.release_id == GenomeRelease.release_id) \
            .join(EnsemblSite,
                  EnsemblRelease.site_id == EnsemblSite.site_id & EnsemblSite.site_id == cfg.ensembl_site_id)
        if not cfg.allow_unreleased:
            logger.debug("NOT Allowed Unreleased")
            genome_query = genome_query.filter(EnsemblRelease.status == ReleaseStatus.RELEASED)
        if release_version is not None and release_version > 0:
            genome_query = genome_query.where(EnsemblRelease.version == release_version)        

        logger.debug(f"by release version: {release_version}")
        with self.metadata_db.session_scope() as session:
            session.expire_on_commit = False
            return session.execute(genome_query).all()
            

    def fetch_sequences(self, genome_id=None, genome_uuid=None, assembly_uuid=None, assembly_accession=None,
                        assembly_sequence_accession=None, assembly_sequence_name=None, chromosomal_only=False):
        """
        Fetches sequences based on the provided parameters.

        Args:
            genome_id (int or None): Genome ID to filter by.
            genome_uuid (str or None): Genome UUID to filter by.
            assembly_uuid (Union[str, List[str]]): The assembly_uuid of the assembly(s) to fetch.
            assembly_accession (str or None): Assembly accession to filter by.
            assembly_sequence_accession (str or None): Assembly Sequence accession to filter by.
            assembly_sequence_name (str or None): Assembly Sequence name to filter by.
            chromosomal_only (bool): Flag indicating whether to fetch only chromosomal sequences.

        Returns:
            list: A list of fetched sequences.
        """
        genome_id = check_parameter(genome_id)
        # genome_uuid = check_parameter(genome_uuid)
        assembly_uuid = check_parameter(assembly_uuid)
        # assembly_accession = check_parameter(assembly_accession)
        assembly_sequence_accession = check_parameter(assembly_sequence_accession)
        assembly_sequence_name = check_parameter(assembly_sequence_name)

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

        if assembly_accession is not None:
            seq_select = seq_select.filter(Assembly.accession == assembly_accession)

        if assembly_uuid is not None:
            seq_select = seq_select.filter(Assembly.assembly_uuid.in_(assembly_uuid))

        if assembly_sequence_accession is not None:
            seq_select = seq_select.filter(AssemblySequence.accession == assembly_sequence_accession)

        if assembly_sequence_name is not None:
            seq_select = seq_select.filter(AssemblySequence.name == assembly_sequence_name)
        logger.debug(f'Query {seq_select}')
        with self.metadata_db.session_scope() as session:
            session.expire_on_commit = False
            return session.execute(seq_select.order_by(AssemblySequence.accession)).all()

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

    def fetch_genome_datasets(self,
                              genome_uuid: (str | List[str]) = None,
                              dataset_uuid: str = None,
                              organism_uuid: str = None,
                              unreleased_only: bool = False,
                              dataset_type_name: str = 'assembly',
                              release_version: float = None) -> List[GenomeDatasetsListItem]:
        """
        Fetches genome datasets based on the provided parameters.

        Args:
            genome_uuid (str or list or None): Genome UUID(s) to filter by.
            dataset_uuid: str filter for this dataset_uuid
            organism_uuid: str filter for this organism_uuid
            unreleased_only (bool): Fetch only unreleased datasets (default: False). allow_unreleased is used by gRPC
                                     to fetch both released and unreleased datasets, while unreleased_only
                                     is used in production pipelines (fetches only unreleased datasets)
            dataset_type_name (str or None): Dataset type name to filter by.
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
        # by default, if no dataset_type is provided, the default value is an empty string '' not None
        # that cancels out the default assignment in the method definition above
        # same thing for release_version, default value IS 0.0 NOT None
        # TODO: figure out if this is a gRPC thing or not
        dataset_type_name = 'assembly' if dataset_type_name == '' else dataset_type_name

        with self.metadata_db.session_scope() as session:
            session.expire_on_commit = False
            # fetch from genome table
            genome_select = db.select(GenomeRelease, Genome, EnsemblRelease).join(EnsemblRelease).join(Genome).join(
                GenomeDataset).join(Dataset).join(Organism)
            if genome_uuid is not None:
                genome_uuid = check_parameter(genome_uuid)
                genome_select = genome_select.where(Genome.genome_uuid.in_(genome_uuid))
            if dataset_uuid is not None:
                genome_select = genome_select.filter(Dataset.dataset_uuid == dataset_uuid)
            if organism_uuid is not None:
                organism_uuid = check_parameter(organism_uuid)
                genome_select = genome_select.filter(Organism.organism_uuid.in_(organism_uuid))
            # We have to fetch from DB
            if not cfg.allow_unreleased:
                genome_select = genome_select.filter(EnsemblRelease.status == ReleaseStatus.RELEASED)
                # only released datasets
                # genome_select = genome_select.filter(Dataset.status == DatasetStatus.RELEASED,
                #                                    GenomeDataset.is_current == 1)
            elif unreleased_only:
                genome_select = genome_select.filter(EnsemblRelease.status != ReleaseStatus.RELEASED)
                genome_select = genome_select.filter(Dataset.status != DatasetStatus.RELEASED)
            else:
                # TODO CHECK THIS if needed further filter
                pass
            logger.debug(f"genome Dataset query {genome_select}")
            genomes = session.execute(genome_select.order_by(Genome.created.desc()).distinct()).all()

            # fetch all genomes datasets
            # filter regarding allow_unreleased / release_version / unreleased_only
            genomes_dataset_info = []
            # Release check

            for genome_release in genomes:
                logger.debug(f"Retrieved genome {genome_release}")
                genome_datasets = [gd for gd in genome_release.Genome.genome_datasets if gd.dataset.parent_id is None]
                if dataset_type_name is not None and dataset_type_name != 'all':
                    genome_datasets = [gd for gd in genome_datasets if
                                       gd.dataset.dataset_type.name == dataset_type_name]
                # filter release / unreleased
                if not cfg.allow_unreleased:
                    # TODO see to add is_current as well
                    genome_datasets = [gd for gd in genome_datasets if gd.dataset.status == DatasetStatus.RELEASED]
                    # TODO Get only the first one when allow_unreleased
                elif unreleased_only:
                    genome_datasets = [gd for gd in genome_datasets if gd.ensembl_release is None or
                                       gd.ensembl_release.status != DatasetStatus.RELEASED]
                if release_version:
                    genome_datasets = [gd for gd in genome_datasets if
                                       float(gd.ensembl_release.version) <= release_version]
                if len(genome_datasets) > 0:
                    genomes_dataset_info.append(
                        GenomeDatasetsListItem(genome=genome_release.Genome,
                                               release=genome_release.EnsemblRelease,
                                               datasets=[
                                                   GenomeDatasetItem(
                                                       dataset=gd.dataset,
                                                       dataset_type=gd.dataset.dataset_type,
                                                       dataset_source=gd.dataset.dataset_source,
                                                       release=gd.dataset.genome_datasets[0],  # I've got the feeling
                                                       # that this [0] will give birth to a bug in the future... we
                                                       # will see, it works for now because we have one genome_dataset
                                                       # linked to a dataset, but we can have many in the future
                                                       # we can deal with this later when the need arises
                                                       attributes=[
                                                           DatasetAttributeItem(name=ds.attribute.name, value=ds.value,
                                                                                type=ds.attribute.type,
                                                                                label=ds.attribute.label) for ds in
                                                           gd.dataset.dataset_attributes]
                                                   ) for gd in genome_datasets
                                               ]))
                else:
                    logger.warning(f"No dataset retrieved for genome and parameters")

            return genomes_dataset_info

    def fetch_genomes_info(self, genome_id=None, genome_uuid=None, biosample_id=None, group=None,
                           dataset_type_name=None, dataset_source=None, dataset_attributes=True,
                           release_version=None):
        try:
            # get genome, assembly and organism information
            genomes: List[Tuple[Genome, Organism, Assembly, EnsemblRelease]] = \
                self.fetch_genomes(genome_id=genome_id,
                                   genome_uuid=genome_uuid,
                                   biosample_id=biosample_id,
                                   group=group,
                                   release_version=release_version)
            genomes_uuids = [genome[0].genome_uuid for genome in genomes]
            logger.debug(f"genomes uuids: {genomes_uuids}")
            genomes_datasets = self.fetch_genome_datasets(genome_uuid=genomes_uuids,
                                                          dataset_type_name=dataset_type_name,
                                                          release_version=release_version)
            indexed_genomes = {}
            # Agglomerated both lists
            logger.debug(f'genome datasets {genomes_datasets[0]}')
            for genome_infos in genomes_datasets:
                genome = next(gen for gen in genomes if gen[0].genome_uuid == genome_infos[0].genome_uuid)
                if genome_infos[0].genome_uuid not in indexed_genomes.keys():
                    indexed_genomes[genome_infos[0].genome_uuid] = {'genome': genome, 'datasets': []}
                if genome_infos[2] not in indexed_genomes[genome_infos[0].genome_uuid]['datasets']:
                    indexed_genomes[genome_infos[0].genome_uuid]['datasets'].append(genome_infos[2])
            res = []
            for genome_uuid, data in indexed_genomes.items():
                logger.debug(f'genome_uuid: {genome_uuid}, datasets {data["datasets"]}')
                res.append({'genome': data['genome'], 'datasets': data['datasets']})
            return res
        except Exception as e:
            raise ValueError(str(e))

    def fetch_organisms_group_counts(self, release_version: float = None, group_code: str = 'popular'):
        import re
        group_code = re.sub('Ensembl', '', group_code).lower()
        o_species = aliased(Organism)
        o = aliased(Organism)
        query = db.select(
            o_species.species_taxonomy_id,
            o_species.common_name,
            o_species.scientific_name,
            OrganismGroupMember.order.label('order'),
            db.func.count().label('count')
        )

        query = query.join(o, o_species.species_taxonomy_id == o.species_taxonomy_id)
        query = query.join(Genome, o.organism_id == Genome.organism_id)
        query = query.join(Assembly, Genome.assembly_id == Assembly.assembly_id)
        query = query.join(OrganismGroupMember, o_species.organism_id == OrganismGroupMember.organism_id)
        query = query.join(OrganismGroup,
                           OrganismGroupMember.organism_group_id == OrganismGroup.organism_group_id)
        query = query.filter(OrganismGroup.code == group_code)

        query = query.group_by(
            o_species.species_taxonomy_id,
            o_species.common_name,
            o_species.scientific_name,
            OrganismGroupMember.order
        )
        query = query.order_by(OrganismGroupMember.order)
        logger.debug("ALLOWED UNRELEASED %s", cfg.allow_unreleased)
        query = query.join(GenomeRelease).join(EnsemblRelease)
        if not cfg.allow_unreleased:
            query = query.where(EnsemblRelease.status == ReleaseStatus.RELEASED)
        if release_version:
            query = query.filter(EnsemblRelease.version <= release_version)
        logger.debug(query)
        with self.metadata_db.session_scope() as session:
            # TODO check if we should return a dictionary instead
            return session.execute(query).all()

    def fetch_assemblies_count(self, species_taxonomy_id: int, release_version: float = None):
        """
        Fetch all assemblies for the same species_taxonomy_id
        release_version is to return only the ones which were available until this release_version
        Args:
            species_taxonomy_id: int The species taxon_id as per ncbi taxonomy
            release_version: float The EnsemblRelease to filter on
        """
        query = db.select(db.func.count(Assembly.assembly_id)) \
            .join(Genome).join(Organism) \
            .filter(Organism.species_taxonomy_id == species_taxonomy_id)
        query = query.join(GenomeRelease).join(EnsemblRelease)
        logger.debug("ALLOWED UNRELEASED: %s", cfg.allow_unreleased)
        if not cfg.allow_unreleased:
            query = query.where(EnsemblRelease.status == ReleaseStatus.RELEASED)
        if release_version:
            query = query.filter(EnsemblRelease.version <= release_version)

        logger.debug(query)
        with self.metadata_db.session_scope() as session:
            return session.execute(query).scalar()
