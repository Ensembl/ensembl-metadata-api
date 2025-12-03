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
import re
from operator import and_
from typing import List, Tuple, NamedTuple
import enum

import sqlalchemy as db
from ensembl.utils.database import DBConnection
from sqlalchemy import select, func, desc, or_, distinct, case
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import aliased

from ensembl.production.metadata.api.adaptors.base import BaseAdaptor, check_parameter, cfg
from ensembl.production.metadata.api.exceptions import TypeNotFoundException
from ensembl.production.metadata.api.models import *

logger = logging.getLogger(__name__)

class GenomeStatus(enum.Enum):
    ALL = "All"
    UNRELEASED_ONLY = "Unreleased_only"
    RELEASED = "Released"
    CURRENT = "Current"

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
                                                 release_version: float = None,
                                                 status = "all"):
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
        genomes = None
        if use_default:
            genomes = self.fetch_genomes(production_name = production_name, use_default_assembly = True, release_version=release_version, status=status, genebuild_date=genebuild)
        else:
            genomes = self.fetch_genomes(production_name = production_name, assembly_name=assembly, release_version=release_version, status=status, genebuild_date = genebuild)

        return genomes
    
    def fetch_genomes(
            self,
            genome_id=None,
            genome_uuid=None,
            genome_tag=None,
            organism_uuid=None,
            assembly_uuid=None,
            assembly_accession=None,
            assembly_name=None,
            use_default_assembly=False,
            biosample_id=None,
            production_name=None,
            taxonomy_id=None,
            group=None,
            genome_group_id=None,
            genome_group_name=None,
            genome_group_type=None,
            genome_group_reference_only=False,
            site_name=None,
            release_type=None,
            release_version=None,
            genebuild_date = None,
            status = "All"
    ):
        """
        Fetches genome information based on the specified parameters.

        Args:
            genome_id (Union[int, List[int]]): The ID(s) of the genome(s) to fetch.
            genome_uuid str|None: The UUID of the genome to fetch.
            genome_tag (Union[str, List[str]]): genome_tag value is genome.url_name
            organism_uuid (Union[str, List[str]]): The UUID(s) of the organism(s) to fetch.
            assembly_uuid (Union[str, List[str]]): The UUID(s) of the assembly(s) to fetch.
            assembly_accession (Union[str, List[str]]): The assenbly accession of the assembly(s) to fetch.
            assembly_name (Union[str, List[str]]): The name(s) of the assembly(s) to fetch.
            use_default_assembly (bool): Whether to use default assembly name or not.
            biosample_id (Union[str, List[str]]): The Ensembl name(s) of the organism(s) to fetch.
            production_name (Union[str, List[str]]): The production name(s) of the organism(s) to fetch.
            taxonomy_id (Union[int, List[int]]): The taxonomy ID(s) of the organism(s) to fetch.
            group (Union[str, List[str]]): The name(s) of the organism group(s) to filter by.
            site_name (str): The name of the Ensembl site to filter by.
            release_type (str): The dataset_type of the Ensembl release to filter by.
            release_version (int): The maximum version of the Ensembl release to filter by.
            status (set: ["all", "unreleased_only", "released", "current"]): Whether to include genomes with particular release status.

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
        # Parameter normalization (to list)
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

        status = GenomeStatus[status.upper()]
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

        # genome group logic
        if genome_group_id or genome_group_name or genome_group_type or genome_group_reference_only:
            genome_select = genome_select.join(
                GenomeGroupMember, Genome.genome_id == GenomeGroupMember.genome_id
            ).join(
                GenomeGroup, GenomeGroup.genome_group_id == GenomeGroupMember.genome_group_id
            )

            if genome_group_id:
                genome_group_id = check_parameter(genome_group_id)
                genome_select = genome_select.where(GenomeGroup.genome_group_id.in_(genome_group_id))

            if genome_group_name:
                genome_group_name = check_parameter(genome_group_name)
                genome_select = genome_select.where(GenomeGroup.name.in_(genome_group_name))

            if genome_group_type:
                genome_group_type = check_parameter(genome_group_type)
                genome_select = genome_select.where(GenomeGroup.type.in_(genome_group_type))

            if genome_group_reference_only:
                genome_select = genome_select.where(GenomeGroupMember.is_reference == 1)

            if status == GenomeStatus.CURRENT:
                genome_select = genome_select.where(GenomeGroupMember.is_current == 1)
        if (genebuild_date):
            genome_select = genome_select.filter(Genome.genebuild_date == genebuild_date)
        # Apply additional filters based on the provided parameters
        if genome_id is not None:
            genome_select = genome_select.filter(Genome.genome_id.in_(genome_id))

        if genome_uuid is not None:
            genome_select = genome_select.filter(Genome.genome_uuid == genome_uuid)

        if genome_tag is not None:
            genome_select = genome_select.filter(Genome.url_name.in_(genome_tag))

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
                # TODO: check the conditional (use_default_assembly is already a boolean)
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
        genome_select = genome_select.add_columns(EnsemblRelease, EnsemblSite) \
            .join(GenomeRelease) \
            .join(EnsemblRelease) \
            .join(EnsemblSite)
        if status == GenomeStatus.CURRENT:
            # This filter will allow us to fetch genomes present in the integrated release
            # and having genome_release.is_current = 0 (see ENSPLAT-169 for more details)
           
            if release_type is not None:
                genome_select = genome_select.where(
                        and_(
                                func.lower(EnsemblRelease.release_type) == release_type,
                                EnsemblRelease.is_current == 1
                            )
                        )    
            else: 
                # We join two pairs of genome_release-ensembl_release they should be with 
                # - Both pairs should have either genome is current or ensembl is currrent and be different releases
                # - We select those pairs where EnsemblRelease (that gives selected fileds) is earlier or no other current release
                # - If dates are equal we select integrated
                Er_2 = aliased(EnsemblRelease)
                Gr_2 = aliased(GenomeRelease)
                genome_select = genome_select\
                .join(Gr_2, GenomeRelease.genome_id == Gr_2.genome_id) \
                .join(Er_2, Gr_2.release_id == Er_2.release_id)
                genome_select = genome_select.where(
                        and_(
                            and_(
                                    GenomeRelease.release_id != Gr_2.release_id,
                                    and_(
                                        or_(
                                            Er_2.is_current == 1,
                                            Gr_2.is_current == 1,
                                        ),
                                        or_(
                                            EnsemblRelease.is_current == 1,
                                            GenomeRelease.is_current == 1,
                                        )
                                    )
                            ),
                            or_(
                                or_(
                                    EnsemblRelease.release_date > Er_2.release_date,
                                    Er_2.release_date is None 
                                ),
                                and_(
                                    EnsemblRelease.release_date == Er_2.release_date,
                                    func.lower(EnsemblRelease.release_type) == "integrated"
                                )
                            )
                        )
                    )

        if status == GenomeStatus.UNRELEASED_ONLY:
            # fetch only unreleased ones
            genome_select = genome_select.filter(EnsemblRelease.status != ReleaseStatus.RELEASED)
        if status == GenomeStatus.RELEASED:
            genome_select = genome_select.filter(EnsemblRelease.status == ReleaseStatus.RELEASED)
        if release_version is not None and release_version > 0:
            # if release is specified
            genome_select = genome_select.filter(EnsemblRelease.version <= release_version)
        if release_type is not None:
            genome_select = genome_select.filter(EnsemblRelease.release_type == release_type)
        if site_name is not None:
            genome_select = genome_select.add_columns(EnsemblSite).filter(EnsemblSite.name == site_name)

        logger.debug(f"fetch_genome: {genome_select} / {release_version}")
        with self.metadata_db.session_scope() as session:
            session.expire_on_commit = False
            return session.execute(genome_select.order_by("production_name", EnsemblRelease.release_date.desc())).all()

    def fetch_genomes_by_genome_uuid(self, genome_uuid, site_name=None, release_type=None, release_version=None,
                                     status="Current"):
        return self.fetch_genomes(genome_uuid=genome_uuid, site_name=site_name, release_type=release_type,
                                  release_version=release_version, status=status)

    def fetch_genomes_by_assembly_accession(self, assembly_accession, site_name=None,
                                            release_type=None, release_version=None, status="Current"):
        return self.fetch_genomes(assembly_accession=assembly_accession, site_name=site_name, release_type=release_type,
                                  release_version=release_version, status=status)

    def fetch_genomes_by_ensembl_name(self, ensembl_name, site_name=None, release_type=None,
                                      release_version=None, status="Current"):
        return self.fetch_genomes(biosample_id=ensembl_name, site_name=site_name, release_type=release_type,
                                  release_version=release_version, status=status)

    def fetch_genomes_by_taxonomy_id(self, taxonomy_id, site_name=None, release_type=None,
                                     release_version=None, status="Current"):
        return self.fetch_genomes(taxonomy_id=taxonomy_id, site_name=site_name, release_type=release_type,
                                  release_version=release_version, status=status)

    # TODO: cleanup (function below not used anywhere)
    def fetch_genomes_by_scientific_name(
            self,
            scientific_name,
            site_name=None,
            release_type=None,
            release_version=None,
            status="Current",
    ):
        taxonomy_ids = self.fetch_taxonomy_ids(scientific_name)

        return self.fetch_genomes_by_taxonomy_id(
            taxonomy_ids,
            site_name=site_name,
            release_type=release_type,
            release_version=release_version,
            status=status,
        )

    def fetch_genome_by_specific_keyword(self,
                                         tolid, assembly_accession_id, assembly_name, ensembl_name,
                                         common_name, scientific_name, scientific_parlance_name,
                                         species_taxonomy_id, release_version=None, status="Released"
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
        status = GenomeStatus[status.upper()]
        #TODO: fix current logic here, to fetch latest release
        genome_query = db.select(Genome, Assembly, Organism, EnsemblRelease).select_from(Genome) \
            .join(Organism, Genome.organism_id == Organism.organism_id) \
            .join(Assembly, Genome.assembly_id == Assembly.assembly_id)
        genome_query = genome_query.add_columns(EnsemblSite) \
            .join(GenomeRelease, GenomeRelease.genome_id == Genome.genome_id) \
            .join(EnsemblRelease, EnsemblRelease.release_id == GenomeRelease.release_id) \
            .join(EnsemblSite,
                  EnsemblRelease.site_id == EnsemblSite.site_id & EnsemblSite.site_id == cfg.ensembl_site_id)
        if status == GenomeStatus.RELEASED:
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
                Organism.species_taxonomy_id == int(species_taxonomy_id)
            )
        else:
            return []

        logger.debug(f"bySpecificKeyword: {genome_query} {release_version}")
        with self.metadata_db.session_scope() as session:
            session.expire_on_commit = False
            return session.execute(genome_query).all()


    def fetch_genome_by_release_version(self, release_version, status="Released"):
        """
        Fetches genomes based on a specific release version.

        Args:            
            release_version (Float): Release version to filter by.

        Returns:
            list: A list of fetched genomes matching the release version.
        """
        status = GenomeStatus[status.upper()]
        genome_query = db.select(Genome, Assembly, Organism, EnsemblRelease).select_from(Genome) \
            .join(Organism, Genome.organism_id == Organism.organism_id) \
            .join(Assembly, Genome.assembly_id == Assembly.assembly_id)
        genome_query = genome_query.add_columns(EnsemblSite) \
            .join(GenomeRelease, GenomeRelease.genome_id == Genome.genome_id) \
            .join(EnsemblRelease, EnsemblRelease.release_id == GenomeRelease.release_id) \
            .join(EnsemblSite,
                  EnsemblRelease.site_id == EnsemblSite.site_id & EnsemblSite.site_id == cfg.ensembl_site_id)
        if status == GenomeStatus.RELEASED or GenomeStatus.CURRENT:
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
            seq_select = seq_select.filter(Genome.genome_id.in_(genome_id))

        if genome_uuid is not None:
            seq_select = seq_select.filter(Genome.genome_uuid == genome_uuid)

        if assembly_accession is not None:
            seq_select = seq_select.filter(Assembly.accession == assembly_accession)

        if assembly_uuid is not None:
            seq_select = seq_select.filter(Assembly.assembly_uuid.in_(assembly_uuid))

        if assembly_sequence_accession is not None:
            seq_select = seq_select.filter(AssemblySequence.accession.in_(assembly_sequence_accession))

        if assembly_sequence_name is not None:
            seq_select = seq_select.filter(AssemblySequence.name.in_(assembly_sequence_name))
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
                              status = "All",
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
        status = GenomeStatus[status.upper()]
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
            #TODO: we have two different status - dataset and genome checked for the same condition. Not sure if it is expected
            if status == GenomeStatus.RELEASED:
                 genome_select = genome_select.filter(EnsemblRelease.status == ReleaseStatus.RELEASED)
            if status == GenomeStatus.UNRELEASED_ONLY:
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
                if status == GenomeStatus.RELEASED:
                    # TODO see to add is_current as well
                    genome_datasets = [gd for gd in genome_datasets if gd.dataset.status == DatasetStatus.RELEASED]
                    # TODO Get only the first one when allow_unreleased
                if status == GenomeStatus.UNRELEASED_ONLY:
                    genome_datasets = [gd for gd in genome_datasets if gd.ensembl_release is None or
                                       gd.ensembl_release.status != DatasetStatus.RELEASED]
                if release_version:
                    genome_datasets = [gd for gd in genome_datasets if
                                       float(gd.ensembl_release.version) <= release_version]
                if len(genome_datasets) > 1:
                    logger.debug(f"{len(genome_release)} genome_datasets found")
                    logger.debug(f"Retrieved genome_datasets {genome_release}")
                    # this means that we have datasets that are released in both integrated and partial,
                    # if it's the case we pick the partial dataset because "if a dataset is provided in a partial release
                    # for an existing genome we would prefer that dataset"
                    # https://genomes-ebi.slack.com/archives/C010QF119N1/p1746101265211759?thread_ts=1746094298.003789&cid=C010QF119N1
                    # TODO: assure that above described logic still valid
                    if status == GenomeStatus.CURRENT:
                        genome_datasets = [gd for gd in genome_datasets if gd.ensembl_release.release_type == "partial"]
                if len(genome_datasets) > 0:
                    datasets_list = []
                    for gd in genome_datasets:
                        # Build attributes first
                        attributes = [
                            DatasetAttributeItem(
                                name=ds.attribute.name,
                                value=ds.value,
                                type=ds.attribute.type,
                                label=ds.attribute.label
                            )
                            for ds in gd.dataset.dataset_attributes
                        ]

                        # Build dataset item
                        dataset_item = GenomeDatasetItem(
                            dataset=gd.dataset,
                            dataset_type=gd.dataset.dataset_type,
                            dataset_source=gd.dataset.dataset_source,
                            # If more than one dataset is available go with the partial dataset.
                            # If only one dataset is available just go with that one
                            # Slack discussion: https://genomes-ebi.slack.com/archives/C010QF119N1/p1746094298003789
                            # Todo: clarify the confusion here (why release is assigned a genome_datasets)
                            release=utils.fetch_proper_dataset(gd.dataset.genome_datasets),
                            attributes=attributes
                        )
                        datasets_list.append(dataset_item)

                    # Finally, build the main GenomeDatasetsListItem
                    genome_item = GenomeDatasetsListItem(
                        genome=genome_release.Genome,
                        release=genome_release.EnsemblRelease,
                        datasets=datasets_list
                    )
                    genomes_dataset_info.append(genome_item)

                else:
                    logger.warning(f"No dataset retrieved for genome and parameters")

            return genomes_dataset_info

    def fetch_genomes_info(self, genome_id=None, genome_uuid=None, biosample_id=None, group=None,
                           dataset_type_name=None, release_version=None):
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

    def fetch_organisms_group_counts(self, release_label: str = None, group_code: str = 'popular', status = "Released"):
        status = GenomeStatus[status.upper()]
        with self.metadata_db.session_scope() as session:
            # Step 1: Alias for organism in group
            OrganismAlias = aliased(Organism)

            # Step 2: Base query
            query = db.select(
                OrganismAlias.species_taxonomy_id,
                OrganismAlias.common_name,
                OrganismAlias.scientific_name,
                OrganismGroupMember.order.label("order"),
                func.count(func.distinct(Genome.genome_id)).label("count")
            ).join(OrganismGroupMember, OrganismGroupMember.organism_id == OrganismAlias.organism_id
                   ).join(OrganismGroup, OrganismGroup.organism_group_id == OrganismGroupMember.organism_group_id
                          ).join(Organism, Organism.species_taxonomy_id == OrganismAlias.species_taxonomy_id
                                 ).join(Genome, Genome.organism_id == Organism.organism_id
                                        ).join(GenomeRelease, GenomeRelease.genome_id == Genome.genome_id
                                               ).join(EnsemblRelease,
                                                      EnsemblRelease.release_id == GenomeRelease.release_id
                                                      ).filter(OrganismGroup.code == group_code)

            # Step 3: Release logic
            # Add current status logic
            if status == GenomeStatus.RELEASED:
                query = query.where(EnsemblRelease.status == ReleaseStatus.RELEASED)

            logger.debug("release_label %s", release_label)
            if release_label:
                rel_stmt = select(EnsemblRelease).where(EnsemblRelease.label == release_label)
                try:
                    rel_test = session.execute(rel_stmt).scalar_one()
                except NoResultFound:
                    raise ValueError(f"Release {release_label} not found")

                if rel_test.status != ReleaseStatus.RELEASED or rel_test.release_type != "integrated":
                    raise ValueError(f"Release {release_label} is not a released integrated release")

                query = query.where(GenomeRelease.release_id == rel_test.release_id)
            else:
                latest_release_stmt = (
                    select(EnsemblRelease.release_id)
                    .where(
                        EnsemblRelease.status == ReleaseStatus.RELEASED,
                        EnsemblRelease.release_type == "integrated"
                    )
                    .order_by(desc(EnsemblRelease.release_date))
                    .limit(1)
                )
                # To make the code backward compatible (in case we don't have any Integrated releases)
                # We use scalar_one_or_none() instead of scalar_one()
                latest_release_id = session.execute(latest_release_stmt).scalar_one_or_none()
                # Check if there are any released integrated releases first
                if latest_release_id is None:
                    logger.warning("No released integrated Ensembl release found")
                    # if not, grab the partial releases instead
                    # This if condition can be deleted later once we have integrated releases as part production DB
                    latest_release_stmt = (
                        select(EnsemblRelease.release_id)
                        .where(
                            EnsemblRelease.status == ReleaseStatus.RELEASED,
                            EnsemblRelease.release_type == "partial",
                            GenomeRelease.is_current.is_(True)  # Ensure we only get current partial releases
                        )
                        .join(GenomeRelease)  # Join to check is_current
                        .order_by(desc(EnsemblRelease.release_date))
                        .limit(1)
                    )
                    try:
                        latest_release_id = session.execute(latest_release_stmt).scalar_one()
                    except NoResultFound:
                        raise ValueError("No released Ensembl releases found (neither integrated nor partial)")

                query = query.where(
                    or_(
                        and_(
                            EnsemblRelease.release_type == "partial",
                            GenomeRelease.is_current.is_(True)
                        ),
                        EnsemblRelease.release_id == latest_release_id
                    )
                )

            # Step 4: Grouping
            query = query.group_by(
                OrganismAlias.species_taxonomy_id,
                OrganismAlias.common_name,
                OrganismAlias.scientific_name,
                OrganismGroupMember.order
            )

            # Step 5: Ordering
            #  This is how we tell the UI what to show first in the species selector
            query = query.order_by(OrganismGroupMember.order)

            return session.execute(query).all()

    def fetch_assemblies_count(self, species_taxonomy_id: int, release_version: float = None, status = "Released"):
        """
        Fetch all genomes for the same species_taxonomy_id
        release_version is to return only the ones which were available until this release_version
        Args:
            species_taxonomy_id: int The species taxon_id as per ncbi taxonomy
            release_version: float The EnsemblRelease to filter on
        """
        status = GenomeStatus[status.upper()]
        query = (
            # count(DISTINCT genome.genome_uuid) because some genome can be linked to both
            # partial and integrated released which result in duplication when counting
            select(func.count(distinct(Genome.genome_uuid)))
            .select_from(Genome)
            # join out to Organism -> GenomeRelease -> EnsemblRelease
            .join(Organism, Genome.organism_id == Organism.organism_id)
            .join(GenomeRelease, GenomeRelease.genome_id == Genome.genome_id)
            .join(EnsemblRelease, EnsemblRelease.release_id == GenomeRelease.release_id)
            # filter on the species_taxonomy_id
            .where(Organism.species_taxonomy_id == species_taxonomy_id)
        )
        # Fetch latest partial genomes using GenomeRelease.is_current (/!\ not EnsemblRelease.is_current)
        latest_partial_genomes = and_(
            EnsemblRelease.release_type == 'partial',
            GenomeRelease.is_current == True,
        )
        # As of now, we return all integrated releases
        all_integrated = EnsemblRelease.release_type == 'integrated'
        # Once we have archives, we will be returning the latest integrated only
        # latest_integrated = and_(
        #     EnsemblRelease.release_type == 'integrated',
        #     EnsemblRelease.is_current == True,
        # )

        query = query.where(or_(latest_partial_genomes, all_integrated))
        # TODO: assure status here should be genome status, notrelease status
        if status == GenomeStatus.RELEASED:
            query = query.where(EnsemblRelease.status == ReleaseStatus.RELEASED)
        if release_version:
            query = query.filter(EnsemblRelease.version <= release_version)

        logger.debug(query)
        with self.metadata_db.session_scope() as session:
            return session.execute(query).scalar()

    def fetch_genome_groups(
            self, genome_id=None, genome_uuid=None, group_type=None, is_current=True, release_version=None
    ):
        """
        Fetch all genome groups that a genome belongs to.

        Note: This is the inverse of filtering by genome_group in fetch_genomes().
        """

        query = select(GenomeGroup).join(
            GenomeGroupMember, GenomeGroup.genome_group_id == GenomeGroupMember.genome_group_id
        ).join(
            Genome, Genome.genome_id == GenomeGroupMember.genome_id
        )

        if genome_id:
            genome_id = check_parameter(genome_id)
            query = query.where(Genome.genome_id.in_(genome_id))

        elif genome_uuid:
            genome_uuid = check_parameter(genome_uuid)
            query = query.where(Genome.genome_uuid.in_(genome_uuid))

        if group_type:
            group_type = check_parameter(group_type)
            query = query.where(GenomeGroup.type.in_(group_type))

        if is_current:
            query = query.where(GenomeGroupMember.is_current == 1)

        if release_version is not None:
            query = query.join(
                EnsemblRelease,
                EnsemblRelease.release_id == GenomeGroupMember.release_id
            ).where(EnsemblRelease.version <= release_version)

        logger.debug(query)
        with self.metadata_db.session_scope() as session:
            session.expire_on_commit = False
            return session.execute(query).scalars().all()

    def fetch_genome_group_members_detailed(
            self, genome_group_id=None, group_name=None, is_current=True, release_version=None, release_status="Released"
    ):
        """
        Fetch genomes and their membership details for a genome group.

        This returns both the genome objects and their membership information (is_reference, etc.)

        Args:
            genome_group_id (Union[int, List[int]]): The ID(s) of the genome group(s).
            group_name (Union[str, List[str]]): The name(s) of the genome group(s).
            is_current (bool): If True, return only current genome group memberships.
            release_version (float): Return memberships up to this release version.

        Returns:
            List of tuples (Genome, GenomeGroupMember) with full membership details.
        """
        status = GenomeStatus[status.upper()]
        member_select = select(Genome, GenomeGroupMember).join(
            GenomeGroupMember, Genome.genome_id == GenomeGroupMember.genome_id
        ).join(
            GenomeGroup, GenomeGroup.genome_group_id == GenomeGroupMember.genome_group_id
        )

        # Apply filters
        if genome_group_id:
            genome_group_id = check_parameter(genome_group_id)
            member_select = member_select.where(GenomeGroup.genome_group_id.in_(genome_group_id))

        if group_name:
            group_name = check_parameter(group_name)
            member_select = member_select.where(GenomeGroup.name.in_(group_name))

        if is_current:
            member_select = member_select.where(GenomeGroupMember.is_current == 1)

        # Handle release filtering
        if release_version is not None:
            member_select = member_select.join(
                EnsemblRelease,
                EnsemblRelease.release_id == GenomeGroupMember.release_id
            ).where(EnsemblRelease.version <= release_version)
        if release_status == GenomeStatus.RELEASED:
            member_select = member_select.where(EnsemblRelease.status == ReleaseStatus.RELEASED)

        # Order by is_reference descending so reference genomes appear first
        member_select = member_select.order_by(desc(GenomeGroupMember.is_reference))

        logger.debug(member_select)
        with self.metadata_db.session_scope() as session:
            session.expire_on_commit = False
            return session.execute(member_select).all()

    def get_public_path(self, genome_uuid, dataset_type='all'):
        paths = []
        scientific_name = None
        accession = None
        genebuild_source_name = None
        last_geneset_update = None
        with self.metadata_db.session_scope() as session:
            # Single combined query to get all required data
            query = select(
                Organism.scientific_name,
                Assembly.accession,
                func.max(case(
                    (Attribute.name == 'genebuild.annotation_source', DatasetAttribute.value),
                    else_=None
                )).label('genebuild_source_name'),
                func.max(case(
                    (Attribute.name == 'genebuild.last_geneset_update', DatasetAttribute.value),
                    else_=None
                )).label('last_geneset_update')
            ).select_from(
                Genome
            ).join(Organism).join(Assembly).join(GenomeDataset).join(Dataset).join(DatasetType).join(
                DatasetAttribute).join(Attribute).where(
                Genome.genome_uuid == genome_uuid,
                DatasetType.name == 'genebuild',
                Attribute.name.in_(['genebuild.annotation_source', 'genebuild.last_geneset_update'])
            ).group_by(
                Organism.scientific_name,
                Assembly.accession
            )

            result = session.execute(query).first()
            if result:
                scientific_name, accession, genebuild_source_name, last_geneset_update = result
            else:
                scientific_name = accession = genebuild_source_name = last_geneset_update = None

            # Query for which of the 5 supported dataset types exist for this genome
            supported_types = ['genebuild', 'assembly', 'homologies', 'regulatory_features', 'variation']
            unique_dataset_types_query = select(DatasetType.name).distinct().join(
                Dataset
            ).join(GenomeDataset).join(Genome).where(
                Genome.genome_uuid == genome_uuid,
                DatasetType.name.in_(supported_types)
            )
            unique_dataset_types = session.execute(unique_dataset_types_query).scalars().all()

        if scientific_name is None or accession is None or genebuild_source_name is None or last_geneset_update is None:
            raise ValueError("Required metadata fields are missing. Please check the database entries.")
        unique_dataset_types = ['regulation' if t == 'regulatory_features' else t for t in unique_dataset_types]
        if dataset_type == 'regulatory_features':
            dataset_type = 'regulation'
        match = re.match(r'^(\d{4}-\d{2})', last_geneset_update)  # Match format YYYY-MM
        last_geneset_update = match.group(1).replace('-', '_')
        scientific_name = re.sub(r'[^a-zA-Z0-9]+', ' ', scientific_name)
        scientific_name = scientific_name.replace(' ', '_')
        scientific_name = re.sub(r'^_+|_+$', '', scientific_name)
        genebuild_source_name = genebuild_source_name.lower()
        base_path = f"{scientific_name}/{accession}"
        common_path = f"{base_path}/{genebuild_source_name}"

        path_templates = {
            'genebuild': f"{common_path}/geneset/{last_geneset_update}",
            'assembly': f"{base_path}/genome",
            'homologies': f"{common_path}/homology/{last_geneset_update}",
            'regulation': f"{common_path}/regulation",
            'variation': f"{common_path}/variation/{last_geneset_update}",
        }

        # Check for invalid dataset type early
        if dataset_type not in unique_dataset_types and dataset_type != 'all':
            raise TypeNotFoundException(f"Dataset Type : {dataset_type} not found in metadata.")

        # If 'all', add paths for all unique dataset types
        if dataset_type == 'all':
            for t in unique_dataset_types:
                paths.append({
                    "dataset_type": t,
                    "path": path_templates[t]
                })
        elif dataset_type in path_templates:
            # Add path for the specific dataset type
            paths.append({
                "dataset_type": dataset_type,
                "path": path_templates[dataset_type]
            })
        else:
            # If the code reaches here, it means there is a logic error
            raise TypeNotFoundException(f"Dataset Type : {dataset_type} has no associated path.")

        return paths
