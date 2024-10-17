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
import logging
import re
import uuid

from sqlalchemy import Column, Integer, String, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.mysql import DATETIME, TINYINT
from sqlalchemy.orm import relationship

from ensembl.production.metadata.api.exceptions import *
from ensembl.production.metadata.api.models.base import Base, LoadAble

__all__ = ['Genome', 'GenomeDataset', 'GenomeRelease']

logger = logging.getLogger(__name__)


class Genome(LoadAble, Base):
    __tablename__ = "genome"

    genome_id = Column(Integer, primary_key=True)
    genome_uuid = Column(String(32), nullable=False, unique=True, default=str(uuid.uuid4))
    assembly_id = Column(ForeignKey("assembly.assembly_id"), nullable=False, index=True)
    organism_id = Column(ForeignKey("organism.organism_id"), nullable=False, index=True)
    created = Column(DATETIME(fsp=6), nullable=False)
    is_best = Column(TINYINT(1), nullable=False, default=0)
    production_name = Column(String(255), nullable=False, unique=False)
    genebuild_version = Column(String(64), nullable=False, unique=False)
    genebuild_date = Column(String(20), nullable=False, unique=False)
    # One to many relationships
    # genome_id to genome_dataset and genome release
    genome_datasets = relationship("GenomeDataset", back_populates="genome", cascade="all, delete, delete-orphan")
    genome_releases = relationship("GenomeRelease", back_populates="genome", cascade="all, delete, delete-orphan")
    # many to one relationships
    # assembly_id to assembly
    assembly = relationship("Assembly", back_populates="genomes")
    # organism_id to organism
    organism = relationship("Organism", back_populates="genomes")

    def get_public_path(self, dataset_type='all', release=None):
        """
        Get the public path for the specified dataset type.

        Args:
            dataset_type (str): Type of dataset to fetch path for. Defaults to 'all'.
            release: Release information for fetching datasets attached to releases before the specified one.

        Returns:
            list: A list of dictionaries containing dataset types and their corresponding paths.

        Raises:
            ValueError: If the genebuild dataset cannot be found for the genome.
            TypeNotFoundException: If the dataset type is not found in the metadata.
        """
        # TODO manage the Release parameter to fetch datasets attached to release anterior to the one specified.
        paths = []

        genebuild_dataset, genebuild_source_name, genebuild_version = self._get_genebuild_info()
        common_path_wo_anno_provider, common_path = self._get_common_paths(genebuild_source_name)
        unique_dataset_types = self._get_unique_dataset_types()
        unique_dataset_types = self._apply_consolidation_rules(unique_dataset_types)
        unique_dataset_types = self._discard_unwanted_types(unique_dataset_types)
        dataset_type = self._transform_dataset_type(dataset_type)
        path_templates = self._define_path_templates(common_path, common_path_wo_anno_provider, genebuild_version)

        # Check for invalid dataset type early.
        if dataset_type not in unique_dataset_types and dataset_type != 'all':
            raise TypeNotFoundException(f"Dataset Type : {dataset_type} not found in metadata.")

        # Add paths for all unique dataset types if 'all' is specified.
        if dataset_type == 'all':
            for t in unique_dataset_types:
                paths.append({
                    "dataset_type": t,
                    "path": path_templates[t]
                })
        elif dataset_type in path_templates:
            # Add path for the specific dataset type.
            paths.append({
                "dataset_type": dataset_type,
                "path": path_templates[dataset_type]
            })
        else:
            # If the code reaches here, it means there is a logic error.
            raise TypeNotFoundException(f"Dataset Type : {dataset_type} has no associated path.")

        return paths


    def _get_genebuild_info(self):
        """
        Retrieve genebuild dataset, source name, and version.

        Returns:
            tuple: Genebuild dataset, source name, and genebuild version.

        Raises:
            ValueError: If the genebuild dataset cannot be found for the genome.
            RuntimeError: If the genebuild version cannot be determined.
        """
        genome_genebuild_dataset = next(
            (gd for gd in self.genome_datasets if gd.dataset.dataset_type.name == "genebuild"),
            None)
        if genome_genebuild_dataset is None:
            raise ValueError("Genebuild dataset not found for the genome")
        genebuild_dataset = genome_genebuild_dataset.dataset

        genebuild_source_name = next(
            (da.value for da in genebuild_dataset.dataset_attributes if
             da.attribute.name == "genebuild.annotation_source"),
            'ensembl')

        try:
            match = re.match(r'^(\d{4}-\d{2})', genebuild_dataset.genebuild_version)
            genebuild_version = match.group(1).replace('-', '_')
        except TypeError as e:
            logger.fatal(f"For genome {self.genome_uuid}, can't find genebuild_version directory")
            raise RuntimeError(e)

        return genebuild_dataset, genebuild_source_name, genebuild_version

    def _get_common_paths(self, genebuild_source_name):
        """
        Define the common paths used in the dataset paths.

        Args:
            genebuild_source_name (str): The genebuild source name.

        Returns:
            tuple: Common paths without and with annotation provider.
        """
        # common path without annotation provider for assembly and regulation as they are independent
        common_path_wo_anno_provider = f"{self.organism.scientific_name.replace(' ', '_')}/{self.assembly.accession}"
        # common path including annotation provider
        common_path = f"{common_path_wo_anno_provider}/{genebuild_source_name}"
        return common_path_wo_anno_provider, common_path

    def _get_unique_dataset_types(self):
        """
        Get unique dataset types available.

        Returns:
            set: A set of unique dataset types.
        """
        return {gd.dataset.dataset_type.name for gd in self.genome_datasets}

    def _apply_consolidation_rules(self, unique_dataset_types):
        """
        Apply consolidation rules to dataset types.

        Args:
            unique_dataset_types (set): A set of unique dataset types.

        Returns:
            set: A set of consolidated dataset types.
        """
        # Mapping dataset types to new consolidated types
        consolidate_mapping = {
            ('regulatory_features', 'regulation_build'): 'regulation',
            ('evidence',): 'variation',
            ('homology_load', 'homology_compute', 'homology_ftp'): 'homologies',
        }

        # Apply consolidation rules
        for types, corresponding_type in consolidate_mapping.items():
            # if any of 'types' values are in unique_dataset_types
            if unique_dataset_types.intersection(types):
                # discard them
                unique_dataset_types.difference_update(types)
                # and add 'corresponding_type' value if it doesn't exist
                unique_dataset_types.add(corresponding_type)

        return unique_dataset_types

    def _discard_unwanted_types(self, unique_dataset_types):
        """
        Discard unwanted dataset types.

        Args:
            unique_dataset_types (set): A set of unique dataset types.

        Returns:
            set: A set of dataset types after discarding unwanted types.
        """
        to_discard = {
            'xref', 'xrefs', 'thoas_dumps', 'protein_features', 'refget_load',
            'checksums', 'ftp_dumps', 'thoas_load', 'alpha_fold', 'blast',
            'genebuild_browser_files', 'genebuild_files', 'genebuild_compute',
            'genebuild_web', 'genebuild_prep', 'genebuild_track',
            'web_genesearch', 'web_genomediscovery', 'vep_cache',
            'variation_ftp', 'genebuild_track', 'vcf_handover',
            'variation_ftp_web', 'variation_register_track', 'regulation_ftp_web',
            'track_handover', 'regulation_handover', 'regulation_register_track'
        }
        unique_dataset_types.difference_update(to_discard)
        return unique_dataset_types

    def _transform_dataset_type(self, dataset_type):
        """
        Transform the dataset type if needed.

        Args:
            dataset_type (str): The original dataset type.

        Returns:
            str: The transformed dataset type.
        """
        if dataset_type in {'regulatory_features', 'regulation_build'}:
            dataset_type = 'regulation'
        return dataset_type

    def _define_path_templates(self, common_path, common_path_wo_anno_provider, genebuild_version):
        """
        Define path templates for each dataset type.

        Args:
            common_path (str): The common path including annotation provider.
            common_path_wo_anno_provider (str): The common path without annotation provider.
            genebuild_version (str): The genebuild version.

        Returns:
            dict: A dictionary of path templates for each dataset type.
        """
        return {
            'genebuild': f"{common_path}/geneset/{genebuild_version}",
            'assembly': f"{common_path_wo_anno_provider}/genome",
            'homologies': f"{common_path}/homology/{genebuild_version}",
            'regulation': f"{common_path_wo_anno_provider}/regulation",
            'variation': f"{common_path}/variation/{genebuild_version}"
        }


class GenomeDataset(LoadAble, Base):
    __tablename__ = "genome_dataset"

    genome_dataset_id = Column(Integer, primary_key=True)
    dataset_id = Column(ForeignKey("dataset.dataset_id"), nullable=False, index=True)
    genome_id = Column(ForeignKey("genome.genome_id"), nullable=False, index=True)
    release_id = Column(ForeignKey("ensembl_release.release_id"), index=True)
    is_current = Column(TINYINT(1), nullable=False, default=0)
    UniqueConstraint("genome_id", "dataset_id", "release_id", name="genome_dataset_release_uidx"),

    # genome_dataset_id to genome
    dataset = relationship("Dataset", back_populates="genome_datasets", order_by='Dataset.name, desc(Dataset.created)')
    # genome_id to genome
    genome = relationship("Genome", back_populates="genome_datasets", order_by='Dataset.name, desc(Genome.created)')
    # release_id to release
    ensembl_release = relationship("EnsemblRelease", back_populates="genome_datasets",
                                   order_by='desc(EnsemblRelease.version)')


class GenomeRelease(LoadAble, Base):
    __tablename__ = "genome_release"

    UniqueConstraint("genome_id", "release_id", name="genome_release_uidx"),
    genome_release_id = Column(Integer, primary_key=True)
    genome_id = Column(ForeignKey("genome.genome_id"), nullable=False, index=True)
    release_id = Column(ForeignKey("ensembl_release.release_id"), nullable=False, index=True)
    is_current = Column(TINYINT(1), nullable=False, default=0)
    # One to many relationships
    # none
    # many to one relationships
    # genome_release_id to genome_release
    genome = relationship("Genome", back_populates="genome_releases")
    # release_id to ensembl release
    ensembl_release = relationship("EnsemblRelease", back_populates="genome_releases")
