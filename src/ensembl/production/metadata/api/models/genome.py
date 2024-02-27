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
import re
import uuid

from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.dialects.mysql import DATETIME, TINYINT
from sqlalchemy.orm import relationship
from ensembl.production.metadata.api.exceptions import *
from ensembl.production.metadata.api.models.base import Base, LoadAble
import logging

logger = logging.getLogger(__name__)


class Genome(LoadAble, Base):
    __tablename__ = "genome"

    genome_id = Column(Integer, primary_key=True)
    genome_uuid = Column(String(128), nullable=False, unique=True, default=str(uuid.uuid4))
    assembly_id = Column(ForeignKey("assembly.assembly_id"), nullable=False, index=True)
    organism_id = Column(ForeignKey("organism.organism_id"), nullable=False, index=True)
    created = Column(DATETIME(fsp=6), nullable=False)
    is_best = Column(TINYINT(1), nullable=False, default=0)
    production_name = Column(String(256), nullable=False, unique=False)
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
        # TODO manage the Release parameter to fetch datasets attached to release anterior to the one specified.
        paths = []
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
        # Genebuild version is either the laste_geneset_update or the start_date if not specified.
        try:
            match = re.match(r'^(\d{4}-\d{2})', genebuild_dataset.genebuild_version)
            genebuild_version = match.group(1).replace('-', '_')
        except TypeError as e:
            logger.fatal(f"For genome {self.genome_uuid}, can't find genebuild_version directory")
            raise RuntimeError(e)
        common_path = f"{self.organism.scientific_name.replace(' ', '_')}/{self.assembly.accession}/{genebuild_source_name}"
        unique_dataset_types = {gd.dataset.dataset_type.name for gd in self.genome_datasets}

        if 'regulatory_features' in unique_dataset_types or 'regulation_build' in unique_dataset_types:
            unique_dataset_types.discard('regulatory_features')
            unique_dataset_types.discard('regulation_build')
            unique_dataset_types.add('regulation')
        if 'evidence' in unique_dataset_types:
            unique_dataset_types.discard('evidence')
            unique_dataset_types.add('variation')
        if 'regulatory_features' == dataset_type or 'regulation_build' == dataset_type:
            dataset_type = 'regulation'

        # Defining path templates
        path_templates = {
            'genebuild': f"{common_path}/geneset/{genebuild_version}",
            'assembly': f"{common_path}/genome",
            'homologies': f"{common_path}/homology/{genebuild_version}",
            'regulation': f"{common_path}/regulation",
            'variation': f"{common_path}/variation/{genebuild_version}"
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


class GenomeDataset(LoadAble, Base):
    __tablename__ = "genome_dataset"

    genome_dataset_id = Column(Integer, primary_key=True)
    dataset_id = Column(ForeignKey("dataset.dataset_id"), nullable=False, index=True)
    genome_id = Column(ForeignKey("genome.genome_id"), nullable=False, index=True)
    release_id = Column(ForeignKey("ensembl_release.release_id"), index=True)
    is_current = Column(TINYINT(1), nullable=False, default=0)

    # One to many relationships
    # none
    # many to one relationships
    # genome_dataset_id to genome
    dataset = relationship("Dataset", back_populates="genome_datasets")
    # genome_id to genome
    genome = relationship("Genome", back_populates="genome_datasets")
    # release_id to release
    ensembl_release = relationship("EnsemblRelease", back_populates="genome_datasets")


class GenomeRelease(LoadAble, Base):
    __tablename__ = "genome_release"

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
