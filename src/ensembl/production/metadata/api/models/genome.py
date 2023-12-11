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
import uuid

from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.dialects.mysql import DATETIME, TINYINT
from sqlalchemy.orm import relationship

from ensembl.production.metadata.api.models.base import Base, LoadAble


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

    def get_public_path(self, type='all', release=None, genebuild_version=None):
        paths = []
        genebuild_dataset = next((gd for gd in self.genome_datasets if gd.dataset.dataset_type.name == "genebuild"),
                                 None)
        if genebuild_dataset is None:
            raise ValueError("Genebuild dataset not found for the genome")

        genebuild_source_name = genebuild_dataset.dataset.dataset_source.name
        common_path = f"{self.organism.scientific_name}/{self.assembly.accession}/{genebuild_source_name}"

        if type in ['genebuild', 'assembly', 'homology', 'regulation', 'variation', 'all']:
            if type == 'genebuild':
                if genebuild_version:
                    # Specific genebuild version
                    paths.append(f"{common_path}/genebuild/{genebuild_version}")
                else:
                    # Use the version from the genebuild dataset
                    paths.append(f"{common_path}/genebuild/{genebuild_dataset.dataset.version}")

            elif type == 'assembly':
                paths.append(f"{common_path}/genome")

            elif type in ['homology', 'regulation', 'variation']:
                paths.append(f"{common_path}/{type}")

            elif type == 'all':
                # Add paths for all types
                for t in ['genebuild', 'assembly', 'homology', 'regulation', 'variation']:
                    paths.extend(self.get_public_path(type=t))

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
