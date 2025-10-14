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
import uuid

from sqlalchemy import Column, Integer, String, ForeignKey, UniqueConstraint, Enum
from sqlalchemy.dialects.mysql import DATETIME, TINYINT
from sqlalchemy.orm import relationship

from ensembl.production.metadata.api.models.base import Base, LoadAble

__all__ = ["Genome", "GenomeDataset", "GenomeRelease", "GenomeGroup", "GenomeGroupMember"]

logger = logging.getLogger(__name__)


class Genome(LoadAble, Base):
    __tablename__ = "genome"

    genome_id = Column(Integer, primary_key=True)
    genome_uuid = Column(String(32), nullable=False, unique=True, default=str(uuid.uuid4))
    assembly_id = Column(ForeignKey("assembly.assembly_id"), nullable=False, index=True)
    organism_id = Column(ForeignKey("organism.organism_id"), nullable=False, index=True)
    created = Column(DATETIME(fsp=6), nullable=False)
    genebuild_version = Column(String(64), nullable=False, unique=False)
    production_name = Column(String(120), nullable=False, unique=False)
    annotation_source = Column(String(120), nullable=False, unique=False)
    genebuild_date = Column(String(20), nullable=False, unique=False)
    suppressed = Column(TINYINT(1), nullable=False, default=0)
    suppression_details = Column(String(255), nullable=True, unique=False)
    url_name = Column(String(128), nullable=True, unique=False)
    # One to many relationships
    # genome_id to genome_dataset and genome release
    genome_datasets = relationship(
        "GenomeDataset", back_populates="genome", cascade="all, delete, delete-orphan"
    )
    genome_releases = relationship(
        "GenomeRelease", back_populates="genome", cascade="all, delete, delete-orphan"
    )
    genome_group_members = relationship(
        "GenomeGroupMember", back_populates="genome", cascade="all, delete, delete-orphan"
    )
    # many to one relationships
    # assembly_id to assembly
    assembly = relationship("Assembly", back_populates="genomes")
    # organism_id to organism
    organism = relationship("Organism", back_populates="genomes")


class GenomeDataset(LoadAble, Base):
    __tablename__ = "genome_dataset"

    genome_dataset_id = Column(Integer, primary_key=True)
    dataset_id = Column(ForeignKey("dataset.dataset_id"), nullable=False, index=True)
    genome_id = Column(ForeignKey("genome.genome_id"), nullable=False, index=True)
    release_id = Column(ForeignKey("ensembl_release.release_id"), index=True)
    is_current = Column(TINYINT(1), nullable=False, default=0)
    UniqueConstraint("genome_id", "dataset_id", "release_id", name="genome_dataset_release_uidx"),

    # genome_dataset_id to genome
    dataset = relationship(
        "Dataset", back_populates="genome_datasets", order_by="Dataset.name, desc(Dataset.created)"
    )
    # genome_id to genome
    genome = relationship(
        "Genome", back_populates="genome_datasets", order_by="Dataset.name, desc(Genome.created)"
    )
    # release_id to release
    ensembl_release = relationship(
        "EnsemblRelease", back_populates="genome_datasets", order_by="desc(EnsemblRelease.version)"
    )


class GenomeRelease(LoadAble, Base):
    __tablename__ = "genome_release"

    UniqueConstraint("genome_id", "release_id", name="genome_release_uidx"),
    genome_release_id = Column(Integer, primary_key=True)
    genome_id = Column(ForeignKey("genome.genome_id"), nullable=False, index=True)
    release_id = Column(ForeignKey("ensembl_release.release_id"), nullable=False, index=True)
    is_current = Column(TINYINT(1), nullable=False, default=0)
    # many to one relationships
    # genome_release_id to genome_release
    genome = relationship("Genome", back_populates="genome_releases")
    # release_id to ensembl release
    ensembl_release = relationship("EnsemblRelease", back_populates="genome_releases")


class GenomeGroup(LoadAble, Base):
    __tablename__ = "genome_group"

    genome_group_id = Column(Integer, primary_key=True)
    type = Column(Enum("compara_reference", "structural_variant", "project"), nullable=False)
    name = Column(String(128), nullable=False, unique=True)
    label = Column(String(128))
    searchable = Column(TINYINT(1), nullable=False, default=0)
    description = Column(String(255))

    # One to many relationships
    # genome_group_id to organism_group_member
    genome_group_members = relationship("GenomeGroupMember", back_populates="genome_group")


class GenomeGroupMember(LoadAble, Base):
    __tablename__ = "genome_group_member"

    genome_group_member_id = Column(Integer, primary_key=True)
    is_reference = Column(TINYINT(1), nullable=False, default=0)
    genome_id = Column(ForeignKey("genome.genome_id"), nullable=False)
    genome_group_id = Column(ForeignKey("genome_group.genome_group_id"), nullable=False)
    release_id = Column(ForeignKey("ensembl_release.release_id"))
    is_current = Column(TINYINT(1), nullable=False, default=0)

    # many to one relationships
    genome_group = relationship("GenomeGroup", back_populates="genome_group_members")
    genome = relationship("Genome", back_populates="genome_group_members")
    ensembl_release = relationship("EnsemblRelease", back_populates="genome_group_members")
