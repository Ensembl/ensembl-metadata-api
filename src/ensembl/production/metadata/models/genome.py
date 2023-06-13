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
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.dialects.mysql import DATETIME, TINYINT
from sqlalchemy.orm import relationship

from ensembl.production.metadata.models.base import Base


class Genome(Base):
    __tablename__ = 'genome'

    genome_id = Column(Integer, primary_key=True)
    genome_uuid = Column(String(128), nullable=False, unique=True)
    assembly_id = Column(ForeignKey('assembly.assembly_id'), nullable=False, index=True)
    organism_id = Column(ForeignKey('organism.organism_id'), nullable=False, index=True)
    created = Column(DATETIME(fsp=6), nullable=False)
    # One to many relationships
    # genome_id to genome_dataset and genome release
    genome_datasets = relationship('GenomeDataset', back_populates='genome')
    genome_releases = relationship('GenomeRelease', back_populates='genome')
    # many to one relationships
    # assembly_id to assembly
    assembly = relationship('Assembly', back_populates="genomes")
    # organism_id to organism
    organism = relationship('Organism', back_populates="genomes")


class GenomeDataset(Base):
    __tablename__ = 'genome_dataset'

    genome_dataset_id = Column(Integer, primary_key=True)
    dataset_id = Column(ForeignKey('dataset.dataset_id'), nullable=False, index=True)
    genome_id = Column(ForeignKey('genome.genome_id'), nullable=False, index=True)
    release_id = Column(ForeignKey('ensembl_release.release_id'), index=True)
    is_current = Column(TINYINT(1), nullable=False)
    # One to many relationships
    # none
    # many to one relationships
    # genome_dataset_id to genome
    dataset = relationship('Dataset', back_populates="genome_datasets")
    # genome_id to genome
    genome = relationship('Genome', back_populates="genome_datasets")
    # release_id to release
    ensembl_release = relationship('EnsemblRelease', back_populates="genome_datasets")


class GenomeRelease(Base):
    __tablename__ = 'genome_release'

    genome_release_id = Column(Integer, primary_key=True)
    genome_id = Column(ForeignKey('genome.genome_id'), nullable=False, index=True)
    release_id = Column(ForeignKey('ensembl_release.release_id'), nullable=False, index=True)
    is_current = Column(TINYINT(1), nullable=False)
    # One to many relationships
    # none
    # many to one relationships
    # genome_release_id to genome_release
    genome = relationship('Genome', back_populates='genome_releases')
    # release_id to ensembl release
    ensembl_release = relationship('EnsemblRelease', back_populates='genome_releases')
