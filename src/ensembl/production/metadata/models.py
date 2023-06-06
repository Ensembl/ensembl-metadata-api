# See the NOTICE file distributed with this work for additional information
# regarding copyright ownership.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from sqlalchemy import Column, DECIMAL, Date, DateTime, ForeignKey, Index, Integer, String, Enum, text
from sqlalchemy.dialects.mysql import DATETIME, TINYINT
from sqlalchemy.orm import relationship, sessionmaker, backref
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, MetaData, inspect

Base = declarative_base()
metadata = Base.metadata


class Assembly(Base):
    __tablename__ = 'assembly'

    assembly_id = Column(Integer, primary_key=True)
    ucsc_name = Column(String(16))
    accession = Column(String(16), nullable=False, unique=True)
    level = Column(String(32), nullable=False)
    name = Column(String(128), nullable=False)
    accession_body = Column(String(32))
    assembly_default = Column(String(32))
    tol_id = Column(String(32), unique=True)
    created = Column(DateTime)
    ensembl_name = Column(String(255), unique=True)
    # One to many relationships
    # assembly_id within assembly_sequence
    assembly_sequences = relationship("AssemblySequence", back_populates="assembly")
    # assembly_id within genome
    genomes = relationship("Genome", back_populates="assembly")


# many to one relationships
# none

class AssemblySequence(Base):
    __tablename__ = 'assembly_sequence'
    __table_args__ = (
        Index('assembly_sequence_assembly_id_accession_5f3e5119_uniq', 'assembly_id', 'accession', unique=True),
    )

    assembly_sequence_id = Column(Integer, primary_key=True)
    name = Column(String(128), unique=True)
    assembly_id = Column(ForeignKey('assembly.assembly_id'), nullable=False, index=True)
    accession = Column(String(128), nullable=False)
    chromosomal = Column(TINYINT(1), nullable=False)
    length = Column(Integer, nullable=False)
    sequence_location = Column(String(10))
    sequence_checksum = Column(String(32))
    ga4gh_identifier = Column(String(32))
    # One to many relationships
    # none
    # many to one relationships
    # assembly_id within assembly
    assembly = relationship('Assembly', back_populates="assembly_sequences")


class Attribute(Base):
    __tablename__ = 'attribute'

    attribute_id = Column(Integer, primary_key=True)
    name = Column(String(128), nullable=False)
    label = Column(String(128), nullable=False)
    description = Column(String(255))
    type = Column(Enum('string', 'percent', 'float', 'integer', 'bp'), server_default=text("'string'"))
    # One to many relationships
    # attribute_id within dataset attribute
    dataset_attributes = relationship("DatasetAttribute", back_populates='attribute')
    # many to one relationships
    # none


class Dataset(Base):
    __tablename__ = 'dataset'

    dataset_id = Column(Integer, primary_key=True)
    dataset_uuid = Column(String(128), nullable=False, unique=True)
    dataset_type_id = Column(ForeignKey('dataset_type.dataset_type_id'), nullable=False, index=True)
    name = Column(String(128), nullable=False)
    version = Column(String(128))
    created = Column(DATETIME(fsp=6), nullable=False)
    dataset_source_id = Column(ForeignKey('dataset_source.dataset_source_id'), nullable=False, index=True)
    label = Column(String(128), nullable=False)
    status = Column(Enum('Submitted', 'Progressing', 'Processed'), server_default=text("'Submitted'"))

    # One to many relationships
    # dataset_id to dataset attribute and genome dataset
    dataset_attributes = relationship("DatasetAttribute", back_populates='dataset')
    genome_datasets = relationship("GenomeDataset", back_populates='dataset')
    # many to one relationships
    # dataset_type_id to dataset_type
    dataset_type = relationship('DatasetType', back_populates="datasets")
    # dataset_source_id to dataset source
    dataset_source = relationship('DatasetSource', back_populates="datasets")


class DatasetAttribute(Base):
    __tablename__ = 'dataset_attribute'
    __table_args__ = (
        Index('dataset_attribute_dataset_id_attribute_id__d3b34d8c_uniq', 'dataset_id', 'attribute_id', 'value',
              unique=True),
    )

    dataset_attribute_id = Column(Integer, primary_key=True)
    value = Column(String(128), nullable=False)
    attribute_id = Column(ForeignKey('attribute.attribute_id'), nullable=False, index=True)
    dataset_id = Column(ForeignKey('dataset.dataset_id'), nullable=False, index=True)
    # One to many relationships
    # none
    # many to one relationships
    # dataset_attribute_id to dataset
    attribute = relationship('Attribute', back_populates="dataset_attributes")
    # attribute_id to attribute
    dataset = relationship('Dataset', back_populates="dataset_attributes")


class DatasetSource(Base):
    __tablename__ = 'dataset_source'

    dataset_source_id = Column(Integer, primary_key=True)
    type = Column(String(32), nullable=False)
    name = Column(String(255), nullable=False, unique=True)
    # One to many relationships
    # dataset_source_id to dataset
    datasets = relationship('Dataset', back_populates='dataset_source')
    # many to one relationships
    # none


class DatasetType(Base):
    __tablename__ = 'dataset_type'

    dataset_type_id = Column(Integer, primary_key=True)
    name = Column(String(32), nullable=False)
    label = Column(String(128), nullable=False)
    topic = Column(String(32), nullable=False)
    description = Column(String(255))
    details_uri = Column(String(255))
    # One to many relationships
    # dataset_type_id to dataset
    datasets = relationship('Dataset', back_populates='dataset_type')
    # many to one relationships
    # none


class EnsemblSite(Base):
    __tablename__ = 'ensembl_site'

    site_id = Column(Integer, primary_key=True)
    name = Column(String(64), nullable=False)
    label = Column(String(64), nullable=False)
    uri = Column(String(64), nullable=False)
    # One to many relationships
    # site_id to ensembl_release
    ensembl_releases = relationship('EnsemblRelease', back_populates='ensembl_site')
    # many to one relationships
    # none


class EnsemblRelease(Base):
    __tablename__ = 'ensembl_release'
    __table_args__ = (
        Index('ensembl_release_version_site_id_b743399a_uniq', 'version', 'site_id', unique=True),
    )

    release_id = Column(Integer, primary_key=True)
    version = Column(DECIMAL(10, 1), nullable=False)
    release_date = Column(Date, nullable=False)
    label = Column(String(64))
    is_current = Column(TINYINT(1), nullable=False)
    site_id = Column(ForeignKey('ensembl_site.site_id'), index=True)
    release_type = Column(String(16), nullable=False)
    # One to many relationships
    # release_id to genome dataset and genome release
    genome_datasets = relationship('GenomeDataset', back_populates='ensembl_release')
    genome_releases = relationship('GenomeRelease', back_populates='ensembl_release')
    # many to one relationships
    # site_id to ensembl_site
    ensembl_site = relationship('EnsemblSite', back_populates='ensembl_releases')


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


class Organism(Base):
    __tablename__ = 'organism'

    organism_id = Column(Integer, primary_key=True)
    taxonomy_id = Column(Integer, nullable=False)
    species_taxonomy_id = Column(Integer)
    display_name = Column(String(128), nullable=False)
    strain = Column(String(128))
    scientific_name = Column(String(128))
    url_name = Column(String(128), nullable=False)
    ensembl_name = Column(String(128), nullable=False, unique=True)
    scientific_parlance_name = Column(String(255))
    # One to many relationships
    # Organism_id to organism_group_member and genome
    genomes = relationship('Genome', back_populates='organism')
    organism_group_members = relationship('OrganismGroupMember', back_populates='organism')

    # many to one relationships
    # organim_id and taxonomy_id to taxonomy_node #DIFFERENT DATABASE
    def __repr__(self):
        return f'organism_id={self.organism_id}, taxonomy_id={self.taxonomy_id}, species_taxonomy_id={self.species_taxonomy_id}, ' \
               f'display_name={self.display_name}, strain={self.strain}, scientific_name={self.scientific_name}, ' \
               f'url_name={self.url_name}, ensembl_name={self.ensembl_name}, scientific_parlance_name={self.scientific_parlance_name}'


class OrganismGroup(Base):
    __tablename__ = 'organism_group'
    __table_args__ = (
        Index('group_type_name_63c2f6ac_uniq', 'type', 'name', unique=True),
    )

    organism_group_id = Column(Integer, primary_key=True)
    type = Column(String(32), nullable=False)
    name = Column(String(255), nullable=False)
    code = Column(String(48), unique=True)
    # One to many relationships
    # Organism_group_id to organism_group_member
    organism_group_members = relationship('OrganismGroupMember', back_populates='organism_group')

    # many to one relationships
    # none
    def __repr__(self):
        return f'organism_group_id={self.organism_group_id}, type={self.type}, name={self.name}, ' \
               f'code={self.code}'


class OrganismGroupMember(Base):
    __tablename__ = 'organism_group_member'
    __table_args__ = (
        Index('organism_group_member_organism_id_organism_gro_fe8f49ac_uniq', 'organism_id', 'organism_group_id',
              unique=True),
    )

    organism_group_member_id = Column(Integer, primary_key=True)
    is_reference = Column(TINYINT(1), nullable=False)
    organism_id = Column(ForeignKey('organism.organism_id'), nullable=False)
    organism_group_id = Column(ForeignKey('organism_group.organism_group_id'), nullable=False, index=True)
    # One to many relationships
    # none
    # many to one relationships
    # Organism_group_id to organism_group_member
    # organism_id to organism
    organism_group = relationship('OrganismGroup', back_populates='organism_group_members')
    organism = relationship('Organism', back_populates='organism_group_members')

    def __repr__(self):
        return f'organism_group_member_id={self.organism_group_member_id}, is_reference={self.is_reference}, organism_id={self.organism_id}, ' \
               f'organism_group_id={self.organism_group_id}'
