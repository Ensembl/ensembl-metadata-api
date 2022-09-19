# coding: utf-8
from sqlalchemy import Column, DECIMAL, Date, DateTime, ForeignKey, Index, Integer, String
from sqlalchemy.dialects.mysql import DATETIME, TINYINT
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

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
    tolid = Column(String(32), unique=True)
    created = Column(DateTime)
    ensembl_name = Column(String(255), unique=True)


class Attribute(Base):
    __tablename__ = 'attribute'

    attribute_id = Column(Integer, primary_key=True)
    name = Column(String(128), nullable=False)
    label = Column(String(128), nullable=False)
    description = Column(String(255))


class DatasetSource(Base):
    __tablename__ = 'dataset_source'

    dataset_source_id = Column(Integer, primary_key=True)
    type = Column(String(32), nullable=False)
    name = Column(String(255), nullable=False, unique=True)


class DatasetType(Base):
    __tablename__ = 'dataset_type'

    dataset_type_id = Column(Integer, primary_key=True)
    name = Column(String(32), nullable=False)
    label = Column(String(128), nullable=False)
    topic = Column(String(32), nullable=False)
    description = Column(String(255))
    details_uri = Column(String(255))


class DjangoMigration(Base):
    __tablename__ = 'django_migrations'

    id = Column(Integer, primary_key=True)
    app = Column(String(255), nullable=False)
    name = Column(String(255), nullable=False)
    applied = Column(DATETIME(fsp=6), nullable=False)


class EnsemblSite(Base):
    __tablename__ = 'ensembl_site'

    site_id = Column(Integer, primary_key=True)
    name = Column(String(64), nullable=False)
    label = Column(String(64), nullable=False)
    uri = Column(String(64), nullable=False)


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


class OrganismGroup(Base):
    __tablename__ = 'organism_group'
    __table_args__ = (
        Index('group_type_name_63c2f6ac_uniq', 'type', 'name', unique=True),
    )

    organism_group_id = Column(Integer, primary_key=True)
    type = Column(String(32), nullable=False)
    name = Column(String(255), nullable=False)
    code = Column(String(48), unique=True)


class AssemblySequence(Base):
    __tablename__ = 'assembly_sequence'
    __table_args__ = (
        Index('assembly_sequence_assembly_id_accession_5f3e5119_uniq', 'assembly_id', 'accession', unique=True),
    )

    assembly_sequence_id = Column(Integer, primary_key=True)
    name = Column(String(128))
    assembly_id = Column(ForeignKey('assembly.assembly_id'), nullable=False, index=True)
    accession = Column(String(32), nullable=False)
    chromosomal = Column(TINYINT(1), nullable=False)
    length = Column(Integer, nullable=False)
    sequence_location = Column(String(10))
    sequence_checksum = Column(String(32))
    ga4gh_identifier = Column(String(32))

    assembly = relationship('Assembly')


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

    dataset_source = relationship('DatasetSource')
    dataset_type = relationship('DatasetType')


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

    site = relationship('EnsemblSite')


class Genome(Base):
    __tablename__ = 'genome'

    genome_id = Column(Integer, primary_key=True)
    genome_uuid = Column(String(128), nullable=False, unique=True)
    assembly_id = Column(ForeignKey('assembly.assembly_id'), nullable=False, index=True)
    organism_id = Column(ForeignKey('organism.organism_id'), nullable=False, index=True)
    created = Column(DATETIME(fsp=6), nullable=False)

    assembly = relationship('Assembly')
    organism = relationship('Organism')


class OrganismGroupMember(Base):
    __tablename__ = 'organism_group_member'
    __table_args__ = (
        Index('organism_group_member_organism_id_organism_gro_fe8f49ac_uniq', 'organism_id', 'organism_group_id', unique=True),
    )

    organism_group_member_id = Column(Integer, primary_key=True)
    is_reference = Column(TINYINT(1), nullable=False)
    organism_id = Column(ForeignKey('organism.organism_id'), nullable=False)
    organism_group_id = Column(ForeignKey('organism_group.organism_group_id'), nullable=False, index=True)

    organism_group = relationship('OrganismGroup')
    organism = relationship('Organism')


class DatasetAttribute(Base):
    __tablename__ = 'dataset_attribute'
    __table_args__ = (
        Index('dataset_attribute_dataset_id_attribute_id__d3b34d8c_uniq', 'dataset_id', 'attribute_id', 'type', 'value', unique=True),
    )

    dataset_attribute_id = Column(Integer, primary_key=True)
    type = Column(String(32), nullable=False)
    value = Column(String(128), nullable=False)
    attribute_id = Column(ForeignKey('attribute.attribute_id'), nullable=False, index=True)
    dataset_id = Column(ForeignKey('dataset.dataset_id'), nullable=False, index=True)

    attribute = relationship('Attribute')
    dataset = relationship('Dataset')


class GenomeDataset(Base):
    __tablename__ = 'genome_dataset'

    genome_dataset_id = Column(Integer, primary_key=True)
    dataset_id = Column(ForeignKey('dataset.dataset_id'), nullable=False, index=True)
    genome_id = Column(ForeignKey('genome.genome_id'), nullable=False, index=True)
    release_id = Column(ForeignKey('ensembl_release.release_id'), nullable=False, index=True)
    is_current = Column(TINYINT(1), nullable=False)

    dataset = relationship('Dataset')
    genome = relationship('Genome')
    release = relationship('EnsemblRelease')


class GenomeRelease(Base):
    __tablename__ = 'genome_release'

    genome_release_id = Column(Integer, primary_key=True)
    genome_id = Column(ForeignKey('genome.genome_id'), nullable=False, index=True)
    release_id = Column(ForeignKey('ensembl_release.release_id'), nullable=False, index=True)
    is_current = Column(TINYINT(1), nullable=False)

    genome = relationship('Genome')
    release = relationship('EnsemblRelease')