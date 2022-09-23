# coding: utf-8
from sqlalchemy import Column, DECIMAL, Date, DateTime, ForeignKey, Index, Integer, String
from sqlalchemy.dialects.mysql import DATETIME, TINYINT
from sqlalchemy.orm import relationship, sessionmaker, backref
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, MetaData, inspect

Base = declarative_base()
metadata = Base.metadata

# Currently the backreference is a plural of the

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
    assembly = relationship('Assembly', backref="assembly")


class Attribute(Base):
    __tablename__ = 'attribute'

    attribute_id = Column(Integer, primary_key=True)
    name = Column(String(128), nullable=False)
    label = Column(String(128), nullable=False)
    description = Column(String(255))



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

    dataset_source = relationship('DatasetSource', backref="dataset")
    dataset_type = relationship('DatasetType', backref="dataset")


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

    attribute = relationship('Attribute', backref="dataset_attribute")
    dataset = relationship('Dataset', backref="dataset_attribute")


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

    site = relationship('EnsemblSite', backref='ensembl_release')


class Genome(Base):
    __tablename__ = 'genome'

    genome_id = Column(Integer, primary_key=True)
    genome_uuid = Column(String(128), nullable=False, unique=True)
    assembly_id = Column(ForeignKey('assembly.assembly_id'), nullable=False, index=True)
    organism_id = Column(ForeignKey('organism.organism_id'), nullable=False, index=True)
    created = Column(DATETIME(fsp=6), nullable=False)

    assembly = relationship('Assembly', backref="genome")
    organism = relationship('Organism', backref="genome")


class GenomeDataset(Base):
    __tablename__ = 'genome_dataset'

    genome_dataset_id = Column(Integer, primary_key=True)
    dataset_id = Column(ForeignKey('dataset.dataset_id'), nullable=False, index=True)
    genome_id = Column(ForeignKey('genome.genome_id'), nullable=False, index=True)
    release_id = Column(ForeignKey('ensembl_release.release_id'), nullable=False, index=True)
    is_current = Column(TINYINT(1), nullable=False)

    dataset = relationship('Dataset', backref="genome_dataset")
    genome = relationship('Genome', backref="genome_dataset")
    release = relationship('EnsemblRelease', backref="genome_dataset")


class GenomeRelease(Base):
    __tablename__ = 'genome_release'

    genome_release_id = Column(Integer, primary_key=True)
    genome_id = Column(ForeignKey('genome.genome_id'), nullable=False, index=True)
    release_id = Column(ForeignKey('ensembl_release.release_id'), nullable=False, index=True)
    is_current = Column(TINYINT(1), nullable=False)

    genome = relationship('Genome', backref='genome_release')
    release = relationship('EnsemblRelease', backref='genome_release')



class Organism(Base):
    __tablename__ = 'organism'

    organism_id = Column(Integer, primary_key=True)
    #taxonomy_id = Column(Integer, nullable=False)
    #species_taxonomy_id = Column(Integer)
    display_name = Column(String(128), nullable=False)
    strain = Column(String(128))
    scientific_name = Column(String(128))
    url_name = Column(String(128), nullable=False)
    ensembl_name = Column(String(128), nullable=False, unique=True)
    scientific_parlance_name = Column(String(255))

    # These are for the taxonomy that is in this document. Commented out fields are for if we remove it.
    taxonomy_id = Column(ForeignKey('taxonomy_node.taxon_id'), nullable=False)
    species_taxonomy_id = Column(ForeignKey('taxonomy_node.taxon_id'))
    taxnode = relationship('TaxonomyNode', backref='organism') #no idea whether these will break in the future 2 to 1 relationship seems wrong.


#Taxonomy relationships. Currently these were added as Ensembl/ensembl-metadata-admin/blob/main/ncbi_taxonomy/models.py
# works on the djano implementation of sqlalchemy rather than the sqlalchemy ORM that is in use here.
class TaxonomyNode(Base):
    __tablename__ = 'taxonomy_node'
    # Not sure what we are doing here, as the db doesn't have it.
    # Could check from the regular metadata....
    # Also check for the unique constraints and whether they are nullable.
    # __table_args__ = (???)
    taxon_id = Column(Integer, primary_key=True)
    parent = Column(Integer)
    rank = Column(String(255))
    genbank_hidden_flag = Column(Integer)
    left_index = Column(Integer)
    right_index = Column(Integer)
    root_id = Column(Integer)
    #Not including the relationship for taxon_id to parent as I think the schema is wrong.


class TaxonomyName(Base):
    __tablename__ = 'taxonomy_name'
    # Not sure what we are doing here, as the db doesn't have it.
    # Could check from the regular metadata....
    # Also check for the unique constraints and whether they are nullable.
    # __table_args__ = (???)
    name_id = Column(Integer, primary_key=True)
    parent = Column(ForeignKey('taxonomy_node.taxon_id'))
    name = Column(String(255))
    name_class = Column(String(255))
    taxnode = relationship('TaxonomyNode', backref='taxonomy_name')




class OrganismGroup(Base):
    __tablename__ = 'organism_group'
    __table_args__ = (
        Index('group_type_name_63c2f6ac_uniq', 'type', 'name', unique=True),
    )

    organism_group_id = Column(Integer, primary_key=True)
    type = Column(String(32), nullable=False)
    name = Column(String(255), nullable=False)
    code = Column(String(48), unique=True)


class OrganismGroupMember(Base):
    __tablename__ = 'organism_group_member'
    __table_args__ = (
        Index('organism_group_member_organism_id_organism_gro_fe8f49ac_uniq', 'organism_id', 'organism_group_id', unique=True),
    )

    organism_group_member_id = Column(Integer, primary_key=True)
    is_reference = Column(TINYINT(1), nullable=False)
    organism_id = Column(ForeignKey('organism.organism_id'), nullable=False)
    organism_group_id = Column(ForeignKey('organism_group.organism_group_id'), nullable=False, index=True)

    organism_group = relationship('OrganismGroup', backref='organism_group_member')
    organism = relationship('Organism', backref='organism_group_member')


