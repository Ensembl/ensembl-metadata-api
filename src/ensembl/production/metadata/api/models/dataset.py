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
from sqlalchemy import Column, Integer, String, Enum, text, ForeignKey, Index
from sqlalchemy.dialects.mysql import DATETIME
from sqlalchemy.orm import relationship

from ensembl.production.metadata.api.models.base import Base


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