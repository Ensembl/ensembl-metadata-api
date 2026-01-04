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
import datetime
import enum
import logging
import uuid

import sqlalchemy
from sqlalchemy import Column, Integer, String, text, ForeignKey, Index
from sqlalchemy.dialects.mysql import DATETIME, TINYINT
from sqlalchemy.orm import relationship, backref
from sqlalchemy.sql import func
from sqlalchemy.types import Enum

from ensembl.production.metadata.api.exceptions import MissingMetaException
from ensembl.production.metadata.api.models.base import Base, LoadAble

__all__ = ['Dataset', 'DatasetType', 'DatasetAttribute', 'DatasetSource', 'DatasetStatus', 'Attribute', ]
logger = logging.getLogger(__name__)


class DatasetStatus(enum.Enum):
    SUBMITTED = "Submitted"
    PROCESSING = "Processing"
    PROCESSED = "Processed"
    RELEASED = "Released"
    FAULTY = "Faulty"
    SUPPRESSED = "Suppressed"


DatasetStatusType = sqlalchemy.types.Enum(
    DatasetStatus,
    name='dataset_status',
    values_callable=lambda obj: [e.value for e in obj]
)


class Attribute(LoadAble, Base):
    __tablename__ = 'attribute'

    attribute_id = Column(Integer, primary_key=True)
    name = Column(String(128), nullable=False, unique=True)
    label = Column(String(128), nullable=False)
    description = Column(String(255))
    required = Column(TINYINT(1), nullable=False, default=0)
    dataset_type_id = Column(ForeignKey('dataset_type.dataset_type_id'), nullable=True)
    type = Column(Enum('string', 'percent', 'float', 'integer', 'bp'), server_default=text("'string'"))
    # One to many relationships
    dataset_attributes = relationship("DatasetAttribute", back_populates='attribute')
    dataset_type = relationship("DatasetType", back_populates='attributes')


class Dataset(LoadAble, Base):
    __tablename__ = 'dataset'

    dataset_id = Column(Integer, primary_key=True)
    dataset_uuid = Column(String(40), nullable=False, unique=True, default=lambda: str(uuid.uuid4()))
    dataset_type_id = Column(ForeignKey('dataset_type.dataset_type_id'), nullable=False, index=True)
    name = Column(String(128), nullable=False)
    version = Column(String(128))
    created = Column(DATETIME(fsp=6), server_default=func.now(), default=datetime.datetime.utcnow)
    dataset_source_id = Column(ForeignKey('dataset_source.dataset_source_id'), nullable=False, index=True)
    label = Column(String(128), nullable=False)
    status = Column(DatasetStatusType, server_default=text('Submitted'))
    parent_id = Column(Integer, ForeignKey('dataset.dataset_id'), nullable=True, index=True)

    # One to many relationships
    dataset_attributes = relationship("DatasetAttribute", back_populates='dataset',
                                      cascade="all, delete, delete-orphan")
    genome_datasets = relationship("GenomeDataset", back_populates='dataset', cascade="all, delete, delete-orphan")
    # many to one relationships
    dataset_type = relationship('DatasetType', back_populates="datasets")
    dataset_source = relationship('DatasetSource', back_populates="datasets")
    children = relationship('Dataset', backref=backref("parent", remote_side=[dataset_id]))

    @property
    def genebuild_version(self):
        logger.debug(f"dataset type {self.dataset_type.name} {self.version}")
        if self.dataset_type.name == 'genebuild':
            # Return version

            return next(
                (att.value for att in self.dataset_attributes if att.attribute.name == 'genebuild.last_geneset_update'),
                next((att.value for att in self.dataset_attributes if att.attribute.name == 'genebuild.start_date'),
                     None))
        else:
            # return Related genebuild version
            logger.debug(F"Related datasets! : {self.genome_datasets.datasets}")
            genebuild_ds = next(
                (dataset for dataset in self.genome_datasets.datasets if dataset.dataset_type.name == 'genebuild'),
                None)
            if genebuild_ds:
                return genebuild_ds.genebuild_version
            else:
                raise MissingMetaException(f"Something is very wrong with dataset {self.dataset_uuid}")


class DatasetAttribute(LoadAble, Base):
    __tablename__ = 'dataset_attribute'
    __table_args__ = (
        Index('dataset_attribute_dataset_id_attribute_id__d3b34d8c_uniq', 'dataset_id', 'attribute_id', 'value',
              unique=True),
    )

    dataset_attribute_id = Column(Integer, primary_key=True)
    value = Column(String(255), nullable=False)
    attribute_id = Column(ForeignKey('attribute.attribute_id'), nullable=False, index=True)
    dataset_id = Column(ForeignKey('dataset.dataset_id'), nullable=False, index=True)
    # many to one relationships
    attribute = relationship('Attribute', back_populates="dataset_attributes")
    dataset = relationship('Dataset', back_populates="dataset_attributes")


class DatasetSource(LoadAble, Base):
    __tablename__ = 'dataset_source'

    dataset_source_id = Column(Integer, primary_key=True)
    type = Column(String(32), nullable=False)
    name = Column(String(255), nullable=False, unique=True)
    location = Column(String(120))
    # One to many relationships
    datasets = relationship('Dataset', back_populates='dataset_source')

class DatasetType(LoadAble, Base):
    __tablename__ = 'dataset_type'

    dataset_type_id = Column(Integer, primary_key=True)
    name = Column(String(32), nullable=False, unique=True)
    label = Column(String(128), nullable=False)
    topic = Column(String(32), nullable=False)
    description = Column(String(255))
    parent = Column(ForeignKey('dataset_type.dataset_type_id'), name='parent_id', nullable=True, index=True)
    # One to many relationships
    datasets = relationship('Dataset', back_populates='dataset_type')
