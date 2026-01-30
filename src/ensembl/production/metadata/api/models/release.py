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
import enum

import sqlalchemy
from sqlalchemy import Column, Integer, String, Index, DECIMAL, Date, ForeignKey, Enum
from sqlalchemy.dialects.mysql import TINYINT
from sqlalchemy.orm import relationship

from ensembl.production.metadata.api.models.base import Base, LoadAble
from ensembl.production.metadata.grpc.config import cfg

__all__ = ['ReleaseStatus', 'EnsemblRelease', 'EnsemblSite']


class ReleaseStatus(enum.Enum):
    PLANNED = "Planned"
    PREPARING = "Preparing"
    PREPARED = "Prepared"
    RELEASED = "Released"
    ARCHIVED = "Archived"


ReleaseStatusType = sqlalchemy.types.Enum(
    ReleaseStatus,
    name='release_status',
    values_callable=lambda obj: [e.value for e in obj]
)


class EnsemblSite(LoadAble, Base):
    __tablename__ = 'ensembl_site'

    site_id = Column(Integer, primary_key=True)
    name = Column(String(64), nullable=False)
    label = Column(String(64), nullable=False)
    uri = Column(String(64), nullable=False)
    ensembl_releases = relationship('EnsemblRelease', back_populates='ensembl_site')


class EnsemblRelease(LoadAble, Base):
    __tablename__ = 'ensembl_release'
    __table_args__ = (
        Index('ensembl_release_version_site_id_b743399a_uniq', 'version', 'site_id', unique=True),
    )

    release_id = Column(Integer, primary_key=True)
    version = Column(DECIMAL(10, 1), nullable=False)
    release_date = Column(Date, nullable=False)
    label = Column(String(64), nullable=False)
    is_current = Column(TINYINT(1), nullable=False, default=0)
    site_id = Column(ForeignKey('ensembl_site.site_id'), index=True)
    release_type = Column(Enum('partial', 'integrated'), nullable=False)
    status = Column(ReleaseStatusType, nullable=False, default=ReleaseStatus.PLANNED)
    name = Column(String(3))
    # One to many relationships
    # release_id to genome dataset and genome release
    genome_datasets = relationship('GenomeDataset', back_populates='ensembl_release')
    genome_releases = relationship('GenomeRelease', back_populates='ensembl_release')
    genome_group_members = relationship('GenomeGroupMember', back_populates='ensembl_release')

    # many to one relationships
    # Added fileter condition on every join to EnsemblSite for code clarity
    # No other than configure site data should be returned
    ensembl_site = relationship('EnsemblSite', back_populates='ensembl_releases',
                                primaryjoin=f"and_(EnsemblSite.site_id==EnsemblRelease.site_id, "
                                            f"EnsemblSite.site_id=={cfg.ensembl_site_id})")
