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
from sqlalchemy import Column, Integer, String, Index, DECIMAL, Date, ForeignKey
from sqlalchemy.dialects.mysql import TINYINT
from sqlalchemy.orm import relationship

from ensembl.production.metadata.api.models.base import Base


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

    def __repr__(self):
        return f"EnsemblSite(" \
                   f"site_id={self.site_id}, " \
                   f"name={self.name}, " \
                   f"label={self.label}, " \
                   f"uri={self.uri}" \
               f")"


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

    def __repr__(self):
        return f"EnsemblRelease(" \
                   f"release_id={self.release_id}, " \
                   f"version={self.version}, " \
                   f"label={self.label}, " \
                   f"is_current={self.is_current}, " \
                   f"release_type={self.release_type}" \
               f")"
