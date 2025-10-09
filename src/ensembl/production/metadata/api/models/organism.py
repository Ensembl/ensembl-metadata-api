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

from sqlalchemy import Column, Integer, String, Index, ForeignKey
from sqlalchemy.dialects.mysql import TINYINT
from sqlalchemy.orm import relationship

from ensembl.production.metadata.api.models.base import Base, LoadAble

__all__ = ["Organism", "OrganismGroup", "OrganismGroupMember"]


class Organism(LoadAble, Base):
    __tablename__ = "organism"

    organism_id = Column(Integer, primary_key=True)
    organism_uuid = Column(String(40), unique=True, nullable=False, default=uuid.uuid4)
    taxonomy_id = Column(Integer, nullable=False)
    species_taxonomy_id = Column(Integer)
    common_name = Column(String(128), nullable=True)
    strain = Column(String(128))
    scientific_name = Column(String(128))
    biosample_id = Column(String(128), nullable=False, unique=True)
    scientific_parlance_name = Column(String(255))
    rank = Column(Integer, default=0)
    strain_type = Column(String(128), nullable=True, unique=False)
    # One to many relationships
    # Organism_id to organism_group_member and genome
    genomes = relationship("Genome", back_populates="organism", cascade="all, delete, delete-orphan")
    organism_group_members = relationship("OrganismGroupMember", back_populates="organism")


class OrganismGroup(LoadAble, Base):
    __tablename__ = "organism_group"
    __table_args__ = (Index("group_type_name_63c2f6ac_uniq", "type", "name", unique=True),)

    organism_group_id = Column(Integer, primary_key=True)
    type = Column(String(32))
    name = Column(String(255), nullable=False)
    code = Column(String(48), unique=True)
    # One to many relationships
    # Organism_group_id to organism_group_member
    organism_group_members = relationship("OrganismGroupMember", back_populates="organism_group")


class OrganismGroupMember(LoadAble, Base):
    __tablename__ = "organism_group_member"
    __table_args__ = (
        Index(
            "organism_group_member_organism_id_organism_gro_fe8f49ac_uniq",
            "organism_id",
            "organism_group_id",
            unique=True,
        ),
    )

    organism_group_member_id = Column(Integer, primary_key=True)
    is_reference = Column(TINYINT(1), nullable=False, default=0)
    order = Column(Integer, nullable=True)
    organism_id = Column(ForeignKey("organism.organism_id"), nullable=False)
    organism_group_id = Column(ForeignKey("organism_group.organism_group_id"), nullable=False, index=True)
    # many to one relationships
    # Organism_group_id to organism_group_member
    # organism_id to organism
    organism_group = relationship("OrganismGroup", back_populates="organism_group_members")
    organism = relationship("Organism", back_populates="organism_group_members")
