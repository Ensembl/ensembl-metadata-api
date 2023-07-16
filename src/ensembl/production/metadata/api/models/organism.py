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

from ensembl.production.metadata.api.models.base import Base


class Organism(Base):
    __tablename__ = "organism"

    organism_id = Column(Integer, primary_key=True)
    organism_uuid = Column(String(128), unique=True, nullable=False, default=uuid.uuid4)
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
    genomes = relationship("Genome", back_populates="organism", cascade="all, delete, delete-orphan")
    organism_group_members = relationship("OrganismGroupMember", back_populates="organism")

    # many to one relationships
    # organim_id and taxonomy_id to taxonomy_node #DIFFERENT DATABASE
    def __repr__(self):
        return f"organism_id={self.organism_id}, taxonomy_id={self.taxonomy_id}, species_taxonomy_id={self.species_taxonomy_id}, " \
               f"display_name={self.display_name}, strain={self.strain}, scientific_name={self.scientific_name}, " \
               f"url_name={self.url_name}, ensembl_name={self.ensembl_name}, scientific_parlance_name={self.scientific_parlance_name}"


class OrganismGroup(Base):
    __tablename__ = "organism_group"
    __table_args__ = (
        Index("group_type_name_63c2f6ac_uniq", "type", "name", unique=True),
    )

    organism_group_id = Column(Integer, primary_key=True)
    type = Column(String(32), nullable=False)
    name = Column(String(255), nullable=False)
    code = Column(String(48), unique=True)
    # One to many relationships
    # Organism_group_id to organism_group_member
    organism_group_members = relationship("OrganismGroupMember", back_populates="organism_group")

    # many to one relationships
    # none
    def __repr__(self):
        return f"organism_group_id={self.organism_group_id}, type={self.type}, name={self.name}, " \
               f"code={self.code}"


class OrganismGroupMember(Base):
    __tablename__ = "organism_group_member"
    __table_args__ = (
        Index("organism_group_member_organism_id_organism_gro_fe8f49ac_uniq", "organism_id", "organism_group_id",
              unique=True),
    )

    organism_group_member_id = Column(Integer, primary_key=True)
    is_reference = Column(TINYINT(1), nullable=False)
    organism_id = Column(ForeignKey("organism.organism_id"), nullable=False)
    organism_group_id = Column(ForeignKey("organism_group.organism_group_id"), nullable=False, index=True)
    # One to many relationships
    # none
    # many to one relationships
    # Organism_group_id to organism_group_member
    # organism_id to organism
    organism_group = relationship("OrganismGroup", back_populates="organism_group_members")
    organism = relationship("Organism", back_populates="organism_group_members")

    def __repr__(self):
        return f"organism_group_member_id={self.organism_group_member_id}, is_reference={self.is_reference}, organism_id={self.organism_id}, " \
               f"organism_group_id={self.organism_group_id}"
