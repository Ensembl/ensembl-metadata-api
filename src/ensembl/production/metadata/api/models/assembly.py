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

from sqlalchemy import Column, Integer, String, DateTime, Index, ForeignKey
from sqlalchemy.dialects.mysql import TINYINT
from sqlalchemy.orm import relationship

from ensembl.production.metadata.api.models.base import Base


class Assembly(Base):
    __tablename__ = 'assembly'

    assembly_id = Column(Integer, primary_key=True)
    assembly_uuid = Column(String(128), unique=True, nullable=False, default=uuid.uuid4)
    ucsc_name = Column(String(16))
    accession = Column(String(16), nullable=False, unique=True)
    level = Column(String(32), nullable=False)
    name = Column(String(128), nullable=False)
    accession_body = Column(String(32))
    assembly_default = Column(String(128))
    tol_id = Column(String(32), unique=True)
    created = Column(DateTime)
    ensembl_name = Column(String(255), unique=True)
    alt_accession = Column(String(16), nullable=True)
    is_reference = Column(TINYINT(1), nullable=False)
    url_name = Column(String(128), nullable=False)
    # One to many relationships
    # assembly_id within assembly_sequence
    assembly_sequences = relationship("AssemblySequence", back_populates="assembly", cascade="all, delete, delete-orphan")
    # assembly_id within genome
    genomes = relationship("Genome", back_populates="assembly", cascade="all, delete, delete-orphan")

    def __repr__(self):
        return f"Assembly(" \
                   f"assembly_id={self.assembly_id}, " \
                   f"assembly_uuid={self.assembly_uuid}, " \
                   f"accession='{self.accession}, " \
                   f"ensembl_name='{self.ensembl_name}'" \
               f")"


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
    chromosome_rank = Column(Integer)
    length = Column(Integer, nullable=False)
    sequence_location = Column(String(10))
    md5 = Column(String(32))
    sha512t4u = Column(String(128))
    # One to many relationships
    # none
    # many to one relationships
    # assembly_id within assembly
    assembly = relationship('Assembly', back_populates="assembly_sequences")

    def __repr__(self):
        return f"AssemblySequence(" \
                   f"assembly_sequence_id={self.assembly_sequence_id}, " \
                   f"assembly_id='{self.assembly_id}, " \
                   f"chromosomal='{self.chromosomal}', " \
                   f"length='{self.length}'" \
               f")"
