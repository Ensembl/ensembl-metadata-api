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

from sqlalchemy import Column, Integer, String, DateTime, Index, ForeignKey, Enum, text
from sqlalchemy.dialects.mysql import TINYINT
from sqlalchemy.orm import relationship

from ensembl.production.metadata.api.models.base import Base, LoadAble

__all__ = ['Assembly', 'AssemblySequence']


class Assembly(LoadAble, Base):
    __tablename__ = 'assembly'

    assembly_id = Column(Integer, primary_key=True)
    assembly_uuid = Column(String(32), unique=True, nullable=False, default=uuid.uuid4)
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
    is_reference = Column(TINYINT(1), nullable=False, default=0)
    url_name = Column(String(128), nullable=False)
    # One to many relationships
    # assembly_id within assembly_sequence
    assembly_sequences = relationship("AssemblySequence", back_populates="assembly",
                                      cascade="all, delete, delete-orphan")
    # assembly_id within genome
    genomes = relationship("Genome", back_populates="assembly", cascade="all, delete, delete-orphan")

    def is_released(self):
        for genome in self.genomes:
            if any(gr.release_id is not None for gr in genome.genome_releases):
                return True
        return False


class AssemblySequence(LoadAble, Base):
    __tablename__ = 'assembly_sequence'
    __table_args__ = (
        Index('assembly_sequence_assembly_id_accession_5f3e5119_uniq', 'assembly_id', 'accession', unique=True),
    )

    assembly_sequence_id = Column(Integer, primary_key=True)
    name = Column(String(128), unique=True)
    assembly_id = Column(ForeignKey('assembly.assembly_id'), nullable=False, index=True)
    accession = Column(String(128), nullable=False)
    chromosomal = Column(TINYINT(1), nullable=False, default=0)
    chromosome_rank = Column(Integer)
    length = Column(Integer, nullable=False)
    sequence_location = Column(String(10))
    md5 = Column(String(32))
    # column need renaming as well
    sha512t24u = Column(String(128))
    type = Column(Enum('chromosome_group', 'plasmid', 'primary_assembly', 'contig', 'chromosome', 'scaffold', 'lrg',
                       'supercontig', 'supscaffold'), server_default=text("'primary_assembly'"))
    is_circular = Column(TINYINT(1), nullable=False, default=0)
    assembly = relationship('Assembly', back_populates="assembly_sequences")

    # backward compatibility with old column name sha512t2u
    @property
    def sha512t4u(self):
        return self.sha512t24u

    @sha512t4u.setter
    def sha512t4u(self, checksum):
        self.sha512t24u = checksum
