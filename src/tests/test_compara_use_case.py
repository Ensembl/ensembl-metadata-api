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
import json
import logging
from pathlib import Path

import pytest
from ensembl.database import DBConnection
from google.protobuf import json_format
from sqlalchemy import Column, Integer, String, SmallInteger
from yagrc import reflector as yagrc_reflector

from ensembl.production.metadata.api.models.base import Base

logger = logging.getLogger(__name__)


@pytest.fixture(scope="class")
def compara_conn(multi_dbs):
    compara_conn = DBConnection(multi_dbs['compara_db'].dbc.url)
    yield compara_conn


class GenomeDB(Base):
    __tablename__ = 'genome_db'

    genome_db_id = Column(Integer, primary_key=True)
    taxon_id = Column(Integer, nullable=False)
    name = Column(String(128), nullable=False)
    assembly = Column(String(100), nullable=False)
    genebuild = Column(String(255), nullable=True)
    genome_component = Column(String(5), nullable=True)
    strain_name = Column(String(100), nullable=True)
    display_name = Column(String(255), nullable=False)
    locator = Column(String(400), nullable=False)
    first_release = Column(SmallInteger, nullable=True)
    last_release = Column(SmallInteger, nullable=True)


@pytest.mark.parametrize("multi_dbs", [[{'src': Path(__file__).parent / "databases/ensembl_genome_metadata"},
                                        {'src': Path(__file__).parent / "databases/ncbi_taxonomy"},
                                        {'src': Path(__file__).parent / "databases/compara_db"}
                                        ]], indirect=True)
class TestComparaUseCase:

    @pytest.mark.parametrize(
        "allow_unreleased, production_name, assembly_name, release_version, expected_count",
        [
            (False, 'homo_sapiens', "GRCh38", None, 1),
            (False, 'homo_sapiens_37', "GRCh37", 108.0, 1),
            (False, 'homo_sapiens_gca018505825v1', "HG02109.pri.mat.f1_v2", None, 0),
            (True, 'homo_sapiens_gca018505825v1', "HG02109.pri.mat.f1_v2", None, 1),
            (False, 'homo_sapiens_gca018473315v1', "HG03540.alt.pat.f1_v2", 108.0, 0),
            (False, 'homo_sapiens_gca018473315v1', "HG03540.alt.pat.f1_v2", 110.1, 1),
        ],
        indirect=['allow_unreleased']
    )
    def test_get_genomes_reference(self, compara_conn, grpc_channel, allow_unreleased, production_name, assembly_name,
                                   release_version, expected_count):
        reflector = yagrc_reflector.GrpcReflectionClient()
        reflector.load_protocols(grpc_channel, symbols=["ensembl_metadata.EnsemblMetadata"])
        stub_class = reflector.service_stub_class("ensembl_metadata.EnsemblMetadata")
        request_class = reflector.message_class("ensembl_metadata.GenomeInfoRequest")
        stub = stub_class(grpc_channel)

        with compara_conn.session_scope() as session:
            genome_db = session.query(GenomeDB).filter(GenomeDB.name == production_name).one()
            logger.debug(genome_db)
            response = stub.GetGenomeUUID(request_class(production_name=genome_db.name,
                                                        assembly_name=genome_db.assembly,
                                                        release_version=release_version,
                                                        genebuild_date=None,
                                                        use_default=True))
            logger.debug(json_format.MessageToJson(response))
            response_json = json.loads(json_format.MessageToJson(response))
            assert len(response_json) == expected_count
