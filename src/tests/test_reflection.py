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
""" Test Server Reflection discovery """

import logging
from pathlib import Path

import pytest
from google.protobuf.descriptor import MethodDescriptor
from google.protobuf.descriptor_pool import DescriptorPool
from grpc_reflection.v1alpha.proto_reflection_descriptor_database import ProtoReflectionDescriptorDatabase
from yagrc import reflector as yagrc_reflector

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("test_dbs", [[{"src": Path(__file__).parent / "databases/ensembl_genome_metadata"},
                                        {"src": Path(__file__).parent / "databases/ncbi_taxonomy"}]],
                         indirect=True)
class TestGRPCReflection:
    dbc = None

    def test_services_discovery(self, test_dbs, grpc_channel, grpc_server):
        from ensembl.production.metadata.grpc.ensembl_metadata_pb2_grpc import EnsemblMetadata

        reflection_db = ProtoReflectionDescriptorDatabase(grpc_channel)
        services = reflection_db.get_services()
        assert 'ensembl_metadata.EnsemblMetadata' in services
        assert 'grpc.reflection.v1alpha.ServerReflection' in services
        desc_pool = DescriptorPool(reflection_db)
        metadata_service = desc_pool.FindServiceByName('ensembl_metadata.EnsemblMetadata')
        method_list = [func for func in dir(EnsemblMetadata) if
                       callable(getattr(EnsemblMetadata, func)) and not func.startswith("__")]
        for method_name in method_list:
            method_desc = metadata_service.FindMethodByName(method_name)
            assert isinstance(method_desc, MethodDescriptor)

    def test_dynamic_invoke(self, test_dbs, grpc_channel, grpc_server):
        logger.warning("multi dbs", test_dbs)
        reflector = yagrc_reflector.GrpcReflectionClient()
        reflector.load_protocols(grpc_channel, symbols=["ensembl_metadata.EnsemblMetadata"])
        stub_class = reflector.service_stub_class("ensembl_metadata.EnsemblMetadata")
        request_class = reflector.message_class("ensembl_metadata.GenomeUUIDRequest")
        stub = stub_class(grpc_channel)
        response = stub.GetGenomeByUUID(request_class(genome_uuid='a73351f7-93e7-11ec-a39d-005056b38ce3',
                                                      release_version=None))
        assert response.genome_uuid == 'a73351f7-93e7-11ec-a39d-005056b38ce3'
        assert response.assembly.accession == 'GCA_000005845.2'
