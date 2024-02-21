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
from grpc_reflection.v1alpha import reflection

from ensembl.production.metadata.grpc import ensembl_metadata_pb2
from ensembl.production.metadata.grpc.ensembl_metadata_pb2_grpc import EnsemblMetadata

logger = logging.getLogger(__name__)


@pytest.fixture(scope='module')
def grpc_add_to_server():
    from ensembl.production.metadata.grpc.ensembl_metadata_pb2_grpc import add_EnsemblMetadataServicer_to_server

    return add_EnsemblMetadataServicer_to_server


@pytest.fixture(scope='module')
def grpc_servicer():
    from ensembl.production.metadata.grpc.servicer import EnsemblMetadataServicer

    return EnsemblMetadataServicer()


@pytest.fixture(scope='module')
def grpc_stub_cls(grpc_channel):
    from ensembl.production.metadata.grpc.ensembl_metadata_pb2_grpc import EnsemblMetadataStub

    return EnsemblMetadataStub


@pytest.fixture(scope='module')
def grpc_server(_grpc_server, grpc_addr):
    SERVICE_NAMES = (
        ensembl_metadata_pb2.DESCRIPTOR.services_by_name['EnsemblMetadata'].full_name,
        reflection.SERVICE_NAME
    )
    reflection.enable_server_reflection(SERVICE_NAMES, _grpc_server)
    # _grpc_server.ad(grpc_addr)
    _grpc_server.add_insecure_port(grpc_addr)
    _grpc_server.start()
    yield _grpc_server
    _grpc_server.stop(grace=None)


sample_path = Path(__file__).parent.parent / "ensembl" / "production" / "metadata" / "api" / "sample"

@pytest.mark.parametrize("multi_dbs", [[{"src": sample_path / "ensembl_metadata"},
                                        {"src": sample_path / "ncbi_taxonomy"}]],
                         indirect=True)
class TestGRPCReflection:

    def test_services_discovery(self, grpc_stub_cls, grpc_channel, grpc_servicer, multi_dbs):
        from grpc_reflection.v1alpha.proto_reflection_descriptor_database import ProtoReflectionDescriptorDatabase
        reflection_db = ProtoReflectionDescriptorDatabase(grpc_channel)
        services = reflection_db.get_services()
        desc_pool = DescriptorPool(reflection_db)
        metadata_service = desc_pool.FindServiceByName('ensembl_metadata.EnsemblMetadata')
        method_list = [func for func in dir(EnsemblMetadata) if
                       callable(getattr(EnsemblMetadata, func)) and not func.startswith("__")]
        for method_name in method_list:
            print(method_name)
            method_desc = metadata_service.FindMethodByName(method_name)
            assert isinstance(method_desc, MethodDescriptor)
        assert 'ensembl_metadata.EnsemblMetadata' in services
        assert 'grpc.reflection.v1alpha.ServerReflection' in services
