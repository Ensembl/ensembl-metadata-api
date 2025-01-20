# See the NOTICE file distributed with this work for additional information
# regarding copyright ownership.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" Necessary fixtures for our GRPC API tests """
import os
from pathlib import Path

import pytest
import sqlalchemy as db
from _pytest.config import Config
from grpc_reflection.v1alpha import reflection

from ensembl.production.metadata.api.adaptors.vep import VepAdaptor
from ensembl.production.metadata.api.factories.datasets import DatasetFactory
from ensembl.production.metadata.api.factories.genomes import GenomeFactory
from ensembl.production.metadata.grpc import ensembl_metadata_pb2
from ensembl.production.metadata.api.adaptors import GenomeAdaptor
from ensembl.production.metadata.api.adaptors import ReleaseAdaptor


def pytest_configure(config: Config) -> None:
    pytest.dbs_dir = Path(__file__).parent / 'databases'


@pytest.fixture(scope="module", autouse=True)
def engine(test_dbs):
    os.environ["METADATA_URI"] = test_dbs["ensembl_genome_metadata"].dbc.url
    os.environ["TAXONOMY_URI"] = test_dbs["ncbi_taxonomy"].dbc.url
    yield db.create_engine(test_dbs["ensembl_genome_metadata"].dbc.url)


@pytest.fixture(scope="function")
def genome_conn(test_dbs):
    genome_conn = GenomeAdaptor(
        metadata_uri=test_dbs["ensembl_genome_metadata"].dbc.url,
        taxonomy_uri=test_dbs["ncbi_taxonomy"].dbc.url
    )
    yield genome_conn

@pytest.fixture(scope="function")
def vep_conn(test_dbs):
    vep_conn = VepAdaptor(
        metadata_uri=test_dbs["ensembl_genome_metadata"].dbc.url,
        file="all"
    )
    yield vep_conn


@pytest.fixture(scope="function")
def allow_unreleased(request):
    """Set ALLOWED_UNRELEASED environment variable, this fixture must be used with `parametrize`"""
    from ensembl.production.metadata.grpc.config import cfg
    cfg.allow_unreleased = request.param
    yield cfg


@pytest.fixture(scope="class")
def release_conn(test_dbs):
    release_conn = ReleaseAdaptor(
        metadata_uri=test_dbs["ensembl_genome_metadata"].dbc.url
    )
    yield release_conn


@pytest.fixture(scope="class")
def genome_factory():
    return GenomeFactory()


@pytest.fixture(scope="class")
def dataset_factory(test_dbs):
    yield DatasetFactory(test_dbs["ensembl_genome_metadata"].dbc.url)


@pytest.fixture(scope='module')
def grpc_add_to_server():
    from ensembl.production.metadata.grpc.ensembl_metadata_pb2_grpc import add_EnsemblMetadataServicer_to_server

    return add_EnsemblMetadataServicer_to_server


@pytest.fixture(scope='module')
def grpc_servicer(test_dbs, engine):
    from ensembl.production.metadata.grpc.servicer import EnsemblMetadataServicer
    return EnsemblMetadataServicer()


@pytest.fixture(scope='module')
def grpc_server(_grpc_server, grpc_addr, grpc_add_to_server, grpc_servicer):
    grpc_add_to_server(grpc_servicer, _grpc_server)
    SERVICE_NAMES = (
        ensembl_metadata_pb2.DESCRIPTOR.services_by_name['EnsemblMetadata'].full_name,
        reflection.SERVICE_NAME
    )
    reflection.enable_server_reflection(SERVICE_NAMES, _grpc_server)
    _grpc_server.add_insecure_port(grpc_addr)
    _grpc_server.start()
    yield _grpc_server
    _grpc_server.stop(grace=None)
