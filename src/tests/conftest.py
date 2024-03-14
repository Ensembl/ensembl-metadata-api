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

from ensembl.production.metadata.api.factories.datasets import DatasetFactory
from ensembl.production.metadata.grpc.adaptors.genome import GenomeAdaptor
from ensembl.production.metadata.grpc.adaptors.release import ReleaseAdaptor

from ensembl.production.metadata.grpc import ensembl_metadata_pb2
from grpc_reflection.v1alpha import reflection


def pytest_configure(config: Config) -> None:
    pytest.dbs_dir = Path(__file__).parent / 'src' / 'ensembl' / 'production' / 'metadata' / 'api' / 'sample'


@pytest.fixture(scope="module", autouse=True)
def engine(multi_dbs):
    os.environ["METADATA_URI"] = multi_dbs["ensembl_genome_metadata"].dbc.url
    os.environ["TAXONOMY_URI"] = multi_dbs["ncbi_taxonomy"].dbc.url
    yield db.create_engine(multi_dbs["ensembl_genome_metadata"].dbc.url)


@pytest.fixture(scope="function")
def genome_db_conn(multi_dbs):
    genome_conn = GenomeAdaptor(
        metadata_uri=multi_dbs["ensembl_genome_metadata"].dbc.url,
        taxonomy_uri=multi_dbs["ncbi_taxonomy"].dbc.url
    )
    yield genome_conn


@pytest.fixture(scope="function")
def genome_db_conn_unreleased(multi_dbs):
    os.environ["ALLOW_UNRELEASED"] = 'True'
    genome_conn = GenomeAdaptor(
        metadata_uri=multi_dbs["ensembl_genome_metadata"].dbc.url,
        taxonomy_uri=multi_dbs["ncbi_taxonomy"].dbc.url
    )
    yield genome_conn
    os.environ["ALLOW_UNRELEASED"] = 'False'


@pytest.fixture(scope="class")
def release_db_conn(multi_dbs):
    release_conn = ReleaseAdaptor(
        metadata_uri=multi_dbs["ensembl_genome_metadata"].dbc.url
    )
    yield release_conn


@pytest.fixture(scope="class")
def dataset_factory(multi_dbs):
    yield DatasetFactory(multi_dbs["ensembl_genome_metadata"].dbc.url)


@pytest.fixture(scope='module')
def grpc_add_to_server():
    from ensembl.production.metadata.grpc.ensembl_metadata_pb2_grpc import add_EnsemblMetadataServicer_to_server

    return add_EnsemblMetadataServicer_to_server


@pytest.fixture(scope='module')
def grpc_servicer(multi_dbs, engine):
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
