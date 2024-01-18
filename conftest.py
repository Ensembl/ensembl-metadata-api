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
import os
from pathlib import Path

from _pytest.config import Config
import pytest
import sqlalchemy as db

from ensembl.production.metadata.grpc.adaptors.genome import GenomeAdaptor
from ensembl.production.metadata.grpc.adaptors.release import ReleaseAdaptor

pytest_plugins = ("ensembl.plugins.pytest_unittest",)


def pytest_configure(config: Config) -> None:
    pytest.dbs_dir = Path(__file__).parent / 'src' / 'ensembl' / 'production' / 'metadata' / 'api' / 'sample'

@pytest.fixture(scope="class")
def engine(multi_dbs):
    os.environ["METADATA_URI"] = multi_dbs["ensembl_metadata"].dbc.url
    os.environ["TAXONOMY_URI"] = multi_dbs["ncbi_taxonomy"].dbc.url
    yield db.create_engine(multi_dbs["ensembl_metadata"].dbc.url)


@pytest.fixture(scope="class")
def genome_db_conn(multi_dbs):
    genome_conn = GenomeAdaptor(
        metadata_uri=multi_dbs["ensembl_metadata"].dbc.url,
        taxonomy_uri=multi_dbs["ncbi_taxonomy"].dbc.url
    )
    yield genome_conn


@pytest.fixture(scope="class")
def release_db_conn(multi_dbs):
    release_conn = ReleaseAdaptor(
        metadata_uri=multi_dbs["ensembl_metadata"].dbc.url
    )
    yield release_conn
