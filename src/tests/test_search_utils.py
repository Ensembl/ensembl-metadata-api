# See the NOTICE file distributed with this work for additional information
# regarding copyright ownership.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pathlib import Path

import pytest
from ensembl.utils.database import DBConnection

from ensembl.production.metadata.api.models import ReleaseStatus
from ensembl.production.metadata.api.search.utils import get_all_live_genomes, get_all_live_genomes_count

db_directory = Path(__file__).parent / "databases"
db_directory = db_directory.resolve()


@pytest.mark.parametrize(
    "test_dbs",
    [[{"src": db_directory / "ensembl_genome_metadata"}, {"src": db_directory / "ncbi_taxonomy"}]],
    indirect=True,
)
class TestGetAllLiveGenomes:
    """Test suite for get_all_live_genomes and get_all_live_genomes_count."""

    def test_count_correct(self, test_dbs):
        """Test that count and is correct."""
        meta_uri = test_dbs.get('ensembl_genome_metadata')
        with DBConnection(meta_uri.dbc.url).session_scope() as session:
            assert get_all_live_genomes_count(session) == 10

    def test_count_matches_genome_list_length(self, test_dbs):
        """Test that count and list return consistent results."""
        meta_uri = test_dbs.get('ensembl_genome_metadata')
        with DBConnection(meta_uri.dbc.url).session_scope() as session:
            assert get_all_live_genomes_count(session) == len(get_all_live_genomes(session))

    def test_no_duplicate_genome_uuids(self, test_dbs):
        """Test that no genome appears more than once in the results."""
        meta_uri = test_dbs.get('ensembl_genome_metadata')
        with DBConnection(meta_uri.dbc.url).session_scope() as session:
            uuids = [g.genome_uuid for g in get_all_live_genomes(session)]
            assert len(uuids) == len(set(uuids))

    def test_all_genomes_have_released_status(self, test_dbs):
        """Test that every returned genome is linked to at least one Released release."""
        meta_uri = test_dbs.get('ensembl_genome_metadata')
        with DBConnection(meta_uri.dbc.url).session_scope() as session:
            for genome in get_all_live_genomes(session):
                statuses = {gr.ensembl_release.status for gr in genome.genome_releases}
                assert ReleaseStatus.RELEASED in statuses
