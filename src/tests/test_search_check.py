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
from pathlib import Path

import pytest
from ensembl.utils.database import DBConnection

from ensembl.production.metadata.api.search.check import check_search_index, get_search_genome_uuids
from ensembl.production.metadata.api.search.utils import get_all_live_genomes

db_directory = (Path(__file__).parent / "databases").resolve()

pytestmark = pytest.mark.parametrize(
    "test_dbs",
    [[{"src": db_directory / "ensembl_genome_metadata"}, {"src": db_directory / "ncbi_taxonomy"}]],
    indirect=True,
)


def test_get_search_genome_uuids_reads_generated_index_structure():
    search_index = {
        "entries": [
            {"fields": [{"name": "id", "value": "first"}, {"name": "genome_uuid", "value": "uuid-1"}]},
            {"fields": [{"name": "genome_uuid", "value": "uuid-2"}]},
        ]
    }

    assert get_search_genome_uuids(search_index) == {"uuid-1", "uuid-2"}


def test_get_search_genome_uuids_rejects_invalid_structure():
    with pytest.raises(ValueError, match="entries"):
        get_search_genome_uuids({})


def test_check_search_index_reports_both_directions(test_dbs, tmp_path):
    metadata_uri = test_dbs["ensembl_genome_metadata"].dbc.url
    with DBConnection(metadata_uri).session_scope() as session:
        live_genome_uuids = sorted(genome.genome_uuid for genome in get_all_live_genomes(session))

    missing_genome = live_genome_uuids.pop()
    search_index = {
        "entries": [
            {"fields": [{"name": "genome_uuid", "value": genome_uuid}]} for genome_uuid in live_genome_uuids
        ]
        + [{"fields": [{"name": "genome_uuid", "value": "not-a-live-genome"}]}]
    }
    search_json_path = tmp_path / "search.json"
    search_json_path.write_text(json.dumps(search_index))

    assert check_search_index(metadata_uri, search_json_path) == {
        "missing_from_search_index": [missing_genome],
        "not_live_in_metadata": ["not-a-live-genome"],
    }
