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
"""
Unit tests for api module
"""

from pathlib import Path

import pytest
from ensembl.utils.database import UnitTestDB

from ensembl.production.metadata.api.adaptors.genome import *


@pytest.mark.parametrize("test_dbs", [[{"src": Path(__file__).parent / "databases/ensembl_genome_metadata"},
                                        {"src": Path(__file__).parent / "databases/ncbi_taxonomy"}]], indirect=True)
class TestApi:
    dbc = None  # type: UnitTestDB

    # Basic test to show full funtionality getting the most recent result for everything.
    def test_get_public_path(self, test_dbs):
        genome_adapter = GenomeAdaptor(test_dbs['ensembl_genome_metadata'].dbc.url, test_dbs['ncbi_taxonomy'].dbc.url)
        genome_uuid = 'a733574a-93e7-11ec-a39d-005056b38ce3'
        paths = genome_adapter.get_public_path(genome_uuid, dataset_type='all')
        assert len(paths) == 4
        # assert all("/genebuild/" in path for path in paths)
        path = genome_adapter.get_public_path(genome_uuid, dataset_type='genebuild')
        assert path[0]['path'] == 'GCA/000/146/045/2/community/2018_10/geneset'
        path = genome_adapter.get_public_path(genome_uuid, dataset_type='assembly')
        assert path[0]['path'] == 'GCA/000/146/045/2/genome'
        path = genome_adapter.get_public_path(genome_uuid, dataset_type='variation')
        assert path[0]['path'] == 'GCA/000/146/045/2/community/2018_10/variation/2023_06_15'
        path = genome_adapter.get_public_path(genome_uuid, dataset_type='homologies')
        assert path[0]['path'] == 'GCA/000/146/045/2/community/2018_10/homology/2023_06_15'

    # specific release:
    def test_public_path_release(self, test_dbs):
        genome_adapter = GenomeAdaptor(test_dbs['ensembl_genome_metadata'].dbc.url, test_dbs['ncbi_taxonomy'].dbc.url)
        genome_uuid = 'a733574a-93e7-11ec-a39d-005056b38ce3'
        paths = genome_adapter.get_public_path(genome_uuid, dataset_type='all', release='2021-10-18')
        assert len(paths) == 3
        # assert all("/genebuild/" in path for path in paths)
        path = genome_adapter.get_public_path(genome_uuid, dataset_type='genebuild', release='2021-10-18')
        assert path[0]['path'] == 'GCA/000/146/045/2/community/2018_10/geneset'
        path = genome_adapter.get_public_path(genome_uuid, dataset_type='assembly', release='2021-10-18')
        assert path[0]['path'] == 'GCA/000/146/045/2/genome'
        path = genome_adapter.get_public_path(genome_uuid, dataset_type='variation', release='2021-10-18')
        assert path[0]['path'] == 'GCA/000/146/045/2/community/2018_10/variation/2020_10_18'
        with pytest.raises(TypeNotFoundException):
            path = genome_adapter.get_public_path(genome_uuid, dataset_type='homologies', release='2021-10-18')

    # Basic test to show full funtionality for a genome with limited datasets.
    def test_get_public_path_limited(self, test_dbs):
        genome_adapter = GenomeAdaptor(test_dbs['ensembl_genome_metadata'].dbc.url, test_dbs['ncbi_taxonomy'].dbc.url)
        genome_uuid = 'a733550b-93e7-11ec-a39d-005056b38ce3'
        # c elegans genome_id=203
        paths = genome_adapter.get_public_path(genome_uuid, dataset_type='all')
        assert len(paths) == 3
        path = genome_adapter.get_public_path(genome_uuid, dataset_type='genebuild')
        assert path[0]['path'] == 'GCA/000/002/985/3/wormbase/2014_10/geneset'
        path = genome_adapter.get_public_path(genome_uuid, dataset_type='assembly')
        assert path[0]['path'] == 'GCA/000/002/985/3/genome'
        path = genome_adapter.get_public_path(genome_uuid, dataset_type='homologies')
        assert path[0]['path'] == 'GCA/000/002/985/3/wormbase/2014_10/homology/2023_06_15'
        with pytest.raises(TypeNotFoundException):
            genome_adapter.get_public_path(genome_uuid, dataset_type='variation')
