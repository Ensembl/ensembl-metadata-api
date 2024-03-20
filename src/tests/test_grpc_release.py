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

import logging
from pathlib import Path

import pytest
from ensembl.database import UnitTestDB

from ensembl.production.metadata.api.models import ReleaseStatus

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("multi_dbs", [[{"src": Path(__file__).parent / "databases/ensembl_genome_metadata"},
                                        {"src": Path(__file__).parent / "databases/ncbi_taxonomy"}]],
                         indirect=True)
class TestGRPCReleaseAdaptor:
    dbc: UnitTestDB = None

    def test_fetch_current_release(self, multi_dbs, release_conn):
        releases = release_conn.fetch_releases(current_only=True)
        logger.debug("Results: %s", releases)
        # test the one to many connection
        assert len(releases) == 1
        assert [release.EnsemblSite.name == 'Ensembl' for release in releases]
        # test the direct access.
        assert releases[0].EnsemblRelease.label == 'MVP Beta-1'

    def test_fetch_all_releases(self, multi_dbs, allow_unreleased, release_conn):
        releases = release_conn.fetch_releases()
        logger.debug("Results: %s", releases)
        assert len(releases) == 5
        assert [release.EnsemblSite.name == 'Ensembl' for release in releases]
        assert releases[1].EnsemblRelease.label == 'MVP Beta-1'

    @pytest.mark.parametrize(
        "genome_uuid, release_name",
        [('a73351f7-93e7-11ec-a39d-005056b38ce3', 'MVP Beta-1')]
    )
    def test_fetch_releases_for_genome(self, release_conn, genome_uuid, release_name):
        releases = release_conn.fetch_releases_for_genome(genome_uuid)
        assert len(releases) == 1
        assert releases[0].EnsemblSite.name == 'Ensembl'
        assert releases[0].EnsemblRelease.label == release_name

    @pytest.mark.parametrize(
        "dataset_uuid, release_name, release_status",
        [('9681f4c2-afb4-4a08-8e4d-f26363f65ddf', None, None),
         ('d57040b6-0ef5-4e6b-97ef-be0ad94d3a61', 'MVP Beta-2', 'Processed')]
    )
    def test_fetch_releases_for_unreleased_dataset(self, release_conn, allow_unreleased, dataset_uuid, release_name,
                                                   release_status):
        releases = release_conn.fetch_releases_for_dataset(dataset_uuid)
        assert len(releases) == 1
        if release_name is not None:
            assert releases[0].EnsemblSite.name == 'Ensembl'
            assert bool(releases[0].EnsemblRelease.is_current) is False
            assert releases[0].EnsemblRelease.label == release_name
            assert releases[0].EnsemblRelease.status == ReleaseStatus(release_status)
