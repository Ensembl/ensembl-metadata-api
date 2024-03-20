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

import pytest
import pkg_resources
from pathlib import Path

from ensembl.database import UnitTestDB
from ensembl.production.metadata.grpc.adaptors.genome import GenomeAdaptor
from ensembl.production.metadata.grpc.adaptors.release import ReleaseAdaptor
import logging

sample_path = Path(__file__).parent.parent / "ensembl" / "production" / "metadata" / "api" / "sample"

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("multi_dbs", [[{"src": sample_path / "ensembl_genome_metadata"},
                                        {"src": sample_path / "ncbi_taxonomy"}]],
                         indirect=True)
class TestGRPCReleaseAdaptor:
    dbc: UnitTestDB = None

    def test_fetch_current_release(self, multi_dbs, release_db_conn_unreleased):
        releases = release_db_conn_unreleased.fetch_releases(current_only=True)
        logger.debug("Results: %s", releases)
        # test the one to many connection
        assert len(releases) == 1
        assert [release.EnsemblSite.name == 'Ensembl' for release in releases]
        # test the direct access.
        assert releases[0].EnsemblRelease.label == 'beta-1'

    def test_fetch_all_releases(self, multi_dbs, release_db_conn_unreleased):
        releases = release_db_conn_unreleased.fetch_releases()
        logger.debug("Results: %s", releases)
        # test the one to many connection
        assert len(releases) == 4
        assert [release.EnsemblSite.name == 'Ensembl' for release in releases]
        # test the direct access.
        assert releases[1].EnsemblRelease.label == 'Scaling Phase 1'

    # currently only have one release, so the testing is not comprehensive
    def test_fetch_releases_for_genome(self, multi_dbs, release_db_conn):
        releases = release_db_conn.fetch_releases_for_genome('84af1d3c-3b8a-477d-99d2-128dbfda2e13')
        assert len(releases) == 1
        assert releases[0].EnsemblSite.name == 'Ensembl'
        assert releases[0].EnsemblRelease.label == 'beta-1'

    def test_fetch_releases_for_unreleased_dataset(self, multi_dbs, release_db_conn_unreleased):
        releases = release_db_conn_unreleased.fetch_releases_for_dataset('1aed28f4-f7d3-4d68-905d-061010cc5d85')
        assert len(releases) == 1
        assert releases[0].EnsemblSite.name == 'Ensembl'
        assert releases[0].EnsemblRelease.is_current == False
        assert releases[0].EnsemblRelease.label == 'Scaling Phase 1'
