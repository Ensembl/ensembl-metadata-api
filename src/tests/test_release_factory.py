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
import logging
from pathlib import Path

import pytest
from ensembl.database import UnitTestDB, DBConnection

from ensembl.production.metadata.api.exceptions import ReleaseDataException
from ensembl.production.metadata.api.factories.genomes import GenomeFactory
from ensembl.production.metadata.api.factories.release import ReleaseFactory
from ensembl.production.metadata.api.models import *

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("multi_dbs", [[{'src': Path(__file__).parent / "databases/ensembl_genome_metadata"},
                                        {'src': Path(__file__).parent / "databases/ncbi_taxonomy"},
                                        ]], indirect=True)
class TestReleaseFactory:
    dbc: UnitTestDB = None
    gen_factory = GenomeFactory()

    def test_reset(self, multi_dbs):
        """ Just reload current txt test file set"""
        assert True

    def test_released_data_consistency(self, multi_dbs):
        """ Test current test datasets are consistent """
        metadata_db = DBConnection(multi_dbs['ensembl_genome_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            releases = session.query(EnsemblRelease).order_by(EnsemblRelease.version).all()
            factory = ReleaseFactory(multi_dbs['ensembl_genome_metadata'].dbc.url)
            [factory.check_release(rel) for rel in releases]

    def test_release_prepare(self, multi_dbs) -> None:
        """
        Move a Release from planed to preparing
        Args:
            multi_dbs: related db connection information
        Returns:
            None
        """
        # fetch genome using genome factory with default filters
        metadata_db = DBConnection(multi_dbs['ensembl_genome_metadata'].dbc.url)
        release_id = None
        factory = ReleaseFactory(multi_dbs['ensembl_genome_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            release_112 = session.query(EnsemblRelease).filter(EnsemblRelease.version == 112.0).one()
            release_id = release_112.release_id
            assert len(release_112.genome_releases) == 3
            release_genomes = factory.prepare(release=release_112)
            assert release_112.status == ReleaseStatus.PREPARING
            logger.debug(f"Genome uuids {release_genomes}")
        # Check that everything is fine
        with metadata_db.session_scope() as session:
            release_112 = session.query(EnsemblRelease).filter(EnsemblRelease.release_id == release_id).one()
            assert len(release_112.genome_releases) == len(release_genomes)
            assert len(release_112.genome_releases) == 2
            assert factory.check_release(release_112) == True
            added_release = session.query(EnsemblRelease).filter(EnsemblRelease.version > release_112.version).one()
            assert float(added_release.version) == float(release_112.version) + float(0.1)
            assert len(added_release.genome_releases) == 1

    def test_release_prepared(self, multi_dbs) -> None:
        """
        Test case for Release status moving from Preparing to Prepared
        Args:
            multi_dbs: related db connection information

        Returns:
            None
        """
        metadata_db = DBConnection(multi_dbs['ensembl_genome_metadata'].dbc.url)
        factory = ReleaseFactory(multi_dbs['ensembl_genome_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            release_110 = session.query(EnsemblRelease).filter(EnsemblRelease.version == 110.3).one()
            genomes = release_110.genome_releases
            release_genomes = factory.prepared(release=release_110)
            session.flush()
            assert len(genomes) == len(release_genomes)
            assert release_110.status == ReleaseStatus.PREPARED

    def test_release_release(self, multi_dbs) -> None:
        """
        Test case for Release status moving from Preparing to Prepared
        Args:
            multi_dbs: related db connection information

        Returns:
            None
        """
        pass

    def test_prepared_release(self, multi_dbs) -> None:
        metadata_db = DBConnection(multi_dbs['ensembl_genome_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            releases = session.query(EnsemblRelease).all()
            factory = ReleaseFactory(multi_dbs['ensembl_genome_metadata'].dbc.url)
            try:
                for rel in releases:
                    factory.check_release(rel)
            except ReleaseDataException:
                pytest.fail("All Releases should be ok")