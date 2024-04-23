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

from ensembl.production.metadata.api.factories.genomes import GenomeFactory
from ensembl.production.metadata.api.factories.release import ReleaseFactory
from ensembl.production.metadata.api.models import EnsemblRelease, ReleaseStatus, DatasetStatus, Dataset, GenomeDataset

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("multi_dbs", [[{'src': Path(__file__).parent / "databases/ensembl_genome_metadata"},
                                        {'src': Path(__file__).parent / "databases/ncbi_taxonomy"},
                                        ]], indirect=True)
class TestReleaseFactory:
    dbc: UnitTestDB = None
    gen_factory = GenomeFactory()

    def test_release_prepare(self, multi_dbs):
        # fetch genome using genome factory with default filters
        metadata_db = DBConnection(multi_dbs['ensembl_genome_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            release = session.query(EnsemblRelease).filter(EnsemblRelease.version == 110.3).one()
            assert release.genome_releases == []
            genomes = self.gen_factory.get_genomes(
                metadata_db_uri=multi_dbs['ensembl_genome_metadata'].dbc.url,
                update_dataset_status=None,
                dataset_topic=['genebuild_annotation', 'variation_annotation', 'compara_annotation',
                               'regulation_annotation'],
                dataset_type=None,  # get all of them
                dataset_status=[DatasetStatus.PROCESSED.value, ],
                organism_group_type=None,
                dataset_unreleased=True,
                root_dataset=True,  # only fetch first level datasets
                batch_size=1000  # set to highest to make sure we get all of them.
            )
            genomes = list(genomes)
            datasets_uuids = list(dict.fromkeys([genome['dataset_uuid'] for genome in genomes]))
            genomes_uuids = list(dict.fromkeys([genome['genome_uuid'] for genome in genomes]))
            factory = ReleaseFactory(multi_dbs['ensembl_genome_metadata'].dbc.url)
            release_genomes = factory.prepare(release=release)
            session.flush()
            assert release.status == ReleaseStatus.PREPARING
            logger.debug(f"Genome uuids {datasets_uuids}")
            assert len(release_genomes) == len(genomes_uuids)
            datasets = session.query(GenomeDataset).join(Dataset.genome_datasets).filter(
                Dataset.dataset_uuid.in_(datasets_uuids))
            assert [dataset.release_id == release.release_id for dataset in datasets]
