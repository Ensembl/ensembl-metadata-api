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
from ensembl.utils.database import UnitTestDB, DBConnection

from ensembl.production.metadata.api.exceptions import DatasetFactoryException
from ensembl.production.metadata.api.factories.genomes import GenomeInputFilters
from ensembl.production.metadata.api.models import Dataset, Genome, DatasetStatus

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def genome_filters(test_dbs):
    return {
        'genome_uuid': [],
        'dataset_uuid': [],
        'division': [],
        'dataset_type': '',
        'species': [],
        'antispecies': [],
        'dataset_status': ["Submitted"],
        'batch_size': 50,
        'organism_group_type': "",
        'metadata_db_uri': test_dbs['ensembl_genome_metadata'].dbc.url
    }


@pytest.fixture(scope="function")
def expected_columns():
    return ['genome_uuid',
            'production_name',
            'dataset_uuid',
            'dataset_status',
            'dataset_source',
            'dataset_type',
            ]


@pytest.mark.parametrize("test_dbs", [[{'src': Path(__file__).parent / "databases/ensembl_genome_metadata"},
                                        {'src': Path(__file__).parent / "databases/ncbi_taxonomy"},
                                        ]], indirect=True)
class TestGenomeFactory:
    dbc: UnitTestDB = None

    def test_input_filters_type(self, genome_filters):
        filters = GenomeInputFilters(**genome_filters)
        assert isinstance(filters.genome_uuid, list)
        assert isinstance(filters.dataset_uuid, list)
        assert isinstance(filters.division, list)
        assert isinstance(filters.dataset_type, str)
        assert isinstance(filters.species, list)
        assert isinstance(filters.antispecies, list)
        assert isinstance(filters.dataset_status, list)
        assert isinstance(filters.batch_size, int)
        assert isinstance(filters.organism_group_type, str)
        assert isinstance(filters.update_dataset_status, str)

    @pytest.mark.parametrize(
        "batch_size, status, expected_count",
        [
            (10, 'Submitted', 2),
            (40, 'Released', 10),
            (50, 'Processed', 8),
        ]
    )
    def test_fetch_genomes_by_default_params(self, genome_factory, genome_filters, status, batch_size, expected_count):
        # fetch genome using genome factory with default filters
        genome_filters['batch_size'] = batch_size
        genome_filters['dataset_status'] = [status]
        genome_filters['dataset_type'] = 'genebuild'
        fetched_genome_factory_count = len([genome for genome in genome_factory.get_genomes(**genome_filters)])
        assert fetched_genome_factory_count == expected_count

    def test_fetch_genomes_by_genome_uuid(self, test_dbs, genome_factory, genome_filters):
        # fetch genome using genome factory with default filters
        genome_filters['genome_uuid'] = ['a73351f7-93e7-11ec-a39d-005056b38ce3']
        genome_filters['dataset_status'] = ['Released']
        genome_factory_result = next(genome_factory.get_genomes(**genome_filters))
        metadata_db = DBConnection(test_dbs['ensembl_genome_metadata'].dbc.url)

        with metadata_db.session_scope() as session:
            genome = session.query(Genome).filter(Genome.genome_uuid == genome_filters['genome_uuid'][0]).one()
            assert genome_factory_result['genome_uuid'] == genome_filters['genome_uuid'][0]
            assert genome.genome_uuid == genome_filters['genome_uuid'][0]
            assert genome.genome_uuid == genome_factory_result['genome_uuid']
            assert genome.production_name == genome_factory_result['species']

    def test_fetch_genomes_by_dataset_uuid(self, test_dbs, genome_factory, genome_filters):
        # TODO from the example in `.test_get_genome_by_uuid`, we could
        #   - add fixtures parameters for released/unreleased
        #   - check multiple genomes_uuid
        genome_filters['dataset_uuid'] = ['11a0be7f-99ae-45d3-a004-dc19bb562330']
        genome_filters['dataset_status'] = ['Submitted', 'Processed']
        # fetch genome using genome factory with dataset uuid
        genome_factory_result = next(genome_factory.get_genomes(**genome_filters), None)
        assert genome_factory_result is not None
        metadata_db = DBConnection(test_dbs['ensembl_genome_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            dataset = session.query(Dataset).filter(Dataset.dataset_uuid == genome_filters['dataset_uuid'][0]).one()
            assert genome_factory_result['dataset_uuid'] == genome_filters['dataset_uuid'][0]
            assert dataset.dataset_uuid == genome_filters['dataset_uuid'][0]

    def test_fetch_genomes_by_default_status_submitted(self, test_dbs, genome_factory, genome_filters):
        genome_filters['dataset_uuid'] = ['bd63a676-45ff-494a-b26f-2b779cb6c180']
        genome_filters['dataset_status'] = []
        # fetch genome using genome factory with dataset uuid
        genome_factory_result = next(genome_factory.get_genomes(**genome_filters))
        metadata_db = DBConnection(test_dbs['ensembl_genome_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            dataset: Dataset = session.query(Dataset).filter(
                Dataset.dataset_uuid == genome_filters['dataset_uuid'][0]).one()
            assert genome_factory_result['dataset_uuid'] == genome_filters['dataset_uuid'][0]
            assert dataset.dataset_uuid == genome_filters['dataset_uuid'][0]
            assert dataset.status.value == genome_factory_result['dataset_status']

    def test_update_dataset_status_submitted_processing_processed_released(self, test_dbs, genome_factory,
                                                                           genome_filters):
        # fetch genome using genome factory with dataset uuid

        genebuild_uuid = '2ef7c056-847e-4742-a68b-18c3ece068aa'
        leaf_uuid = '7bb8919c-d9e0-4eca-9a49-7a6d9e311c8d'
        genome_filters['genome_uuid'] = []
        genome_filters['dataset_uuid'] = [leaf_uuid]

        # update dataset status to processing
        genome_filters['update_dataset_status'] = 'Processing'

        # fetch genomes by status submitted and update to processing
        genome_factory_result = [genome for genome in genome_factory.get_genomes(**genome_filters)][0]
        # logger.debug(f"Factory Results 1 {genome_factory_result}")
        metadata_db = DBConnection(test_dbs['ensembl_genome_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            # check genebuild one has been updated to Processing as well
            dataset: Dataset = session.query(Dataset).filter(Dataset.dataset_uuid == genebuild_uuid).one()
            # logger.debug(f"Dataset 1 {dataset}")
            assert genome_factory_result['updated_dataset_status'] == dataset.status.value
            dataset: Dataset = session.query(Dataset).filter(Dataset.dataset_uuid == leaf_uuid).one()
            # logger.debug(f"Dataset 1 {dataset}")
            assert genome_factory_result['updated_dataset_status'] == dataset.status.value

        # update dataset status to processed
        genome_filters['update_dataset_status'] = DatasetStatus.PROCESSED.value  # 'Processed'
        genome_filters['dataset_status'] = [DatasetStatus.PROCESSING.value]  # 'Processing']

        # fetch genomes by status processing and update to processed
        genome_factory_result = [genome for genome in genome_factory.get_genomes(**genome_filters)][0]
        # logger.debug(f"Factory Results 2 {genome_factory_result}")
        with metadata_db.session_scope() as session:
            dataset = session.query(Dataset).filter(Dataset.dataset_uuid == genebuild_uuid).one()
            # logger.debug(f"Dataset 2 {dataset}")
            assert 'Processing' == dataset.status.value
            dataset = session.query(Dataset).filter(Dataset.dataset_uuid == leaf_uuid).one()
            # logger.debug(f"Dataset 2b {dataset}")
            assert genome_factory_result['updated_dataset_status'] == dataset.status.value

        # update dataset status to processed
        genome_filters['update_dataset_status'] = DatasetStatus.RELEASED.value  # 'Released'
        genome_filters['dataset_status'] = [DatasetStatus.PROCESSED.value]  # 'Processed']

        # fetch genomes by status processed and update to released
        with pytest.raises(DatasetFactoryException):
            genome_factory_result = [genome for genome in genome_factory.get_genomes(**genome_filters)][0]
        # logger.debug(f"Factory Results 3 {genome_factory_result}")
        # assert nothing happened in DB
        with metadata_db.session_scope() as session:
            dataset = session.query(Dataset).filter(Dataset.dataset_uuid == leaf_uuid).one()
            # logger.debug(f"Dataset 3 {dataset}")
            assert 'Processed' == dataset.status.value
        # TODO complete the test with all sub datasets updated to processed before moving leaf to
        #  release then asses that genebuild is now released

    def test_expected_columns(self, genome_factory, genome_filters, expected_columns):
        # fetch genomes with default filters
        genome_filters['dataset_uuid'] = ['f32b7f9a-97fd-41cd-86be-a5fb5becd335']
        genome_filters['dataset_type'] = 'homologies'
        genome_filters['dataset_status'] = ['Processed']

        returned_columns = list(next(genome_factory.get_genomes(**genome_filters)).keys())
        assert returned_columns.sort() == expected_columns.sort()

    def test_expected_columns_on_update_status(self, genome_factory, expected_columns, genome_filters):
        genome_filters['dataset_uuid'] = ['f2734f34-36a0-4594-871d-f7f6d317d05a']
        genome_filters['dataset_type'] = 'homologies'
        genome_filters['update_dataset_status'] = DatasetStatus.PROCESSING.value
        expected_columns.append('updated_dataset_status')
        returned_columns = list(next(genome_factory.get_genomes(**genome_filters)).keys())
        assert returned_columns.sort() == expected_columns.sort()

