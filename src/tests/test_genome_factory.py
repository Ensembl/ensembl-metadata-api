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
from pathlib import Path

import pytest
from ensembl.database import UnitTestDB, DBConnection
from ensembl.production.metadata.api.factories.genomes import GenomeFactory, GenomeInputFilters
from sqlalchemy import func
from ensembl.production.metadata.api.models import Dataset, Genome, DatasetStatus


@pytest.mark.parametrize("multi_dbs", [[{'src': Path(__file__).parent / "databases/ensembl_genome_metadata"},
                                        {'src': Path(__file__).parent / "databases/ncbi_taxonomy"},
                                        ]], indirect=True)
@pytest.fixture(scope="function")
def genome_filters(multi_dbs):
    return {
        'genome_uuid': [],
        'dataset_uuid': [],
        'division': [],
        'dataset_type': 'genebuild',
        'species': [],
        'antispecies': [],
        'dataset_status': ["Submitted"],
        'batch_size': 50,
        'organism_group_type': "DIVISION",
        'metadata_db_uri': multi_dbs['ensembl_genome_metadata'].dbc.url
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


@pytest.mark.parametrize("multi_dbs", [[{'src': Path(__file__).parent / "databases/ensembl_genome_metadata"},
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
            (10, 'Submitted', 3),
            (40, 'Released', 10),
            (50, 'Processed', 10),
        ]
    )
    def test_fetch_genomes_by_default_params(self, genome_factory, genome_filters, status, batch_size, expected_count):
        # fetch genome using genome factory with default filters
        genome_filters['batch_size'] = batch_size
        genome_filters['dataset_status'] = [status]
        fetched_genome_factory_count = len([genome for genome in genome_factory.get_genomes(**genome_filters)])
        assert fetched_genome_factory_count == expected_count

    def test_fetch_genomes_by_genome_uuid(self, multi_dbs, genome_factory, genome_filters):
        # fetch genome using genome factory with default filters
        genome_filters['genome_uuid'] = ['a73351f7-93e7-11ec-a39d-005056b38ce3']
        genome_filters['dataset_status'] = ['Released']
        genome_factory_result = next(genome_factory.get_genomes(**genome_filters))
        metadata_db = DBConnection(multi_dbs['ensembl_genome_metadata'].dbc.url)

        with metadata_db.session_scope() as session:
            genome = session.query(Genome).filter(Genome.genome_uuid == genome_filters['genome_uuid']).one()
            assert genome_factory_result['genome_uuid'] == genome_filters['genome_uuid'][0]
            assert genome.genome_uuid == genome_filters['genome_uuid'][0]
            assert genome.genome_uuid == genome_factory_result['genome_uuid']
            assert genome.production_name == genome_factory_result['species']

    def test_fetch_genomes_by_dataset_uuid(self, multi_dbs, genome_factory, genome_filters):
        genome_filters['dataset_uuid'] = ['bd63a676-45ff-494a-b26f-2b779cb6c180']
        # fetch genome using genome factory with dataset uuid
        genome_factory_result = next(genome_factory.get_genomes(**genome_filters))
        metadata_db = DBConnection(multi_dbs['ensembl_genome_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            dataset = session.query(Dataset).filter(Dataset.dataset_uuid == genome_filters['dataset_uuid']).one()
            assert genome_factory_result['dataset_uuid'] == genome_filters['dataset_uuid'][0]
            assert dataset.dataset_uuid == genome_filters['dataset_uuid'][0]

    def test_fetch_genomes_by_default_status_submitted(self, multi_dbs, genome_factory, genome_filters):
        genome_filters['dataset_uuid'] = ['bd63a676-45ff-494a-b26f-2b779cb6c180']
        genome_filters['dataset_status'] = []
        # fetch genome using genome factory with dataset uuid
        genome_factory_result = next(genome_factory.get_genomes(**genome_filters))
        metadata_db = DBConnection(multi_dbs['ensembl_genome_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            dataset = session.query(Dataset).filter(Dataset.dataset_uuid == genome_filters['dataset_uuid']).one()
            assert genome_factory_result['dataset_uuid'] == genome_filters['dataset_uuid'][0]
            assert dataset.dataset_uuid == genome_filters['dataset_uuid'][0]
            assert dataset.status == genome_factory_result['dataset_status']

    def test_update_dataset_status_submitted_processing_processed_released(self, multi_dbs, genome_factory,
                                                                           genome_filters):
        # fetch genome using genome factory with dataset uuid
        genome_filters['genome_uuid'] = []
        genome_filters['dataset_uuid'] = ['bd63a676-45ff-494a-b26f-2b779cb6c180']

        # update dataset status to processing
        genome_filters['update_dataset_status'] = DatasetStatus.PROCESSING  # 'Processing'

        # fetch genomes by status submitted and update to processing
        genome_factory_result = [genome for genome in genome_factory.get_genomes(**genome_filters)][0]
        metadata_db = DBConnection(multi_dbs['ensembl_genome_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            dataset = session.query(Dataset).filter(Dataset.dataset_uuid == genome_filters['dataset_uuid']).one()
            assert genome_factory_result['updated_dataset_status'] == dataset.status

        # update dataset status to processed
        genome_filters['update_dataset_status'] = DatasetStatus.PROCESSED  # 'Processed'
        genome_filters['dataset_status'] = [DatasetStatus.PROCESSING.value]  # 'Processing']

        # fetch genomes by status processing and update to processed
        genome_factory_result = [genome for genome in genome_factory.get_genomes(**genome_filters)][0]

        with metadata_db.session_scope() as session:
            dataset = session.query(Dataset).filter(Dataset.dataset_uuid == genome_filters['dataset_uuid']).one()
            assert genome_factory_result['updated_dataset_status'] == dataset.status

        # update dataset status to processed
        genome_filters['update_dataset_status'] = DatasetStatus.RELEASED  # 'Released'
        genome_filters['dataset_status'] = [DatasetStatus.PROCESSED.value]  # 'Processed']

        # fetch genomes by status processed and update to released
        genome_factory_result = [genome for genome in genome_factory.get_genomes(**genome_filters)][0]

        with metadata_db.session_scope() as session:
            dataset = session.query(Dataset).filter(Dataset.dataset_uuid == genome_filters['dataset_uuid']).one()
            assert genome_factory_result['updated_dataset_status'] == dataset.status

    def test_expected_columns(self, genome_factory, genome_filters, expected_columns):
        # fetch genomes with default filters
        genome_filters['dataset_uuid'] = ['f32b7f9a-97fd-41cd-86be-a5fb5becd335']
        genome_filters['dataset_type'] = 'homologies'

        returned_columns = list(next(genome_factory.get_genomes(**genome_filters)).keys())
        assert returned_columns.sort() == expected_columns.sort()

    def test_expected_columns_on_update_status(self, genome_factory, expected_columns, genome_filters):
        genome_filters['dataset_uuid'] = ['f32b7f9a-97fd-41cd-86be-a5fb5becd335']
        genome_filters['dataset_type'] = 'homologies'
        genome_filters['update_dataset_status'] = DatasetStatus.PROCESSING  # 'Processing'
        expected_columns.append('updated_dataset_status')
        returned_columns = list(next(genome_factory.get_genomes(**genome_filters)).keys())
        assert returned_columns.sort() == expected_columns.sort()
