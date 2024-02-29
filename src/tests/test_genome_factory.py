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
from ensembl.production.metadata.api.factories.genome import GenomeFactory, GenomeInputFilter

from ensembl.production.metadata.api.models import (Dataset, DatasetSource, DatasetType, GenomeDataset, Genome)

db_directory = Path(__file__).parent / 'databases'
db_directory = db_directory.resolve()

sample_path = Path(__file__).parent.parent / "ensembl" / "production" / "metadata" / "api" / "sample"



@pytest.mark.parametrize("multi_dbs", [[{'src': sample_path / 'ensembl_genome_metadata'},
                                        {'src': sample_path / 'ncbi_taxonomy'},
                                        ]], indirect=True)
@pytest.fixture(scope="class")
def metadata_db(multi_dbs):
    return DBConnection(multi_dbs['ensembl_genome_metadata'].dbc.url)


@pytest.fixture(scope="class")
def genome_factory():
    return GenomeFactory()


@pytest.fixture(scope="class")
def genome_filters():
    return {
        'genome_uuid': ['a73351f7-93e7-11ec-a39d-005056b38ce3'],
        'dataset_uuid': ['02104faf-3fee-4f28-b53c-605843dac941'],
        'division': [],
        'dataset_type' : 'assembly',
        'species' : [],
        'antispecies': [],
        'dataset_status':  ["Submitted"],
        'batch_size': 50,
        'organism_group_type': "DIVISION",
    }


@pytest.fixture(scope="class")
def expected_columns():
    return ['genome_uuid',
            'production_name',
            'dataset_uuid',
            'dataset_status',
            'dataset_source',
            'dataset_type',
            ]


@pytest.mark.parametrize("multi_dbs", [[{'src': sample_path / 'ensembl_genome_metadata'},
                                        {'src': sample_path / 'ncbi_taxonomy'},
                                        ]], indirect=True)
class TestGenomeFactory:
    dbc = None  # type: UnitTestDB
    def test_fetch_genomes_by_genome_uuid(self, multi_dbs, metadata_db, genome_factory, genome_filters):

        #fetch genome using genomefacotry with default filters
        genome_factory_result = next(genome_factory.get_genomes(multi_dbs['ensembl_genome_metadata'].dbc.url, **genome_filters))
        with metadata_db.session_scope() as session:
            genome = session.query(Genome).filter(Genome.genome_uuid == genome_filters['genome_uuid']).one()
            assert genome_factory_result['genome_uuid'] == genome_filters['genome_uuid'][0]
            assert genome.genome_uuid == genome_filters['genome_uuid'][0]
            assert genome.genome_uuid == genome_factory_result['genome_uuid']
            assert genome.production_name == genome_factory_result['species']

    def test_fetch_genomes_by_dataset_uuid(self, multi_dbs, metadata_db, genome_factory, genome_filters):
        genome_filters['genome_uuid'] = []
        #fetch genome using genomefacotry with default filters
        genome_factory_result = next(genome_factory.get_genomes(multi_dbs['ensembl_genome_metadata'].dbc.url, **genome_filters))
        with metadata_db.session_scope() as session:
            dataset = session.query(Dataset).filter(Dataset.dataset_uuid == genome_filters['dataset_uuid']).one()

            assert genome_filters['genome_uuid'] == []
            assert genome_factory_result['dataset_uuid'] == genome_filters['dataset_uuid'][0]
            assert dataset.dataset_uuid == genome_filters['dataset_uuid'][0]

    def test_fetch_genomes_by_genome_uuid_with_default_status_submit(self, multi_dbs, metadata_db, genome_factory, genome_filters):
        #fetch genome using genomefacotry with default filters
        genome_filters['dataset_status'] = [] #set status to empty array
        genome_factory_result = next(genome_factory.get_genomes(multi_dbs['ensembl_genome_metadata'].dbc.url, **genome_filters))
        with metadata_db.session_scope() as session:
            dataset = session.query(Dataset).filter(Dataset.dataset_uuid == genome_filters['dataset_uuid']).one()
            assert dataset.status == genome_factory_result['dataset_status']

    def test_expected_columns(self, multi_dbs, genome_factory, genome_filters, expected_columns):
        #fetch genomes with default filters
        returned_columns = list(next(genome_factory.get_genomes(multi_dbs['ensembl_genome_metadata'].dbc.url, **genome_filters)).keys())
        assert returned_columns.sort() == expected_columns.sort()





    #
    #
    # def test_fetch_genomes_by_dataset_status(self, multi_dbs):
    #
    #     gf = GenomeFactory()
    #
    #     metadata_db = DBConnection(multi_dbs['ensembl_genome_metadata'].dbc.url)
    #     with metadata_db.session_scope() as session:
    #
    #         assert 1==2
    #
    # def test_fetch_genomes_by_dataset_type(self, multi_dbs):
    #
    #     gf = GenomeFactory()
    #
    #     metadata_db = DBConnection(multi_dbs['ensembl_genome_metadata'].dbc.url)
    #     with metadata_db.session_scope() as session:
    #
    #         assert 1==2
    #
    # def test_fetch_genomes_of_default_batchsize_50(self, multi_dbs):
    #
    #     gf = GenomeFactory()
    #
    #     metadata_db = DBConnection(multi_dbs['ensembl_genome_metadata'].dbc.url)
    #     with metadata_db.session_scope() as session:
    #
    #         assert 1==2
    #
    # def test_fetch_all_genomes(self, multi_dbs):
    #
    #     gf = GenomeFactory()
    #
    #     metadata_db = DBConnection(multi_dbs['ensembl_genome_metadata'].dbc.url)
    #     with metadata_db.session_scope() as session:
    #
    #         assert 1==2
    #
    #
    #
    # def test_update_dataset_status_processing(self, multi_dbs):
    #
    #     gf = GenomeFactory()
    #
    #     metadata_db = DBConnection(multi_dbs['ensembl_genome_metadata'].dbc.url)
    #     with metadata_db.session_scope() as session:
    #
    #         assert 1==2
    #
    #
    # def test_update_dataset_status_processed(self, multi_dbs):
    #
    #     gf = GenomeFactory()
    #
    #     metadata_db = DBConnection(multi_dbs['ensembl_genome_metadata'].dbc.url)
    #     with metadata_db.session_scope() as session:
    #
    #         assert 1==2
    #
    # def test_update_dataset_status_released(self, multi_dbs):
    #
    #     gf = GenomeFactory()
    #
    #     metadata_db = DBConnection(multi_dbs['ensembl_genome_metadata'].dbc.url)
    #     with metadata_db.session_scope() as session:
    #
    #         assert 1==2

