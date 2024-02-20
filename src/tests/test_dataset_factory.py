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
from ensembl.production.metadata.api.hive.dataset_factory import DatasetFactory
from ensembl.production.metadata.api.models import Dataset, DatasetAttribute, Attribute, DatasetSource, DatasetType

db_directory = Path(__file__).parent / 'databases'
db_directory = db_directory.resolve()


@pytest.mark.parametrize("multi_dbs", [[{'src': 'ensembl_metadata'}, {'src': 'ncbi_taxonomy'}]], indirect=True)
class TestDatasetFactory:
    dbc = None  # type: UnitTestDB

    def test_update_dataset_status(self, multi_dbs):
        dataset_factory = DatasetFactory(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url)
        test_uuid = 'fc5d3e13-340c-4e2a-9f49-256fc319331e'
        dataset_factory.update_dataset_status(test_uuid)
        metadata_db = DBConnection(multi_dbs['ensembl_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            dataset = session.query(Dataset).filter(Dataset.dataset_uuid == test_uuid).one()
            assert dataset.status == 'Processing'
            dataset_factory = DatasetFactory(session=session)
            dataset_factory.update_dataset_status(test_uuid)
            dataset = session.query(Dataset).filter(Dataset.dataset_uuid == test_uuid).one()
            assert dataset.status == 'Processed'

    def test_update_dataset_attributes(self, multi_dbs):
        dataset_factory = DatasetFactory(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url)
        test_uuid = 'fc5d3e13-340c-4e2a-9f49-256fc319331e'
        test_attributes = {"assembly.contig_n50": "test1", "assembly.total_genome_length": "test2"}
        #    def update_dataset_attributes(self,dataset_uuid, attribut_dict):
        dataset_factory.update_dataset_attributes(test_uuid, test_attributes)
        metadata_db = DBConnection(multi_dbs['ensembl_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            dataset = session.query(Dataset).filter(Dataset.dataset_uuid == test_uuid).one()
            dataset_attribute = session.query(DatasetAttribute) \
                .join(Attribute, DatasetAttribute.attribute_id == Attribute.attribute_id) \
                .filter(DatasetAttribute.dataset_id == dataset.dataset_id,
                        Attribute.name == 'assembly.contig_n50',
                        DatasetAttribute.value == 'test1') \
                .one_or_none()
            assert dataset_attribute is not None
            dataset_factory = DatasetFactory(session=session)
            test_attributes = {"assembly.gc_percentage": "test3", "genebuild.nc_longest_gene_length": "test4"}
            dataset_factory.update_dataset_attributes(test_uuid, test_attributes)
            session.commit()
            dataset = session.query(Dataset).filter(Dataset.dataset_uuid == test_uuid).one()
            test_attribute = session.query(DatasetAttribute) \
                .join(Attribute, DatasetAttribute.attribute_id == Attribute.attribute_id) \
                .filter(DatasetAttribute.dataset_id == dataset.dataset_id,
                        Attribute.name == 'genebuild.nc_longest_gene_length',
                        DatasetAttribute.value == 'test4') \
                .all()
            assert test_attribute is not None

    def test_create_dataset(self, multi_dbs):
        metadata_db = DBConnection(multi_dbs['ensembl_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            test_attributes = {"assembly.contig_n50": "test1", "assembly.total_genome_length": "test2"}
            test_genome_uuid = '48b1b849-3b73-4242-ae83-af2290aeb071'
            test_dataset_source = session.query(DatasetSource).filter(
                DatasetSource.name == 'mus_musculus_nodshiltj_core_110_1').one()
            test_dataset_type = session.query(DatasetType).filter(DatasetType.name == 'regulatory_features').one()
            test_name = 'test_name'
            test_label = 'test_label'
            test_version = 'test_version'
            dataset_factory = DatasetFactory(session=session)
            dataset_uuid, new_dataset_attributes, new_genome_dataset = dataset_factory.create_dataset(session,
                                                                                                      test_genome_uuid,
                                                                                                      test_dataset_source,
                                                                                                      test_dataset_type,
                                                                                                      test_attributes,
                                                                                                      test_name,
                                                                                                      test_label,
                                                                                                      test_version)
            session.commit()
            created_dataset = session.query(Dataset).filter(Dataset.dataset_uuid == dataset_uuid).one()
            assert created_dataset.name == test_name
            assert created_dataset.label == test_label
            assert created_dataset.version == test_version
            assert test_dataset_source == session.query(DatasetSource).filter(
                DatasetSource.dataset_source_id == created_dataset.dataset_source_id).one()
            assert test_dataset_type == session.query(DatasetType).filter(
                DatasetType.dataset_type_id == created_dataset.dataset_type_id).one()
            test_attribute = session.query(DatasetAttribute) \
                .join(Attribute, DatasetAttribute.attribute_id == Attribute.attribute_id) \
                .filter(DatasetAttribute.dataset_id == created_dataset.dataset_id,
                        Attribute.name == 'genebuild.nc_longest_gene_length',
                        DatasetAttribute.value == 'test4') \
                .all()
            assert test_attribute is not None

    def test_create_child_datasets_get_parent(self, multi_dbs):
        # Tests for individual calling via dataset_uuid or genome_uuid
        dataset_factory = DatasetFactory(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url)
        test_uuid = '90ba6c03-5161-4f9a-911c-1961b9c0470d'
        data = dataset_factory.create_child_datasets(dataset_uuid=test_uuid)
        metadata_db = DBConnection(multi_dbs['ensembl_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            dataset = session.query(Dataset).join(DatasetType).filter(DatasetType.name == 'xref').one()
            assert dataset.status == 'Submitted'
            dataset_factory = DatasetFactory(session=session)
            dataset_factory.update_dataset_status(dataset.dataset_uuid, 'Processed')
            session.commit()
            parent, parent_type = dataset_factory.get_parent_datasets(dataset.dataset_uuid)
            assert parent[0] == test_uuid
            assert parent_type[0] == 'genebuild'
            dataset_factory.create_child_datasets(genome_uuid='9cc516a8-529e-4919-a429-0d7032e295c9',
                                                  child_type='protein_features')
            # dataset_factory.create_child_datasets(dataset_uuid=data[0],
            #                                       child_type='protein_features')
            session.commit()
            new_dataset = session.query(Dataset).join(DatasetType).filter(DatasetType.name == 'protein_features').one()
            assert new_dataset.status == 'Submitted'

        # Tests for bulk calling.
        dataset_factory = DatasetFactory(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url)
        dataset_factory.create_child_datasets(parent_type='genebuild')
        metadata_db = DBConnection(multi_dbs['ensembl_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            dataset = session.query(Dataset).join(DatasetType).filter(DatasetType.name == 'xref').all()
            assert len(dataset) == 240
