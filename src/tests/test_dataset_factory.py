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
from unittest import mock
from unittest.mock import Mock, patch

import pytest
import re

import sqlalchemy
from ensembl.database import UnitTestDB, DBConnection

from ensembl.production.metadata.api.hive.dataset_factory import DatasetFactory
from ensembl.production.metadata.api.models import Dataset, DatasetAttribute, Attribute

db_directory = Path(__file__).parent / 'databases'
db_directory = db_directory.resolve()


@pytest.mark.parametrize("multi_dbs", [[{'src': 'ensembl_metadata'}, {'src': 'ncbi_taxonomy'}]],indirect=True)
class TestDatasetFactory:
    dbc = None  # type: UnitTestDB

    def test_update_dataset_status(self, multi_dbs):
        dataset_factory = DatasetFactory(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url)
        test_uuid = '385f1ec2-bd06-40ce-873a-98e199f10534'
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
        test_uuid = '385f1ec2-bd06-40ce-873a-98e199f10534'
        test_attributes = {"contig_n50" : "test1", "total_genome_length": "test2"}
        #    def update_dataset_attributes(self,dataset_uuid, attribut_dict):
        dataset_factory.update_dataset_attributes(test_uuid, test_attributes)
        metadata_db = DBConnection(multi_dbs['ensembl_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            dataset = session.query(Dataset).filter(Dataset.dataset_uuid == test_uuid).one()
            dataset_attribute = session.query(DatasetAttribute) \
                .join(Attribute, DatasetAttribute.attribute_id == Attribute.attribute_id) \
                .filter(DatasetAttribute.dataset_id == dataset.dataset_id,
                        Attribute.name == 'contig_n50',
                        DatasetAttribute.value == 'test1') \
                .one_or_none()
            assert dataset_attribute is not None
            dataset_factory = DatasetFactory(session=session)
            test_attributes = {"gc_percentage": "test3", "longest_gene_length": "test4"}
            dataset_factory.update_dataset_attributes(test_uuid, test_attributes)
            session.commit()
            dataset = session.query(Dataset).filter(Dataset.dataset_uuid == test_uuid).one()
            test_attribute = session.query(DatasetAttribute) \
                .join(Attribute, DatasetAttribute.attribute_id == Attribute.attribute_id) \
                .filter(DatasetAttribute.dataset_id == dataset.dataset_id,
                        Attribute.name == 'longest_gene_length',
                        DatasetAttribute.value == 'test4') \
                .all()
            assert test_attribute is not None



