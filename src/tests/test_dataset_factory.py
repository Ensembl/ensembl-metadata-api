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

from ensembl.production.metadata.api.factories.datasets import DatasetFactory
from ensembl.production.metadata.api.models import (Dataset, DatasetAttribute, Attribute, DatasetSource, DatasetType,
                                                    DatasetStatus, GenomeDataset, Genome)

db_directory = Path(__file__).parent / 'databases'
db_directory = db_directory.resolve()

sample_path = Path(__file__).parent.parent / "ensembl" / "production" / "metadata" / "api" / "sample"


@pytest.mark.parametrize("multi_dbs", [[{'src': sample_path / 'ensembl_genome_metadata'},
                                        {'src': sample_path / 'ncbi_taxonomy'},
                                        ]], indirect=True)
class TestDatasetFactory:
    dbc = None  # type: UnitTestDB

    def test_update_dataset_attributes(self, multi_dbs):
        # Test that  the dataset attribute creation works fine and that the dataset_factory works with a session or a url
        dataset_factory = DatasetFactory()
        test_uuid = 'fc5d3e13-340c-4e2a-9f49-256fc319331e'
        test_attributes = {"assembly.contig_n50": "test1", "assembly.total_genome_length": "test2"}
        dataset_factory.update_dataset_attributes(test_uuid, test_attributes,
                                                  metadata_uri=multi_dbs['ensembl_genome_metadata'].dbc.url)
        metadata_db = DBConnection(multi_dbs['ensembl_genome_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            dataset = session.query(Dataset).filter(Dataset.dataset_uuid == test_uuid).one()
            dataset_attribute = session.query(DatasetAttribute) \
                .join(Attribute, DatasetAttribute.attribute_id == Attribute.attribute_id) \
                .filter(DatasetAttribute.dataset_id == dataset.dataset_id,
                        Attribute.name == 'assembly.contig_n50',
                        DatasetAttribute.value == 'test1') \
                .one_or_none()
            assert dataset_attribute is not None
            dataset_factory = DatasetFactory()
            test_attributes = {"assembly.gc_percentage": "test3", "genebuild.nc_longest_gene_length": "test4"}
            dataset_factory.update_dataset_attributes(test_uuid, test_attributes, session=session)
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
        metadata_db = DBConnection(multi_dbs['ensembl_genome_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            test_attributes = {"assembly.contig_n50": "test1", "assembly.total_genome_length": "test2"}
            test_genome_uuid = '48b1b849-3b73-4242-ae83-af2290aeb071'
            test_dataset_source = session.query(DatasetSource).filter(
                DatasetSource.name == 'mus_musculus_nodshiltj_core_110_1').one()
            test_dataset_type = session.query(DatasetType).filter(DatasetType.name == 'regulatory_features').one()
            test_name = 'test_name'
            test_label = 'test_label'
            test_version = 'test_version'
            dataset_factory = DatasetFactory()
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

    def test_create_genebuild_children(self, multi_dbs):
        metadata_db = DBConnection(multi_dbs['ensembl_genome_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            genebuild_uuid = 'cc3c7f95-b5dc-4cc1-aa15-2817c89bd1e2'
            assembly_uuid = '02104faf-3fee-4f28-b53c-605843dac941'

            dataset_factory = DatasetFactory()

            dataset_factory.create_all_child_datasets(session, genebuild_uuid)
            session.commit()
            data = session.query(Dataset).join(DatasetType).filter(
                DatasetType.name == 'genome_browser_track').one()
            assert data.status == DatasetStatus.Submitted
            # test get parent
            test_parent, test_status = dataset_factory.get_parent_datasets(data.dataset_uuid, session=session)
            assert test_parent == genebuild_uuid

    def test_update_dataset_status(self, multi_dbs):
        metadata_db = DBConnection(multi_dbs['ensembl_genome_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            genebuild_uuid = 'cc3c7f95-b5dc-4cc1-aa15-2817c89bd1e2'
            dataset_factory = DatasetFactory()
            genebuild_dataset = session.query(Dataset).filter(Dataset.dataset_uuid == genebuild_uuid).one()
            # Get the genome for this one
            genome_uuid = genebuild_dataset.genome_datasets[0].genome.genome_uuid
            # Check that xref is made
            xref_uuid = session.query(Dataset.dataset_uuid) \
                .join(GenomeDataset, GenomeDataset.dataset_id == Dataset.dataset_id) \
                .join(Genome, Genome.genome_id == GenomeDataset.genome_id) \
                .join(DatasetType, DatasetType.dataset_type_id == Dataset.dataset_type_id) \
                .filter(Genome.genome_uuid == genome_uuid) \
                .filter(DatasetType.name == "xrefs").one()
            protfeat_uuid = session.query(Dataset.dataset_uuid) \
                .join(GenomeDataset, GenomeDataset.dataset_id == Dataset.dataset_id) \
                .join(Genome, Genome.genome_id == GenomeDataset.genome_id) \
                .join(DatasetType, DatasetType.dataset_type_id == Dataset.dataset_type_id) \
                .filter(Genome.genome_uuid == genome_uuid) \
                .filter(DatasetType.name == "protein_features").one()
            protfeat_uuid = protfeat_uuid[0]
            xref_uuid = xref_uuid[0]
            # Processing
            # Fail to update protein_features
            temp, failed_status = dataset_factory.update_dataset_status(protfeat_uuid, DatasetStatus.Processing,
                                                                        session=session)
            session.commit()
            failed_status_check = session.query(Dataset.status).filter(Dataset.dataset_uuid == protfeat_uuid).one()
            assert failed_status == DatasetStatus.Submitted
            assert failed_status_check[0] == DatasetStatus.Submitted
            # succeed on xref
            temp, succeed_status = dataset_factory.update_dataset_status(xref_uuid, DatasetStatus.Processing,
                                                                         session=session)
            session.commit()
            succeed_status_check = session.query(Dataset.status).filter(Dataset.dataset_uuid == xref_uuid).one()
            genebuild_status_check = session.query(Dataset.status).filter(Dataset.dataset_uuid == genebuild_uuid).one()
            assert succeed_status == DatasetStatus.Processing
            assert succeed_status_check[0] == DatasetStatus.Processing
            assert genebuild_status_check[0] == DatasetStatus.Processing

            # Processed
            # Fail to update genebuild
            temp, failed_status = dataset_factory.update_dataset_status(genebuild_uuid, DatasetStatus.Processed,
                                                                        session=session)
            session.commit()
            genebuild_status_check = session.query(Dataset.status).filter(Dataset.dataset_uuid == genebuild_uuid).one()
            assert failed_status == DatasetStatus.Processing
            assert genebuild_status_check[0] == DatasetStatus.Processing
            # Change all the children
            child_dataset_uuids = session.query(Dataset.dataset_uuid) \
                .join(GenomeDataset, GenomeDataset.dataset_id == Dataset.dataset_id) \
                .join(Genome, Genome.genome_id == GenomeDataset.genome_id) \
                .join(DatasetType, DatasetType.dataset_type_id == Dataset.dataset_type_id) \
                .filter(Genome.genome_uuid == genome_uuid) \
                .filter(DatasetType.name != "genebuild").all()
            for temp_uuid in child_dataset_uuids:
                temp_uuid = temp_uuid[0]
                dataset_factory.update_dataset_status(temp_uuid, DatasetStatus.Processed, session=session)
                session.commit()
            genebuild_status_check = session.query(Dataset.status).filter(
                Dataset.dataset_uuid == genebuild_uuid).one()
            assert genebuild_status_check[0] == DatasetStatus.Processed
            dataset_factory.update_dataset_status(genebuild_uuid, DatasetStatus.Released, session=session)
            session.commit()
            genebuild_status_check = session.query(Dataset.status).filter(
                Dataset.dataset_uuid == genebuild_uuid).one()
            assert genebuild_status_check[0] == DatasetStatus.Released
            protfeat_status_check = session.query(Dataset.status).filter(Dataset.dataset_uuid == protfeat_uuid).one()
            assert protfeat_status_check[0] == DatasetStatus.Released

            # Check for submitted change
            dataset_factory.update_dataset_status(protfeat_uuid, DatasetStatus.Submitted, session=session)
            session.commit()
            submitted_status = session.query(Dataset.status).filter(Dataset.dataset_uuid == protfeat_uuid).one()
            assert submitted_status[0] == DatasetStatus.Submitted
