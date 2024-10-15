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
from sqlalchemy import func

from ensembl.production.metadata.api.models import (Dataset, DatasetAttribute, Attribute, DatasetSource, DatasetType,
                                                    GenomeDataset, Genome, DatasetStatus)

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("test_dbs", [[{'src': Path(__file__).parent / "databases/ensembl_genome_metadata"},
                                        {'src': Path(__file__).parent / "databases/ncbi_taxonomy"},
                                        ]], indirect=True)
class TestDatasetFactory:
    dbc: UnitTestDB = None

    def test_update_dataset_attributes(self, test_dbs, dataset_factory):
        """
        Test that  the dataset attribute creation works fine and that the dataset_factory works with a session or a url
        """
        metadata_db = DBConnection(test_dbs['ensembl_genome_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            test_uuid = session.query(Dataset.dataset_uuid).filter(Dataset.dataset_id == 1).scalar()
            test_attributes = {"assembly.stats.contig_n50": "test1", "assembly.stats.total_genome_length": "test2"}
            dataset_attribute = dataset_factory.update_dataset_attributes(test_uuid, test_attributes)
            for attrib in dataset_attribute:
                session.add(attrib)

            session.commit()

            dataset = session.query(Dataset).filter(Dataset.dataset_uuid == test_uuid).one()
            dataset_attribute = session.query(DatasetAttribute) \
                .join(Attribute, DatasetAttribute.attribute_id == Attribute.attribute_id) \
                .filter(DatasetAttribute.dataset_id == dataset.dataset_id,
                        Attribute.name == 'assembly.stats.contig_n50',
                        DatasetAttribute.value == 'test1') \
                .one_or_none()
            assert dataset_attribute is not None
            test_attributes = {"assembly.stats.gc_percentage": "test3", "genebuild.stats.nc_longest_gene_length": "test4"}
            dataset_attribute = dataset_factory.update_dataset_attributes(test_uuid, test_attributes, session=session)
            for attrib in dataset_attribute:
                session.add(attrib)
            session.commit()
            dataset = session.query(Dataset).filter(Dataset.dataset_uuid == test_uuid).one()
            test_attribute = session.query(DatasetAttribute) \
                .join(Attribute, DatasetAttribute.attribute_id == Attribute.attribute_id) \
                .filter(DatasetAttribute.dataset_id == dataset.dataset_id,
                        Attribute.name == 'genebuild.stats.nc_longest_gene_length',
                        DatasetAttribute.value == 'test4') \
                .one_or_none()
            assert test_attribute is not None

    def test_create_dataset(self, test_dbs, dataset_factory):
        metadata_db = DBConnection(test_dbs['ensembl_genome_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            test_attributes = {"assembly.stats.contig_n50": "test1", "assembly.stats.total_genome_length": "test2"}
            test_genome_uuid = session.query(Genome.genome_uuid).filter(Genome.genome_id == 4).scalar()  # one human
            test_dataset_source = session.query(DatasetSource).filter(
                DatasetSource.name == 'homo_sapiens_gca018473315v1_core_110_1').one()
            test_dataset_type = session.query(DatasetType).filter(DatasetType.name == 'regulatory_features').one()
            test_name = 'test_name'
            test_label = 'test_label'
            test_version = 'test_version'
            dataset_uuid, created_dataset, new_dataset_attributes, new_genome_dataset = dataset_factory.create_dataset(
                session,
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
                        Attribute.name == 'genebuild.stats.nc_longest_gene_length',
                        DatasetAttribute.value == 'test4') \
                .all()
            assert test_attribute is not None

    def test_genebuild_workflow(self, test_dbs, dataset_factory):
        metadata_db = DBConnection(test_dbs['ensembl_genome_metadata'].dbc.url)
        genebuild_uuid = 'a3352834-cea1-40aa-9dad-99981620c36b'
        # Test children creation
        with metadata_db.session_scope() as session:
            genome = Genome(genebuild_version="1.0",
                            production_name="new_grch37",
                            assembly_id=40,
                            created=func.now(),
                            organism_id=9)
            session.add(genome)
            genebuild = Dataset(
                dataset_type_id=2,
                dataset_source=DatasetSource(
                    name="fake",
                    type="core"
                ),
                created=func.now(),
                name="fake genebuild",
                status=DatasetStatus.SUBMITTED,
                label="fake genebuild",
                version="1.0",
                dataset_uuid=genebuild_uuid
            )
            session.add(genebuild)
            session.add(GenomeDataset(
                genome=genome,
                dataset=genebuild,
                is_current=0,
            ))

            session.commit()
            assert genebuild.dataset_uuid is not None
            logger.debug(f" new GB uuid {genebuild.dataset_uuid}")
            dataset_factory.create_all_child_datasets(genebuild.dataset_uuid, session)
            session.commit()
            genebuild_ds = session.query(Dataset).filter(Dataset.dataset_uuid == genebuild.dataset_uuid).one()
            assert genebuild_ds.name == 'fake genebuild'
            assert genebuild_ds.dataset_uuid == genebuild.dataset_uuid
            session.commit()

            data_q = session.query(Dataset).join(DatasetType).filter(
                DatasetType.name == 'genebuild_web', Dataset.parent == genebuild_ds)
            logger.debug(data_q)
            data = data_q.one()
            sdata = session.query(Dataset).join(GenomeDataset).join(DatasetType).filter(
                DatasetType.name == 'alpha_fold', GenomeDataset.genome_id == genome.genome_id).one()
            assert data.status == DatasetStatus.SUBMITTED  # "Submitted"
            assert sdata.status == DatasetStatus.SUBMITTED  # "Submitted"
            # test get parent
            test_parent, test_status = dataset_factory.get_parent_datasets(data.dataset_uuid, session=session)
            assert test_parent == genebuild.dataset_uuid
            stest_parent, test_status = dataset_factory.get_parent_datasets(data.dataset_uuid, session=session)
            assert test_parent == stest_parent
            assert len(data.children) == 3
            assert any(x for x in data.children if x.name == 'checksums')
            assert any(x for x in data.children if x.name == 'thoas_dumps')

        # Genebuild child datasets are now created, test updating status
        with metadata_db.session_scope() as session:
            genebuild_dataset = session.query(Dataset).filter(Dataset.dataset_uuid == genebuild_uuid).one()
            # Get the genome for this one
            genome_uuid = genebuild_dataset.genome_datasets[0].genome.genome_uuid
            print(f"GEnome UUID {genome_uuid}")
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
            temp, failed_status = dataset_factory.update_dataset_status(protfeat_uuid, DatasetStatus.PROCESSING,
                                                                        session=session)
            session.commit()
            # Check str / DatasetSetStatus get the same result
            datasets = dataset_factory.get_genomes_by_status_and_type(DatasetStatus.SUBMITTED, dataset_type='genebuild',
                                                                      session=session)
            datasets_2 = dataset_factory.get_genomes_by_status_and_type('Submitted', 'genebuild', session=session)
            assert datasets == datasets_2
            temp, succeed_status = dataset_factory.update_dataset_status(xref_uuid, 'Processing', session=session)
            session.commit()
            succeed_status_check = session.query(Dataset.status).filter(Dataset.dataset_uuid == xref_uuid).one()
            assert succeed_status == succeed_status_check[0]

            failed_status_check = session.query(Dataset.status).filter(Dataset.dataset_uuid == protfeat_uuid).one()
            assert failed_status == DatasetStatus.SUBMITTED  # "Submitted"
            assert failed_status_check[0] == DatasetStatus.SUBMITTED  # "Submitted"
            # succeed on xref
            temp, succeed_status = dataset_factory.update_dataset_status(xref_uuid, DatasetStatus.PROCESSING,
                                                                         session=session)
            session.commit()
            succeed_status_check = session.query(Dataset.status).filter(Dataset.dataset_uuid == xref_uuid).one()
            genebuild_status_check = session.query(Dataset.status).filter(Dataset.dataset_uuid == genebuild_uuid).one()
            assert succeed_status == DatasetStatus.PROCESSING  # "Processing"
            assert succeed_status_check[0] == DatasetStatus.PROCESSING  # "Processing"
            assert genebuild_status_check[0] == DatasetStatus.PROCESSING  # "Processing"

            # Processed
            # Fail to update genebuild
            temp, failed_status = dataset_factory.update_dataset_status(genebuild_uuid, DatasetStatus.PROCESSED,
                                                                        session=session)
            session.commit()
            genebuild_status_check = session.query(Dataset.status).filter(Dataset.dataset_uuid == genebuild_uuid).one()
            assert failed_status == DatasetStatus.PROCESSING  # "Processing"
            assert genebuild_status_check[0] == DatasetStatus.PROCESSING  # "Processing"
            # Change all the children
            child_dataset_uuids = session.query(Dataset.dataset_uuid) \
                .join(GenomeDataset, GenomeDataset.dataset_id == Dataset.dataset_id) \
                .join(Genome, Genome.genome_id == GenomeDataset.genome_id) \
                .join(DatasetType, DatasetType.dataset_type_id == Dataset.dataset_type_id) \
                .filter(Genome.genome_uuid == genome_uuid) \
                .filter(DatasetType.name != "genebuild").all()
            for temp_uuid in child_dataset_uuids:
                temp_uuid = temp_uuid[0]
                dataset_factory.update_dataset_status(temp_uuid, DatasetStatus.PROCESSED,
                                                      session=session)  # "Processed", session=session)
                session.commit()
            genebuild_status_check = session.query(Dataset.status).filter(
                Dataset.dataset_uuid == genebuild_uuid).one()
            assert genebuild_status_check[0] == DatasetStatus.PROCESSED
            # Check for submitted change
            dataset_factory.update_dataset_status(protfeat_uuid, DatasetStatus.SUBMITTED,
                                                  session=session)  # "Submitted", session=session)
            session.commit()
            submitted_status = session.query(Dataset.status).filter(Dataset.dataset_uuid == protfeat_uuid).one()
            assert submitted_status[0] == DatasetStatus.SUBMITTED  # "Submitted"
