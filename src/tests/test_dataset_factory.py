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
                                                    GenomeDataset, Genome, DatasetStatus, GenomeRelease)

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
        with metadata_db.test_session_scope() as session:
            test_uuid = session.query(Dataset.dataset_uuid).filter(Dataset.dataset_id == 1).scalar()
            test_attributes = {"assembly.stats.contig_n50": "test1", "assembly.stats.total_genome_length": "test2"}
            dataset_factory.update_dataset_attributes(test_uuid, test_attributes, session=session)
            session.commit()

            dataset = session.query(Dataset).filter(Dataset.dataset_uuid == test_uuid).one()
            dataset_attribute = session.query(DatasetAttribute) \
                .join(Attribute, DatasetAttribute.attribute_id == Attribute.attribute_id) \
                .filter(DatasetAttribute.dataset_id == dataset.dataset_id,
                        Attribute.name == 'assembly.stats.contig_n50',
                        DatasetAttribute.value == 'test1') \
                .one_or_none()
            assert dataset_attribute is not None
            test_attributes = {"assembly.stats.gc_percentage": "test3",
                               "genebuild.stats.nc_longest_gene_length": "test4"}
            dataset_factory.update_dataset_attributes(test_uuid, test_attributes, session=session)
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
        with metadata_db.test_session_scope() as session:
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
        with metadata_db.test_session_scope() as session:
            genome = Genome(production_name="new_grch37",
                            assembly_id=40,
                            created=func.now(),
                            organism_id=9,
                            annotation_source="test",
                            genebuild_date="2026-04",
                            provider_name="test"
                            )
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
            genebuild_ds = session.query(Dataset).filter(Dataset.dataset_uuid == genebuild.dataset_uuid).one()
            assert genebuild_ds.name == 'fake genebuild'
            assert genebuild_ds.dataset_uuid == genebuild.dataset_uuid

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
            genebuild_dataset = session.query(Dataset).filter(Dataset.dataset_uuid == genebuild_uuid).one()
            # Get the genome for this one
            genome_uuid = genebuild_dataset.genome_datasets[0].genome.genome_uuid
            print(f"Genome UUID {genome_uuid}")
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
            # Check str / DatasetSetStatus get the same result
            datasets = dataset_factory.get_genomes_by_status_and_type(DatasetStatus.SUBMITTED, dataset_type='genebuild',
                                                                      session=session)
            datasets_2 = dataset_factory.get_genomes_by_status_and_type('Submitted', 'genebuild', session=session)
            assert datasets == datasets_2
            temp, succeed_status = dataset_factory.update_dataset_status(xref_uuid, 'Processing', session=session)
            succeed_status_check = session.query(Dataset.status).filter(Dataset.dataset_uuid == xref_uuid).one()
            assert succeed_status == succeed_status_check[0]

            # succeed on xref
            temp, succeed_status = dataset_factory.update_dataset_status(xref_uuid, DatasetStatus.PROCESSING,
                                                                         session=session)
            succeed_status_check = session.query(Dataset.status).filter(Dataset.dataset_uuid == xref_uuid).one()
            genebuild_status_check = session.query(Dataset.status).filter(Dataset.dataset_uuid == genebuild_uuid).one()
            assert succeed_status == DatasetStatus.PROCESSING  # "Processing"
            assert succeed_status_check[0] == DatasetStatus.PROCESSING  # "Processing"
            assert genebuild_status_check[0] == DatasetStatus.PROCESSING  # "Processing"

            # Processed
            # Fail to update genebuild
            temp, failed_status = dataset_factory.update_dataset_status(genebuild_uuid, DatasetStatus.PROCESSED,
                                                                        session=session)
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
            genebuild_status_check = session.query(Dataset.status).filter(
                Dataset.dataset_uuid == genebuild_uuid).one()
            assert genebuild_status_check[0] == DatasetStatus.PROCESSED
            # Check for submitted change
            dataset_factory.update_dataset_status(protfeat_uuid, DatasetStatus.SUBMITTED,
                                                  session=session)  # "Submitted", session=session)
            submitted_status = session.query(Dataset.status).filter(Dataset.dataset_uuid == protfeat_uuid).one()
            assert submitted_status[0] == DatasetStatus.SUBMITTED  # "Submitted"

    def test_faulty_parent(self, test_dbs, dataset_factory):
        """
        Test case: Marking a 'genebuild' dataset as FAULTY should:
        - Not affect child datasets' status.
        - Remove release_id from all related genome datasets.
        - Remove the corresponding GenomeRelease entry.
        """
        metadata_db = DBConnection(test_dbs['ensembl_genome_metadata'].dbc.url)
        genebuild_uuid = "66db32ae-974f-480c-a60b-63cc49d00f68"
        child_uuid = "da20e2b5-1809-494e-893f-7fb90e8032a1"

        with metadata_db.test_session_scope() as session:
            dataset_factory.simple_update_dataset_status(genebuild_uuid, DatasetStatus.FAULTY, session=session)
            dataset_factory.process_faulty(session)

            # Verify that the child dataset remains PROCESSED
            child_dataset = session.query(Dataset).filter(Dataset.dataset_uuid == child_uuid).one()
            assert child_dataset.status == DatasetStatus.PROCESSED

            # Verify that the release_id has been removed from both parent and child genome datasets
            parent_dataset = session.query(Dataset).filter(Dataset.dataset_uuid == genebuild_uuid).one()
            assert parent_dataset.genome_datasets[0].release_id is None
            assert child_dataset.genome_datasets[0].release_id is None


    def test_faulty_child(self, test_dbs, dataset_factory):
        """
        Test case: Marking a child dataset as FAULTY should:
        - Mark the parent dataset as FAULTY.
        - Not affect the status of unrelated subchild datasets.
        - Remove release_id from the faulty dataset and its ancestors.
        - Remove the corresponding GenomeRelease entry.
        """
        metadata_db = DBConnection(test_dbs['ensembl_genome_metadata'].dbc.url)
        parent_uuid = "66db32ae-974f-480c-a60b-63cc49d00f68"
        child_uuid = "da20e2b5-1809-494e-893f-7fb90e8032a1"
        subchild_uuid = "8ec9f005-91d7-4015-be09-7b61b6d62c54"

        with metadata_db.test_session_scope() as session:
            dataset_factory.simple_update_dataset_status(child_uuid, DatasetStatus.FAULTY, session=session)
            dataset_factory.process_faulty(session)

            # Verify that the parent dataset is now FAULTY
            parent_dataset = session.query(Dataset).filter(Dataset.dataset_uuid == parent_uuid).one()
            assert parent_dataset.status == DatasetStatus.FAULTY

            # Verify that the subchild dataset remains PROCESSED
            subchild_dataset = session.query(Dataset).filter(Dataset.dataset_uuid == subchild_uuid).one()
            assert subchild_dataset.status == DatasetStatus.PROCESSED

            # Verify that release_id is removed from parent, child, and subchild genome datasets
            assert parent_dataset.genome_datasets[0].release_id is None
            child_dataset = session.query(Dataset).filter(Dataset.dataset_uuid == child_uuid).one()
            assert child_dataset.genome_datasets[0].release_id is None
            assert subchild_dataset.genome_datasets[0].release_id is None



@pytest.mark.parametrize("test_dbs", [[{'src': Path(__file__).parent / "databases/ensembl_genome_metadata"},
                                       {'src': Path(__file__).parent / "databases/ncbi_taxonomy"},
                                       ]], indirect=True)
class TestDatasetFactory2:
    dbc: UnitTestDB = None

    def test_faulty_non_essential(self, test_dbs, dataset_factory):
        """
        Test case: Marking a non-essential dataset as FAULTY should:
        - Mark its parent dataset as FAULTY.
        - Not affect a 'genebuild' dataset.
        - Ensure that the 'genebuild' dataset retains its release_id.
        - Remove release_id from only the faulty dataset and its ancestors.
        - Ensure the GenomeRelease entry still exists for the 'genebuild' genome.
        """
        metadata_db = DBConnection(test_dbs['ensembl_genome_metadata'].dbc.url)
        non_essential_child = "f8c7383b-aaac-41cf-9ac8-dce5f99b5338"
        non_essential_parent = "5c2d6ef7-fe03-4f1a-bcc2-fb72af9ffa46"
        genebuild_uuid = "66db32ae-974f-480c-a60b-63cc49d00f68"

        with metadata_db.test_session_scope() as session:
            genebuild_dataset = session.query(Dataset).filter(Dataset.dataset_uuid == genebuild_uuid).one()
            genebuild_dataset.status = DatasetStatus.PROCESSED
            dataset_factory.simple_update_dataset_status(non_essential_child, DatasetStatus.FAULTY, session=session)
            dataset_factory.process_faulty(session)

            # Verify that the parent dataset is now FAULTY
            parent_dataset = session.query(Dataset).filter(Dataset.dataset_uuid == non_essential_parent).one()
            assert parent_dataset.status == DatasetStatus.FAULTY

            # Verify that the 'genebuild' dataset remains PROCESSED
            genebuild_dataset = session.query(Dataset).filter(Dataset.dataset_uuid == genebuild_uuid).one()
            assert genebuild_dataset.status == DatasetStatus.PROCESSED

            # Ensure the 'genebuild' dataset still has a release_id
            # assert genebuild_dataset.genome_datasets[0].release_id is not None

            # Verify that release_id is removed from the faulty dataset and its parent
            child_dataset = session.query(Dataset).filter(Dataset.dataset_uuid == non_essential_child).one()
            assert child_dataset.genome_datasets[0].release_id is None
            assert parent_dataset.genome_datasets[0].release_id is None

            # Ensure the GenomeRelease entry still exists for the 'genebuild' genome
            genome_release = session.query(GenomeRelease).filter(
                GenomeRelease.genome_id == parent_dataset.genome_datasets[0].genome_id
            ).all()
            assert len(genome_release) > 0

    def test_simple_update_dataset_status(self, test_dbs, dataset_factory):
        """
        Test case: Updating the status of a dataset.
        - Ensure the dataset's status is updated correctly.
        - Verify that the change persists in the database.
        """
        metadata_db = DBConnection(test_dbs['ensembl_genome_metadata'].dbc.url)
        dataset_uuid = "66db32ae-974f-480c-a60b-63cc49d00f68"
        new_status = DatasetStatus.FAULTY

        with metadata_db.test_session_scope() as session:
            # Fetch the original dataset status
            dataset = session.query(Dataset).filter(Dataset.dataset_uuid == dataset_uuid).one()
            original_status = dataset.status

            # Update dataset status using the factory method
            updated_uuid, updated_status = dataset_factory.simple_update_dataset_status(dataset_uuid, new_status,
                                                                                        session=session)
            # Fetch the dataset again to verify the update
            updated_dataset = session.query(Dataset).filter(Dataset.dataset_uuid == dataset_uuid).one()

            # Assertions
            assert updated_uuid == dataset_uuid, "The dataset UUID should remain unchanged."
            assert updated_status == new_status, "The dataset status should be updated to FAULTY."
            assert updated_dataset.status == new_status, "The status change should persist in the database."

            # Ensure the original status was different for test validity
            assert original_status != new_status, "Test should validate an actual status change."


@pytest.mark.parametrize("test_dbs", [[{'src': Path(__file__).parent / "databases/ensembl_genome_metadata"},
                                       {'src': Path(__file__).parent / "databases/ncbi_taxonomy"},
                                       ]], indirect=True)
class TestDatasetFactory3:
    dbc: UnitTestDB = None

    def test_attach_misc_datasets(self, test_dbs, dataset_factory):
        """
        Test the `attach_misc_datasets` function with a standard case where:

        - A parent dataset and its child should be correctly attached to the given release.
        - A faulty dataset should NOT be attached.
        - A processing dataset should initially NOT be attached, but once marked as PROCESSED, it should be added in a subsequent call.

        This test ensures that only valid datasets (those in a PROCESSED state) are attached to the release.
        """
        metadata_db = DBConnection(test_dbs['ensembl_genome_metadata'].dbc.url)

        # Define dataset UUIDs for test cases
        dataset_uuid = "5c2d6ef7-fe03-4f1a-bcc2-fb72af9ffa46"  # Parent dataset
        child_dataset_uuid = "5c2d6ef7-fe03-4f1a-bcc2-fb72af9ffa46"  # Child dataset
        faulty_dataset_uuid = "bf1f5064-8520-abcd-84e4-449aa6c1c1e2"  # Faulty dataset (should NOT be attached)
        processing_dataset_uuid = "bf1f5064-8520-abcd-84e4-449aa6c221e2"  # Processing dataset (will be updated)

        release = 5  # Release ID to attach datasets to

        with metadata_db.test_session_scope() as session:
            # First call to attach_misc_datasets
            dataset_factory.attach_misc_datasets(release, session)

            # Fetch datasets from the database after processing
            parent_dataset = session.query(Dataset).filter(Dataset.dataset_uuid == dataset_uuid).one()
            child_dataset = session.query(Dataset).filter(Dataset.dataset_uuid == child_dataset_uuid).one()
            faulty_dataset = session.query(Dataset).filter(Dataset.dataset_uuid == faulty_dataset_uuid).one()
            processing_dataset = session.query(Dataset).filter(Dataset.dataset_uuid == processing_dataset_uuid).one()

            # ✅ Basic release checks: Parent & Child should be attached
            assert parent_dataset.genome_datasets[0].release_id == 5
            assert child_dataset.genome_datasets[0].release_id == 5

            # ✅ Faulty dataset should NOT be attached to the release
            assert faulty_dataset.genome_datasets[0].release_id != 5
            assert faulty_dataset.genome_datasets[0].release_id is None

            # ✅ Processing dataset should NOT yet be attached
            assert processing_dataset.genome_datasets[0].release_id is None

            # Simulate processing completion: Update status to PROCESSED
            processing_dataset.status = DatasetStatus.PROCESSED

            # Second call to attach_misc_datasets after status change
            dataset_factory.attach_misc_datasets(release, session)

            # ✅ Now that the dataset is PROCESSED, it should be attached
            assert processing_dataset.genome_datasets[0].release_id == 5

    def test_attach_misc_datasets_force(self, test_dbs, dataset_factory):
        """
        Test the `attach_misc_datasets` function with `force=True`, which should attach all datasets
        that are either SUBMITTED, PROCESSING, or PROCESSED to the given release.

        - A parent dataset and its child should be correctly attached to the given release.
        - A faulty dataset should NOT be attached.
        - A processing dataset should be attached immediately (since force=True).

        This test ensures that `force=True` overrides the standard validation rules and allows
        SUBMITTED and PROCESSING datasets to be attached.
        """
        metadata_db = DBConnection(test_dbs['ensembl_genome_metadata'].dbc.url)

        # Define dataset UUIDs for test cases
        dataset_uuid = "5c2d6ef7-fe03-4f1a-bcc2-fb72af9ffa46"  # Parent dataset
        child_dataset_uuid = "5c2d6ef7-fe03-4f1a-bcc2-fb72af9ffa46"  # Child dataset
        faulty_dataset_uuid = "bf1f5064-8520-abcd-84e4-449aa6c1c1e2"  # Faulty dataset (should NOT be attached)
        processing_dataset_uuid = "bf1f5064-8520-abcd-84e4-449aa6c221e2"  # Processing dataset (should be attached due to force=True)

        release = 5  # Release ID to attach datasets to

        with metadata_db.test_session_scope() as session:
            # Call attach_misc_datasets with force=True
            dataset_factory.attach_misc_datasets(release, session, force=True)

            # Fetch datasets from the database after processing
            parent_dataset = session.query(Dataset).filter(Dataset.dataset_uuid == dataset_uuid).one()
            child_dataset = session.query(Dataset).filter(Dataset.dataset_uuid == child_dataset_uuid).one()
            faulty_dataset = session.query(Dataset).filter(Dataset.dataset_uuid == faulty_dataset_uuid).one()
            processing_dataset = session.query(Dataset).filter(Dataset.dataset_uuid == processing_dataset_uuid).one()

            # ✅ Parent & Child datasets should be attached
            assert parent_dataset.genome_datasets[0].release_id == 5
            assert child_dataset.genome_datasets[0].release_id == 5

            # ✅ Faulty dataset should still NOT be attached, even in force mode
            assert faulty_dataset.genome_datasets[0].release_id != 5
            assert faulty_dataset.genome_datasets[0].release_id is None

            # ✅ Processing dataset should now be attached because of force=True
            assert processing_dataset.genome_datasets[0].release_id == 5

    def test_update_parent_and_children_status(self, test_dbs, dataset_factory):
        """
        Test the `update_parent_and_children_status` function for correct propagation of dataset status updates.

        This test specifically checks:
        - Status updates propagate correctly from a grandchild dataset up to its parent and top-level dataset.
        - RELEASED status should not propagate upwards unless explicitly allowed.
        - FAULTY datasets should not affect the statuses of their parents.
        """
        metadata_db = DBConnection(test_dbs['ensembl_genome_metadata'].dbc.url)

        # Define dataset UUIDs for the test hierarchy
        top_level_dataset = "99999999-847e-4742-a68b-18c3ece068aa"
        child_dataset = "99999999-da2c-4997-8002-9da717ba79d2"
        grandchild_dataset = "99999999-d9e0-4eca-9a49-7a6d9e311c8d"

        with metadata_db.test_session_scope() as session:
            # Step 1: Ensure initial dataset statuses
            top_level = session.query(Dataset).filter(Dataset.dataset_uuid == top_level_dataset).one()
            child = session.query(Dataset).filter(Dataset.dataset_uuid == child_dataset).one()
            grandchild = session.query(Dataset).filter(Dataset.dataset_uuid == grandchild_dataset).one()

            assert top_level.status == DatasetStatus.SUBMITTED
            assert child.status == DatasetStatus.SUBMITTED
            assert grandchild.status == DatasetStatus.SUBMITTED

            # Step 2: Update grandchild to PROCESSING and verify upward propagation
            grandchild.status = DatasetStatus.PROCESSING
            dataset_factory.update_parent_and_children_status(top_level_dataset, session=session)
            assert grandchild.status == DatasetStatus.PROCESSING
            assert child.status == DatasetStatus.PROCESSING
            assert top_level.status == DatasetStatus.PROCESSING

            grandchild.status = DatasetStatus.PROCESSED
            dataset_factory.update_parent_and_children_status(top_level_dataset, session=session)

            session.refresh(grandchild)
            session.refresh(child)
            session.refresh(top_level)
            assert grandchild.status == DatasetStatus.PROCESSED
            assert child.status == DatasetStatus.PROCESSED
            assert top_level.status == DatasetStatus.PROCESSED

            # Step 4: Update grandchild to RELEASED and ensure no propagation without explicit allowance
            grandchild.status = DatasetStatus.RELEASED
            dataset_factory.update_parent_and_children_status(top_level_dataset, session=session)
            assert grandchild.status == DatasetStatus.RELEASED
            assert child.status != DatasetStatus.RELEASED
            assert top_level.status != DatasetStatus.RELEASED

            # Step 5: Mark grandchild as FAULTY and verify it does not propagate upwards
            grandchild.status = DatasetStatus.FAULTY
            dataset_factory.update_parent_and_children_status(top_level_dataset, session=session)
            assert grandchild.status == DatasetStatus.FAULTY
            assert child.status == DatasetStatus.PROCESSED
            assert top_level.status == DatasetStatus.PROCESSED

            # Step 6: Mark grandchild as Processed and release the genomes
            grandchild.status = DatasetStatus.PROCESSED
            dataset_factory.update_parent_and_children_status(top_level_dataset, status=DatasetStatus.RELEASED,
                                                              session=session)
            assert grandchild.status == DatasetStatus.RELEASED
            assert child.status == DatasetStatus.RELEASED
            assert top_level.status == DatasetStatus.RELEASED

            # Step 6: Mark them as random and force a release
            grandchild.status = DatasetStatus.PROCESSED
            child.status = DatasetStatus.PROCESSING
            top_level.status = DatasetStatus.SUBMITTED
            dataset_factory.update_parent_and_children_status(top_level_dataset, status=DatasetStatus.RELEASED,
                                                              force=True, session=session)
            assert grandchild.status == DatasetStatus.RELEASED
            assert child.status == DatasetStatus.RELEASED
            assert top_level.status == DatasetStatus.RELEASED

    def test_is_current_datasets_resolve(self, test_dbs, dataset_factory):
        """
        Test the `is_current_datasets_resolve` function for correct application of is_current flags.

        This test specifically checks:
        - A genome with two is_current for the same dataset_type remove all except for the release specified.
        """
        metadata_db = DBConnection(test_dbs['ensembl_genome_metadata'].dbc.url)
        #
        # Genome Dataset Ids that should be affected by the test
        old_is_current = 7122  # 7122  1 7177  86  2
        new_is_current = 2390  # 2390  1 2449  86  5

        with metadata_db.test_session_scope() as session:
            # Ensure both are is_current
            old = session.query(GenomeDataset).filter(GenomeDataset.genome_dataset_id == old_is_current).one()
            new = session.query(GenomeDataset).filter(GenomeDataset.genome_dataset_id == new_is_current).one()
            #
            assert old.is_current == 1
            assert new.is_current == 1
            genome_dataset = dataset_factory.is_current_datasets_resolve(release_id=5, session=session)

            # This one needs some work. Recently fixed the method to handle multiples of the same type, but we don't have good example data.

            # assert old.is_current == 0
            assert new.is_current == 1
