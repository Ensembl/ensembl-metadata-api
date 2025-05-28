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
from decimal import Decimal
from pathlib import Path

import pytest
from ensembl.utils.database import UnitTestDB, DBConnection

from ensembl.production.metadata.api.exceptions import MissingMetaException
from ensembl.production.metadata.api.factories.genomes import GenomeFactory
from ensembl.production.metadata.api.factories.release import ReleaseFactory
from ensembl.production.metadata.api.models import *

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("test_dbs", [[{'src': Path(__file__).parent / "databases/ensembl_genome_metadata"},
                                       {'src': Path(__file__).parent / "databases/ncbi_taxonomy"},
                                       ]], indirect=True)
class TestReleaseFactory:
    dbc: UnitTestDB = None
    gen_factory = GenomeFactory()

    def test_init_release_default(self, test_dbs) -> None:
        """
        Test `init_release` with default values.
        """
        metadata_db = DBConnection(test_dbs['ensembl_genome_metadata'].dbc.url)
        factory = ReleaseFactory(test_dbs['ensembl_genome_metadata'].dbc.url)

        with metadata_db.session_scope() as session:
            last_release = session.query(EnsemblRelease).order_by(EnsemblRelease.version.desc()).first()
            expected_version = Decimal("1.0") if last_release is None else last_release.version + Decimal("0.1")

            try:
                # Call init_release but don't assert on the returned object
                factory.init_release(label=str(expected_version))
            except Exception as e:
                pytest.fail(f"Unexpected exception: {e}")

        # ✅ Re-fetch in a new session
        with metadata_db.session_scope() as session:
            release = session.query(EnsemblRelease).filter(EnsemblRelease.version == expected_version).one_or_none()

            assert release is not None, "Release was not inserted into the database"
            assert release.version == expected_version
            assert release.release_date is None  # Should allow NULL
            assert release.label == str(expected_version)  # Default label behavior
            assert release.release_type == "partial"
            assert release.status == ReleaseStatus.PLANNED

    def test_init_release_custom_values(self, test_dbs) -> None:
        """
        Test `init_release` with custom values.
        """
        metadata_db = DBConnection(test_dbs['ensembl_genome_metadata'].dbc.url)
        factory = ReleaseFactory(test_dbs['ensembl_genome_metadata'].dbc.url)

        with metadata_db.session_scope() as session:
            try:
                release = factory.init_release(
                    version=Decimal("2.5"),
                    release_date="2024-02-23",
                    label="Spring Release",
                    release_type="integrated",
                    status="Released"
                )
            except Exception as e:
                pytest.fail(f"Unexpected exception: {e}")

            with metadata_db.session_scope() as session:
                release = session.query(EnsemblRelease).filter(
                    EnsemblRelease.version == Decimal("2.5")).one_or_none()

                assert release.version == Decimal("2.5")
                assert release.label == "Spring Release"
                assert release.release_type == "integrated"
                assert release.status == ReleaseStatus.RELEASED

    def test_init_release_invalid_inputs(self, test_dbs) -> None:
        """
        Ensure `init_release` raises appropriate exceptions for invalid inputs.
        """
        metadata_db = DBConnection(test_dbs['ensembl_genome_metadata'].dbc.url)
        factory = ReleaseFactory(test_dbs['ensembl_genome_metadata'].dbc.url)

        with metadata_db.session_scope():
            # 🚨 Invalid date format
            with pytest.raises(ValueError, match="Invalid release_date format"):
                factory.init_release(release_date="23-02-2024")  # Wrong format

            # 🚨 Invalid release type
            with pytest.raises(ValueError, match="Invalid release_type"):
                factory.init_release(release_date="2024-02-23", release_type="full")  # Invalid type

            # 🚨 Invalid status
            with pytest.raises(ValueError, match="Invalid status"):
                factory.init_release(release_date="2024-02-23", status="ongoing")  # Invalid status

            # 🚨 Both release_date and label missing
            with pytest.raises(ValueError, match="Either release_date or label must be specified"):
                factory.init_release(release_date=None, label=None)

    def test_init_release_missing_site(self, test_dbs) -> None:
        """
        Ensure `init_release` raises MissingMetaException for an unknown site.
        """
        metadata_db = DBConnection(test_dbs['ensembl_genome_metadata'].dbc.url)
        factory = ReleaseFactory(test_dbs['ensembl_genome_metadata'].dbc.url)

        with metadata_db.session_scope():
            with pytest.raises(MissingMetaException, match="Site 'InvalidSite' not found"):
                factory.init_release(release_date="2024-02-23", site="InvalidSite")  # Nonexistent site

    def test_pre_release_check_valid_release(self, test_dbs) -> None:
        """Test pre_release_check on a valid release with no errors.
                Includeds a variation dataset that will be ignored. Genome dataset (9015)
        """

        metadata_db = DBConnection(test_dbs['ensembl_genome_metadata'].dbc.url)
        factory = ReleaseFactory(test_dbs['ensembl_genome_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            errors = factory.pre_release_check("4")
            assert errors == [], f"Unexpected errors found: {errors}"

    def test_pre_release_check_invalid_dataset_status(self, test_dbs):
        """Test pre_release_check when a dataset has an invalid status."""
        metadata_db = DBConnection(test_dbs['ensembl_genome_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            dataset = session.query(Dataset).filter(Dataset.dataset_id == 8936).one()
            dataset.status = "Processing"
            genome_dataset = session.query(GenomeDataset).filter(GenomeDataset.dataset_id == 8936).one()
            genome_dataset.release_id = 4
            session.add(genome_dataset)
            session.add(dataset)
            session.commit()
            factory = ReleaseFactory(test_dbs['ensembl_genome_metadata'].dbc.url)
            errors = factory.pre_release_check("4")
            assert f"Dataset [f9ef4142-f4c9-4def-84af-c9480934d408] is neither processed nor released." in errors

    def test_pre_release_check_processed_alternative(self, test_dbs):
        """Test pre_release_check when an alternative dataset of the same type is processed."""
        metadata_db = DBConnection(test_dbs['ensembl_genome_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            dataset = session.query(Dataset).filter(Dataset.dataset_id == 8936).one()
            genome_dataset = session.query(GenomeDataset).filter(GenomeDataset.dataset_id == 8936).one()
            genome_dataset.release_id = 4
            session.add(genome_dataset)
            session.add(dataset)
            session.commit()
            factory = ReleaseFactory(test_dbs['ensembl_genome_metadata'].dbc.url)
            errors = factory.pre_release_check("4")
            assert f"Dataset [f9ef4142-f4c9-4def-84af-c9480934d408] is neither processed nor released." in errors
            # Create a dataset of the same type

    def test_pre_release_check_processed_alternative(self, test_dbs):
        """Test pre_release_check when an alternative dataset of the same type is processed."""
        metadata_db = DBConnection(test_dbs['ensembl_genome_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            # Run the same test as above to ensure we have a dataset with an invalid status
            dataset = session.query(Dataset).filter(Dataset.dataset_id == 8936).one()
            genome_dataset = session.query(GenomeDataset).filter(GenomeDataset.dataset_id == 8936).one()
            genome_dataset.release_id = 4
            session.add(genome_dataset)
            session.add(dataset)
            session.commit()
            factory = ReleaseFactory(test_dbs['ensembl_genome_metadata'].dbc.url)
            errors = factory.pre_release_check("4")
            assert f"Dataset [f9ef4142-f4c9-4def-84af-c9480934d408] is neither processed nor released." in errors

            # Create a dataset of the same type, but with processed.
            processed_dataset = Dataset(
                dataset_type=dataset.dataset_type,
                status="Processed",
                dataset_uuid="new-processed-dataset",
                name="New Processed Dataset",
                dataset_source_id=dataset.dataset_source_id,
                label="New Processed Dataset"
            )
            session.add(processed_dataset)
            session.flush()
            processed_genome_dataset = GenomeDataset(
                genome_id=genome_dataset.genome_id,
                dataset_id=processed_dataset.dataset_id,
                release_id=4
            )
            session.add(processed_genome_dataset)
            session.commit()

            factory = ReleaseFactory(test_dbs['ensembl_genome_metadata'].dbc.url)
            errors = factory.pre_release_check("4")
            assert not errors, f"Unexpected errors found: {errors}"
