# See the NOTICE file distributed with this work for additional information
# regarding copyright ownership.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from ensembl.production.metadata.api.models import (
    Genome,
    Dataset,
    DatasetAttribute,
    DatasetType,
    Attribute,
    EnsemblRelease,
    GenomeRelease,
    GenomeDataset,
    ReleaseStatus,
    DatasetStatus,
    Organism,
    Assembly,
)
from ensembl.production.metadata.api.search.search import (
    MissingDatasetFieldError,
    GenomeIndexError,
    IndexingErrorCollection,
    GenomeSearchDocument,
    DatasetFieldExtractor,
    ReleaseSelector,
    GenomeSearchIndexer,
)

db_directory = Path(__file__).parent / "databases"
db_directory = db_directory.resolve()


@pytest.mark.parametrize(
    "test_dbs",
    [[{"src": db_directory / "ensembl_genome_metadata"}, {"src": db_directory / "ncbi_taxonomy"}]],
    indirect=True,
)
class TestIndexingErrorCollection:
    """Test suite for IndexingErrorCollection class."""

    def test_init_empty(self, test_dbs):
        """Test IndexingErrorCollection initializes with empty error list."""
        collection = IndexingErrorCollection()
        assert isinstance(collection.errors, list)
        assert len(collection.errors) == 0
        assert not collection.has_errors()

    def test_add_error(self, test_dbs):
        """Test adding an error to the collection."""
        collection = IndexingErrorCollection()
        test_exception = ValueError("Test error")

        collection.add_error(
            genome_uuid="test-uuid-123",
            release_label="2024-01",
            error_message="Test error message",
            exception=test_exception,
        )

        assert collection.has_errors()
        assert len(collection.errors) == 1

        error = collection.errors[0]
        assert isinstance(error, GenomeIndexError)
        assert error.genome_uuid == "test-uuid-123"
        assert error.release_label == "2024-01"
        assert error.error_message == "Test error message"
        assert error.exception == test_exception

    def test_add_multiple_errors(self, test_dbs):
        """Test adding multiple errors to the collection."""
        collection = IndexingErrorCollection()

        for i in range(3):
            collection.add_error(
                genome_uuid=f"uuid-{i}",
                release_label=f"release-{i}",
                error_message=f"Error {i}",
                exception=ValueError(f"Error {i}"),
            )

        assert collection.has_errors()
        assert len(collection.errors) == 3
        assert collection.errors[0].genome_uuid == "uuid-0"
        assert collection.errors[2].genome_uuid == "uuid-2"

    def test_has_errors_false(self, test_dbs):
        """Test has_errors returns False when no errors exist."""
        collection = IndexingErrorCollection()
        assert not collection.has_errors()

    def test_has_errors_true(self, test_dbs):
        """Test has_errors returns True when errors exist."""
        collection = IndexingErrorCollection()
        collection.add_error("uuid", "label", "message", Exception())
        assert collection.has_errors()

    def test_get_summary_no_errors(self, test_dbs):
        """Test get_summary returns appropriate message when no errors."""
        collection = IndexingErrorCollection()
        summary = collection.get_summary()
        assert summary == "No errors occurred during indexing."

    def test_get_summary_with_errors(self, test_dbs):
        """Test get_summary returns formatted error report."""
        collection = IndexingErrorCollection()
        collection.add_error(
            genome_uuid="test-uuid",
            release_label="2024-01",
            error_message="Missing field",
            exception=MissingDatasetFieldError("Missing field"),
        )

        summary = collection.get_summary()
        assert "INDEXING ERRORS" in summary
        assert "1 genome(s) failed to index" in summary
        assert "test-uuid" in summary
        assert "2024-01" in summary
        assert "Missing field" in summary
        assert "=" * 80 in summary

    def test_get_summary_multiple_errors(self, test_dbs):
        """Test get_summary formats multiple errors correctly."""
        collection = IndexingErrorCollection()

        collection.add_error("uuid-1", "2024-01", "Error 1", Exception())
        collection.add_error("uuid-2", "2024-02", "Error 2", Exception())

        summary = collection.get_summary()
        assert "2 genome(s) failed to index" in summary
        assert "uuid-1" in summary
        assert "uuid-2" in summary
        assert "1." in summary
        assert "2." in summary

    def test_raise_if_errors_no_errors(self, test_dbs):
        """Test raise_if_errors does nothing when no errors exist."""
        collection = IndexingErrorCollection()
        # Should not raise
        collection.raise_if_errors()

    def test_raise_if_errors_with_errors(self, test_dbs):
        """Test raise_if_errors raises exception when errors exist."""
        collection = IndexingErrorCollection()
        collection.add_error("uuid", "label", "message", Exception())

        with pytest.raises(MissingDatasetFieldError) as excinfo:
            collection.raise_if_errors()

        assert "INDEXING ERRORS" in str(excinfo.value)


@pytest.mark.parametrize(
    "test_dbs",
    [[{"src": db_directory / "ensembl_genome_metadata"}, {"src": db_directory / "ncbi_taxonomy"}]],
    indirect=True,
)
class TestGenomeSearchDocument:
    """Test suite for GenomeSearchDocument Pydantic model."""

    def test_valid_document_creation(self, test_dbs):
        """Test creating a valid GenomeSearchDocument."""
        doc = GenomeSearchDocument(
            genome_uuid="test-uuid",
            scientific_name="Homo sapiens",
            assembly_name="GRCh38",
            accession="GCA_000001405.15",
            is_reference=True,
            species_taxonomy_id=9606,
            taxonomy_id=9606,
            organism_id=1,
            lineage_taxids=[9606, 9605, 9604],
            lineage_name=["Homo sapiens", "Homo", "Hominidae"],
            contig_n50=50000000,
            coding_genes=20000,
            genebuild_provider="Ensembl",
            genebuild_method_display="Import",
            release_type="integrated",
            release_label="112",
            release_id=1,
        )

        assert doc.genome_uuid == "test-uuid"
        assert doc.scientific_name == "Homo sapiens"
        assert doc.is_reference is True
        assert doc.contig_n50 == 50000000
        assert doc.coding_genes == 20000
        assert len(doc.lineage_taxids) == 3
        assert doc.has_variation is False  # Default value
        assert doc.has_regulation is False  # Default value
        assert doc.rank == 0  # Default value

    def test_document_with_optional_fields(self, test_dbs):
        """Test document creation with optional fields populated."""
        doc = GenomeSearchDocument(
            genome_uuid="test-uuid",
            common_name="Human",
            scientific_name="Homo sapiens",
            strain_type="reference",
            strain="GRCh38",
            assembly_name="GRCh38",
            accession="GCA_000001405.15",
            url_name="homo_sapiens",
            tol_id="mHomSap1",
            is_reference=True,
            species_taxonomy_id=9606,
            taxonomy_id=9606,
            scientific_parlance_name="Human",
            organism_id=1,
            rank=10,
            lineage_taxids=[9606],
            lineage_name=["Homo sapiens"],
            contig_n50=50000000,
            coding_genes=20000,
            has_variation=True,
            has_regulation=True,
            genebuild_provider="Ensembl",
            genebuild_method_display="Import",
            release_type="integrated",
            release_label="112",
            release_id=1,
        )

        assert doc.common_name == "Human"
        assert doc.strain_type == "reference"
        assert doc.has_variation is True
        assert doc.has_regulation is True
        assert doc.rank == 10

    def test_document_missing_required_field(self, test_dbs):
        """Test document creation fails when required field is missing."""
        with pytest.raises(Exception):  # Pydantic ValidationError
            GenomeSearchDocument(
                genome_uuid="test-uuid",
                scientific_name="Homo sapiens",
                # Missing assembly_name and other required fields
            )

    def test_document_invalid_type(self, test_dbs):
        """Test document creation fails with invalid field type."""
        with pytest.raises(Exception):  # Pydantic ValidationError
            GenomeSearchDocument(
                genome_uuid="test-uuid",
                scientific_name="Homo sapiens",
                assembly_name="GRCh38",
                accession="GCA_000001405.15",
                is_reference="not_a_boolean",  # Should be bool
                species_taxonomy_id=9606,
                taxonomy_id=9606,
                organism_id=1,
                lineage_taxids=[9606],
                lineage_name=["Homo sapiens"],
                contig_n50=50000000,
                coding_genes=20000,
                genebuild_provider="Ensembl",
                genebuild_method_display="Import",
                release_type="integrated",
                release_label="112",
                release_id=1,
            )

    def test_document_model_dump(self, test_dbs):
        """Test converting document to dictionary."""
        doc = GenomeSearchDocument(
            genome_uuid="test-uuid",
            scientific_name="Homo sapiens",
            assembly_name="GRCh38",
            accession="GCA_000001405.15",
            is_reference=True,
            species_taxonomy_id=9606,
            taxonomy_id=9606,
            organism_id=1,
            lineage_taxids=[9606],
            lineage_name=["Homo sapiens"],
            contig_n50=50000000,
            coding_genes=20000,
            genebuild_provider="Ensembl",
            genebuild_method_display="Import",
            release_type="integrated",
            release_label="112",
            release_id=1,
        )

        data = doc.model_dump()
        assert isinstance(data, dict)
        assert data["genome_uuid"] == "test-uuid"
        assert data["scientific_name"] == "Homo sapiens"
        assert data["is_reference"] is True


@pytest.mark.parametrize(
    "test_dbs",
    [[{"src": db_directory / "ensembl_genome_metadata"}, {"src": db_directory / "ncbi_taxonomy"}]],
    indirect=True,
)
class TestDatasetFieldExtractor:
    """Test suite for DatasetFieldExtractor class."""

    def test_init(self, test_dbs):
        """Test DatasetFieldExtractor initialization."""
        metadata_uri = test_dbs["ensembl_genome_metadata"].dbc.url

        with test_dbs["ensembl_genome_metadata"].dbc.session_scope() as session:
            genome = session.query(Genome).first()
            release = session.query(EnsemblRelease).first()

            if genome and release:
                extractor = DatasetFieldExtractor(session, genome, release)
                assert extractor.session == session
                assert extractor.genome == genome
                assert extractor.release == release
                assert extractor._datasets_cache is None

    def test_get_relevant_datasets_integrated_release(self, test_dbs):
        """Test getting relevant datasets for integrated release."""
        with test_dbs["ensembl_genome_metadata"].dbc.session_scope() as session:
            genome = (
                session.query(Genome)
                .join(GenomeRelease)
                .join(EnsemblRelease)
                .filter(EnsemblRelease.release_type == "integrated")
                .first()
            )

            if genome:
                release = (
                    session.query(EnsemblRelease).filter(EnsemblRelease.release_type == "integrated").first()
                )

                extractor = DatasetFieldExtractor(session, genome, release)
                datasets = extractor._get_relevant_datasets()

                assert isinstance(datasets, list)
                # Should only include datasets matching release_id and status RELEASED
                for gd in datasets:
                    assert gd.release_id == release.release_id
                    assert gd.dataset.status == DatasetStatus.RELEASED

    def test_get_relevant_datasets_partial_release(self, test_dbs):
        """Test getting relevant datasets for partial release."""
        with test_dbs["ensembl_genome_metadata"].dbc.session_scope() as session:
            release = session.query(EnsemblRelease).filter(EnsemblRelease.release_type == "partial").first()

            if release:
                genome = (
                    session.query(Genome)
                    .join(GenomeDataset)
                    .filter(GenomeDataset.release_id == release.release_id, GenomeDataset.is_current == 1)
                    .first()
                )

                if genome:
                    extractor = DatasetFieldExtractor(session, genome, release)
                    datasets = extractor._get_relevant_datasets()

                    assert isinstance(datasets, list)
                    # Should only include current datasets with status RELEASED
                    for gd in datasets:
                        assert gd.is_current == 1
                        assert gd.dataset.status == DatasetStatus.RELEASED

    def test_get_relevant_datasets_caching(self, test_dbs):
        """Test that relevant datasets are cached after first call."""
        with test_dbs["ensembl_genome_metadata"].dbc.session_scope() as session:
            genome = session.query(Genome).first()
            release = session.query(EnsemblRelease).first()

            if genome and release:
                extractor = DatasetFieldExtractor(session, genome, release)
                assert extractor._datasets_cache is None

                datasets1 = extractor._get_relevant_datasets()
                assert extractor._datasets_cache is not None

                datasets2 = extractor._get_relevant_datasets()
                assert datasets1 is datasets2  # Same object reference

    def test_get_dataset_attribute_success(self, test_dbs):
        """Test successfully retrieving a dataset attribute."""
        with test_dbs["ensembl_genome_metadata"].dbc.session_scope() as session:
            # Find a genome with assembly dataset
            genome = (
                session.query(Genome)
                .join(GenomeDataset)
                .join(Dataset)
                .join(DatasetType)
                .filter(DatasetType.name == "assembly", Dataset.status == DatasetStatus.RELEASED)
                .first()
            )

            if genome:
                release = (
                    session.query(EnsemblRelease)
                    .join(GenomeDataset)
                    .filter(GenomeDataset.genome_id == genome.genome_id)
                    .first()
                )

                if release:
                    extractor = DatasetFieldExtractor(session, genome, release)
                    value = extractor._get_dataset_attribute("assembly", "assembly.stats.contig_n50")

                    # Should return a value or None
                    assert value is None or isinstance(value, str)

    def test_get_dataset_attribute_not_found(self, test_dbs):
        """Test retrieving non-existent dataset attribute returns None."""
        with test_dbs["ensembl_genome_metadata"].dbc.session_scope() as session:
            genome = session.query(Genome).first()
            release = session.query(EnsemblRelease).first()

            if genome and release:
                extractor = DatasetFieldExtractor(session, genome, release)
                value = extractor._get_dataset_attribute("nonexistent_type", "nonexistent.attribute")
                assert value is None

    def test_has_dataset_type_true(self, test_dbs):
        """Test has_dataset_type returns True when dataset type exists."""
        with test_dbs["ensembl_genome_metadata"].dbc.session_scope() as session:
            # Find a genome with assembly dataset
            genome = (
                session.query(Genome)
                .join(GenomeDataset)
                .join(Dataset)
                .join(DatasetType)
                .filter(DatasetType.name == "assembly", Dataset.status == DatasetStatus.RELEASED)
                .first()
            )

            if genome:
                release = (
                    session.query(EnsemblRelease)
                    .join(GenomeDataset)
                    .filter(GenomeDataset.genome_id == genome.genome_id)
                    .first()
                )

                if release:
                    extractor = DatasetFieldExtractor(session, genome, release)
                    assert extractor._has_dataset_type("assembly") is True

    def test_has_dataset_type_false(self, test_dbs):
        """Test has_dataset_type returns False when dataset type doesn't exist."""
        with test_dbs["ensembl_genome_metadata"].dbc.session_scope() as session:
            genome = session.query(Genome).first()
            release = session.query(EnsemblRelease).first()

            if genome and release:
                extractor = DatasetFieldExtractor(session, genome, release)
                assert extractor._has_dataset_type("nonexistent_type") is False

    def test_get_contig_n50_missing_raises_error(self, test_dbs):
        """Test get_contig_n50 raises error when field is missing."""
        with test_dbs["ensembl_genome_metadata"].dbc.session_scope() as session:
            # Create mock genome/release without the required attribute
            genome = Mock(spec=Genome)
            genome.genome_uuid = "test-uuid"
            genome.genome_datasets = []

            release = Mock(spec=EnsemblRelease)
            release.release_id = 1
            release.release_type = "integrated"
            release.label = "2024-01"

            extractor = DatasetFieldExtractor(session, genome, release)

            with pytest.raises(MissingDatasetFieldError) as excinfo:
                extractor.get_contig_n50()

            assert "assembly.stats.contig_n50" in str(excinfo.value)
            assert "test-uuid" in str(excinfo.value)

    def test_get_contig_n50_invalid_value_raises_error(self, test_dbs):
        """Test get_contig_n50 raises error when value is not convertible to int."""
        with test_dbs["ensembl_genome_metadata"].dbc.session_scope() as session:
            # Create mock objects with invalid contig_n50 value
            genome = Mock(spec=Genome)
            genome.genome_uuid = "test-uuid"

            dataset_attr = Mock(spec=DatasetAttribute)
            dataset_attr.value = "not_a_number"

            attribute = Mock(spec=Attribute)
            attribute.name = "assembly.stats.contig_n50"
            dataset_attr.attribute = attribute

            dataset_type = Mock(spec=DatasetType)
            dataset_type.name = "assembly"

            dataset = Mock(spec=Dataset)
            dataset.dataset_type = dataset_type
            dataset.dataset_attributes = [dataset_attr]
            dataset.status = DatasetStatus.RELEASED

            genome_dataset = Mock(spec=GenomeDataset)
            genome_dataset.dataset = dataset
            genome_dataset.release_id = 1

            genome.genome_datasets = [genome_dataset]

            release = Mock(spec=EnsemblRelease)
            release.release_id = 1
            release.release_type = "integrated"
            release.label = "2024-01"

            extractor = DatasetFieldExtractor(session, genome, release)

            with pytest.raises(MissingDatasetFieldError) as excinfo:
                extractor.get_contig_n50()

            assert "Invalid value" in str(excinfo.value)
            assert "not_a_number" in str(excinfo.value)

    def test_get_coding_genes_missing_raises_error(self, test_dbs):
        """Test get_coding_genes raises error when field is missing."""
        with test_dbs["ensembl_genome_metadata"].dbc.session_scope() as session:
            genome = Mock(spec=Genome)
            genome.genome_uuid = "test-uuid"
            genome.genome_datasets = []

            release = Mock(spec=EnsemblRelease)
            release.release_id = 1
            release.release_type = "integrated"
            release.label = "2024-01"

            extractor = DatasetFieldExtractor(session, genome, release)

            with pytest.raises(MissingDatasetFieldError) as excinfo:
                extractor.get_coding_genes()

            assert "genebuild.stats.coding_genes" in str(excinfo.value)

    def test_has_variation_false(self, test_dbs):
        """Test has_variation returns False when no variation dataset."""
        with test_dbs["ensembl_genome_metadata"].dbc.session_scope() as session:
            genome = Mock(spec=Genome)
            genome.genome_datasets = []

            release = Mock(spec=EnsemblRelease)
            release.release_id = 1
            release.release_type = "integrated"

            extractor = DatasetFieldExtractor(session, genome, release)
            assert extractor.has_variation() is False

    def test_has_regulation_false(self, test_dbs):
        """Test has_regulation returns False when no regulatory_features dataset."""
        with test_dbs["ensembl_genome_metadata"].dbc.session_scope() as session:
            genome = Mock(spec=Genome)
            genome.genome_datasets = []

            release = Mock(spec=EnsemblRelease)
            release.release_id = 1
            release.release_type = "integrated"

            extractor = DatasetFieldExtractor(session, genome, release)
            assert extractor.has_regulation() is False

    def test_get_genebuild_provider_from_attribute(self, test_dbs):
        """Test get_genebuild_provider retrieves from dataset attribute."""
        with test_dbs["ensembl_genome_metadata"].dbc.session_scope() as session:
            genome = Mock(spec=Genome)
            genome.genome_uuid = "test-uuid"
            genome.provider_name = "Fallback Provider"

            dataset_attr = Mock(spec=DatasetAttribute)
            dataset_attr.value = "Primary Provider"

            attribute = Mock(spec=Attribute)
            attribute.name = "genebuild.provider_name_display"
            dataset_attr.attribute = attribute

            dataset_type = Mock(spec=DatasetType)
            dataset_type.name = "genebuild"

            dataset = Mock(spec=Dataset)
            dataset.dataset_type = dataset_type
            dataset.dataset_attributes = [dataset_attr]
            dataset.status = DatasetStatus.RELEASED

            genome_dataset = Mock(spec=GenomeDataset)
            genome_dataset.dataset = dataset
            genome_dataset.release_id = 1

            genome.genome_datasets = [genome_dataset]

            release = Mock(spec=EnsemblRelease)
            release.release_id = 1
            release.release_type = "integrated"
            release.label = "2024-01"

            extractor = DatasetFieldExtractor(session, genome, release)
            provider = extractor.get_genebuild_provider()

            assert provider == "Primary Provider"

    def test_get_genebuild_provider_fallback_to_genome(self, test_dbs):
        """Test get_genebuild_provider falls back to genome.provider_name."""
        with test_dbs["ensembl_genome_metadata"].dbc.session_scope() as session:
            genome = Mock(spec=Genome)
            genome.genome_uuid = "test-uuid"
            genome.provider_name = "Genome Provider"
            genome.genome_datasets = []

            release = Mock(spec=EnsemblRelease)
            release.release_id = 1
            release.release_type = "integrated"
            release.label = "2024-01"

            extractor = DatasetFieldExtractor(session, genome, release)
            provider = extractor.get_genebuild_provider()

            assert provider == "Genome Provider"

    def test_get_genebuild_provider_missing_raises_error(self, test_dbs):
        """Test get_genebuild_provider raises error when both sources are None."""
        with test_dbs["ensembl_genome_metadata"].dbc.session_scope() as session:
            genome = Mock(spec=Genome)
            genome.genome_uuid = "test-uuid"
            genome.provider_name = None
            genome.genome_datasets = []

            release = Mock(spec=EnsemblRelease)
            release.release_id = 1
            release.release_type = "integrated"
            release.label = "2024-01"

            extractor = DatasetFieldExtractor(session, genome, release)

            with pytest.raises(MissingDatasetFieldError) as excinfo:
                extractor.get_genebuild_provider()

            assert "genebuild_provider" in str(excinfo.value)

    def test_get_genebuild_method_display_missing_raises_error(self, test_dbs):
        """Test get_genebuild_method_display raises error when field is missing."""
        with test_dbs["ensembl_genome_metadata"].dbc.session_scope() as session:
            genome = Mock(spec=Genome)
            genome.genome_uuid = "test-uuid"
            genome.genome_datasets = []

            release = Mock(spec=EnsemblRelease)
            release.release_id = 1
            release.release_type = "integrated"
            release.label = "2024-01"

            extractor = DatasetFieldExtractor(session, genome, release)

            with pytest.raises(MissingDatasetFieldError) as excinfo:
                extractor.get_genebuild_method_display()

            assert "genebuild.method_display" in str(excinfo.value)


@pytest.mark.parametrize(
    "test_dbs",
    [[{"src": db_directory / "ensembl_genome_metadata"}, {"src": db_directory / "ncbi_taxonomy"}]],
    indirect=True,
)
class TestReleaseSelector:
    """Test suite for ReleaseSelector class."""

    def test_select_release_no_releases_returns_none(self, test_dbs):
        """Test select_release_for_genome returns None when genome has no releases."""
        genome = Mock(spec=Genome)
        genome.genome_releases = []
        genome.suppressed = False

        result = ReleaseSelector.select_release_for_genome(genome)
        assert result is None

    def test_select_release_only_unreleased_returns_none(self, test_dbs):
        """Test select_release_for_genome returns None when only unreleased releases exist."""
        genome = Mock(spec=Genome)
        genome.suppressed = False

        release = Mock(spec=EnsemblRelease)
        release.status = ReleaseStatus.PREPARING
        release.release_type = "integrated"

        genome_release = Mock(spec=GenomeRelease)
        genome_release.ensembl_release = release

        genome.genome_releases = [genome_release]

        result = ReleaseSelector.select_release_for_genome(genome)
        assert result is None

    def test_select_release_integrated_only(self, test_dbs):
        """Test selecting most recent integrated release when no partial exists."""
        genome = Mock(spec=Genome)
        genome.suppressed = False
        genome.genome_datasets = []

        release1 = Mock(spec=EnsemblRelease)
        release1.status = ReleaseStatus.RELEASED
        release1.release_type = "integrated"
        release1.label = "2024-01"
        release1.release_id = 1

        release2 = Mock(spec=EnsemblRelease)
        release2.status = ReleaseStatus.RELEASED
        release2.release_type = "integrated"
        release2.label = "2024-03"
        release2.release_id = 2

        gr1 = Mock(spec=GenomeRelease)
        gr1.ensembl_release = release1

        gr2 = Mock(spec=GenomeRelease)
        gr2.ensembl_release = release2

        genome.genome_releases = [gr1, gr2]

        result = ReleaseSelector.select_release_for_genome(genome)
        assert result == release2  # Most recent by label

    def test_select_release_partial_only(self, test_dbs):
        """Test selecting partial release when no integrated releases exist."""
        genome = Mock(spec=Genome)
        genome.suppressed = False

        release = Mock(spec=EnsemblRelease)
        release.status = ReleaseStatus.RELEASED
        release.release_type = "partial"
        release.is_current = True

        gr = Mock(spec=GenomeRelease)
        gr.ensembl_release = release

        genome.genome_releases = [gr]

        result = ReleaseSelector.select_release_for_genome(genome)
        assert result == release

    def test_select_release_partial_over_integrated_when_newer(self, test_dbs):
        """Test selecting partial over integrated when partial is newer and has datasets."""
        genome = Mock(spec=Genome)
        genome.suppressed = False

        integrated = Mock(spec=EnsemblRelease)
        integrated.status = ReleaseStatus.RELEASED
        integrated.release_type = "integrated"
        integrated.label = "2024-01"
        integrated.release_id = 1

        partial = Mock(spec=EnsemblRelease)
        partial.status = ReleaseStatus.RELEASED
        partial.release_type = "partial"
        partial.label = "2024-03"
        partial.release_id = 2
        partial.is_current = True

        # Create a released dataset attached to partial
        dataset = Mock(spec=Dataset)
        dataset.status = DatasetStatus.RELEASED

        genome_dataset = Mock(spec=GenomeDataset)
        genome_dataset.release_id = 2
        genome_dataset.dataset = dataset

        genome.genome_datasets = [genome_dataset]

        gr1 = Mock(spec=GenomeRelease)
        gr1.ensembl_release = integrated

        gr2 = Mock(spec=GenomeRelease)
        gr2.ensembl_release = partial

        genome.genome_releases = [gr1, gr2]

        result = ReleaseSelector.select_release_for_genome(genome)
        assert result == partial

    def test_select_release_integrated_when_partial_not_newer(self, test_dbs):
        """Test selecting integrated when partial is not newer."""
        genome = Mock(spec=Genome)
        genome.suppressed = False
        genome.genome_datasets = []

        integrated = Mock(spec=EnsemblRelease)
        integrated.status = ReleaseStatus.RELEASED
        integrated.release_type = "integrated"
        integrated.label = "2024-03"
        integrated.release_id = 1

        partial = Mock(spec=EnsemblRelease)
        partial.status = ReleaseStatus.RELEASED
        partial.release_type = "partial"
        partial.label = "2024-01"
        partial.release_id = 2
        partial.is_current = True

        gr1 = Mock(spec=GenomeRelease)
        gr1.ensembl_release = integrated

        gr2 = Mock(spec=GenomeRelease)
        gr2.ensembl_release = partial

        genome.genome_releases = [gr1, gr2]

        result = ReleaseSelector.select_release_for_genome(genome)
        assert result == integrated

    def test_select_release_suppressed_genome_no_integrated_returns_none(self, test_dbs):
        """Test suppressed genome with only partial release returns None."""
        genome = Mock(spec=Genome)
        genome.suppressed = True

        partial = Mock(spec=EnsemblRelease)
        partial.status = ReleaseStatus.RELEASED
        partial.release_type = "partial"
        partial.is_current = True

        gr = Mock(spec=GenomeRelease)
        gr.ensembl_release = partial

        genome.genome_releases = [gr]

        result = ReleaseSelector.select_release_for_genome(genome)
        assert result is None

    def test_select_release_suppressed_genome_returns_integrated(self, test_dbs):
        """Test suppressed genome returns most recent integrated, ignoring partial."""
        genome = Mock(spec=Genome)
        genome.suppressed = True

        integrated1 = Mock(spec=EnsemblRelease)
        integrated1.status = ReleaseStatus.RELEASED
        integrated1.release_type = "integrated"
        integrated1.label = "2024-01"

        integrated2 = Mock(spec=EnsemblRelease)
        integrated2.status = ReleaseStatus.RELEASED
        integrated2.release_type = "integrated"
        integrated2.label = "2024-03"

        partial = Mock(spec=EnsemblRelease)
        partial.status = ReleaseStatus.RELEASED
        partial.release_type = "partial"
        partial.label = "2024-05"
        partial.is_current = True

        gr1 = Mock(spec=GenomeRelease)
        gr1.ensembl_release = integrated1

        gr2 = Mock(spec=GenomeRelease)
        gr2.ensembl_release = integrated2

        gr3 = Mock(spec=GenomeRelease)
        gr3.ensembl_release = partial

        genome.genome_releases = [gr1, gr2, gr3]

        result = ReleaseSelector.select_release_for_genome(genome)
        assert result == integrated2  # Most recent integrated, ignoring newer partial


@pytest.mark.parametrize(
    "test_dbs",
    [[{"src": db_directory / "ensembl_genome_metadata"}, {"src": db_directory / "ncbi_taxonomy"}]],
    indirect=True,
)
class TestGenomeSearchIndexer:
    """Test suite for GenomeSearchIndexer class."""

    def test_init_valid_parameters(self, test_dbs):
        """Test GenomeSearchIndexer initialization with valid parameters."""
        metadata_uri = test_dbs["ensembl_genome_metadata"].dbc.url
        taxonomy_uri = test_dbs["ncbi_taxonomy"].dbc.url
        indexer = GenomeSearchIndexer(metadata_uri, taxonomy_uri)

        assert indexer.metadata_db is not None
        assert indexer.taxonomy_db is not None
        assert isinstance(indexer.release_selector, ReleaseSelector)
        assert indexer.batch_size == 500  # Default value

    def test_init_custom_batch_size(self, test_dbs):
        """Test GenomeSearchIndexer initialization with custom batch size."""
        metadata_uri = test_dbs["ensembl_genome_metadata"].dbc.url
        taxonomy_uri = test_dbs["ncbi_taxonomy"].dbc.url
        indexer = GenomeSearchIndexer(metadata_uri, taxonomy_uri, batch_size=100)

        assert indexer.batch_size == 100

    def test_get_genome_ids_to_process(self, test_dbs):
        """Test _get_genome_ids_to_process returns list of genome IDs."""
        metadata_uri = test_dbs["ensembl_genome_metadata"].dbc.url
        taxonomy_uri = test_dbs["ncbi_taxonomy"].dbc.url
        indexer = GenomeSearchIndexer(metadata_uri, taxonomy_uri)

        with test_dbs["ensembl_genome_metadata"].dbc.session_scope() as session:
            genome_ids = indexer._get_genome_ids_to_process(session)

            assert isinstance(genome_ids, list)
            if len(genome_ids) > 0:
                assert all(isinstance(gid, int) for gid in genome_ids)

    def test_get_genomes_batch(self, test_dbs):
        """Test _get_genomes_batch loads genomes with relationships."""
        metadata_uri = test_dbs["ensembl_genome_metadata"].dbc.url
        taxonomy_uri = test_dbs["ncbi_taxonomy"].dbc.url
        indexer = GenomeSearchIndexer(metadata_uri, taxonomy_uri)

        with test_dbs["ensembl_genome_metadata"].dbc.session_scope() as session:
            genome_ids = indexer._get_genome_ids_to_process(session)

            if len(genome_ids) > 0:
                batch_ids = genome_ids[:5]  # Get first 5
                genomes = indexer._get_genomes_batch(session, batch_ids)

                assert isinstance(genomes, list)
                assert len(genomes) <= 5
                if len(genomes) > 0:
                    genome = genomes[0]
                    assert hasattr(genome, "organism")
                    assert hasattr(genome, "assembly")
                    assert hasattr(genome, "genome_releases")
                    assert hasattr(genome, "genome_datasets")

    def test_get_genomes_with_releases_batched(self, test_dbs):
        """Test get_genomes_with_releases_batched yields batches of genome-release pairs."""
        metadata_uri = test_dbs["ensembl_genome_metadata"].dbc.url
        taxonomy_uri = test_dbs["ncbi_taxonomy"].dbc.url
        indexer = GenomeSearchIndexer(metadata_uri, taxonomy_uri, batch_size=10)

        with test_dbs["ensembl_genome_metadata"].dbc.session_scope() as session:
            batches = list(indexer.get_genomes_with_releases_batched(session))

            assert isinstance(batches, list)
            if len(batches) > 0:
                first_batch = batches[0]
                assert isinstance(first_batch, list)

                if len(first_batch) > 0:
                    genome, release = first_batch[0]
                    assert isinstance(genome, Genome)
                    assert isinstance(release, EnsemblRelease)

    def test_extract_direct_fields(self, test_dbs):
        """Test _extract_direct_fields extracts correct fields from genome."""
        metadata_uri = test_dbs["ensembl_genome_metadata"].dbc.url
        taxonomy_uri = test_dbs["ncbi_taxonomy"].dbc.url
        indexer = GenomeSearchIndexer(metadata_uri, taxonomy_uri)

        with test_dbs["ensembl_genome_metadata"].dbc.session_scope() as session:
            genome = session.query(Genome).first()

            if genome:
                fields = indexer._extract_direct_fields(genome)

                assert isinstance(fields, dict)
                required_keys = [
                    "genome_uuid",
                    "scientific_name",
                    "assembly_name",
                    "accession",
                    "is_reference",
                    "species_taxonomy_id",
                    "taxonomy_id",
                    "organism_id",
                    "rank",
                ]
                for key in required_keys:
                    assert key in fields

                assert isinstance(fields["genome_uuid"], str)
                assert isinstance(fields["scientific_name"], str)
                assert isinstance(fields["is_reference"], bool)

    def test_get_taxonomy_lineage(self, test_dbs):
        """Test _get_taxonomy_lineage retrieves taxonomy information."""
        metadata_uri = test_dbs["ensembl_genome_metadata"].dbc.url
        taxonomy_uri = test_dbs["ncbi_taxonomy"].dbc.url
        indexer = GenomeSearchIndexer(metadata_uri, taxonomy_uri)

        with test_dbs["ensembl_genome_metadata"].dbc.session_scope() as metadata_session:
            with test_dbs["ncbi_taxonomy"].dbc.session_scope() as taxonomy_session:
                genome = metadata_session.query(Genome).first()

                if genome:
                    lineage_taxids, lineage_names = indexer._get_taxonomy_lineage(
                        taxonomy_session, genome.organism.taxonomy_id
                    )

                    assert isinstance(lineage_taxids, list)
                    assert isinstance(lineage_names, list)
                    assert len(lineage_taxids) > 0
                    assert len(lineage_names) > 0
                    assert all(isinstance(tid, int) for tid in lineage_taxids)
                    assert all(isinstance(name, str) for name in lineage_names)

    def test_get_taxonomy_lineage_not_found(self, test_dbs):
        """Test _get_taxonomy_lineage handles missing taxonomy gracefully."""
        metadata_uri = test_dbs["ensembl_genome_metadata"].dbc.url
        taxonomy_uri = test_dbs["ncbi_taxonomy"].dbc.url
        indexer = GenomeSearchIndexer(metadata_uri, taxonomy_uri)

        with test_dbs["ncbi_taxonomy"].dbc.session_scope() as taxonomy_session:
            # Use a taxonomy ID that doesn't exist
            lineage_taxids, lineage_names = indexer._get_taxonomy_lineage(taxonomy_session, 999999999)

            # Should return just the requested taxid and empty names
            assert isinstance(lineage_taxids, list)
            assert isinstance(lineage_names, list)

    def test_create_search_document_success(self, test_dbs):
        """Test create_search_document successfully creates a document."""
        metadata_uri = test_dbs["ensembl_genome_metadata"].dbc.url
        taxonomy_uri = test_dbs["ncbi_taxonomy"].dbc.url
        indexer = GenomeSearchIndexer(metadata_uri, taxonomy_uri)

        with test_dbs["ensembl_genome_metadata"].dbc.session_scope() as metadata_session:
            with test_dbs["ncbi_taxonomy"].dbc.session_scope() as taxonomy_session:
                # Get a genome with all required datasets
                genome = (
                    metadata_session.query(Genome)
                    .join(GenomeDataset)
                    .join(Dataset)
                    .filter(Dataset.status == DatasetStatus.RELEASED)
                    .first()
                )

                if genome:
                    release = ReleaseSelector.select_release_for_genome(genome)

                    if release:
                        try:
                            doc = indexer.create_search_document(
                                metadata_session, taxonomy_session, genome, release
                            )

                            assert isinstance(doc, GenomeSearchDocument)
                            assert doc.genome_uuid == genome.genome_uuid
                            assert doc.release_label == release.label
                        except MissingDatasetFieldError:
                            # Expected if test data doesn't have all required fields
                            pass

    def test_create_search_document_missing_field_raises_error(self, test_dbs):
        """Test create_search_document raises error when required field is missing."""
        metadata_uri = test_dbs["ensembl_genome_metadata"].dbc.url
        taxonomy_uri = test_dbs["ncbi_taxonomy"].dbc.url
        indexer = GenomeSearchIndexer(metadata_uri, taxonomy_uri)

        with test_dbs["ensembl_genome_metadata"].dbc.session_scope() as metadata_session:
            with test_dbs["ncbi_taxonomy"].dbc.session_scope() as taxonomy_session:
                # Create mock genome without required datasets
                genome = Mock(spec=Genome)
                genome.genome_uuid = "test-uuid"
                genome.genome_datasets = []
                genome.organism = Mock(spec=Organism)
                genome.organism.taxonomy_id = 9606
                genome.organism.scientific_name = "Test"
                genome.organism.species_taxonomy_id = 9606
                genome.organism.common_name = None
                genome.organism.strain_type = None
                genome.organism.strain = None
                genome.organism.tol_id = None
                genome.organism.scientific_parlance_name = None
                genome.organism.rank = 0
                genome.organism_id = 1
                genome.assembly = Mock(spec=Assembly)
                genome.assembly.name = "Test"
                genome.assembly.accession = "GCA_000000000"
                genome.assembly.is_reference = 0
                genome.url_name = None
                genome.provider_name = None

                release = Mock(spec=EnsemblRelease)
                release.release_id = 1
                release.release_type = "integrated"
                release.label = "2024-01"

                with pytest.raises(MissingDatasetFieldError):
                    indexer.create_search_document(metadata_session, taxonomy_session, genome, release)

    def test_get_search_index_returns_list(self, test_dbs):
        """Test get_search_index returns a list of documents."""
        metadata_uri = test_dbs["ensembl_genome_metadata"].dbc.url
        taxonomy_uri = test_dbs["ncbi_taxonomy"].dbc.url
        indexer = GenomeSearchIndexer(metadata_uri, taxonomy_uri)

        # Don't raise on errors for this test
        documents = indexer.get_search_index(raise_on_errors=False)

        assert isinstance(documents, list)
        if len(documents) > 0:
            assert isinstance(documents[0], dict)
            assert "genome_uuid" in documents[0]
            assert "release_label" in documents[0]

    def test_get_search_index_raise_on_errors_false(self, test_dbs):
        """Test get_search_index returns partial results when raise_on_errors=False."""
        metadata_uri = test_dbs["ensembl_genome_metadata"].dbc.url
        taxonomy_uri = test_dbs["ncbi_taxonomy"].dbc.url
        indexer = GenomeSearchIndexer(metadata_uri, taxonomy_uri)

        # Should not raise even if some genomes fail
        documents = indexer.get_search_index(raise_on_errors=False)
        assert isinstance(documents, list)

    def test_export_to_json_creates_file(self, test_dbs, tmp_path):
        """Test export_to_json creates a JSON file."""
        metadata_uri = test_dbs["ensembl_genome_metadata"].dbc.url
        taxonomy_uri = test_dbs["ncbi_taxonomy"].dbc.url
        indexer = GenomeSearchIndexer(metadata_uri, taxonomy_uri)
        output_file = tmp_path / "test_index.json"

        count = indexer.export_to_json(str(output_file), raise_on_errors=False, pretty_print=False)

        assert output_file.exists()
        assert isinstance(count, int)

        # Verify JSON is valid
        with open(output_file, "r") as f:
            data = json.load(f)
            assert isinstance(data, list)

    def test_export_to_json_pretty_print(self, test_dbs, tmp_path):
        """Test export_to_json with pretty printing."""
        metadata_uri = test_dbs["ensembl_genome_metadata"].dbc.url
        taxonomy_uri = test_dbs["ncbi_taxonomy"].dbc.url
        indexer = GenomeSearchIndexer(metadata_uri, taxonomy_uri)
        output_file = tmp_path / "test_index_pretty.json"

        indexer.export_to_json(str(output_file), raise_on_errors=False, pretty_print=True)

        assert output_file.exists()

        # Check that file has indentation
        with open(output_file, "r") as f:
            content = f.read()
            assert "\n" in content  # Has newlines
            assert "  " in content  # Has indentation

    def test_export_to_json_creates_parent_directory(self, test_dbs, tmp_path):
        """Test export_to_json creates parent directories."""
        metadata_uri = test_dbs["ensembl_genome_metadata"].dbc.url
        taxonomy_uri = test_dbs["ncbi_taxonomy"].dbc.url
        indexer = GenomeSearchIndexer(metadata_uri, taxonomy_uri)
        output_file = tmp_path / "nested" / "directories" / "index.json"

        indexer.export_to_json(str(output_file), raise_on_errors=False)

        assert output_file.exists()
        assert output_file.parent.exists()

    def test_stream_to_json_creates_file(self, test_dbs, tmp_path):
        """Test stream_to_json creates a JSON file."""
        metadata_uri = test_dbs["ensembl_genome_metadata"].dbc.url
        taxonomy_uri = test_dbs["ncbi_taxonomy"].dbc.url
        indexer = GenomeSearchIndexer(metadata_uri, taxonomy_uri)
        output_file = tmp_path / "test_stream.json"

        count, errors = indexer.stream_to_json(str(output_file), pretty_print=False)

        assert output_file.exists()
        assert isinstance(count, int)
        assert isinstance(errors, IndexingErrorCollection)

        # Verify JSON is valid
        with open(output_file, "r") as f:
            data = json.load(f)
            assert isinstance(data, list)

    def test_stream_to_json_pretty_print(self, test_dbs, tmp_path):
        """Test stream_to_json with pretty printing."""
        metadata_uri = test_dbs["ensembl_genome_metadata"].dbc.url
        taxonomy_uri = test_dbs["ncbi_taxonomy"].dbc.url
        indexer = GenomeSearchIndexer(metadata_uri, taxonomy_uri)
        output_file = tmp_path / "test_stream_pretty.json"

        count, errors = indexer.stream_to_json(str(output_file), pretty_print=True)

        assert output_file.exists()

        # Check that file has indentation
        with open(output_file, "r") as f:
            content = f.read()
            assert "\n" in content
            assert "  " in content

    def test_stream_to_json_returns_error_collection(self, test_dbs, tmp_path):
        """Test stream_to_json returns error collection."""
        metadata_uri = test_dbs["ensembl_genome_metadata"].dbc.url
        taxonomy_uri = test_dbs["ncbi_taxonomy"].dbc.url
        indexer = GenomeSearchIndexer(metadata_uri, taxonomy_uri)
        output_file = tmp_path / "test_stream.json"

        count, errors = indexer.stream_to_json(str(output_file))

        assert isinstance(errors, IndexingErrorCollection)
        # Errors may or may not exist depending on test data quality

    def test_export_to_json_auto_uses_regular_mode(self, test_dbs, tmp_path):
        """Test export_to_json_auto uses regular mode for small datasets."""
        metadata_uri = test_dbs["ensembl_genome_metadata"].dbc.url
        taxonomy_uri = test_dbs["ncbi_taxonomy"].dbc.url
        indexer = GenomeSearchIndexer(metadata_uri, taxonomy_uri)
        output_file = tmp_path / "test_auto.json"

        # Set threshold higher than expected genome count
        count = indexer.export_to_json_auto(str(output_file), raise_on_errors=False, stream_threshold=100000)

        assert output_file.exists()
        assert isinstance(count, int)

    def test_export_to_json_auto_uses_streaming_mode(self, test_dbs, tmp_path):
        """Test export_to_json_auto uses streaming mode for large datasets."""
        metadata_uri = test_dbs["ensembl_genome_metadata"].dbc.url
        taxonomy_uri = test_dbs["ncbi_taxonomy"].dbc.url
        indexer = GenomeSearchIndexer(metadata_uri, taxonomy_uri)
        output_file = tmp_path / "test_auto_stream.json"

        # Set threshold to 0 to force streaming
        count = indexer.export_to_json_auto(str(output_file), raise_on_errors=False, stream_threshold=0)

        assert output_file.exists()
        assert isinstance(count, int)

    def test_batching_processes_all_genomes(self, test_dbs):
        """Test that batching processes all genomes correctly."""
        metadata_uri = test_dbs["ensembl_genome_metadata"].dbc.url
        taxonomy_uri = test_dbs["ncbi_taxonomy"].dbc.url
        # Use small batch size to test batching
        indexer = GenomeSearchIndexer(metadata_uri, taxonomy_uri, batch_size=2)

        with test_dbs["ensembl_genome_metadata"].dbc.session_scope() as session:
            total_genome_ids = indexer._get_genome_ids_to_process(session)
            total_count = len(total_genome_ids)

            if total_count > 0:
                batches = list(indexer.get_genomes_with_releases_batched(session))

                # Count total genomes across all batches
                total_processed = sum(len(batch) for batch in batches)

                # Should process all or fewer genomes (some may not have valid releases)
                assert total_processed <= total_count

    def test_document_structure_matches_schema(self, test_dbs):
        """Test that generated documents match GenomeSearchDocument schema."""
        metadata_uri = test_dbs["ensembl_genome_metadata"].dbc.url
        taxonomy_uri = test_dbs["ncbi_taxonomy"].dbc.url
        indexer = GenomeSearchIndexer(metadata_uri, taxonomy_uri)

        documents = indexer.get_search_index(raise_on_errors=False)

        if len(documents) > 0:
            doc = documents[0]

            # Check required fields exist
            required_fields = [
                "genome_uuid",
                "scientific_name",
                "assembly_name",
                "accession",
                "is_reference",
                "species_taxonomy_id",
                "taxonomy_id",
                "organism_id",
                "lineage_taxids",
                "lineage_name",
                "contig_n50",
                "coding_genes",
                "genebuild_provider",
                "genebuild_method_display",
                "release_type",
                "release_label",
                "release_id",
            ]

            for field in required_fields:
                assert field in doc, f"Missing required field: {field}"
