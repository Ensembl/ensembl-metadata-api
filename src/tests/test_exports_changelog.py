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

import csv
from pathlib import Path

import pytest

from ensembl.production.metadata.api.exports.changelog_generator import ChangelogGenerator
from ensembl.production.metadata.api.models import EnsemblRelease

db_directory = Path(__file__).parent / 'databases'
db_directory = db_directory.resolve()


@pytest.mark.parametrize(
    "test_dbs",
    [[{'src': db_directory / "ensembl_genome_metadata"}, {'src': db_directory / "ncbi_taxonomy"}]],
    indirect=True,
)
class TestChangelogGenerator:
    """Test suite for ChangelogGenerator class."""

    def test_init_valid_parameters(self, test_dbs):
        """Test ChangelogGenerator initialization with valid parameters."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        generator = ChangelogGenerator(
            metadata_uri=metadata_uri,
            release_label="2024-01"
        )
        assert generator.metadata_db is not None
        assert generator.release_label == "2024-01"
        assert generator.output_path is None

    def test_init_with_output_path(self, test_dbs):
        """Test ChangelogGenerator initialization with custom output path."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        output_path = "/tmp/test_changelog.csv"

        generator = ChangelogGenerator(
            metadata_uri=metadata_uri,
            release_label="2024-01",
            output_path=output_path
        )
        assert generator.metadata_db is not None
        assert generator.release_label == "2024-01"
        assert generator.output_path == output_path

    def test_init_invalid_metadata_uri_empty(self, test_dbs):
        """Test initialization fails with empty metadata URI."""
        with pytest.raises(ValueError) as excinfo:
            ChangelogGenerator(
                metadata_uri="",
                release_label="2024-01"
            )
        assert "metadata_uri must be a non-empty string" in str(excinfo.value)

    def test_init_invalid_metadata_uri_none(self, test_dbs):
        """Test initialization fails with None metadata URI."""
        with pytest.raises(ValueError) as excinfo:
            ChangelogGenerator(
                metadata_uri=None,
                release_label="2024-01"
            )
        assert "metadata_uri must be a non-empty string" in str(excinfo.value)

    def test_init_invalid_metadata_uri_not_string(self, test_dbs):
        """Test initialization fails with non-string metadata URI."""
        with pytest.raises(ValueError) as excinfo:
            ChangelogGenerator(
                metadata_uri=123,
                release_label="2024-01"
            )
        assert "metadata_uri must be a non-empty string" in str(excinfo.value)

    def test_init_invalid_release_label_empty(self, test_dbs):
        """Test initialization fails with empty release label."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url

        with pytest.raises(ValueError) as excinfo:
            ChangelogGenerator(
                metadata_uri=metadata_uri,
                release_label=""
            )
        assert "release_label must be a non-empty string" in str(excinfo.value)

    def test_init_invalid_release_label_none(self, test_dbs):
        """Test initialization fails with None release label."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url

        with pytest.raises(ValueError) as excinfo:
            ChangelogGenerator(
                metadata_uri=metadata_uri,
                release_label=None
            )
        assert "release_label must be a non-empty string" in str(excinfo.value)

    def test_init_invalid_release_label_not_string(self, test_dbs):
        """Test initialization fails with non-string release label."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        with pytest.raises(ValueError) as excinfo:
            ChangelogGenerator(
                metadata_uri=metadata_uri,
                release_label=123
            )
        assert "release_label must be a non-empty string" in str(excinfo.value)

    def test_verify_release_exists(self, test_dbs):
        """Test verify_release returns correct type for existing release."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        with test_dbs['ensembl_genome_metadata'].dbc.session_scope() as session:
            release = session.query(EnsemblRelease).first()
            if release:
                generator = ChangelogGenerator(
                    metadata_uri=metadata_uri,
                    release_label=release.label
                )
                release_type = generator.verify_release()
                assert release_type in ['partial', 'integrated']
                assert release_type == release.release_type

    def test_verify_release_not_found(self, test_dbs):
        """Test verify_release raises error for non-existent release."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        generator = ChangelogGenerator(
            metadata_uri=metadata_uri,
            release_label="nonexistent-release-99999"
        )
        with pytest.raises(ValueError) as excinfo:
            generator.verify_release()
        assert "Release not found" in str(excinfo.value)
        assert "nonexistent-release-99999" in str(excinfo.value)

    def test_gather_partial_data_structure(self, test_dbs):
        """Test gather_partial_data returns correct structure."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url

        with test_dbs['ensembl_genome_metadata'].dbc.session_scope() as session:
            partial_release = session.query(EnsemblRelease).filter(
                EnsemblRelease.release_type == 'partial'
            ).first()

            generator = ChangelogGenerator(
                metadata_uri=metadata_uri,
                release_label=partial_release.label
            )

            data = generator.gather_partial_data()

            assert isinstance(data, list)
            first_entry = data[0]
            required_keys = [
                'scientific_name', 'common_name', 'assembly_name',
                'assembly_accession', 'annotation_provider',
                'geneset_updated', 'variation_updated', 'regulation_updated'
            ]
            for key in required_keys:
                assert key in first_entry, f"Missing key: {key}"
            assert isinstance(first_entry['scientific_name'], str)
            assert first_entry['common_name'] is None or isinstance(first_entry['common_name'], str)
            assert isinstance(first_entry['assembly_name'], str)
            assert isinstance(first_entry['assembly_accession'], str)
            assert first_entry['annotation_provider'] is None or isinstance(first_entry['annotation_provider'],
                                                                            str)
            assert isinstance(first_entry['geneset_updated'], int)
            assert isinstance(first_entry['variation_updated'], int)
            assert isinstance(first_entry['regulation_updated'], int)
            assert first_entry['geneset_updated'] in [0, 1]
            assert first_entry['variation_updated'] in [0, 1]
            assert first_entry['regulation_updated'] in [0, 1]

    def test_gather_integrated_data_structure(self, test_dbs):
        """Test gather_integrated_data returns correct structure."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        with test_dbs['ensembl_genome_metadata'].dbc.session_scope() as session:
            integrated_release = session.query(EnsemblRelease).filter(
                EnsemblRelease.release_type == 'integrated'
            ).first()
            if integrated_release:
                generator = ChangelogGenerator(
                    metadata_uri=metadata_uri,
                    release_label=integrated_release.label
                )
                data = generator.gather_integrated_data()
                assert isinstance(data, list)
                if len(data) > 0:
                    first_entry = data[0]
                    required_keys = [
                        'scientific_name', 'common_name', 'assembly_name',
                        'assembly_accession', 'annotation_provider',
                        'geneset_updated', 'variation_updated', 'regulation_updated',
                        'status'
                    ]
                    for key in required_keys:
                        assert key in first_entry, f"Missing key: {key}"
                    assert first_entry['status'] in ['New', 'Removed', 'Updated', 'Unchanged']

    def test_get_annotation_sources_bulk(self, test_dbs):
        """Test _get_annotation_sources_bulk retrieves annotation sources."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        with test_dbs['ensembl_genome_metadata'].dbc.session_scope() as session:
            partial_release = session.query(EnsemblRelease).filter(
                EnsemblRelease.release_type == 'partial'
            ).first()
            generator = ChangelogGenerator(
                metadata_uri=metadata_uri,
                release_label=partial_release.label
            )
            from ensembl.production.metadata.api.models import GenomeDataset
            genome_ids = [gr.genome_id for gr in session.query(GenomeDataset.genome_id).filter(
                GenomeDataset.release_id == partial_release.release_id
            ).distinct().limit(5).all()]
            if genome_ids:
                annotation_sources = generator._get_annotation_sources_bulk(
                    session, genome_ids
                )
                assert isinstance(annotation_sources, dict)
                for genome_id in annotation_sources.keys():
                    assert isinstance(genome_id, int)
                for source in annotation_sources.values():
                    assert source is None or isinstance(source, str)

    def test_get_annotation_sources_bulk_empty_list(self, test_dbs):
        """Test _get_annotation_sources_bulk handles empty genome list."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        with test_dbs['ensembl_genome_metadata'].dbc.session_scope() as session:
            partial_release = session.query(EnsemblRelease).filter(
                EnsemblRelease.release_type == 'partial'
            ).first()
            if partial_release:
                generator = ChangelogGenerator(
                    metadata_uri=metadata_uri,
                    release_label=partial_release.label
                )
                annotation_sources = generator._get_annotation_sources_bulk(session, [])
                assert isinstance(annotation_sources, dict)
                assert len(annotation_sources) == 0

    def test_export_to_csv_partial_release(self, test_dbs, tmp_path):
        """Test export_to_csv creates file with correct structure for partial release."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        output_file = tmp_path / "test_changelog.csv"
        generator = ChangelogGenerator(
            metadata_uri=metadata_uri,
            release_label="2024-01",
            output_path=str(output_file)
        )

        sample_data = [
            {
                'scientific_name': 'homo sapiens',
                'common_name': 'human',
                'assembly_name': 'GRCh38',
                'assembly_accession': 'GCA_000001405.15',
                'annotation_provider': 'Ensembl',
                'geneset_updated': 1,
                'variation_updated': 0,
                'regulation_updated': 1
            }
        ]
        generator.export_to_csv(sample_data)

        assert output_file.exists()
        with open(output_file, 'r') as f:
            lines = f.readlines()
            assert lines[0].startswith('# Changelog for release')
            assert '2024-01' in lines[0]
            reader = csv.DictReader(lines[1:])
            rows = list(reader)
            assert len(rows) == 1
            assert rows[0]['scientific_name'] == 'homo sapiens'
            assert rows[0]['geneset_updated'] == '1'
            assert rows[0]['variation_updated'] == '0'
            assert 'status' not in rows[0]  # Partial releases don't have status

    def test_export_to_csv_integrated_release(self, test_dbs, tmp_path):
        """Test export_to_csv creates file with correct structure for integrated release."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        output_file = tmp_path / "test_changelog_integrated.csv"
        generator = ChangelogGenerator(
            metadata_uri=metadata_uri,
            release_label="112",
            output_path=str(output_file)
        )
        sample_data = [
            {
                'scientific_name': 'homo sapiens',
                'common_name': 'human',
                'assembly_name': 'GRCh38',
                'assembly_accession': 'GCA_000001405.15',
                'annotation_provider': 'Ensembl',
                'geneset_updated': '2024-01',
                'variation_updated': None,
                'regulation_updated': '2024-01',
                'status': 'Updated'
            }
        ]
        generator.export_to_csv(sample_data)
        assert output_file.exists()
        with open(output_file, 'r') as f:
            lines = f.readlines()
            assert lines[0].startswith('# Changelog for release')
            reader = csv.DictReader(lines[1:])
            rows = list(reader)
            assert len(rows) == 1
            assert rows[0]['scientific_name'] == 'homo sapiens'
            assert rows[0]['status'] == 'Updated'  # Integrated releases have status

    def test_export_to_csv_default_output_path(self, test_dbs, tmp_path, monkeypatch):
        """Test export_to_csv uses default output path when none specified."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        monkeypatch.chdir(tmp_path)
        generator = ChangelogGenerator(
            metadata_uri=metadata_uri,
            release_label="2024-01"
        )
        sample_data = [
            {
                'scientific_name': 'test species',
                'common_name': 'test',
                'assembly_name': 'test',
                'assembly_accession': 'test',
                'annotation_provider': 'test',
                'geneset_updated': 0,
                'variation_updated': 0,
                'regulation_updated': 0
            }
        ]
        generator.export_to_csv(sample_data)
        default_file = tmp_path / "2024-01.csv"
        assert default_file.exists()

    def test_export_to_csv_empty_data(self, test_dbs, tmp_path):
        """Test export_to_csv handles empty data correctly."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        output_file = tmp_path / "test_empty.csv"
        generator = ChangelogGenerator(
            metadata_uri=metadata_uri,
            release_label="2024-01",
            output_path=str(output_file)
        )
        generator.export_to_csv([])
        assert output_file.exists()
        with open(output_file, 'r') as f:
            lines = f.readlines()
            assert lines[0].startswith('# Changelog for release')
            assert len(lines) >= 2

    def test_export_to_csv_creates_parent_directory(self, test_dbs, tmp_path):
        """Test export_to_csv creates parent directories if they don't exist."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        output_file = tmp_path / "nested" / "directories" / "changelog.csv"
        generator = ChangelogGenerator(
            metadata_uri=metadata_uri,
            release_label="2024-01",
            output_path=str(output_file)
        )
        sample_data = [
            {
                'scientific_name': 'test',
                'common_name': 'test',
                'assembly_name': 'test',
                'assembly_accession': 'test',
                'annotation_provider': 'test',
                'geneset_updated': 0,
                'variation_updated': 0,
                'regulation_updated': 0
            }
        ]
        generator.export_to_csv(sample_data)
        assert output_file.exists()
        assert output_file.parent.exists()

    def test_generate_partial_release(self, test_dbs, tmp_path):
        """Test generate method works end-to-end for partial release."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        with test_dbs['ensembl_genome_metadata'].dbc.session_scope() as session:
            partial_release = session.query(EnsemblRelease).filter(
                EnsemblRelease.release_type == 'partial'
            ).first()
            output_file = tmp_path / "test_generate.csv"
            generator = ChangelogGenerator(
                metadata_uri=metadata_uri,
                release_label=partial_release.label,
                output_path=str(output_file)
            )
            generator.generate()
            assert output_file.exists()

    def test_generate_integrated_release(self, test_dbs, tmp_path):
        """Test generate method works end-to-end for integrated release."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url

        with test_dbs['ensembl_genome_metadata'].dbc.session_scope() as session:
            integrated_release = session.query(EnsemblRelease).filter(
                EnsemblRelease.release_type == 'integrated'
            ).first()
            if integrated_release:
                output_file = tmp_path / "test_generate_integrated.csv"
                generator = ChangelogGenerator(
                    metadata_uri=metadata_uri,
                    release_label=integrated_release.label,
                    output_path=str(output_file)
                )
                generator.generate()
                assert output_file.exists()

    def test_generate_invalid_release(self, test_dbs, tmp_path):
        """Test generate method raises error for invalid release."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        output_file = tmp_path / "test_invalid.csv"
        generator = ChangelogGenerator(
            metadata_uri=metadata_uri,
            release_label="invalid-release-999",
            output_path=str(output_file)
        )
        with pytest.raises(ValueError) as excinfo:
            generator.generate()
        assert "Release not found" in str(excinfo.value)

    def test_gather_partial_data_no_genomes(self, test_dbs):
        """Test gather_partial_data returns empty list when no genomes found."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        with test_dbs['ensembl_genome_metadata'].dbc.session_scope() as session:
            partial_release = session.query(EnsemblRelease).filter(
                EnsemblRelease.release_type == 'partial'
            ).first()
            generator = ChangelogGenerator(
                metadata_uri=metadata_uri,
                release_label=partial_release.label
            )

            data = generator.gather_partial_data()
            assert isinstance(data, list)

    def test_csv_fieldnames_partial_vs_integrated(self, test_dbs, tmp_path):
        """Test that CSV has different fieldnames for partial vs integrated releases."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url

        # Partial release data
        partial_file = tmp_path / "partial.csv"
        generator_partial = ChangelogGenerator(
            metadata_uri=metadata_uri,
            release_label="2024-01",
            output_path=str(partial_file)
        )

        partial_data = [{
            'scientific_name': 'test', 'common_name': 'test', 'assembly_name': 'test',
            'assembly_accession': 'test', 'annotation_provider': 'test',
            'geneset_updated': 0, 'variation_updated': 0, 'regulation_updated': 0
        }]

        generator_partial.export_to_csv(partial_data)

        integrated_file = tmp_path / "integrated.csv"
        generator_integrated = ChangelogGenerator(
            metadata_uri=metadata_uri,
            release_label="112",
            output_path=str(integrated_file)
        )

        integrated_data = [{
            'scientific_name': 'test', 'common_name': 'test', 'assembly_name': 'test',
            'assembly_accession': 'test', 'annotation_provider': 'test',
            'geneset_updated': '2024-01', 'variation_updated': None, 'regulation_updated': None,
            'status': 'New'
        }]

        generator_integrated.export_to_csv(integrated_data)

        with open(partial_file, 'r') as f:
            lines = f.readlines()
            header = lines[1].strip()  # Skip comment line
            assert 'status' not in header

        with open(integrated_file, 'r') as f:
            lines = f.readlines()
            header = lines[1].strip()
            assert 'status' in header
