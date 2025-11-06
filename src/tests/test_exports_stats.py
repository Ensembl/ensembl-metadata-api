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

from ensembl.production.metadata.api.exports.stats_generator import StatsGenerator

db_directory = Path(__file__).parent / 'databases'
db_directory = db_directory.resolve()


@pytest.mark.parametrize(
    "test_dbs",
    [[{'src': db_directory / "ensembl_genome_metadata"}, {'src': db_directory / "ncbi_taxonomy"}]],
    indirect=True,
)
class TestStatsGenerator:
    """Test suite for StatsGenerator class."""

    def test_init_valid_uri(self, test_dbs):
        """Test StatsGenerator initialization with valid metadata URI."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        generator = StatsGenerator(metadata_uri)
        assert generator.metadata_db is not None
        assert generator.output_path == Path.cwd()

    def test_init_with_output_path(self, test_dbs, tmp_path):
        """Test StatsGenerator initialization with custom output path."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        output_path = tmp_path / "test_output"
        generator = StatsGenerator(metadata_uri, output_path=str(output_path))
        assert generator.metadata_db is not None
        assert generator.output_path == output_path
        assert output_path.exists()

    def test_init_invalid_uri_empty(self, test_dbs):
        """Test StatsGenerator initialization fails with empty URI."""
        with pytest.raises(ValueError) as excinfo:
            StatsGenerator("")
        assert "metadata_uri must be a non-empty string" in str(excinfo.value)

    def test_init_invalid_uri_none(self, test_dbs):
        """Test StatsGenerator initialization fails with None URI."""
        with pytest.raises(ValueError) as excinfo:
            StatsGenerator(None)
        assert "metadata_uri must be a non-empty string" in str(excinfo.value)

    def test_init_invalid_uri_not_string(self, test_dbs):
        """Test StatsGenerator initialization fails with non-string URI."""
        with pytest.raises(ValueError) as excinfo:
            StatsGenerator(123)
        assert "metadata_uri must be a non-empty string" in str(excinfo.value)

    def test_get_partial_data(self, test_dbs):
        """Test get_partial_data returns correct structure and values."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        generator = StatsGenerator(metadata_uri)
        partial_data = generator.get_partial_data()
        assert isinstance(partial_data, list)
        if len(partial_data) > 0:
            first_release = partial_data[0]
            required_keys = [
                'release', 'new_genomes', 'total_genomes',
                'new_assemblies', 'total_assemblies',
                'new_variation_datasets', 'total_variation_datasets',
                'new_regulation_datasets', 'total_regulation_datasets'
            ]
            for key in required_keys:
                assert key in first_release, f"Missing key: {key}"
            assert isinstance(first_release['release'], str)
            assert isinstance(first_release['new_genomes'], int)
            assert isinstance(first_release['total_genomes'], int)
            assert isinstance(first_release['new_assemblies'], int)
            assert isinstance(first_release['total_assemblies'], int)
            assert isinstance(first_release['new_variation_datasets'], int)
            assert isinstance(first_release['total_variation_datasets'], int)
            assert isinstance(first_release['new_regulation_datasets'], int)
            assert isinstance(first_release['total_regulation_datasets'], int)
            # Verify cumulative totals are non-decreasing
            for i in range(1, len(partial_data)):
                assert partial_data[i]['total_genomes'] >= partial_data[i - 1]['total_genomes']
                assert partial_data[i]['total_assemblies'] >= partial_data[i - 1]['total_assemblies']
                assert partial_data[i]['total_variation_datasets'] >= partial_data[i - 1]['total_variation_datasets']
                assert partial_data[i]['total_regulation_datasets'] >= partial_data[i - 1]['total_regulation_datasets']

            assert len(partial_data) == 2
            assert partial_data[0]['release'] == '2020-10-18'
            assert partial_data[0]['new_genomes'] == 3

    def test_get_partial_data_specific_values(self, test_dbs):
        """Test get_partial_data returns specific expected values from test database."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        generator = StatsGenerator(metadata_uri)
        partial_data = generator.get_partial_data()

        assert len(partial_data) == 2
        if len(partial_data) >= 1:
            assert partial_data[0]['release'] == '2020-10-18'
            assert partial_data[0]['new_genomes'] == 3
            assert partial_data[0]['total_genomes'] == 3

    def test_get_integrated_data(self, test_dbs):
        """Test get_integrated_data returns correct structure and values."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        generator = StatsGenerator(metadata_uri)
        integrated_data = generator.get_integrated_data()

        assert isinstance(integrated_data, list)

        # Don't actually have any integrated data in the test db.
        # TODO: Add some integrated data.
        if len(integrated_data) > 0:
            first_release = integrated_data[0]
            required_keys = [
                'release', 'genomes', 'assemblies',
                'variation_datasets', 'regulation_datasets'
            ]
            for key in required_keys:
                assert key in first_release, f"Missing key: {key}"
            assert isinstance(first_release['release'], str)
            assert isinstance(first_release['genomes'], int)
            assert isinstance(first_release['assemblies'], int)
            assert isinstance(first_release['variation_datasets'], int)
            assert isinstance(first_release['regulation_datasets'], int)

            assert len(integrated_data) == 0
            assert integrated_data[0]['release'] == '2025-01'
            assert integrated_data[0]['genomes'] == 12

    # def test_get_integrated_data_specific_values(self, test_dbs):
    #     """Test get_integrated_data returns specific expected values from test database."""
    #     metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
    #     generator = StatsGenerator(metadata_uri)
    #
    #     integrated_data = generator.get_integrated_data()
    #
    #     assert len(integrated_data) == 2
    #     if len(integrated_data) >= 1:
    #         assert integrated_data[0]['release'] == '112'
    #         assert integrated_data[0]['genomes'] == 50
    #         assert integrated_data[0]['assemblies'] == 45

    def test_count_datasets(self, test_dbs):
        """Test _count_datasets returns correct count for a specific release and dataset type."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        generator = StatsGenerator(metadata_uri)
        with generator.metadata_db.session_scope() as session:
            release_id = 1
            variation_count = generator._count_datasets(session, release_id, 'variation')
            assert variation_count == 3

            regulation_count = generator._count_datasets(session, release_id, 'regulatory_features')
            assert regulation_count == 0

    def test_count_and_get_dataset_ids(self, test_dbs):
        """Test _count_and_get_dataset_ids returns correct count and IDs."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        generator = StatsGenerator(metadata_uri)

        with generator.metadata_db.session_scope() as session:
            release_id = 1
            count, dataset_ids = generator._count_and_get_dataset_ids(
                session, release_id, 'variation'
            )

            assert isinstance(count, int)
            assert isinstance(dataset_ids, set)
            assert count == len(dataset_ids)
            assert count == 3

    def test_export_to_csv(self, test_dbs, tmp_path):
        """Test export_to_csv creates files with correct structure."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        output_path = tmp_path / "csv_output"
        generator = StatsGenerator(metadata_uri, output_path=str(output_path))

        # Create sample data
        partial_data = [
            {
                'release': 'R1',
                'new_genomes': 10,
                'total_genomes': 10,
                'new_assemblies': 8,
                'total_assemblies': 8,
                'new_variation_datasets': 5,
                'total_variation_datasets': 5,
                'new_regulation_datasets': 3,
                'total_regulation_datasets': 3,
            }
        ]

        integrated_data = [
            {
                'release': 'R1',
                'genomes': 10,
                'assemblies': 8,
                'variation_datasets': 5,
                'regulation_datasets': 3,
            }
        ]

        generator.export_to_csv(partial_data, integrated_data)

        partial_file = output_path / 'stats.partial.csv'
        integrated_file = output_path / 'stats.integrated.csv'
        assert partial_file.exists()
        assert integrated_file.exists()
        with open(partial_file, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 1
            assert rows[0]['release'] == 'R1'
            assert rows[0]['new_genomes'] == '10'
            assert rows[0]['total_genomes'] == '10'
        with open(integrated_file, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 1
            assert rows[0]['release'] == 'R1'
            assert rows[0]['genomes'] == '10'
            assert rows[0]['assemblies'] == '8'

    def test_export_to_csv_sorting(self, test_dbs, tmp_path):
        """Test export_to_csv sorts data by release label."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        output_path = tmp_path / "csv_output_sorted"
        generator = StatsGenerator(metadata_uri, output_path=str(output_path))
        partial_data = [
            {'release': 'R3', 'new_genomes': 30, 'total_genomes': 60,
             'new_assemblies': 20, 'total_assemblies': 50,
             'new_variation_datasets': 10, 'total_variation_datasets': 30,
             'new_regulation_datasets': 5, 'total_regulation_datasets': 15},
            {'release': 'R1', 'new_genomes': 10, 'total_genomes': 10,
             'new_assemblies': 8, 'total_assemblies': 8,
             'new_variation_datasets': 5, 'total_variation_datasets': 5,
             'new_regulation_datasets': 3, 'total_regulation_datasets': 3},
            {'release': 'R2', 'new_genomes': 20, 'total_genomes': 30,
             'new_assemblies': 12, 'total_assemblies': 20,
             'new_variation_datasets': 5, 'total_variation_datasets': 10,
             'new_regulation_datasets': 2, 'total_regulation_datasets': 5},
        ]

        generator.export_to_csv(partial_data, [])
        partial_file = output_path / 'stats.partial.csv'
        assert partial_file.exists()

        with open(partial_file, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 3
            assert rows[0]['release'] == 'R1'
            assert rows[1]['release'] == 'R2'
            assert rows[2]['release'] == 'R3'

    def test_export_to_csv_empty_data(self, test_dbs, tmp_path):
        """Test export_to_csv handles empty data correctly."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        output_path = tmp_path / "csv_output_empty"
        generator = StatsGenerator(metadata_uri, output_path=str(output_path))
        generator.export_to_csv([], [])

        partial_file = output_path / 'stats.partial.csv'
        integrated_file = output_path / 'stats.integrated.csv'

        assert partial_file.exists()
        assert integrated_file.exists()
        with open(partial_file, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 0

        with open(integrated_file, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 0

    def test_generate_integration(self, test_dbs, tmp_path):
        """Test generate method integrates all components correctly."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        output_path = tmp_path / "generate_output"
        generator = StatsGenerator(metadata_uri, output_path=str(output_path))
        generator.generate()
        partial_file = output_path / 'stats.partial.csv'
        integrated_file = output_path / 'stats.integrated.csv'

        assert partial_file.exists()
        assert integrated_file.exists()
        with open(partial_file, 'r') as f:
            reader = csv.DictReader(f)
            assert reader.fieldnames is not None
            partial_fieldnames = [
                'release', 'new_genomes', 'total_genomes',
                'new_assemblies', 'total_assemblies',
                'new_variation_datasets', 'total_variation_datasets',
                'new_regulation_datasets', 'total_regulation_datasets'
            ]
            assert reader.fieldnames == partial_fieldnames

        with open(integrated_file, 'r') as f:
            reader = csv.DictReader(f)
            assert reader.fieldnames is not None
            integrated_fieldnames = [
                'release', 'genomes', 'assemblies',
                'variation_datasets', 'regulation_datasets'
            ]
            assert reader.fieldnames == integrated_fieldnames

    def test_partial_data_ordering(self, test_dbs):
        """Test that partial data is returned in correct order by release label."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        generator = StatsGenerator(metadata_uri)
        partial_data = generator.get_partial_data()
        if len(partial_data) > 1:
            release_labels = [item['release'] for item in partial_data]
            assert release_labels == sorted(release_labels)
        # Assert that partial data exists and is not empty
        assert partial_data is not None, "Partial data should not be None"
        assert len(partial_data) >= 1, "Partial data should contain at least one item"

    def test_integrated_data_ordering(self, test_dbs):
        """Test that integrated data is returned in correct order by release label."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        generator = StatsGenerator(metadata_uri)
        integrated_data = generator.get_integrated_data()
        if len(integrated_data) > 1:
            release_labels = [item['release'] for item in integrated_data]
            assert release_labels == sorted(release_labels)
        if len(integrated_data) == 0:
            release_labels = [item['release'] for item in integrated_data]
            assert release_labels == []
