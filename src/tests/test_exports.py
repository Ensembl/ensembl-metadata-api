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

import csv
import json
from pathlib import Path

import pytest

from ensembl.production.metadata.api.exports.changelog_generator import ChangelogGenerator
from ensembl.production.metadata.api.exports.ftp_index import FTPMetadataExporter
from ensembl.production.metadata.api.exports.stats_generator import StatsGenerator
from ensembl.production.metadata.api.models import Genome, ReleaseStatus, EnsemblRelease

db_directory = Path(__file__).parent / 'databases'
db_directory = db_directory.resolve()


@pytest.mark.parametrize("test_dbs", [[{'src': Path(__file__).parent / "databases/ensembl_genome_metadata"},
                                       {'src': Path(__file__).parent / "databases/ncbi_taxonomy"},
                                       ]], indirect=True)
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
            pass

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
            pass

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

    def test_integrated_data_ordering(self, test_dbs):
        """Test that integrated data is returned in correct order by release label."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        generator = StatsGenerator(metadata_uri)
        integrated_data = generator.get_integrated_data()
        if len(integrated_data) > 1:
            release_labels = [item['release'] for item in integrated_data]
            assert release_labels == sorted(release_labels)


@pytest.mark.parametrize("test_dbs", [[{'src': Path(__file__).parent / "databases/ensembl_genome_metadata"},
                                       {'src': Path(__file__).parent / "databases/ncbi_taxonomy"},
                                       ]], indirect=True)
class TestFTPMetadataExporter:
    """Test suite for FTPMetadataExporter class."""

    def test_init_valid_uri(self, test_dbs):
        """Test FTPMetadataExporter initialization with valid metadata URI."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        exporter = FTPMetadataExporter(metadata_uri)
        assert exporter.metadata_db is not None

    def test_export_to_json_returns_dict(self, test_dbs):
        """Test export_to_json returns dictionary when no output file specified."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        exporter = FTPMetadataExporter(metadata_uri)
        result = exporter.export_to_json()
        assert isinstance(result, dict)
        assert 'last_updated' in result
        assert 'species' in result
        assert isinstance(result['species'], dict)

    def test_export_to_json_creates_file(self, test_dbs, tmp_path):
        """Test export_to_json creates file when output_file is specified."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        exporter = FTPMetadataExporter(metadata_uri)
        output_file = tmp_path / "ftp_metadata.json"
        result = exporter.export_to_json(str(output_file))
        assert result is None
        assert output_file.exists()
        with open(output_file, 'r') as f:
            data = json.load(f)
            assert 'last_updated' in data
            assert 'species' in data

    def test_build_ftp_metadata_json_structure(self, test_dbs):
        """Test build_ftp_metadata_json returns correct structure."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        exporter = FTPMetadataExporter(metadata_uri)
        metadata = exporter.build_ftp_metadata_json()
        assert isinstance(metadata, dict)
        assert 'last_updated' in metadata
        assert 'species' in metadata
        assert isinstance(metadata['species'], dict)
        first_species = next(iter(metadata['species'].values()))
        assert 'assemblies' in first_species
        assert isinstance(first_species['assemblies'], dict)

    def test_load_all_genome_data(self, test_dbs):
        """Test _load_all_genome_data returns correct structure."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        exporter = FTPMetadataExporter(metadata_uri)
        with exporter.metadata_db.session_scope() as session:
            genome_data = exporter._load_all_genome_data(session)
        assert isinstance(genome_data, dict)

        first_genome_uuid = next(iter(genome_data.keys()))
        first_genome_data = genome_data[first_genome_uuid]
        assert 'genome' in first_genome_data
        assert 'datasets' in first_genome_data
        assert 'attributes' in first_genome_data
        assert 'genebuild_metadata' in first_genome_data
        assert isinstance(first_genome_data['datasets'], list)
        assert isinstance(first_genome_data['attributes'], dict)

    def test_normalize_species_name(self, test_dbs):
        """Test _normalize_species_name correctly normalizes species names."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        exporter = FTPMetadataExporter(metadata_uri)
        assert exporter._normalize_species_name('homo sapiens') == 'homo_sapiens'
        assert exporter._normalize_species_name('species.name') == 'species_name'
        assert exporter._normalize_species_name('species__name') == 'species_name'
        assert exporter._normalize_species_name('species___name') == 'species_name'
        assert exporter._normalize_species_name('homo. sapiens') == 'homo_sapiens'
        assert exporter._normalize_species_name('homo  sapiens') == 'homo_sapiens'
        assert exporter._normalize_species_name('') == ''
        assert exporter._normalize_species_name('homo_sapiens') == 'homo_sapiens'
        assert exporter._normalize_species_name('Homo. Sapiens') == 'Homo_Sapiens'
        assert exporter._normalize_species_name('homo   sapiens') == 'homo_sapiens'
        assert exporter._normalize_species_name(' homo sapiens ') == '_homo_sapiens_'

    def test_extract_provider_from_path(self, test_dbs):
        """Test _extract_provider_from_path extracts provider correctly."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        exporter = FTPMetadataExporter(metadata_uri)
        genebuild_metadata = {
            'genebuild_source_name': 'Ensembl'
        }
        assert exporter._extract_provider_from_path(genebuild_metadata) == 'ensembl'
        genebuild_metadata = {
            'genebuild_source_name': 'REFSEQ'
        }
        assert exporter._extract_provider_from_path(genebuild_metadata) == 'refseq'
        assert exporter._extract_provider_from_path(None) == 'unknown'
        assert exporter._extract_provider_from_path({}) == 'unknown'

    def test_extract_genebuild_release_info(self, test_dbs):
        """Test _extract_genebuild_release_info extracts release correctly."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        exporter = FTPMetadataExporter(metadata_uri)
        genebuild_metadata = {
            'last_geneset_update': '2024-01-01'
        }
        result = exporter._extract_genebuild_release_info(genebuild_metadata)
        assert result['release'] == '2024_01'
        genebuild_metadata = {
            'last_geneset_update': '2023-12-15'
        }
        result = exporter._extract_genebuild_release_info(genebuild_metadata)
        assert result['release'] == '2023_12'
        result = exporter._extract_genebuild_release_info(None)
        assert result['release'] == 'unknown'
        result = exporter._extract_genebuild_release_info({})
        assert result['release'] == 'unknown'
        genebuild_metadata = {
            'last_geneset_update': 'invalid-date'
        }
        result = exporter._extract_genebuild_release_info(genebuild_metadata)
        assert result['release'] == 'unknown'

    def test_extract_release_info_from_ensembl_release(self, test_dbs):
        """Test _extract_release_info_from_ensembl_release extracts release correctly."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        exporter = FTPMetadataExporter(metadata_uri)
        with exporter.metadata_db.session_scope() as session:
            genome = session.query(Genome).first()

            if genome:
                result = exporter._extract_release_info_from_ensembl_release(genome)

                assert isinstance(result, dict)
                assert 'release' in result
                has_released = any(
                    gr.ensembl_release and gr.ensembl_release.status == ReleaseStatus.RELEASED
                    for gr in genome.genome_releases
                )
                if has_released:
                    assert result['release'] != 'unknown'

    def test_has_released_dataset_bulk(self, test_dbs):
        """Test _has_released_dataset_bulk correctly identifies dataset types."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        exporter = FTPMetadataExporter(metadata_uri)

        datasets = [
            {'dataset_type_name': 'genebuild'},
            {'dataset_type_name': 'assembly'}
        ]
        assert exporter._has_released_dataset_bulk(datasets, 'genebuild') is True
        assert exporter._has_released_dataset_bulk(datasets, 'assembly') is True
        assert exporter._has_released_dataset_bulk(datasets, 'variation') is False
        datasets = [
            {'dataset_type_name': 'regulatory_features'}
        ]
        assert exporter._has_released_dataset_bulk(datasets, 'regulation') is True
        assert exporter._has_released_dataset_bulk([], 'genebuild') is False

    def test_get_dataset_file_paths_genebuild(self, test_dbs):
        """Test _get_dataset_file_paths generates correct file paths for genebuild."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        exporter = FTPMetadataExporter(metadata_uri)
        base_path = "homo_sapiens/GRCh38/ensembl/geneset/2024_01"
        with exporter.metadata_db.session_scope() as session:
            genome = session.query(Genome).first()
            assembly_data = {'accession': 'GRCh38'} if genome else {}
            file_paths = exporter._get_dataset_file_paths(
                base_path, 'genebuild', genome, assembly_data
            )

        assert 'annotations' in file_paths
        assert 'cdna.fa.gz' in file_paths['annotations']
        assert 'genes.gff3.gz' in file_paths['annotations']
        assert 'genes.gtf.gz' in file_paths['annotations']
        assert 'pep.fa.gz' in file_paths['annotations']
        assert 'vep' in file_paths
        assert 'genes.gff3.bgz' in file_paths['vep']

    def test_get_dataset_file_paths_assembly(self, test_dbs):
        """Test _get_dataset_file_paths generates correct file paths for assembly."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        exporter = FTPMetadataExporter(metadata_uri)
        base_path = "homo_sapiens/GRCh38/genome"
        with exporter.metadata_db.session_scope() as session:
            genome = session.query(Genome).first()
            assembly_data = {'accession': 'GRCh38'} if genome else {}
            file_paths = exporter._get_dataset_file_paths(
                base_path, 'assembly', genome, assembly_data
            )
        assert 'genome_sequences' in file_paths
        assert 'chromosomes.tsv.gz' in file_paths['genome_sequences']
        assert 'hardmasked.fa.gz' in file_paths['genome_sequences']
        assert 'softmasked.fa.gz' in file_paths['genome_sequences']
        assert 'unmasked.fa.gz' in file_paths['genome_sequences']
        assert 'vep' in file_paths
        assert 'softmasked.fa.bgz' in file_paths['vep']

    def test_get_dataset_file_paths_variation(self, test_dbs):
        """Test _get_dataset_file_paths generates correct file paths for variation."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        exporter = FTPMetadataExporter(metadata_uri)
        base_path = "homo_sapiens/GRCh38/ensembl/variation/2024_01"
        with exporter.metadata_db.session_scope() as session:
            genome = session.query(Genome).first()
            assembly_data = {'accession': 'GRCh38'} if genome else {}
            file_paths = exporter._get_dataset_file_paths(
                base_path, 'variation', genome, assembly_data
            )
        assert 'variation_data' in file_paths
        assert 'variation.vcf.gz' in file_paths['variation_data']

    def test_get_dataset_file_paths_regulation(self, test_dbs):
        """Test _get_dataset_file_paths generates correct file paths for regulation."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        exporter = FTPMetadataExporter(metadata_uri)
        base_path = "homo_sapiens/GRCh38/ensembl/regulation"
        with exporter.metadata_db.session_scope() as session:
            genome = session.query(Genome).first()
            assembly_data = {'accession': 'GRCh38'} if genome else {}
            file_paths = exporter._get_dataset_file_paths(
                base_path, 'regulation', genome, assembly_data
            )

        assert 'regulatory_features' in file_paths
        assert 'regulation.gff' in file_paths['regulatory_features']

    def test_get_dataset_file_paths_homologies(self, test_dbs):
        """Test _get_dataset_file_paths generates correct file paths for homologies."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        exporter = FTPMetadataExporter(metadata_uri)
        base_path = "homo_sapiens/GRCh38/ensembl/homology/2024_01"
        with exporter.metadata_db.session_scope() as session:
            genome = session.query(Genome).first()
            if genome:
                assembly_data = {'accession': genome.assembly.accession}
                file_paths = exporter._get_dataset_file_paths(
                    base_path, 'homologies', genome, assembly_data
                )
                assert 'homology_data' in file_paths
                homology_files = file_paths['homology_data']
                assert len(homology_files) > 0
                first_file = next(iter(homology_files.keys()))
                assert 'homology.tsv.gz' in first_file

    def test_export_json_with_actual_data(self, test_dbs):
        """Test export generates valid JSON structure with actual database data."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        exporter = FTPMetadataExporter(metadata_uri)
        metadata = exporter.export_to_json()
        assert metadata is not None
        assert 'last_updated' in metadata
        assert 'species' in metadata

        for species_name, species_data in metadata['species'].items():
            assert isinstance(species_name, str)
            assert 'assemblies' in species_data
            for assembly_name, assembly_data in species_data['assemblies'].items():
                assert isinstance(assembly_name, str)
                if 'providers' in assembly_data:
                    for provider_name, provider_data in assembly_data['providers'].items():
                        assert isinstance(provider_name, str)
                        if 'releases' in provider_data:
                            for release_name, release_data in provider_data['releases'].items():
                                assert isinstance(release_name, str)
                                if 'datasets' in release_data:
                                    assert isinstance(release_data['datasets'], dict)

    def test_export_handles_empty_database(self, test_dbs):
        """Test export handles database with no released genomes gracefully."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        exporter = FTPMetadataExporter(metadata_uri)
        metadata = exporter.export_to_json()
        assert metadata is not None
        assert 'last_updated' in metadata
        assert 'species' in metadata
        assert isinstance(metadata['species'], dict)

    def test_json_file_is_valid_json(self, test_dbs, tmp_path):
        """Test that exported JSON file can be read back and is valid."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        exporter = FTPMetadataExporter(metadata_uri)
        output_file = tmp_path / "test_output.json"
        exporter.export_to_json(str(output_file))

        with open(output_file, 'r') as f:
            data = json.load(f)
        assert 'last_updated' in data
        assert 'species' in data
        from datetime import datetime
        try:
            datetime.fromisoformat(data['last_updated'])
        except ValueError:
            pytest.fail("last_updated is not in valid ISO format")


@pytest.mark.parametrize("test_dbs", [[{'src': Path(__file__).parent / "databases/ensembl_genome_metadata"},
                                       {'src': Path(__file__).parent / "databases/ncbi_taxonomy"},
                                       ]], indirect=True)
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

            if partial_release:
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
            if partial_release:
                generator = ChangelogGenerator(
                    metadata_uri=metadata_uri,
                    release_label=partial_release.label
                )
                from ensembl.production.metadata.api.models import Genome, GenomeDataset
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
