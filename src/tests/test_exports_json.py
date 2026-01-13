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
from datetime import datetime
from pathlib import Path

import pytest

from ensembl.production.metadata.api.exports.ftp_index import FTPMetadataExporter
from ensembl.production.metadata.api.models import Genome, ReleaseStatus

db_directory = Path(__file__).parent / 'databases'
db_directory = db_directory.resolve()


@pytest.mark.parametrize(
    "test_dbs",
    [[{'src': db_directory / "ensembl_genome_metadata"}, {'src': db_directory / "ncbi_taxonomy"}]],
    indirect=True,
)
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

    @pytest.mark.parametrize(
        ("input_name", "expected_name"),
        [
            ("homo sapiens", "homo_sapiens"),
            ("species.name", "species_name"),
            ("species__name", "species_name"),
            ("species___name", "species_name"),
            ("homo. sapiens", "homo_sapiens"),
            ("homo  sapiens", "homo_sapiens"),
            ("", ""),
            ("homo_sapiens", "homo_sapiens"),
            ("Homo. Sapiens", "Homo_Sapiens"),
            ("homo   sapiens", "homo_sapiens"),
            (" homo sapiens ", "homo_sapiens"),
            (" Escherichia coli str. K-12 substr. MG1655 str. K12 ", "Escherichia_coli_str_K_12_substr_MG1655_str_K12")
        ],
    )
    def test_normalize_species_name(self, test_dbs, input_name, expected_name):
        """Test _normalize_species_name correctly normalizes species names."""
        metadata_uri = test_dbs["ensembl_genome_metadata"].dbc.url
        exporter = FTPMetadataExporter(metadata_uri)
        assert exporter._normalize_species_name(input_name) == expected_name

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
        datetime.fromisoformat(data['last_updated'])
