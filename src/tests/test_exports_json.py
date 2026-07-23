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

from ensembl.production.metadata.api.exports.ftp_index import FTPMetadataExporter, format_accession_path
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

    def test_load_all_genome_data_genebuild_metadata_fields(self, test_dbs):
        """Test _load_all_genome_data populates genebuild_metadata with all required fields."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        exporter = FTPMetadataExporter(metadata_uri)
        with exporter.metadata_db.session_scope() as session:
            genome_data = exporter._load_all_genome_data(session)

        for genome_uuid, data in genome_data.items():
            gm = data['genebuild_metadata']
            if gm is not None:
                assert 'accession' in gm
                assert 'genebuild_source_name' in gm
                assert 'last_geneset_update' in gm
                # These keys must always be present (value may be None if no partial release)
                assert 'homology_release' in gm
                assert 'variation_release' in gm

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
        assert exporter._extract_provider_from_path({'genebuild_source_name': 'Ensembl'}) == 'ensembl'
        assert exporter._extract_provider_from_path({'genebuild_source_name': 'REFSEQ'}) == 'refseq'
        assert exporter._extract_provider_from_path(None) == 'unknown'
        assert exporter._extract_provider_from_path({}) == 'unknown'

    def test_extract_genebuild_release_info(self, test_dbs):
        """Test _extract_genebuild_release_info extracts release correctly."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        exporter = FTPMetadataExporter(metadata_uri)
        assert exporter._extract_genebuild_release_info({'last_geneset_update': '2024-01-01'})['release'] == '2024_01'
        assert exporter._extract_genebuild_release_info({'last_geneset_update': '2023-12-15'})['release'] == '2023_12'
        assert exporter._extract_genebuild_release_info(None)['release'] == 'unknown'
        assert exporter._extract_genebuild_release_info({})['release'] == 'unknown'
        assert exporter._extract_genebuild_release_info({'last_geneset_update': 'invalid-date'})['release'] == 'unknown'

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

        datasets = [{'dataset_type_name': 'genebuild'}, {'dataset_type_name': 'assembly'}]
        assert exporter._has_released_dataset_bulk(datasets, 'genebuild') is True
        assert exporter._has_released_dataset_bulk(datasets, 'assembly') is True
        assert exporter._has_released_dataset_bulk(datasets, 'variation') is False
        assert exporter._has_released_dataset_bulk([], 'genebuild') is False

    # ------------------------------------------------------------------
    # format_accession_path
    # ------------------------------------------------------------------

    @pytest.mark.parametrize(
        ("accession", "expected_path"),
        [
            ("GCA_000001405.29", "GCA/000/001/405/29"),
            ("GCF_000001405.40", "GCF/000/001/405/40"),
            ("GCA_003339765.3", "GCA/003/339/765/3"),
            ("GCA_000005845.2", "GCA/000/005/845/2"),
        ],
    )
    def test_format_accession_path(self, test_dbs, accession, expected_path):
        """Test format_accession_path converts accessions to correct directory paths."""
        assert format_accession_path(accession) == expected_path

    def test_format_accession_path_invalid(self, test_dbs):
        """Test format_accession_path raises ValueError for invalid accessions."""
        with pytest.raises(ValueError, match="Invalid accession format"):
            format_accession_path("GRCh38")
        with pytest.raises(ValueError, match="Invalid accession format"):
            format_accession_path("GCA_00000140529")  # missing dot-version
        with pytest.raises(ValueError, match="Invalid accession format"):
            format_accession_path("")

    # ------------------------------------------------------------------
    # _get_dataset_file_paths
    # ------------------------------------------------------------------

    def test_get_dataset_file_paths_genebuild(self, test_dbs):
        """Test _get_dataset_file_paths generates correct file paths for genebuild."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        exporter = FTPMetadataExporter(metadata_uri)
        base_path = "GCA/000/001/405/29/ensembl/2024_01/geneset"
        file_paths = exporter._get_dataset_file_paths(base_path, 'genebuild')

        assert 'annotations' in file_paths
        annotations = file_paths['annotations']
        # Core files always present
        for fname in ('cdna.fa.bgz', 'genes.embl.gz',
                      'genes.gff3.gz', 'genes.gff3.bgz',
                      'genes.gtf.gz', 'genes.gtf.bgz',
                      'pep.fa.bgz', 'xref.tsv.gz'):
            assert fname in annotations, f"Expected {fname} in genebuild annotations"
        # FASTA files should only be block-gzipped.
        for fname in ('cdna.fa.gz', 'pep.fa.gz'):
            assert fname not in annotations, f"Unexpected gzipped FASTA {fname} in genebuild annotations"
        # Paths should be rooted under base_path
        assert annotations['cdna.fa.bgz'] == f"{base_path}/cdna.fa.bgz"
        # Index files must NOT be present
        for fname in annotations:
            assert not fname.endswith('.fai'), f"Index file {fname} should not be listed"
            assert not fname.endswith('.gzi'), f"Index file {fname} should not be listed"
            assert not fname.endswith('.csi'), f"Index file {fname} should not be listed"
        # No VEP section in new structure
        assert 'vep' not in file_paths
        # genes-including_alt files are intentionally omitted (not always present)
        for fname in annotations:
            assert 'including_alt' not in fname

    def test_get_dataset_file_paths_assembly(self, test_dbs):
        """Test _get_dataset_file_paths generates correct file paths for assembly."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        exporter = FTPMetadataExporter(metadata_uri)
        base_path = "GCA/000/001/405/29/ensembl/2024_01/genome"
        file_paths = exporter._get_dataset_file_paths(base_path, 'assembly')

        assert 'genome_sequences' in file_paths
        genome_seqs = file_paths['genome_sequences']
        for fname in ('chromosomes.tsv.gz',
                      'hardmasked.fa.bgz',
                      'softmasked.fa.bgz',
                      'unmasked.fa.bgz'):
            assert fname in genome_seqs, f"Expected {fname} in genome_sequences"
        # FASTA files should only be block-gzipped.
        for fname in ('hardmasked.fa.gz', 'softmasked.fa.gz', 'unmasked.fa.gz'):
            assert fname not in genome_seqs, f"Unexpected gzipped FASTA {fname} in genome_sequences"
        assert genome_seqs['softmasked.fa.bgz'] == f"{base_path}/softmasked.fa.bgz"
        # Index files must NOT be present
        for fname in genome_seqs:
            assert not fname.endswith('.fai')
            assert not fname.endswith('.gzi')
        # No VEP section in new structure
        assert 'vep' not in file_paths
        # md5sum must NOT be present
        assert 'md5sum.txt' not in genome_seqs

    def test_get_dataset_file_paths_homologies(self, test_dbs):
        """Test _get_dataset_file_paths generates correct file paths for homologies."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        exporter = FTPMetadataExporter(metadata_uri)
        base_path = "GCA/000/001/405/29/ensembl/2024_01/homology/2024_10_18"
        file_paths = exporter._get_dataset_file_paths(base_path, 'homologies')

        assert 'homology_data' in file_paths
        homology_files = file_paths['homology_data']
        assert 'homology.tsv.gz' in homology_files
        assert homology_files['homology.tsv.gz'] == f"{base_path}/homology.tsv.gz"
        # Only one file in the terminal directory
        assert len(homology_files) == 1

    def test_get_dataset_file_paths_variation(self, test_dbs):
        """Test _get_dataset_file_paths generates correct file paths for variation."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        exporter = FTPMetadataExporter(metadata_uri)
        base_path = "homo_sapiens/GRCh38/ensembl/variation/2024_01"
        with exporter.metadata_db.session_scope() as session:
            genome = session.query(Genome).first()
            assembly_data = {'accession': 'GRCh38'} if genome else {}
            file_paths = exporter._get_dataset_file_paths(base_path, "short_variants", genome, assembly_data)
        assert 'variation_data' in file_paths
        variation_files = file_paths['variation_data']
        assert 'variation.vcf.gz' in variation_files
        assert variation_files['variation.vcf.gz'] == f"{base_path}/variation.vcf.gz"
        assert len(variation_files) == 1

    def test_get_dataset_file_paths_unknown_type(self, test_dbs):
        """Test _get_dataset_file_paths returns empty dict for unknown dataset type."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        exporter = FTPMetadataExporter(metadata_uri)
        file_paths = exporter._get_dataset_file_paths("some/path", 'nonexistent_type')
        assert file_paths == {}

    # ------------------------------------------------------------------
    # Integration / JSON structure
    # ------------------------------------------------------------------

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
            for assembly_accession, assembly_data in species_data['assemblies'].items():
                assert isinstance(assembly_accession, str)
                assert 'genebuild_providers' in assembly_data
                for provider_name, provider_data in assembly_data['genebuild_providers'].items():
                    assert isinstance(provider_name, str)
                    for release_key, release_data in provider_data.items():
                        assert isinstance(release_key, str)
                        assert 'release' in release_data
                        assert 'paths' in release_data

    def test_export_assembly_paths_use_accession_format(self, test_dbs):
        """Test that generated paths use the new GCA/NNN/NNN/NNN/VV accession format."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        exporter = FTPMetadataExporter(metadata_uri)
        metadata = exporter.export_to_json()

        for species_data in metadata['species'].values():
            for assembly_data in species_data['assemblies'].values():
                if assembly_data.get('assembly'):
                    for category in assembly_data['assembly']['files'].values():
                        for path in category.values():
                            # Path must start with GCA/ or GCF/ followed by 3-digit chunks
                            assert path.startswith('GCA/') or path.startswith('GCF/'), (
                                f"Path does not use accession format: {path}"
                            )

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
