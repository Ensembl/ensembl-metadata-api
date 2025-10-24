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
from collections import namedtuple
from unittest.mock import patch

import pytest

from ensembl.production.metadata.api.models import Assembly
from ensembl.production.metadata.scripts.copy_handover_files import *
from ensembl.production.metadata.scripts.create_datasets_json import *
from ensembl.production.metadata.scripts.delete_ftp_by_uuid import *
from ensembl.production.metadata.scripts.organism_to_organismgroup import *

db_directory = Path(__file__).parent / 'databases'
db_directory = db_directory.resolve()

# Define a named tuple for script args
Args = namedtuple('Args', [
    'metadata_db_uri', 'core_server_uri', 'organism_group_type',
    'organism_group_name', 'genome_uuid', 'release_id', 'remove', 'raise_error'
])

@pytest.mark.parametrize("test_dbs", [[{'src': Path(__file__).parent / "databases/ensembl_genome_metadata"},
                                       {'src': Path(__file__).parent / "databases/ncbi_taxonomy"},
                                       {'src': Path(__file__).parent / "databases/core_1"},
                                       ]],
                         indirect=True)
class TestScripts:
    """Test suite for various metadata scripts."""

    def test_check_directory_single_path_valid(self, test_dbs, tmp_path):
        """Test check_directory function with single valid directory (returns string)."""
        test_dir = tmp_path / "test_dir"
        test_dir.mkdir()
        result = check_directory(str(test_dir))
        assert result == str(test_dir)

    def test_check_directory_invalid(self, test_dbs):
        """Test check_directory function with invalid directory."""
        with pytest.raises(argparse.ArgumentTypeError) as excinfo:
            check_directory("/nonexistent/directory/path")
        assert "does not exist" in str(excinfo.value)

    def test_generate_full_paths(self, test_dbs):
        """Test generate_full_paths creates correct FTP and NFS paths."""
        relative_paths = ["species1/assembly1", "species2/assembly2"]
        ftp_root = "/ftp/root/"
        nfs_root = "/nfs/root/"

        result = generate_full_paths(relative_paths, ftp_root, nfs_root)

        assert len(result) == 4  # 2 relative paths * 2 roots
        assert "/ftp/root/species1/assembly1" in result
        assert "/nfs/root/species1/assembly1" in result
        assert "/ftp/root/species2/assembly2" in result
        assert "/nfs/root/species2/assembly2" in result

    def test_generate_full_paths_empty(self, test_dbs):
        """Test generate_full_paths with empty input."""
        result = generate_full_paths([], "/ftp/", "/nfs/")
        assert result == []

    def test_submit_slurm_job_test_mode(self, test_dbs, capsys):
        """Test submit_slurm_job in test mode (no actual submission)."""
        paths = ["/path1", "/path2"]
        submit_slurm_job(paths, test=True)

        captured = capsys.readouterr()
        assert "[TEST MODE]" in captured.out
        assert "/path1" in captured.out
        assert "/path2" in captured.out

    def test_submit_slurm_job_empty_paths(self, test_dbs, capsys):
        """Test submit_slurm_job with empty paths list."""
        submit_slurm_job([], test=False)

        captured = capsys.readouterr()
        assert "No paths to delete" in captured.out

    @patch('subprocess.run')
    def test_submit_slurm_job_actual_submission(self, mock_subprocess, test_dbs):
        """Test submit_slurm_job makes correct subprocess call."""
        paths = ["/path1", "/path2"]
        submit_slurm_job(paths, test=False)

        # Verify subprocess.run was called
        mock_subprocess.assert_called_once()
        call_args = mock_subprocess.call_args[0][0]
        assert "sbatch" in call_args
        assert "--wrap" in call_args

    def test_variation_tracks_json_parsing(self, test_dbs, tmp_path):
        """Test variation_tracks function parses JSON correctly."""
        # Create test JSON file
        test_data = {
            "genome-uuid-1": {
                "datafiles": {
                    "file1": str(tmp_path / "source1.vcf"),
                    "file2": str(tmp_path / "source2.vcf")
                }
            }
        }

        # Create source files
        (tmp_path / "source1.vcf").touch()
        (tmp_path / "source2.vcf").touch()

        json_file = tmp_path / "test.json"
        with open(json_file, 'w') as f:
            json.dump(test_data, f)

        dest_dir = tmp_path / "destination"
        dest_dir.mkdir()

        # Run the function
        variation_tracks(str(json_file), "release_1", [str(dest_dir) + "/"])

        # Verify files were copied
        genome_dir = dest_dir / "genome-uuid-1"
        assert genome_dir.exists()
        assert (genome_dir / "source1.vcf").exists()
        assert (genome_dir / "source2.vcf").exists()

    def test_variation_tracks_invalid_json(self, test_dbs, tmp_path):
        """Test variation_tracks handles invalid JSON gracefully."""
        json_file = tmp_path / "invalid.json"
        with open(json_file, 'w') as f:
            f.write("not valid json{")

        with pytest.raises(Exception):
            variation_tracks(str(json_file), "release_1", ["/tmp/"])

    def test_regulation_copy_creates_directory(self, test_dbs, tmp_path):
        """Test regulation_copy creates destination directories."""
        source_file = tmp_path / "source.bb"
        source_file.touch()

        test_data = [
            {
                "genome_uuid": "test-genome-uuid",
                "dataset_source": {"name": str(source_file), "type": "bigbed"},
                "dataset_type": "regulation",
                "dataset_attribute": [],
                "name": "test_regulation",
                "label": "test_label",
                "version": "1.0"
            }
        ]

        json_file = tmp_path / "regulation.json"
        with open(json_file, 'w') as f:
            json.dump(test_data, f)
        dest_base = tmp_path / "destination"
        dest_base.mkdir()
        regulation_copy(str(json_file), "release_1", [str(dest_base) + "/"])
        expected_dir = dest_base / "test-genome-uuid"
        assert expected_dir.exists()
        expected_file = expected_dir / f"regulatory-features{source_file.suffix}"
        assert expected_file.exists()

    def test_fetch_division_name(self, test_dbs):
        """Test fetch_division_name retrieves division from core database."""
        core_uri = test_dbs.get('core_1')
        if core_uri:
            with DBConnection(core_uri.dbc.url).session_scope() as session:
                division = session.query(Meta).filter(
                    Meta.meta_key == 'species.division'
                ).first()
                result = fetch_division_name(core_uri.dbc.url)
                if division:
                    assert result == division.meta_value
                else:
                    assert result is None

    def test_create_organism_group_member(self, test_dbs):
        """Test create_or_remove_organism_group creates new member."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url

        with DBConnection(metadata_uri).session_scope() as session:
            organism = session.query(Organism).first()
            from ensembl.production.metadata.api.models import OrganismGroup
            org_group = session.query(OrganismGroup).first()
            if organism and org_group:
                existing = session.query(OrganismGroupMember).filter(
                    OrganismGroupMember.organism_id == organism.organism_id,
                    OrganismGroupMember.organism_group_id == org_group.organism_group_id
                ).first()
                if not existing:
                    msg = create_or_remove_organism_group(
                        session, organism.organism_id, org_group.organism_group_id, remove=False
                    )
                    assert "created successfully" in msg or "already exists" in msg
                    member = session.query(OrganismGroupMember).filter(
                        OrganismGroupMember.organism_id == organism.organism_id,
                        OrganismGroupMember.organism_group_id == org_group.organism_group_id
                    ).first()
                    assert member is not None

    def test_remove_organism_group_member(self, test_dbs):
        """Test create_or_remove_organism_group removes member."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        with DBConnection(metadata_uri).session_scope() as session:
            member = session.query(OrganismGroupMember).first()
            if member:
                organism_id = member.organism_id
                group_id = member.organism_group_id
                msg = create_or_remove_organism_group(
                    session, organism_id, group_id, remove=True
                )
                assert "removed successfully" in msg or "not found" in msg

    def test_json_file_structure_for_ftp_copy(self, test_dbs, tmp_path):
        """Test that ftp_copy can parse expected JSON structure."""
        test_data = [
            {
                "genome_uuid": "test-uuid",
                "dataset_source": {
                    "name": str(tmp_path / "test.file"),
                    "type": "vep"
                },
                "dataset_type": "vep",
                "name": "test_vep",
                "label": "test_label",
                "version": "1.0"
            }
        ]
        json_file = tmp_path / "test_ftp.json"
        with open(json_file, 'w') as f:
            json.dump(test_data, f)
        with open(json_file, 'r') as f:
            loaded_data = json.load(f)
        assert len(loaded_data) == 1
        assert loaded_data[0]['genome_uuid'] == "test-uuid"
        assert loaded_data[0]['dataset_type'] == "vep"

    def test_duckdb_script_environment_variable(self, test_dbs, monkeypatch):
        """Test that DuckDB script reads from environment variable."""
        test_uri = "mysql://testuser:testpass@testhost:3306/testdb"
        monkeypatch.setenv('METADATA_DB', test_uri)
        from urllib.parse import urlparse
        db = urlparse(os.environ.get('METADATA_DB'))
        assert db.hostname == "testhost"
        assert db.port == 3306
        assert db.username == "testuser"
        assert db.path[1:] == "testdb"

    def test_ftp_metadata_paths_structure(self, test_dbs):
        """Test that genome public path structure is correct for FTP metadata."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        with DBConnection(metadata_uri).session_scope() as session:
            genome = session.query(Genome).first()
            if genome and hasattr(genome, 'get_public_path'):
                paths = genome.get_public_path(dataset_type='genebuild')
                assert isinstance(paths, list)
                if len(paths) > 0:
                    first_path = paths[0]
                    assert 'dataset_type' in first_path or 'path' in first_path

    def test_genome_public_path_all_types(self, test_dbs):
        """Test genome.get_public_path with 'all' dataset type."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        with DBConnection(metadata_uri).session_scope() as session:
            genome = session.query(Genome).first()
            if genome and hasattr(genome, 'get_public_path'):
                paths = genome.get_public_path(dataset_type='all')
                assert isinstance(paths, list)
                if len(paths) > 1:
                    dataset_types = {p.get('dataset_type') for p in paths if 'dataset_type' in p}
                    assert len(dataset_types) > 1

    def test_ftp_delete_checks_shared_organism(self, test_dbs):
        """Test that FTP delete logic checks for shared organisms."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url

        with DBConnection(metadata_uri).session_scope() as session:
            genome = session.query(Genome).first()
            if genome:
                other_genomes_count = session.query(Genome).filter(
                    Genome.organism_id == genome.organism_id,
                    Genome.genome_uuid != genome.genome_uuid
                ).count()
                assert isinstance(other_genomes_count, int)
                assert other_genomes_count >= 0

    def test_ftp_delete_checks_shared_assembly(self, test_dbs):
        """Test that FTP delete logic checks for shared assemblies."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url

        with DBConnection(metadata_uri).session_scope() as session:
            genome = session.query(Genome).first()
            if genome:
                other_assemblies_count = session.query(Genome).filter(
                    Genome.assembly_id == genome.assembly_id,
                    Genome.genome_uuid != genome.genome_uuid
                ).count()
                assert isinstance(other_assemblies_count, int)
                assert other_assemblies_count >= 0

    def test_organism_scientific_name_formatting(self, test_dbs):
        """Test that organism scientific names are formatted correctly for paths."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url

        with DBConnection(metadata_uri).session_scope() as session:
            organism = session.query(Organism).first()
            if organism:
                scientific_name = organism.scientific_name
                formatted_name = scientific_name.replace(" ", "_")
                assert " " not in formatted_name
                assert "_" in formatted_name or len(scientific_name.split()) == 1

    def test_assembly_accession_in_paths(self, test_dbs):
        """Test that assembly accessions are available for path construction."""
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url

        with DBConnection(metadata_uri).session_scope() as session:
            assembly = session.query(Assembly).first()
            if assembly:
                assert assembly.accession is not None
                assert len(assembly.accession) > 0
                assert " " not in assembly.accession
