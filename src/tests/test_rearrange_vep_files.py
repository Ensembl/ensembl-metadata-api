# See the NOTICE file distributed with this work for additional information
# regarding copyright ownership.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

import importlib.util
import shutil
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

SCRIPT_PATH = Path(__file__).parents[1] / "ensembl/production/metadata/scripts/rearrange_vep_files.py"
SPEC = importlib.util.spec_from_file_location("rearrange_vep_files", SCRIPT_PATH)
assert SPEC and SPEC.loader
rearrange_vep_files = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = rearrange_vep_files
SPEC.loader.exec_module(rearrange_vep_files)

DB_DIRECTORY = Path(__file__).parent / "databases"
pytestmark = pytest.mark.parametrize(
    "test_dbs",
    [
        [
            {"src": DB_DIRECTORY / "ensembl_genome_metadata"},
            {"src": DB_DIRECTORY / "ncbi_taxonomy"},
        ]
    ],
    indirect=True,
)


def test_file_already_copied_requires_matching_size_and_timestamp(tmp_path):
    source_path = tmp_path / "source"
    target_path = tmp_path / "target"
    source_path.write_text("source")
    shutil.copy2(source_path, target_path)

    assert rearrange_vep_files.file_already_copied(source_path, target_path)

    target_path.write_text("different size")
    assert not rearrange_vep_files.file_already_copied(source_path, target_path)


def test_copy_genome_files_skips_files_already_copied(tmp_path):
    old_base_dir = tmp_path / "old"
    new_base_dir = tmp_path / "new"
    record = rearrange_vep_files.GenomeVepRecord(
        genome_uuid="01234567-89ab-cdef-0123-456789abcdef",
        assembly_uuid="fedcba98-7654-3210-fedc-ba9876543210",
        assembly_accession="GCA_000000001.1",
        scientific_name="Test species",
        annotation_source="test",
        last_geneset_update="2026-01-01",
    )
    source_paths = {
        filename: [Path("source") / filename] for filename in rearrange_vep_files.REQUIRED_OUTPUTS
    }
    for filename in rearrange_vep_files.REQUIRED_OUTPUTS:
        source_path = old_base_dir / source_paths[filename][0]
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_text(filename)

    assert rearrange_vep_files.copy_genome_files(record, old_base_dir, new_base_dir, source_paths) == []

    with patch.object(rearrange_vep_files.shutil, "copy2") as copy_file:
        assert rearrange_vep_files.copy_genome_files(record, old_base_dir, new_base_dir, source_paths) == []

    copy_file.assert_not_called()

    target_path = (
        new_base_dir / rearrange_vep_files.new_relative_paths(record)[rearrange_vep_files.REQUIRED_OUTPUTS[0]]
    )
    target_path.write_text("outdated")
    with patch.object(rearrange_vep_files.shutil, "copy2") as copy_file:
        assert rearrange_vep_files.copy_genome_files(record, old_base_dir, new_base_dir, source_paths) == []

    copy_file.assert_called_once()
