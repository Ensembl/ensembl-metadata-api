#!/usr/bin/env python3
# See the NOTICE file distributed with this work for additional information
# regarding copyright ownership.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
"""Create missing tabix indexes for published short-variant VCFs.

The script finds current released ``short_variants`` datasets in metadata and
expects their renamed VCF to be named ``short_variant-<dataset.name>.vcf.gz``.
For every VCF without an existing ``.tbi``, ``.csi``, or other sidecar index,
it runs::

    tabix -f -C -p vcf <vcf>

This creates ``<vcf>.csi``.  It is a dry run by default; use ``--execute`` to
create indexes.
"""

import argparse
import logging
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

# Permit direct execution from any working directory without requiring callers
# to set PYTHONPATH.  This is deliberately local to this disposable script.
SOURCE_ROOT = Path(__file__).resolve().parents[4]
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

# Prefer this checkout's virtual environment when the caller invokes the
# script directly with a Python that lacks the project's dependencies.  This
# keeps the one-off command free of activation/PYTHONPATH boilerplate.
try:
    import ensembl.utils.database  # noqa: F401
except ModuleNotFoundError as error:
    venv_python = SOURCE_ROOT.parent / ".venv" / "bin" / "python"
    if venv_python.is_file() and Path(sys.executable) != venv_python:
        os.execv(str(venv_python), [str(venv_python), str(Path(__file__).resolve()), *sys.argv[1:]])
    raise RuntimeError(
        "Cannot import ensembl-utils. Run this script from a checkout with its .venv available."
    ) from error

from ensembl.production.metadata.scripts.rename_variation_files import (
    VariationDataset,
    add_report_file_handler,
    fetch_variation_datasets,
)

LOGGER = logging.getLogger(__name__)
INDEX_SUFFIXES = (".tbi", ".csi")


@dataclass
class IndexAudit:
    dataset: VariationDataset
    vcf: Path | None
    indexes: list[Path]
    errors: list[str]

    @property
    def needs_index(self) -> bool:
        return self.vcf is not None and not self.indexes and not self.errors


def audit_dataset(ftp_root: Path, dataset: VariationDataset) -> IndexAudit:
    """Determine whether *dataset* has a VCF which needs a tabix index."""
    errors = list(dataset.errors)
    if dataset.ftp_path is None:
        return IndexAudit(dataset, None, [], errors)

    vcf = ftp_root / dataset.ftp_path / dataset.new_filename
    if not vcf.is_file():
        errors.append(f"VCF not found: {vcf}")
        return IndexAudit(dataset, None, [], errors)

    indexes = [
        vcf.with_name(f"{vcf.name}{suffix}")
        for suffix in INDEX_SUFFIXES
        if vcf.with_name(f"{vcf.name}{suffix}").is_file()
    ]
    return IndexAudit(dataset, vcf, indexes, errors)


def log_audit(results: list[IndexAudit], execute: bool, tabix: str) -> None:
    """Write a complete, readable index audit to the configured log handlers."""
    missing_vcf = [result for result in results if result.vcf is None]
    existing_index = [result for result in results if result.indexes]
    to_create = [result for result in results if result.needs_index]

    LOGGER.info(
        "Variation-index audit: %s dataset(s), %s existing index(es), %s index(es) to create, %s error(s)",
        len(results),
        len(existing_index),
        len(to_create),
        len(missing_vcf),
    )
    if missing_vcf:
        LOGGER.error("Datasets that cannot be indexed:")
        for result in missing_vcf:
            LOGGER.error(
                "  %s (%s; genome %s)",
                result.dataset.dataset_uuid,
                result.dataset.dataset_name,
                result.dataset.genome_uuid,
            )
            for error in result.errors:
                LOGGER.error("    - %s", error)

    if to_create:
        action = "Will run" if execute else "Would run"
        LOGGER.info("%s:", action)
        for result in to_create:
            LOGGER.info("  %s -f -C -p vcf %s", tabix, result.vcf)

    LOGGER.info(
        "Summary: short_variant datasets: %s; existing indexes: %s; indexes to create: %s; "
        "VCFs missing/unusable: %s",
        len(results),
        len(existing_index),
        len(to_create),
        len(missing_vcf),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata-uri", required=True, help="Metadata database URI.")
    parser.add_argument("--ftp-root", required=True, type=Path, help="Root containing the public FTP layout.")
    parser.add_argument("--tabix", default="tabix", help="Path or command name for tabix (default: tabix).")
    parser.add_argument(
        "--report-file",
        type=Path,
        help="Write the audit report to this file as well as standard error (overwrites an existing file).",
    )
    parser.add_argument("--execute", action="store_true", help="Create missing indexes (default: dry run).")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.report_file:
        add_report_file_handler(args.report_file)

    ftp_root = args.ftp_root.resolve()
    if not ftp_root.is_dir():
        raise NotADirectoryError(f"FTP root does not exist or is not a directory: {ftp_root}")
    if args.execute and shutil.which(args.tabix) is None:
        raise FileNotFoundError(f"tabix executable not found: {args.tabix}")

    datasets = fetch_variation_datasets(args.metadata_uri)
    results = [audit_dataset(ftp_root, dataset) for dataset in datasets]
    log_audit(results, args.execute, args.tabix)

    failures = 0
    if args.execute:
        for result in results:
            if not result.needs_index:
                continue
            try:
                subprocess.run([args.tabix, "-f", "-C", "-p", "vcf", str(result.vcf)], check=True)
                LOGGER.info("Created %s.csi", result.vcf)
            except subprocess.CalledProcessError as error:
                failures += 1
                LOGGER.error("Failed to index %s (exit %s)", result.vcf, error.returncode)

    if failures:
        LOGGER.error("Failed to create %s index(es)", failures)
        return 1
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    raise SystemExit(main())
