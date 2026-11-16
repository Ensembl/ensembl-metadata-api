#!/usr/bin/env python3
# See the NOTICE file distributed with this work for additional information
# regarding copyright ownership.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
"""Rename published short-variant VCFs and their indexes.

This is a one-off migration.  It gets every current, released short-variant
dataset from metadata, derives its public FTP directory, and renames::

    variation.vcf.gz      -> short_variant-<dataset.name>.vcf.gz
    variation.vcf.gz.*    -> short_variant-<dataset.name>.vcf.gz.*

The index extension is deliberately discovered rather than assumed to be
``.tbi``; for example, both ``.tbi`` and ``.csi`` are supported.  Use
``--report-file`` to retain the audit output.  With ``--execute``,
``--ignore-errors`` renames a present VCF even when its index is missing,
while still refusing global safety conflicts.

Run a dry run first (the default), then repeat with ``--execute`` once its
output has been checked.
"""

import argparse
import logging
from dataclasses import dataclass
from pathlib import Path

from ensembl.utils.database import DBConnection
from sqlalchemy import select

from ensembl.production.metadata.api.factories.utils import format_accession_path
from ensembl.production.metadata.api.models import (
    Assembly,
    Dataset,
    DatasetStatus,
    DatasetType,
    EnsemblRelease,
    Genome,
    GenomeDataset,
    ReleaseStatus,
)

LOGGER = logging.getLogger(__name__)

OLD_FILENAME = "variation.vcf.gz"
NEW_DATASET_TYPE = "short_variant"


@dataclass(frozen=True)
class VariationDataset:
    """The metadata needed to identify one published short-variant VCF."""

    genome_uuid: str
    dataset_uuid: str
    dataset_name: str
    ftp_path: Path | None
    errors: tuple[str, ...] = ()

    @property
    def new_filename(self) -> str:
        return f"{NEW_DATASET_TYPE}-{self.dataset_name}.vcf.gz"


@dataclass
class AuditResult:
    """Filesystem checks and planned changes for one metadata dataset."""

    dataset: VariationDataset
    operations: list[tuple[Path, Path]]
    errors: list[str]
    good_vcfs: int
    bad_vcfs: int
    good_indexes: int
    bad_indexes: int
    can_rename: bool


def fetch_variation_datasets(metadata_uri: str) -> list[VariationDataset]:
    """Return current released short-variant datasets with their FTP paths."""
    query = (
        select(
            Genome.genome_uuid,
            Dataset.dataset_uuid,
            Dataset.name.label("dataset_name"),
            Assembly.accession,
            Genome.annotation_source,
            Genome.genebuild_date,
            EnsemblRelease.label.label("partial_release_label"),
        )
        .select_from(GenomeDataset)
        .join(Genome, GenomeDataset.genome_id == Genome.genome_id)
        .join(Dataset, GenomeDataset.dataset_id == Dataset.dataset_id)
        .join(DatasetType, Dataset.dataset_type_id == DatasetType.dataset_type_id)
        .join(Assembly, Genome.assembly_id == Assembly.assembly_id)
        .join(EnsemblRelease, GenomeDataset.release_id == EnsemblRelease.release_id)
        .where(
            DatasetType.name == "short_variants",
            Dataset.status == DatasetStatus.RELEASED,
            GenomeDataset.is_current.is_(True),
            EnsemblRelease.release_type == "partial",
            EnsemblRelease.status == ReleaseStatus.RELEASED,
        )
        .order_by(Genome.genome_uuid, Dataset.dataset_uuid)
    )

    with DBConnection(metadata_uri).session_scope() as session:
        rows = session.execute(query).all()

    datasets = []
    for row in rows:
        errors = []
        if not row.dataset_name or Path(row.dataset_name).name != row.dataset_name:
            errors.append(f"unsafe dataset name: {row.dataset_name!r}")
        if not row.accession or not row.annotation_source or not row.genebuild_date:
            errors.append("incomplete genome metadata (accession, annotation source, or genebuild date)")

        geneset_date = str(row.genebuild_date)[:7]
        if len(geneset_date) != 7 or geneset_date[4] != "-":
            errors.append(f"invalid genebuild date: {row.genebuild_date!r}")
        if not row.partial_release_label:
            errors.append("missing partial release label")

        ftp_path = None
        if not errors:
            ftp_path = (
                Path(format_accession_path(row.accession))
                / str(row.annotation_source).lower()
                / Path(geneset_date.replace("-", "_"))
                / "variation"
                / str(row.partial_release_label).replace("-", "_")
            )
        datasets.append(
            VariationDataset(
                genome_uuid=row.genome_uuid,
                dataset_uuid=row.dataset_uuid,
                dataset_name=row.dataset_name,
                ftp_path=ftp_path,
                errors=tuple(errors),
            )
        )
    return datasets


def audit_dataset(ftp_root: Path, dataset: VariationDataset) -> AuditResult:
    """Check one dataset, retaining every file error that can be identified."""
    errors = list(dataset.errors)
    if dataset.ftp_path is None:
        return AuditResult(dataset, [], errors, 0, 1, 0, 1, can_rename=False)

    source = ftp_root / dataset.ftp_path / OLD_FILENAME
    vcf_exists = source.is_file()
    if not vcf_exists:
        errors.append(f"VCF not found for {dataset.dataset_uuid}: {source}")

    index_files = sorted(source.parent.glob(f"{OLD_FILENAME}.*"))
    valid_indexes = [index_file for index_file in index_files if index_file.is_file()]
    invalid_indexes = [index_file for index_file in index_files if not index_file.is_file()]
    if not index_files:
        errors.append(f"No index found for {dataset.dataset_uuid}: {source}.*")
    for index_file in invalid_indexes:
        errors.append(f"Index is not a file for {dataset.dataset_uuid}: {index_file}")

    operations = []
    if vcf_exists and not dataset.errors:
        target = source.with_name(dataset.new_filename)
        operations.append((source, target))
        for index_file in valid_indexes:
            suffix = index_file.name.removeprefix(OLD_FILENAME)
            operations.append((index_file, target.with_name(f"{target.name}{suffix}")))

    return AuditResult(
        dataset,
        operations,
        errors,
        good_vcfs=int(vcf_exists),
        bad_vcfs=int(not vcf_exists),
        good_indexes=len(valid_indexes),
        bad_indexes=max(1, len(invalid_indexes)) if not valid_indexes else len(invalid_indexes),
        can_rename=bool(operations),
    )


def preflight_errors(operations: list[tuple[Path, Path]]) -> list[str]:
    """Return global safety errors for a set of otherwise valid operations."""
    errors = []
    sources = [source for source, _ in operations]
    targets = [target for _, target in operations]
    if len(sources) != len(set(sources)):
        errors.append("metadata produced duplicate source files")
    if len(targets) != len(set(targets)):
        errors.append("metadata produced conflicting destination files")
    existing_targets = [str(target) for target in targets if target.exists()]
    if existing_targets:
        errors.append("refusing to overwrite existing destination(s): " + ", ".join(existing_targets))
    return errors


def audit_datasets(ftp_root: Path, datasets: list[VariationDataset]) -> tuple[list[AuditResult], list[str]]:
    """Check every dataset and collect errors without changing the filesystem."""
    results = []
    for dataset in datasets:
        results.append(audit_dataset(ftp_root, dataset))

    operations = [operation for result in results if result.can_rename for operation in result.operations]
    return results, preflight_errors(operations)


def log_audit(results: list[AuditResult], global_errors: list[str], execute: bool) -> None:
    """Print a readable complete audit report before optionally renaming files."""
    failed_results = [result for result in results if result.errors]
    ready_results = [result for result in results if not result.errors]
    LOGGER.info(
        "Variation-file rename audit: %s dataset(s), %s ready, %s with errors",
        len(results),
        len(ready_results),
        len(failed_results),
    )

    if failed_results:
        LOGGER.error("Dataset errors:")
        for result in failed_results:
            dataset = result.dataset
            LOGGER.error(
                "  %s (%s; genome %s)", dataset.dataset_uuid, dataset.dataset_name, dataset.genome_uuid
            )
            if dataset.ftp_path:
                LOGGER.error("    FTP path: %s", dataset.ftp_path)
            for error in result.errors:
                LOGGER.error("    - %s", error)
    if global_errors:
        LOGGER.error("Global safety errors:")
        for error in global_errors:
            LOGGER.error("  - %s", error)

    renamable_results = [result for result in results if result.can_rename]
    if not global_errors and renamable_results:
        action = "Will rename" if execute else "Would rename"
        LOGGER.info("%s:", action)
        for result in renamable_results:
            for source, target in result.operations:
                LOGGER.info("  %s -> %s", source, target)

    LOGGER.info(
        "Summary: short_variant datasets: %s; VCFs changed/good: %s; bad VCFs: %s; "
        "indexes changed/good: %s; bad/missing indexes: %s",
        len(results),
        sum(result.good_vcfs for result in results),
        sum(result.bad_vcfs for result in results),
        sum(result.good_indexes for result in results),
        sum(result.bad_indexes for result in results),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata-uri", required=True, help="Metadata database URI.")
    parser.add_argument("--ftp-root", required=True, type=Path, help="Root containing the public FTP layout.")
    parser.add_argument(
        "--report-file",
        type=Path,
        help="Write the audit report to this file as well as standard error (overwrites an existing file).",
    )
    parser.add_argument("--execute", action="store_true", help="Perform the renames (default: dry run).")
    parser.add_argument(
        "--ignore-errors",
        action="store_true",
        help="With --execute, rename present VCFs even if their indexes are missing; skip unavailable VCFs/metadata.",
    )
    return parser.parse_args()


def add_report_file_handler(report_file: Path) -> None:
    """Mirror the audit log to *report_file*, replacing a prior report."""
    if not report_file.parent.is_dir():
        raise NotADirectoryError(f"Report directory does not exist: {report_file.parent}")
    handler = logging.FileHandler(report_file, mode="w")
    handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    logging.getLogger().addHandler(handler)


def main() -> int:
    args = parse_args()
    if args.ignore_errors and not args.execute:
        raise ValueError("--ignore-errors requires --execute")
    if args.report_file:
        add_report_file_handler(args.report_file)

    ftp_root = args.ftp_root.resolve()
    if not ftp_root.is_dir():
        raise NotADirectoryError(f"FTP root does not exist or is not a directory: {ftp_root}")

    datasets = fetch_variation_datasets(args.metadata_uri)
    if not datasets:
        LOGGER.warning("No current released short-variant datasets found")
        return 0

    results, global_errors = audit_datasets(ftp_root, datasets)
    log_audit(results, global_errors, args.execute)
    failed_results = [result for result in results if result.errors]
    if global_errors:
        LOGGER.error("No files were renamed; fix the reported errors and run again.")
        return 1
    if failed_results and not args.ignore_errors:
        LOGGER.error("No files were renamed; fix the reported errors and run again.")
        return 1
    if failed_results:
        skipped_results = [result for result in failed_results if not result.can_rename]
        LOGGER.warning(
            "Ignoring errors: renaming %s dataset(s) with a present VCF and skipping %s unavailable dataset(s)",
            len(results) - len(skipped_results),
            len(skipped_results),
        )

    operations = [operation for result in results if result.can_rename for operation in result.operations]
    ready_dataset_count = len([result for result in results if result.can_rename])
    if args.execute:
        for source, target in operations:
            source.rename(target)
        LOGGER.info("Renamed %s file(s) for %s dataset(s)", len(operations), ready_dataset_count)
    else:
        LOGGER.info(
            "Dry run complete: %s file(s) for %s dataset(s) are ready to rename",
            len(operations),
            ready_dataset_count,
        )

    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    raise SystemExit(main())
