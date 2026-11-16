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
``.tbi``; for example, both ``.tbi`` and ``.csi`` are supported.

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
    ftp_path: Path

    @property
    def new_filename(self) -> str:
        return f"{NEW_DATASET_TYPE}-{self.dataset_name}.vcf.gz"


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
        if not row.dataset_name or Path(row.dataset_name).name != row.dataset_name:
            raise ValueError(f"Dataset {row.dataset_uuid} has an unsafe name: {row.dataset_name!r}")
        if not row.accession or not row.annotation_source or not row.genebuild_date:
            raise ValueError(f"Dataset {row.dataset_uuid} has incomplete genome metadata")

        geneset_date = str(row.genebuild_date)[:7]
        if len(geneset_date) != 7 or geneset_date[4] != "-":
            raise ValueError(
                f"Dataset {row.dataset_uuid} has an invalid genebuild date: {row.genebuild_date!r}"
            )

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
            )
        )
    return datasets


def planned_renames(ftp_root: Path, dataset: VariationDataset) -> list[tuple[Path, Path]]:
    """Build the VCF and detected-index rename operations for one dataset."""
    source = ftp_root / dataset.ftp_path / OLD_FILENAME
    index_files = sorted(source.parent.glob(f"{OLD_FILENAME}.*"))
    if not source.is_file():
        raise FileNotFoundError(f"VCF not found for {dataset.dataset_uuid}: {source}")
    if not index_files:
        raise FileNotFoundError(f"No index found for {dataset.dataset_uuid}: {source}.*")

    target = source.with_name(dataset.new_filename)
    renames = [(source, target)]
    for index_file in index_files:
        if not index_file.is_file():
            raise FileNotFoundError(f"Index is not a file for {dataset.dataset_uuid}: {index_file}")
        suffix = index_file.name.removeprefix(OLD_FILENAME)
        renames.append((index_file, target.with_name(f"{target.name}{suffix}")))
    return renames


def preflight(operations: list[tuple[Path, Path]]) -> None:
    """Reject duplicate sources/targets and any operation that would overwrite a file."""
    sources = [source for source, _ in operations]
    targets = [target for _, target in operations]
    if len(sources) != len(set(sources)):
        raise ValueError("Metadata produced duplicate source files")
    if len(targets) != len(set(targets)):
        raise ValueError("Metadata produced conflicting destination files")
    existing_targets = [str(target) for target in targets if target.exists()]
    if existing_targets:
        raise FileExistsError("Refusing to overwrite existing destination(s): " + ", ".join(existing_targets))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata-uri", required=True, help="Metadata database URI.")
    parser.add_argument("--ftp-root", required=True, type=Path, help="Root containing the public FTP layout.")
    parser.add_argument("--execute", action="store_true", help="Perform the renames (default: dry run).")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    ftp_root = args.ftp_root.resolve()
    if not ftp_root.is_dir():
        raise NotADirectoryError(f"FTP root does not exist or is not a directory: {ftp_root}")

    datasets = fetch_variation_datasets(args.metadata_uri)
    if not datasets:
        LOGGER.warning("No current released short-variant datasets found")
        return 0

    operations = [operation for dataset in datasets for operation in planned_renames(ftp_root, dataset)]
    preflight(operations)

    mode = "Renaming" if args.execute else "Would rename"
    for source, target in operations:
        LOGGER.info("%s %s -> %s", mode, source, target)
        if args.execute:
            source.rename(target)

    LOGGER.info("%s %s file(s) for %s dataset(s)", mode, len(operations), len(datasets))
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    raise SystemExit(main())
