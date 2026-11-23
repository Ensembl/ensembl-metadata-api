#!/usr/bin/env python3
"""Generate the release taxonomy counts.

The metadata rows are read and written through the SQLAlchemy ORM.  Taxonomy
lookups use :class:`ensembl.taxonomy.db.TaxonDB`, which reads a cached Parcade
/ DuckDB taxonomy directory rather than the former MySQL taxonomy schema.
"""
from __future__ import annotations

import argparse
import itertools
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Protocol

from ensembl.utils.database import DBConnection
from sqlalchemy import or_
from sqlalchemy.orm import joinedload

from ensembl.production.metadata.api.models import (
    EnsemblRelease,
    Genome,
    GenomeRelease,
    GenomeTaxonomyCount,
    ReleaseStatus,
)


class TaxonomyDatabase(Protocol):
    """The small portion of TaxonDB needed by this report."""

    def __enter__(self) -> "TaxonomyDatabase": ...

    def __exit__(self, exc_type, exc_value, traceback) -> None: ...

    def lineage_ids(self, taxon_id: int) -> list[int]: ...

    def primary_id(self, taxon_id: int) -> int | None: ...

    def query_taxa_by_name(
        self, pattern: str, name_classes: list[str], case_sensitive: bool, full_match: bool
    ) -> list[int]: ...


@dataclass(frozen=True)
class GenomeTaxonomyCountReport:
    """The rows written to metadata and taxonomy IDs that could not be resolved."""

    rows: list[GenomeTaxonomyCount]
    failed_taxonomy_ids: list[int]


# Ordered exactly as the former SQL script's tax_stop_levels table.
TAXONOMY_GROUPS = (
    ("metazoa", "Animals", 1),
    ("archaea", "Archaea", 2),
    ("bacteria", "Bacteria", 3),
    ("fungi", "Fungi", 4),
    ("viridiplantae", "Green Plants", 5),
    ("root", "Others", 6),
)

ProgressCallback = Callable[[str, int, int], None]
QUERY_BATCH_SIZE = 1_000


def _taxonomy_uri(path_or_uri: str) -> str:
    """Accept a taxonomy cache directory or its ``file:`` URI."""
    if path_or_uri.startswith("file:"):
        return path_or_uri
    return Path(path_or_uri).expanduser().resolve().as_uri()


def _ensure_itertools_batched() -> None:
    """Provide the Python 3.12 itertools API used by ensembl-taxonomy on Python 3.10/3.11."""
    if hasattr(itertools, "batched"):
        return

    def batched(iterable, size):
        if size < 1:
            raise ValueError("n must be at least one")
        iterator = iter(iterable)
        while batch := tuple(itertools.islice(iterator, size)):
            yield batch

    itertools.batched = batched


def _taxon_db(taxonomy_location: str) -> TaxonomyDatabase:
    try:
        _ensure_itertools_batched()
        from ensembl.taxonomy.db import TaxonDB
    except ImportError as exc:  # pragma: no cover - depends on installed extras
        raise RuntimeError(
            "ensembl-taxonomy >= 2.0.0 is required to read Parcade taxonomy data"
        ) from exc
    return TaxonDB.from_uri(_taxonomy_uri(taxonomy_location))


class GenomeTaxonomyCountGenerator:
    """Build and persist the all-release and per-release taxonomy counts."""

    def __init__(self, metadata_uri: str, taxonomy_location: str, taxon_db_factory=_taxon_db):
        self.metadata_db = DBConnection(metadata_uri)
        self.taxonomy_location = taxonomy_location
        self.taxon_db_factory = taxon_db_factory

    @staticmethod
    def _stop_levels(taxon_db: TaxonomyDatabase) -> dict[int, tuple[str, int]]:
        stop_levels = {}
        for name, display_name, order in TAXONOMY_GROUPS:
            taxon_ids = taxon_db.query_taxa_by_name(
                name,
                name_classes=["scientific name"],
                case_sensitive=False,
                full_match=True,
            )
            if len(taxon_ids) != 1:
                raise ValueError(
                    f"expected one scientific-name taxonomy entry for {name!r}, found {len(taxon_ids)}"
                )
            stop_levels[taxon_ids[0]] = (display_name, order)
        return stop_levels

    @staticmethod
    def _current_taxon_id(taxon_db: TaxonomyDatabase, taxon_id: int) -> int | None:
        # primary_id is the new taxon for an NCBI merged ID and the ID itself
        # for current IDs.  It is None for a deleted or unknown ID, which does
        # not contribute a count, as in the old left join.
        return taxon_db.primary_id(taxon_id)

    @staticmethod
    def _group_taxon_id(taxon_db: TaxonomyDatabase, taxon_id: int, stop_levels: dict[int, tuple[str, int]]):
        lineage = taxon_db.lineage_ids(taxon_id)

        # The SQL macro started at the parent.  Therefore a taxon that itself
        # is a stop level belongs to the next matching ancestral stop level.
        for ancestor_id in reversed(lineage[:-1]):
            if ancestor_id in stop_levels:
                return ancestor_id
        # The root taxon is its own parent in the NCBI taxonomy.
        return taxon_id if taxon_id in stop_levels else None

    @staticmethod
    def _all_release_genomes(session):
        return (
            session.query(Genome)
            .join(GenomeRelease, GenomeRelease.genome_id == Genome.genome_id)
            .join(EnsemblRelease, EnsemblRelease.release_id == GenomeRelease.release_id)
            .filter(
                or_(Genome.suppressed.is_(False), EnsemblRelease.release_type == "integrated"),
                or_(EnsemblRelease.release_type == "integrated", GenomeRelease.is_current.is_(True)),
            )
            .distinct()
        )

    @staticmethod
    def _released_genomes(session):
        return (
            session.query(EnsemblRelease, Genome)
            .join(GenomeRelease, GenomeRelease.release_id == EnsemblRelease.release_id)
            .join(Genome, Genome.genome_id == GenomeRelease.genome_id)
            .filter(
                Genome.suppressed.is_(False),
                GenomeRelease.is_current.is_(True),
                EnsemblRelease.status == ReleaseStatus.RELEASED,
            )
        )

    @staticmethod
    def _report_progress(
        callback: ProgressCallback | None, phase: str, processed: int, total: int, progress_every: int
    ) -> None:
        if callback and (processed == 0 or processed == total or processed % progress_every == 0):
            callback(phase, processed, total)

    def generate(
        self,
        progress_callback: ProgressCallback | None = None,
        progress_every: int = QUERY_BATCH_SIZE,
    ) -> GenomeTaxonomyCountReport:
        """Replace the report table and return its rows and unresolved taxonomy IDs."""
        if progress_every < 1:
            raise ValueError("progress_every must be at least one")
        # ``create`` is SQLAlchemy schema management, not handwritten DDL;
        # checkfirst keeps this safe to run for every release.
        with self.metadata_db.connect() as connection:
            GenomeTaxonomyCount.__table__.create(connection, checkfirst=True)

        with self.taxon_db_factory(self.taxonomy_location) as taxon_db:
            stop_levels = self._stop_levels(taxon_db)
            group_cache: dict[int, int | None] = {}
            failed_taxonomy_ids: set[int] = set()

            def group_for(taxonomy_id: int) -> int | None:
                if taxonomy_id not in group_cache:
                    current_id = self._current_taxon_id(taxon_db, taxonomy_id)
                    group_cache[taxonomy_id] = (
                        self._group_taxon_id(taxon_db, current_id, stop_levels)
                        if current_id is not None
                        else None
                    )
                    if group_cache[taxonomy_id] is None:
                        failed_taxonomy_ids.add(taxonomy_id)
                return group_cache[taxonomy_id]

            with self.metadata_db.session_scope() as session:
                counts: Counter[tuple[int, str, int]] = Counter()
                all_release_query = self._all_release_genomes(session)
                all_release_total = all_release_query.count()
                self._report_progress(
                    progress_callback, "Current-release genomes", 0, all_release_total, progress_every
                )
                for processed, genome in enumerate(
                    all_release_query.options(joinedload(Genome.organism)).yield_per(QUERY_BATCH_SIZE),
                    start=1,
                ):
                    group_id = group_for(genome.organism.taxonomy_id)
                    if group_id is not None:
                        counts[(0, "", group_id)] += 1
                    self._report_progress(
                        progress_callback,
                        "Current-release genomes",
                        processed,
                        all_release_total,
                        progress_every,
                    )

                released_query = self._released_genomes(session)
                released_total = released_query.count()
                self._report_progress(
                    progress_callback, "Released-release genomes", 0, released_total, progress_every
                )
                for processed, (release, genome) in enumerate(
                    released_query.options(joinedload(Genome.organism)).yield_per(QUERY_BATCH_SIZE), start=1
                ):
                    group_id = group_for(genome.organism.taxonomy_id)
                    if group_id is not None:
                        counts[(release.release_id, release.label or "", group_id)] += 1
                    self._report_progress(
                        progress_callback,
                        "Released-release genomes",
                        processed,
                        released_total,
                        progress_every,
                    )

                session.query(GenomeTaxonomyCount).delete()
                rows = []
                for (release_id, label, taxon_id), count in counts.items():
                    display_name, order = stop_levels[taxon_id]
                    rows.append(
                        GenomeTaxonomyCount(
                            release_id=release_id,
                            label=label,
                            taxon_id=taxon_id,
                            ensembl_taxon_name=display_name,
                            count=count,
                            ord=order,
                        )
                    )
                session.add_all(rows)
                session.flush()
                # DBConnection commits on leaving the context and expires
                # attached instances.  Detach the result rows so callers can
                # use the generated report after that commit.
                session.expunge_all()
                return GenomeTaxonomyCountReport(
                    rows=sorted(rows, key=lambda row: (row.release_id, row.ord)),
                    failed_taxonomy_ids=sorted(failed_taxonomy_ids),
                )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create metadata genome taxonomy counts from a Parcade cache.")
    parser.add_argument("--metadata-uri", required=True, help="SQLAlchemy metadata database URI")
    parser.add_argument(
        "--taxonomy-location",
        required=True,
        help="Taxonomy cache directory or file:// URI containing ncbi_taxa_*.parquet",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=QUERY_BATCH_SIZE,
        help="Report progress to stderr after this many genomes (default: %(default)s)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    def print_progress(phase: str, processed: int, total: int) -> None:
        percent = (processed / total * 100) if total else 100
        print(f"{phase}: {processed:,}/{total:,} ({percent:.1f}%)", file=sys.stderr)

    report = GenomeTaxonomyCountGenerator(args.metadata_uri, args.taxonomy_location).generate(
        progress_callback=print_progress,
        progress_every=args.progress_every,
    )
    print(f"Created genome_taxonomy_counts with {sum(row.count for row in report.rows)} counted genomes.")
    for row in report.rows:
        print(f"{row.release_id}\t{row.label}\t{row.ensembl_taxon_name}\t{row.count}")
    if report.failed_taxonomy_ids:
        failed_ids = ", ".join(map(str, report.failed_taxonomy_ids))
        print(f"Skipped taxonomy IDs that could not be resolved: {failed_ids}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
