#!/usr/bin/env python
"""One-off script: backfill the `compara.homology_reference_set` dataset attribute.

For every genome *released* in a given **partial** release (regardless of
which release its current `homologies` dataset happens to be stamped with)
that is missing the `compara.homology_reference_set` dataset attribute,
compute the reference collection via `get_homology_reference_collection()`
and store it.

Usage:
    python backfill_homology_reference_set.py \
        --metadata_uri mysql://user:pass@host:port/ensembl_genome_metadata \
        --taxonomy_uri mysql://user:pass@host:port/ncbi_taxonomy \
        --release_name 110 \
        [--commit] [--force] [--limit N]

By default this is a DRY RUN: it logs what it would do but does not write
anything. Pass --commit to actually persist changes.
"""
import argparse
import logging

from ensembl.production.metadata.api.exceptions import UpdaterException
from ensembl.production.metadata.api.models import (
    Attribute,
    Dataset,
    EnsemblRelease,
    Genome,
    GenomeDataset,
    GenomeGroup,
    GenomeRelease,
)
from ensembl.production.metadata.updater.updater_utils import (
    get_homology_reference_collection,
    update_attributes,
)
from ensembl.utils.database import DBConnection
from sqlalchemy.orm import joinedload

ATTRIBUTE_NAME = "compara.homology_reference_set"
DATASET_TYPE_NAME = "homologies"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("backfill_homology_reference_set.log", mode="w"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def resolve_partial_release(session, release_name: str) -> EnsemblRelease:
    """Look up the `EnsemblRelease` named ``release_name`` and check it is a partial release."""
    release = session.query(EnsemblRelease).filter(EnsemblRelease.name == release_name).one_or_none()
    if release is None:
        raise SystemExit(f"No EnsemblRelease with name '{release_name}' found.")
    if release.release_type != "partial":
        raise SystemExit(
            f"EnsemblRelease '{release_name}' is a '{release.release_type}' release. "
            "This script only backfills genomes from a 'partial' release."
        )
    return release


def check_preconditions(session) -> None:
    """Fail fast with a clear message rather than erroring out genome-by-genome."""
    if session.query(Attribute).filter(Attribute.name == ATTRIBUTE_NAME).one_or_none() is None:
        raise SystemExit(
            f"Attribute '{ATTRIBUTE_NAME}' does not exist in the `attribute` table of the target metadata "
            "database. Add it before running this script."
        )
    n_collections = session.query(GenomeGroup).filter(GenomeGroup.type == "compara_reference").count()
    if n_collections == 0:
        raise SystemExit(
            "No `genome_group` rows of type 'compara_reference' found in the target metadata database. "
            "get_homology_reference_collection() cannot resolve anything without these."
        )
    logger.info(f"Found {n_collections} compara_reference genome_group rows.")


def iter_current_homology_datasets(session, release_id: int):
    """Yield (Genome, Dataset) for every genome *released* in ``release_id``, paired with its
    current 'homologies' dataset (which may itself be stamped with a different, e.g. later,
    release_id than the genome's own release)."""
    query = (
        session.query(Genome, Dataset)
        .join(GenomeRelease, GenomeRelease.genome_id == Genome.genome_id)
        .join(GenomeDataset, GenomeDataset.genome_id == Genome.genome_id)
        .join(Dataset, Dataset.dataset_id == GenomeDataset.dataset_id)
        .options(joinedload(Genome.organism))
        .filter(GenomeRelease.release_id == release_id)
        .filter(Dataset.dataset_type.has(name=DATASET_TYPE_NAME))
        .filter(GenomeDataset.is_current == 1)
    )
    yield from query.all()


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--metadata_uri", required=True, help="Metadata DB URI")
    parser.add_argument("--taxonomy_uri", required=True, help="NCBI taxonomy DB URI")
    parser.add_argument(
        "--release_name",
        required=True,
        help="Name (EnsemblRelease.name) of the partial release whose genomes should be backfilled",
    )
    parser.add_argument("--commit", action="store_true", help="Actually write changes (default: dry run)")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Recompute and replace the attribute even if it is already set on the dataset",
    )
    parser.add_argument(
        "--limit", type=int, default=None, help="Only process the first N datasets (for testing)"
    )
    args = parser.parse_args()

    dbc = DBConnection(args.metadata_uri)

    # Cache reference-collection lookups by taxonomy_id: many genomes (e.g. multiple
    # assemblies of the same species) share a taxonomy_id, and each lookup opens its
    # own connection/session against the taxonomy DB, so this avoids redundant work.
    taxonomy_cache: dict[int, str | Exception] = {}

    stats = {"total": 0, "already_set": 0, "set": 0, "unresolved": 0, "errors": 0}

    with dbc.session_scope() as session:
        check_preconditions(session)
        release = resolve_partial_release(session, args.release_name)

        rows = list(iter_current_homology_datasets(session, release.release_id))
        if args.limit:
            rows = rows[: args.limit]
        logger.info(
            f"Found {len(rows)} genomes with a '{DATASET_TYPE_NAME}' dataset in partial release "
            f"'{args.release_name}'."
        )

        for genome, dataset in rows:
            stats["total"] += 1
            existing = next(
                (da for da in dataset.dataset_attributes if da.attribute.name == ATTRIBUTE_NAME), None
            )
            if existing is not None and not args.force:
                stats["already_set"] += 1
                logger.debug(
                    f"Genome {genome.genome_uuid} dataset {dataset.dataset_uuid}: "
                    f"'{ATTRIBUTE_NAME}' already set to '{existing.value}', skipping."
                )
                continue

            taxonomy_id = genome.organism.taxonomy_id
            if taxonomy_id not in taxonomy_cache:
                try:
                    taxonomy_cache[taxonomy_id] = get_homology_reference_collection(
                        taxonomy_id, args.taxonomy_uri, session
                    )
                except Exception as exc:  # noqa: BLE001 - one bad taxon must not abort the whole backfill
                    taxonomy_cache[taxonomy_id] = exc

            result = taxonomy_cache[taxonomy_id]
            if isinstance(result, Exception):
                stats["unresolved"] += 1
                logger.warning(f"Genome {genome.genome_uuid} (taxonomy_id={taxonomy_id}): {result}")
                continue

            logger.info(
                f"Genome {genome.genome_uuid} dataset {dataset.dataset_uuid}: "
                f"setting '{ATTRIBUTE_NAME}' = '{result}'"
            )
            if args.commit:
                try:
                    update_attributes(
                        dataset, {ATTRIBUTE_NAME: result}, session, replace=(existing is not None)
                    )
                    session.flush()
                    stats["set"] += 1
                except UpdaterException as exc:
                    stats["errors"] += 1
                    logger.error(f"Genome {genome.genome_uuid}: failed to set attribute: {exc}")
            else:
                stats["set"] += 1

        if args.commit:
            session.commit()
            logger.info("Changes committed.")
        else:
            session.rollback()
            logger.info("Dry run: no changes were written. Re-run with --commit to persist.")

    logger.info(f"Summary: {stats}")


if __name__ == "__main__":
    main()
