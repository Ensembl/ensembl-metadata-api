import argparse
import logging
import re
import shutil
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from ensembl.utils.database import DBConnection
from sqlalchemy import select

from ensembl.production.metadata.api.models import (
    Assembly,
    EnsemblRelease,
    Genome,
    GenomeRelease,
    Organism,
)

LOGGER = logging.getLogger(__name__)

REQUIRED_OUTPUTS = (
    "genes.gff3.bgz",
    "genes.gff3.bgz.csi",
    "unmasked.fa.bgz",
    "unmasked.fa.bgz.gzi",
    "unmasked.fa.bgz.fai",
)


@dataclass(frozen=True)
class GenomeVepRecord:
    genome_uuid: str
    assembly_uuid: str
    assembly_accession: str
    scientific_name: str
    annotation_source: str
    last_geneset_update: str


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")


def normalise_scientific_name(scientific_name: str) -> str:
    value = re.sub(r"[^a-zA-Z0-9]+", " ", scientific_name)
    return re.sub(r" +", "_", value).strip("_")


def normalise_last_geneset_update(last_geneset_update: str) -> str:
    match = re.match(r"^(\d{4}-\d{2})", last_geneset_update)
    if match:
        return match.group(1).replace("-", "_")
    return last_geneset_update.replace("-", "_")


def format_assembly_uuid_path(assembly_uuid: str) -> str:
    compact_uuid = assembly_uuid.replace("-", "")
    return f"{compact_uuid[:3]}/{compact_uuid}"


def format_genome_uuid(genome_uuid: str) -> str:
    return genome_uuid.replace("-", "")


def old_relative_paths(record: GenomeVepRecord) -> dict[str, list[Path]]:
    scientific_name = normalise_scientific_name(record.scientific_name)
    geneset_update = normalise_last_geneset_update(record.last_geneset_update)

    genome_dir = Path(scientific_name) / record.assembly_accession / "vep" / "genome"
    geneset_dir = (
        Path(scientific_name)
        / record.assembly_accession
        / "vep"
        / record.annotation_source
        / "geneset"
        / geneset_update
    )

    return {
        "genes.gff3.bgz": [geneset_dir / "genes.gff3.bgz"],
        "genes.gff3.bgz.csi": [geneset_dir / "genes.gff3.bgz.csi"],
        "unmasked.fa.bgz": [genome_dir / "unmasked.fa.bgz"],
        "unmasked.fa.bgz.gzi": [genome_dir / "unmasked.fa.bgz.gzi"],
        "unmasked.fa.bgz.fai": [genome_dir / "unmasked.fa.bgz.fai"],
    }


def new_relative_paths(record: GenomeVepRecord) -> dict[str, Path]:
    assembly_dir = Path(format_assembly_uuid_path(record.assembly_uuid))
    genome_dir = assembly_dir / format_genome_uuid(record.genome_uuid)
    return {
        "genes.gff3.bgz": genome_dir / "genes.gff3.bgz",
        "genes.gff3.bgz.csi": genome_dir / "genes.gff3.bgz.csi",
        "unmasked.fa.bgz": assembly_dir / "unmasked.fa.bgz",
        "unmasked.fa.bgz.gzi": assembly_dir / "unmasked.fa.bgz.gzi",
        "unmasked.fa.bgz.fai": assembly_dir / "unmasked.fa.bgz.fai",
    }


def fetch_genomes(
    metadata_uri: str, release: str | float | Decimal | None = None, genome_uuid: str | None = None
) -> list[GenomeVepRecord]:
    query = (
        select(
            Genome.genome_uuid,
            Assembly.assembly_uuid,
            Assembly.accession,
            Organism.scientific_name,
            Genome.annotation_source,
            Genome.genebuild_date.label("last_geneset_update"),
        )
        .select_from(Genome)
        .join(Assembly, Genome.assembly_id == Assembly.assembly_id)
        .join(Organism, Genome.organism_id == Organism.organism_id)
    )

    if release is not None:
        query = (
            query.join(GenomeRelease, GenomeRelease.genome_id == Genome.genome_id)
            .join(EnsemblRelease, EnsemblRelease.release_id == GenomeRelease.release_id)
            .where(EnsemblRelease.name == release)
        )
    if genome_uuid:
        query = query.where(Genome.genome_uuid == genome_uuid)

    with DBConnection(metadata_uri).session_scope() as session:
        rows = session.execute(query).all()

    records = [
        GenomeVepRecord(
            genome_uuid=row.genome_uuid,
            assembly_uuid=row.assembly_uuid,
            assembly_accession=row.accession,
            scientific_name=row.scientific_name,
            annotation_source=row.annotation_source,
            last_geneset_update=row.last_geneset_update,
        )
        for row in rows
    ]

    missing_metadata = [
        record.genome_uuid
        for record in records
        if not record.annotation_source or not record.last_geneset_update or not record.scientific_name
    ]
    if missing_metadata:
        raise ValueError(
            "Missing required genebuild metadata for genomes: " + ", ".join(sorted(missing_metadata))
        )

    return records


def copy_genome_files(
    record: GenomeVepRecord, old_base_dir: Path, new_base_dir: Path, dry_run: bool = False
) -> list[str]:
    warnings: list[str] = []
    source_paths = old_relative_paths(record)
    target_paths = new_relative_paths(record)

    for filename in REQUIRED_OUTPUTS:
        target_path = new_base_dir / target_paths[filename]
        source_path = next(
            (
                old_base_dir / candidate
                for candidate in source_paths[filename]
                if (old_base_dir / candidate).exists()
            ),
            None,
        )

        if source_path is None:
            warnings.append(
                f"{record.genome_uuid}: missing source for {filename} "
                f"(checked: {', '.join(str(path) for path in source_paths[filename])})"
            )
            continue

        LOGGER.info("Copying %s -> %s", source_path, target_path)
        if not dry_run:
            target_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_path, target_path)

    missing_outputs = [
        filename
        for filename in REQUIRED_OUTPUTS
        if not dry_run and not (new_base_dir / target_paths[filename]).exists()
    ]
    if missing_outputs:
        warnings.append(
            f"{record.genome_uuid}: required outputs missing after copy: {', '.join(missing_outputs)}"
        )

    return warnings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rearrange VEP files from the legacy layout into the UUID-based layout."
    )
    parser.add_argument(
        "--old_base_dir", required=True, help="Root directory containing the legacy VEP layout."
    )
    parser.add_argument(
        "--new_base_dir", required=True, help="Root directory to populate with the new layout."
    )
    parser.add_argument("--metadata_uri", required=True, help="Metadata database URI.")
    parser.add_argument("--release", help="Exact Ensembl release name to process.")
    parser.add_argument("--genome_uuid", help="Optional single genome UUID to copy.")
    parser.add_argument("--dry_run", action="store_true", help="Log planned copies without writing files.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    args = parser.parse_args()
    if not args.release and not args.genome_uuid:
        parser.error("at least one of --release or --genome_uuid is required")
    return args


def main() -> int:
    args = parse_args()
    configure_logging(args.verbose)

    old_base_dir = Path(args.old_base_dir).expanduser().resolve()
    new_base_dir = Path(args.new_base_dir).expanduser().resolve()

    if not old_base_dir.is_dir():
        raise FileNotFoundError(f"Old base directory does not exist: {old_base_dir}")

    records = fetch_genomes(
        metadata_uri=args.metadata_uri,
        release=args.release,
        genome_uuid=args.genome_uuid,
    )
    if not records:
        filters = []
        if args.release:
            filters.append(f"release {args.release}")
        if args.genome_uuid:
            filters.append(f"genome_uuid {args.genome_uuid}")
        raise ValueError(f"No genomes found for {' and '.join(filters)}")

    warnings: list[str] = []
    for record in records:
        warnings.extend(copy_genome_files(record, old_base_dir, new_base_dir, dry_run=args.dry_run))

    LOGGER.info("Processed %s genome(s)", len(records))
    if warnings:
        LOGGER.warning("Completed with %s warning(s):", len(warnings))
        for warning in warnings:
            LOGGER.warning(warning)
        return 1

    LOGGER.info("Completed without warnings")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
