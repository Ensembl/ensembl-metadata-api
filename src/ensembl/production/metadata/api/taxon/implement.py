#   See the NOTICE file distributed with this work for additional information
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
"""Apply a taxonomy_updates.json report (from compare.py) to the metadata DB organism table.

Usage:
    python implement.py \\
        --metadata-uri=mysql://user@host:port/ensembl_genome_metadata \\
        --json-path=taxonomy_updates.json \\
        [--dry-run] [--update-common [--yes]] \\
        [--confidence all | case_diff order_diff similar different]

By default only id and scientific name changes are applied: taxonomy_id
(taxon_id_update), species_taxonomy_id (species_id_update) and scientific_name.

--update-common also applies common_name changes (the JSON must come from
compare.py --include-common-name). Previous common names are not kept anywhere
else, so this asks for a typed "yes" before writing unless --yes is given.

--confidence keeps only name changes whose confidence is in the given set. It
applies only to organisms whose taxonomy_id is not changing; an organism with a
taxonomy_id merge always gets its name changes applied together with it.

Before writing, each change is checked against the DB: changes already applied
are skipped, and changes whose DB value no longer matches the report's "current"
value are skipped as stale. Every real run first writes a revert SQL file holding
the previous values of every field it changes.
"""

import argparse
import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from ensembl.utils.database import DBConnection
from sqlalchemy import select, update

from ensembl.production.metadata.api.models.organism import Organism
from ensembl.production.metadata.api.taxon.schema import ConfidenceLevel, TaxonUpdateReport

logger = logging.getLogger(__name__)

_FIELDS = ("taxonomy_id", "species_taxonomy_id", "scientific_name", "common_name")
_NAME_FIELDS = ("scientific_name", "common_name")
_CONFIDENCE_ALIASES = {"diff": ConfidenceLevel.DIFFERENT.value}


@dataclass
class FieldChange:
    current: int | str | None
    new: int | str
    confidence: ConfidenceLevel | None
    sources: list[str] = field(default_factory=list)


@dataclass
class OrganismUpdate:
    organism_id: int
    changes: dict[str, FieldChange] = field(default_factory=dict)

    @property
    def sources(self) -> list[str]:
        return sorted({s for change in self.changes.values() for s in change.sources})


# ── Collecting updates from the report ───────────────────────────────────────

def collect_updates(report: TaxonUpdateReport) -> dict[int, OrganismUpdate]:
    """Merge every update entry's targets into one OrganismUpdate per organism_id.

    An organism can be named as a target by more than one entry (e.g. a
    taxonomy_id merge and a separate name change). Entries agreeing on a field
    are merged; entries disagreeing on the same field raise instead of silently
    picking one.
    """
    updates: dict[int, OrganismUpdate] = {}
    for update_key, entry in report.updates.items():
        for target in entry.targets:
            pending = updates.setdefault(target.organism_id, OrganismUpdate(target.organism_id))
            for field_name in _FIELDS:
                change = getattr(entry.changes, field_name, None)
                if change is None or change.new is None:
                    continue
                existing = pending.changes.get(field_name)
                if existing is None:
                    pending.changes[field_name] = FieldChange(
                        current=change.current, new=change.new,
                        confidence=getattr(change, "confidence", None), sources=[update_key],
                    )
                elif existing.new == change.new:
                    existing.sources.append(update_key)
                else:
                    raise ValueError(
                        f"Conflicting {field_name} for organism_id={target.organism_id} from "
                        f"{', '.join(existing.sources)} and {update_key}: {existing.new!r} vs {change.new!r}"
                    )
    return updates


# ── Filtering by --update-common and --confidence ────────────────────────────

def filter_updates(
    updates: dict[int, OrganismUpdate], update_common: bool, confidences: set[str] | None
) -> dict[int, OrganismUpdate]:
    """Return a copy of updates holding only the changes to apply.

    common_name is dropped unless update_common. For organisms with no
    taxonomy_id change, name changes outside `confidences` are dropped
    (None means keep all); a taxonomy_id merge keeps its names unfiltered.
    """
    filtered: dict[int, OrganismUpdate] = {}
    for oid, pending in updates.items():
        changes = dict(pending.changes)
        if not update_common:
            changes.pop("common_name", None)
        if confidences is not None and "taxonomy_id" not in changes:
            for name in _NAME_FIELDS:
                change = changes.get(name)
                if change is not None and (change.confidence is None or change.confidence.value not in confidences):
                    changes.pop(name)
        if changes:
            filtered[oid] = OrganismUpdate(oid, changes)
    return filtered


# ── Stale check against the DB ──────────────────────────────────────────────

def drop_stale(session, updates: dict[int, OrganismUpdate]) -> dict[int, OrganismUpdate]:
    """Check each change against the DB and keep only those still to be made.

    Per field: DB already at the new value -> already applied, dropped quietly;
    DB at the report's "current" value -> kept, with `current` set to the exact
    DB value for the revert file; anything else -> stale, dropped with a warning.
    A stale taxonomy_id also drops its scientific_name change. Organisms missing
    from the DB are skipped.
    """
    if not updates:
        return {}
    rows = session.execute(
        select(Organism.organism_id, *(getattr(Organism, f) for f in _FIELDS))
        .where(Organism.organism_id.in_(list(updates)))
    ).all()
    db_values = {row[0]: dict(zip(_FIELDS, row[1:])) for row in rows}

    fresh: dict[int, OrganismUpdate] = {}
    for oid, pending in updates.items():
        if oid not in db_values:
            logger.warning("Skipping organism_id=%d: not found in DB", oid)
            continue
        changes: dict[str, FieldChange] = {}
        stale: list[str] = []
        for name, change in pending.changes.items():
            db_value = db_values[oid][name]
            if (db_value or None) == change.new:
                continue
            if (db_value or None) == (change.current or None):
                changes[name] = FieldChange(db_value, change.new, change.confidence, change.sources)
            else:
                stale.append(f"{name}: report={change.current!r} db={db_value!r}")
        if any(s.startswith("taxonomy_id:") for s in stale) and changes.pop("scientific_name", None):
            stale.append("scientific_name: dropped with stale taxonomy_id")
        if stale:
            logger.warning("organism_id=%d: skipping stale changes: %s", oid, "; ".join(stale))
        if changes:
            fresh[oid] = OrganismUpdate(oid, changes)
    return fresh


# ── Preview / apply ─────────────────────────────────────────────────────────

def _sql_literal(value: int | str | None) -> str:
    if value is None:
        return "NULL"
    return str(value) if isinstance(value, int) else "'" + value.replace("'", "''") + "'"


def render_sql(updates: dict[int, OrganismUpdate], revert: bool = False) -> list[str]:
    """UPDATE statements setting each change's new value, or its previous value if revert."""
    statements: list[str] = []
    for oid in sorted(updates):
        pending = updates[oid]
        assignments = ", ".join(
            f"{name} = {_sql_literal(change.current if revert else change.new)}"
            for name, change in pending.changes.items()
        )
        statements.append(f"-- organism_id {oid}; from {', '.join(pending.sources)}")
        statements.append(f"UPDATE organism SET {assignments} WHERE organism_id = {oid};")
    return statements


def apply_updates(session, updates: dict[int, OrganismUpdate]) -> int:
    for oid in sorted(updates):
        values = {name: change.new for name, change in updates[oid].changes.items()}
        session.execute(update(Organism).where(Organism.organism_id == oid).values(**values))
    return len(updates)


def _field_counts(updates: dict[int, OrganismUpdate]) -> dict[str, int]:
    counts = {name: 0 for name in _FIELDS}
    for pending in updates.values():
        for name in pending.changes:
            counts[name] += 1
    return counts


def summarize(updates: dict[int, OrganismUpdate]) -> str:
    lines = [f"organisms affected: {len(updates)}"]
    lines += [f"  {name} changes: {count}" for name, count in _field_counts(updates).items()]
    return "\n".join(lines)


def _confirm_common_names(updates: dict[int, OrganismUpdate], revert_path: Path) -> bool:
    """Ask for a typed "yes" before overwriting common names."""
    n_common = _field_counts(updates)["common_name"]
    if n_common == 0:
        return True
    if not sys.stdin.isatty():
        print("Refusing to update common names without a terminal to confirm; pass --yes to proceed.")
        return False
    print(f"About to overwrite {n_common} common_name value(s). Previous values are only kept in {revert_path}.")
    try:
        return input('Type "yes" to continue: ').strip() == "yes"
    except EOFError:
        return False


def run(
    metadata_uri: str,
    json_path: Path,
    update_common: bool,
    confidences: set[str] | None,
    dry_run: bool,
    revert_path: Path,
    assume_yes: bool,
) -> None:
    report = TaxonUpdateReport.model_validate_json(json_path.read_text())
    selected = filter_updates(collect_updates(report), update_common, confidences)

    metadata_db = DBConnection(metadata_uri)
    with metadata_db.session_scope() as session:
        selected = drop_stale(session, selected)
        print(summarize(selected))
        if dry_run:
            print("-- Dry run: no changes made. SQL that would be executed:")
            print("\n".join(render_sql(selected)))
            return
        if not selected:
            return
        if update_common and not assume_yes and not _confirm_common_names(selected, revert_path):
            print("Aborted: no changes made.")
            return
        revert_path.write_text("\n".join([
            f"-- Revert for {json_path} applied at {datetime.now().isoformat()}",
            *render_sql(selected, revert=True),
        ]) + "\n")
        print(f"Revert SQL written to {revert_path}")
        apply_updates(session, selected)
    print(f"Applied updates to {len(selected)} organisms")


def _parse_confidences(values: list[str]) -> set[str] | None:
    confidences = {_CONFIDENCE_ALIASES.get(v, v) for v in values}
    if "all" in confidences:
        if len(confidences) > 1:
            raise argparse.ArgumentTypeError("--confidence all cannot be combined with other levels")
        return None
    return confidences


def main() -> None:
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Apply a taxonomy update JSON report to the metadata DB organism table"
    )
    parser.add_argument(
        "--metadata-uri",
        required=True,
        help="Database URI for the metadata database",
    )
    parser.add_argument(
        "--json-path",
        required=True,
        type=Path,
        help="Path to the taxonomy_updates.json produced by compare.py",
    )
    parser.add_argument(
        "--dry-run", "--dry_run",
        action="store_true",
        help="Print the SQL that would be executed without changing the DB",
    )
    parser.add_argument(
        "--update-common",
        action="store_true",
        help="Also update common_name (asks for confirmation; JSON must include common names)",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip the --update-common confirmation prompt, for non-interactive runs",
    )
    confidence_choices = ["all", *(level.value for level in ConfidenceLevel), *_CONFIDENCE_ALIASES]
    parser.add_argument(
        "--confidence",
        nargs="+",
        default=["all"],
        choices=confidence_choices,
        help="Confidence levels of name-only changes to apply: 'all' (default) or any of "
             "case_diff order_diff similar different. Ignored for taxonomy_id merges.",
    )
    parser.add_argument(
        "--revert-path",
        type=Path,
        help="Where to write the revert SQL. Defaults to <json-path stem>_revert_<timestamp>.sql "
             "next to --json-path.",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    try:
        confidences = _parse_confidences(args.confidence)
    except argparse.ArgumentTypeError as e:
        parser.error(str(e))
    revert_path = args.revert_path or args.json_path.with_name(
        f"{args.json_path.stem}_revert_{datetime.now():%Y%m%d_%H%M%S}.sql"
    )

    try:
        run(args.metadata_uri, args.json_path, args.update_common, confidences, args.dry_run,
            revert_path, args.yes)
    except Exception as e:  # pylint: disable=broad-except
        print(f"Error applying taxonomy updates: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
