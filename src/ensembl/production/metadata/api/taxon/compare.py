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
"""Compare organism taxonomy in the metadata DB against NCBI Taxonomy and emit JSON reports.

The pipeline is:
  1. Load one row per organism from the metadata DB (taxonomy ids, names,
     current genebuild source/method and genome UUIDs).
  2. Enrich each row with NCBI resolution status and name comparisons.
  3. Group the enriched rows by taxonomy_id to build update/delete records.
  4. Write the JSON reports using taxon.schema.

Update entries are of three kinds: taxon_id_update (a merged taxonomy_id,
with the scientific_name change it brings), species_id_update (a merged
species_taxonomy_id; only the id changes), taxon_scientific_name_update
(scientific_name changes not covered by a taxon_id_update) and
taxon_common_name_update (common_name changes, with --include-common-name).
Each target lists its genomes with the latest release each genome is in.

Usage:
    python compare.py \\
        --metadata-uri=mysql://user@host:port/ensembl_genome_metadata \\
        --taxon-parquet-dir=/path/to/parquet_snapshot \\
        --output-path=taxonomy_updates.json \\
        [--include-common-name] [--release-name=116]

Common-name changes are left out of the JSON unless --include-common-name is given.
Proposed names are capitalised (first letter upper case, as in the metadata
DB). A name differing from NCBI's only in letter case is never reported.

With --release-name, the run first checks that the release's
ensembl_release.taxonomy_snapshot matches the TaxonDB snapshot (its taxdump_id)
and stops if it doesn't. Without it, no snapshot check is done.
"""

import argparse
import difflib
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
from ensembl.taxonomy.db import TaxonDB
from ensembl.utils.database import DBConnection
from sqlalchemy import and_, select, text

from ensembl.production.metadata.api.taxon.schema import (
    ConfidenceLevel,
    IdChanges,
    IntChange,
    Metadata,
    StrChange,
    CommonNameChanges,
    ScientificNameChanges,
    Target,
    TargetGenome,
    TaxonDelete,
    TaxonDeleteReport,
    SpeciesIdChanges,
    TaxonIdUpdateEntry,
    TaxonCommonNameUpdateEntry,
    TaxonScientificNameUpdateEntry,
    TaxonSpeciesIdUpdateEntry,
    TaxonUpdateReport,
)
from ensembl.production.metadata.api.models.dataset import Attribute, Dataset, DatasetAttribute
from ensembl.production.metadata.api.models.genome import Genome, GenomeDataset, GenomeRelease
from ensembl.production.metadata.api.models.organism import Organism
from ensembl.production.metadata.api.models.release import EnsemblRelease

logger = logging.getLogger(__name__)

_SCI_CLASS = "scientific name"
_COMMON_CLASS = "genbank common name"
_SOURCE = "NCBI Taxonomy"
_DELNODES_PARQUET = "taxa_delnodes.parquet"
_PAST_PARQUET = "ncbi_taxa_past.parquet"
_DEFAULT_DELETES_NAME = "taxonomy_deletes_human_review.json"

# Merged ids with the organisms using them, and whether any organism on the id
# has genomes. Taxonomy merges are split by current scientific name.
_SQL_MERGED_TIDS = """
    SELECT taxonomy_id, any_value(ncbi_taxonomy_id), scientific_name, any_value(ncbi_scientific_name),
           list(organism_id ORDER BY organism_id), any_value(tid_has_genomes)
    FROM (
        SELECT *, bool_or(len(genome_uuids) > 0) OVER (PARTITION BY taxonomy_id) AS tid_has_genomes
        FROM checks_df WHERE taxon_status = 'MERGED'
    )
    GROUP BY taxonomy_id, scientific_name ORDER BY taxonomy_id, scientific_name
"""
_SQL_MERGED_STIDS = """
    SELECT species_taxonomy_id, any_value(ncbi_species_taxonomy_id), list(organism_id ORDER BY organism_id),
           bool_or(len(genome_uuids) > 0)
    FROM checks_df WHERE species_status = 'MERGED'
    GROUP BY species_taxonomy_id ORDER BY species_taxonomy_id
"""
_SQL_SCI_GROUPS = """
    SELECT taxonomy_id, organism_id, scientific_name, ncbi_scientific_name
    FROM checks_df
    WHERE taxon_status IN ('SAME', 'MERGED')
    ORDER BY taxonomy_id, organism_id
"""
_SQL_COMMON_GROUPS = """
    SELECT taxonomy_id, organism_id, common_name, proposed_common_name
    FROM checks_df
    WHERE taxon_status IN ('SAME', 'MERGED') AND proposed_common_name IS NOT NULL
    ORDER BY taxonomy_id, organism_id
"""
_SQL_TAXON_DELETE_ROWS = """
    SELECT taxonomy_id, species_taxonomy_id, taxon_status, scientific_name, common_name FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY taxonomy_id ORDER BY organism_id ASC) AS rn
        FROM checks_df WHERE taxon_status IN ('DELETED', 'NOT FOUND')
    ) WHERE rn = 1
    ORDER BY taxonomy_id
"""
_SQL_SPECIES_DELETE_ROWS = """
    SELECT taxonomy_id, species_taxonomy_id, species_status, scientific_name, common_name FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY taxonomy_id ORDER BY organism_id ASC) AS rn
        FROM checks_df
        WHERE species_status IN ('DELETED', 'NOT FOUND')
          AND taxon_status NOT IN ('DELETED', 'NOT FOUND')
    ) WHERE rn = 1
    ORDER BY taxonomy_id
"""


# ── Small normalization helpers ─────────────────────────────────────────────

def _clean_text(value: object | None) -> str:
    text = str(value or "").strip()
    return "" if text == "NA" else text


# ── Metadata DB loading ──────────────────────────────────────────────────────

def load_organism_rows(metadata_uri: str) -> list[dict]:
    """One row per organism with all its genomes, for driving the taxon check pipeline.

    Each genome is {"genome_uuid", "release_name", "release_version",
    "genebuild_source", "genebuild_method"}:
      release_*          — the latest release (highest version) the genome is
                           in; None for a genome in no release.
      genebuild_source   — the genome's annotation_source.
      genebuild_method   — the 'genebuild.method' attribute of the genome's
                           current 'genebuild' dataset (max value if several).
    """
    metadata_db = DBConnection(metadata_uri)
    current_gd = and_(GenomeDataset.genome_id == Genome.genome_id, GenomeDataset.is_current == 1)

    organisms_query = select(
        Organism.organism_id,
        Organism.taxonomy_id,
        Organism.species_taxonomy_id,
        Organism.common_name,
        Organism.scientific_name,
    )
    genomes_query = (
        select(Genome.organism_id, Genome.genome_uuid, Genome.annotation_source,
               EnsemblRelease.name, EnsemblRelease.version)
        .outerjoin(GenomeRelease, GenomeRelease.genome_id == Genome.genome_id)
        .outerjoin(EnsemblRelease, EnsemblRelease.release_id == GenomeRelease.release_id)
    )
    genebuild_method_query = (
        select(Genome.genome_uuid, DatasetAttribute.value)
        .join(GenomeDataset, current_gd)
        .join(Dataset, and_(Dataset.dataset_id == GenomeDataset.dataset_id, Dataset.name == "genebuild"))
        .join(DatasetAttribute, DatasetAttribute.dataset_id == Dataset.dataset_id)
        .join(Attribute, and_(Attribute.attribute_id == DatasetAttribute.attribute_id,
                              Attribute.name == "genebuild.method"))
        .distinct()
    )

    with metadata_db.session_scope() as session:
        organisms = session.execute(organisms_query).all()
        genebuild_methods: dict[str, str] = {}
        for uuid, method in session.execute(genebuild_method_query):
            if method is not None:
                genebuild_methods[uuid] = max(genebuild_methods.get(uuid, method), method)
        # {organism_id: {genome_uuid: genome dict}}, keeping each genome's latest release.
        genomes: dict[int, dict[str, dict]] = {}
        for oid, uuid, source, release_name, release_version in session.execute(genomes_query):
            version = float(release_version) if release_version is not None else None
            current = genomes.setdefault(oid, {}).get(uuid)
            if current is not None and (version is None or (
                    current["release_version"] is not None and version <= current["release_version"])):
                continue
            genomes[oid][uuid] = {
                "genome_uuid": uuid, "release_name": release_name, "release_version": version,
                "genebuild_source": source, "genebuild_method": genebuild_methods.get(uuid),
            }

    return [
        {
            "organism_id": oid,
            "taxonomy_id": tid,
            "species_taxonomy_id": stid,
            "common_name": common,
            "scientific_name": sci,
            "genomes": [genome for _, genome in sorted(genomes.get(oid, {}).items())],
        }
        for oid, tid, stid, common, sci in organisms
    ]


# ── Release snapshot check ───────────────────────────────────────────────────

# Raw SQL: taxonomy_snapshot isn't on the EnsemblRelease model yet, and adding
# it there would break ORM queries against DBs without the column.
_SQL_RELEASE_SNAPSHOT = text("SELECT taxonomy_snapshot FROM ensembl_release WHERE name = :name")


def check_taxonomy_snapshot(metadata_uri: str, release_name: str, taxdump_id: str) -> None:
    """Raise ValueError unless ensembl_release.taxonomy_snapshot for `release_name`
    equals the TaxonDB snapshot's taxdump_id."""
    with DBConnection(metadata_uri).session_scope() as session:
        snapshots = session.execute(_SQL_RELEASE_SNAPSHOT, {"name": release_name}).scalars().all()
    if not snapshots:
        raise ValueError(f"No ensembl_release named {release_name!r} in the metadata DB")
    if len(snapshots) > 1:
        raise ValueError(f"{len(snapshots)} ensembl_release rows named {release_name!r}; expected one")
    snapshot = (snapshots[0] or "").strip()
    if not snapshot:
        raise ValueError(f"ensembl_release {release_name!r} has no taxonomy_snapshot set")
    if snapshot != taxdump_id:
        raise ValueError(
            f"Taxonomy snapshot mismatch for release {release_name!r}: "
            f"metadata DB has {snapshot!r}, TaxonDB is {taxdump_id!r}"
        )
    logger.info("Release %s taxonomy_snapshot matches TaxonDB (%s)", release_name, taxdump_id)


# ── TaxonDB lookups ──────────────────────────────────────────────────────────

def fetch_sci_names(conn, taxon_ids: list[int]) -> dict[int, str]:
    """Return {taxon_id: scientific_name} for the given ids."""
    if not taxon_ids:
        return {}
    ids_sql = ", ".join(str(t) for t in taxon_ids)
    rows = conn.execute(f"""
        SELECT taxon_id, name FROM ncbi_taxa_name
        WHERE  taxon_id IN ({ids_sql})
          AND  name_class = '{_SCI_CLASS}'
    """).fetchall()
    return {tid: name.strip() for tid, name in rows}


def fetch_common_names(conn, taxon_ids: list[int]) -> dict[int, str]:
    """Return {taxon_id: common_name} for the given ids."""
    if not taxon_ids:
        return {}
    ids_sql = ", ".join(str(t) for t in taxon_ids)
    rows = conn.execute(f"""
        SELECT taxon_id, name FROM ncbi_taxa_name
        WHERE  taxon_id IN ({ids_sql})
          AND  name_class = '{_COMMON_CLASS}'
    """).fetchall()
    return {tid: name.strip() for tid, name in rows}


def classify_missing_ids(
    conn, taxon_ids: list[int], taxonomy_path: Path
) -> dict[int, tuple[str, int | None]]:
    """Return {taxon_id: (behavior, new_id)} for IDs not in TaxonDB.

    behavior – "DELETED" | "MERGED" | "NOT FOUND"
    new_id   – merged target taxon_id, or None

    Newer TaxonCache snapshots record retired ids in ncbi_taxa_past.parquet
    (taxon_id, status 'deleted'/'merged', primary_id); older ones use
    taxa_delnodes.parquet plus merged_taxon_id rows in ncbi_taxa_name.
    """
    if not taxon_ids:
        return {}
    ids_sql = ", ".join(str(t) for t in taxon_ids)

    past_path = taxonomy_path / _PAST_PARQUET
    if past_path.exists():
        statuses: dict[int, tuple[str, int | None]] = {}
        for tid, status, primary_id in conn.execute(
            f"SELECT taxon_id, status, primary_id FROM read_parquet(?) WHERE taxon_id IN ({ids_sql})",
            [str(past_path)],
        ).fetchall():
            if status == "merged" and primary_id is not None:
                statuses[tid] = ("MERGED", primary_id)
            elif status == "deleted":
                statuses[tid] = ("DELETED", None)
            else:
                logger.warning("taxon_id=%d: unrecognised %s status %r", tid, _PAST_PARQUET, status)
        return {tid: statuses.get(tid, ("NOT FOUND", None)) for tid in taxon_ids}

    # TaxonDB only loads ncbi_taxa_name/ncbi_taxa_node, so read deleted nodes
    # straight from the parquet next to them.
    deleted_ids = set()
    delnodes_path = taxonomy_path / _DELNODES_PARQUET
    if delnodes_path.exists():
        deleted_ids = {r[0] for r in conn.execute(
            f"SELECT taxon_id FROM read_parquet(?) WHERE taxon_id IN ({ids_sql})",
            [str(delnodes_path)],
        ).fetchall()}
    else:
        logger.warning("%s not found; deleted taxa will be reported as NOT FOUND", delnodes_path)

    # Merged-node mappings live in ncbi_taxa_name (name_class='merged_taxon_id'):
    # taxon_id holds the new (merged-into) id, name holds the old id as a string.
    old_ids_sql = ", ".join(f"'{t}'" for t in taxon_ids)
    merged = {
        int(old_tid): new_tid for new_tid, old_tid in conn.execute(f"""
            SELECT taxon_id, name FROM ncbi_taxa_name
            WHERE  name_class = 'merged_taxon_id'
              AND  name IN ({old_ids_sql})
        """).fetchall()
    }

    return {
        tid: ("DELETED",   None)        if tid in deleted_ids else
             ("MERGED",    merged[tid]) if tid in merged  else
             ("NOT FOUND", None)
        for tid in taxon_ids
    }


# ── Name similarity ──────────────────────────────────────────────────────────

def _name_similarity(a: str, b: str) -> float:
    return difflib.SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()


def _capitalise(name: str) -> str:
    """Upper-case the first letter, as metadata names are stored; the rest is kept
    as NCBI has it ("little brown bat" -> "Little brown bat")."""
    name = name.strip()
    return name[:1].upper() + name[1:]


def _same_name(a: str, b: str) -> bool:
    """True if two names match ignoring surrounding spaces and letter case
    ("little brown bat" vs "Little brown bat", "homo Sapiens" vs "Homo sapiens").
    Case-only differences are never reported."""
    return a.strip().casefold() == b.strip().casefold()


def classify_name_pair(given: str, ncbi: str) -> ConfidenceLevel:
    """Categorize a current-vs-NCBI name pair. Assumes both are non-empty and
    differ (callers filter exact matches and blanks out via SQL beforehand)."""
    g, n = given.strip(), ncbi.strip()
    if g.lower() == n.lower():
        return ConfidenceLevel.CASE_DIFF
    if sorted(g.lower().split()) == sorted(n.lower().split()):
        return ConfidenceLevel.ORDER_DIFF
    return ConfidenceLevel.SIMILAR if _name_similarity(g, n) >= 0.7 else ConfidenceLevel.DIFFERENT


def _common_name_decision(current: str, ncbi: str, genebuilds: list[tuple[str, str]]) -> str | None:
    """Rule D: reconcile one organism's common_name with NCBI's, given the
    (genebuild_source, genebuild_method) of each of its genomes. Returns the
    proposed new value, or None if no change should be proposed.

    D1/D2 — exactly one side blank: adopt the non-blank one (a no-op when
    it's `current` that's already non-blank). Both blank: no change.
    D3 — both non-blank, differing only in letter case: no change.
    D4 — both non-blank, genuinely different, and any genome's
    genebuild_method contains "import" (substring — "external_annotation_import"
    etc. count too): keep current, regardless of source.
    D5 — both non-blank, genuinely different, no import: adopt NCBI's name
    only if the organism has genomes and every genome's genebuild_source is
    'ensembl'; otherwise leave unchanged.
    """
    current = current.strip()
    ncbi = ncbi.strip()
    sources = [_clean_text(source).lower() for source, _ in genebuilds]
    methods = [_clean_text(method).lower() for _, method in genebuilds]
    if current == "" and ncbi == "":
        return None
    if current == "":
        return _capitalise(ncbi)
    if ncbi == "" or _same_name(current, ncbi):
        return None
    if any("import" in method for method in methods):
        return None
    return _capitalise(ncbi) if sources and all(source == "ensembl" for source in sources) else None


# ── Step 1: build the enriched per-organism check table ──────────────────────

def _resolve_status(
    taxon_id: int | None,
    db_ids: set[int],
    statuses: dict[int, tuple[str, int | None]],
) -> tuple[str | None, int | None]:
    """Return (status, ncbi_id) for one id; (None, None) when the id is unset."""
    if taxon_id is None:
        return None, None
    if taxon_id in db_ids:
        return "SAME", taxon_id
    return statuses[taxon_id]


def build_check_table(
    con: duckdb.DuckDBPyConnection,
    rows: list[dict],
    db_ids: set[int],
    statuses: dict[int, tuple[str, int | None]],
    present_sci_names: dict[int, str],
    ncbi_common: dict[int, str],
    merged_ncbi_sci_names: dict[int, str],
    merged_ncbi_common_names: dict[int, str],
) -> duckdb.DuckDBPyRelation:
    """One row per organism, with every check already computed:

      taxon_status / ncbi_taxonomy_id       — SAME/MERGED/DELETED/NOT FOUND,
                                               and the id it resolves to (the
                                               merged-into id if MERGED, empty
                                               otherwise if DELETED/NOT FOUND).
      species_status / ncbi_species_taxonomy_id — the same, for species_taxonomy_id
                                               (both empty when the organism has none).
      ncbi_scientific_name / scientific_name_compare_score — NCBI's name for
                                               whichever id it resolves to, and
                                               a 0-1 similarity score.
      ncbi_common_name / common_name_compare_score — the same, for common_name.
      genebuild_sources / genebuild_methods — distinct values across the
                                               organism's genomes, for reading.
      proposed_common_name                   — rule D's proposed common_name,
                                               or empty for no change.

    Report generation queries this table instead of re-deriving these facts
    per taxon, so a taxon's organisms only ever need grouping, not re-checking.
    """
    records = []
    for r in rows:
        tid = r["taxonomy_id"]
        stid = r["species_taxonomy_id"]

        tid_status, ncbi_tid = _resolve_status(tid, db_ids, statuses)
        stid_status, ncbi_stid = _resolve_status(stid, db_ids, statuses)

        current_sci = _clean_text(r["scientific_name"])
        current_common = _clean_text(r["common_name"])
        if tid_status == "SAME":
            ncbi_sci, ncbi_com = present_sci_names.get(tid, ""), ncbi_common.get(tid, "")
        elif tid_status == "MERGED":
            ncbi_sci = merged_ncbi_sci_names.get(ncbi_tid, "")
            ncbi_com = merged_ncbi_common_names.get(ncbi_tid, "")
        else:  # DELETED / NOT FOUND — no NCBI record to compare against
            ncbi_sci, ncbi_com = "", ""

        genebuilds = [(g["genebuild_source"], g["genebuild_method"]) for g in r["genomes"]]

        records.append({
            "organism_id": r["organism_id"],
            "taxonomy_id": tid,
            "taxon_status": tid_status,
            "ncbi_taxonomy_id": ncbi_tid,
            "species_taxonomy_id": stid,
            "species_status": stid_status,
            "ncbi_species_taxonomy_id": ncbi_stid,
            "scientific_name": current_sci,
            "ncbi_scientific_name": ncbi_sci,
            "scientific_name_compare_score": _name_similarity(current_sci, ncbi_sci) if current_sci and ncbi_sci else None,
            "common_name": current_common,
            "ncbi_common_name": ncbi_com,
            "common_name_compare_score": _name_similarity(current_common, ncbi_com) if current_common and ncbi_com else None,
            "genebuild_sources": ", ".join(sorted({_clean_text(s) for s, _ in genebuilds} - {""})) or None,
            "genebuild_methods": ", ".join(sorted({_clean_text(m) for _, m in genebuilds} - {""})) or None,
            "proposed_common_name": _common_name_decision(current_common, ncbi_com, genebuilds),
            "genome_uuids": [g["genome_uuid"] for g in r["genomes"]],
        })

    df = pd.DataFrame.from_records(records)
    for column in ("species_taxonomy_id", "ncbi_taxonomy_id", "ncbi_species_taxonomy_id"):
        df[column] = df[column].astype("Int64")
    con.register("checks_df", df)
    return con.sql("SELECT * FROM checks_df")


# ── Step 2: query the check table into per-taxon change sets ────────────────

def _str_change(current: str, new: str) -> StrChange:
    """A proposed name change; `new` is capitalised to match metadata names."""
    return StrChange(current=current, new=_capitalise(new), confidence=classify_name_pair(current, new))


Updates = dict[
    str,
    TaxonIdUpdateEntry | TaxonSpeciesIdUpdateEntry | TaxonScientificNameUpdateEntry | TaxonCommonNameUpdateEntry,
]


@dataclass(frozen=True)
class ReportCtx:
    """Per-organism facts used to build targets."""

    oid_to_genomes: dict[int, list[TargetGenome]]


def _load_ctx(oid_to_genomes: dict[int, list[dict]], include_genebuild: bool) -> ReportCtx:
    """genebuild source/method only matter to the common-name rules, so they're
    listed on genomes only when common names are compared."""
    excluded = set() if include_genebuild else {"genebuild_source", "genebuild_method"}
    return ReportCtx(oid_to_genomes={
        oid: [TargetGenome(**{k: v for k, v in g.items() if k not in excluded}) for g in genomes]
        for oid, genomes in oid_to_genomes.items()
    })


def _build_targets(organism_ids: list[int], context: ReportCtx) -> list[Target]:
    targets = []
    for oid in sorted(organism_ids):
        targets.append(Target(organism_id=oid, genomes=context.oid_to_genomes.get(oid, [])))
    return targets


def _group_rows(
    con: duckdb.DuckDBPyConnection,
    query: str,
) -> dict[int, dict[tuple[str, ...], list[int]]]:
    grouped: dict[int, dict[tuple[str, ...], list[int]]] = {}
    for row in con.sql(query).fetchall():
        taxon_id = row[0]
        organism_id = row[1]
        key = tuple(row[2:])
        grouped.setdefault(taxon_id, {}).setdefault(key, []).append(organism_id)
    return grouped


def _next_update_key(updates: dict[str, object], prefix: str) -> str:
    update_n = sum(1 for key in updates if key.startswith(prefix))
    return f"{prefix}_{update_n + 1}"


def _add_sci_updates(
    updates: Updates,
    *,
    entries: dict[int, dict[tuple[str, ...], list[int]]],
    skip_tids: set[int],
    context: ReportCtx,
) -> None:
    for tid in sorted(entries):
        if tid in skip_tids:
            continue
        for (current, ncbi_name), oids in entries[tid].items():
            if not ncbi_name or _same_name(current, ncbi_name):
                continue
            updates[_next_update_key(updates, "taxon_scientific_name_update")] = TaxonScientificNameUpdateEntry(
                taxonomy_id=tid,
                changes=ScientificNameChanges(scientific_name=_str_change(current, ncbi_name)),
                targets=_build_targets(oids, context),
            )


def _add_common_updates(
    updates: Updates,
    *,
    entries: dict[int, dict[tuple[str, ...], list[int]]],
    skip_tids: set[int],
    context: ReportCtx,
) -> None:
    for tid in sorted(entries):
        if tid in skip_tids:
            continue
        for (current, new_common), oids in entries[tid].items():
            updates[_next_update_key(updates, "taxon_common_name_update")] = TaxonCommonNameUpdateEntry(
                taxonomy_id=tid,
                changes=CommonNameChanges(common_name=_str_change(current, new_common)),
                targets=_build_targets(oids, context),
            )


def _add_taxon_id_updates(updates: Updates, con: duckdb.DuckDBPyConnection, context: ReportCtx) -> set[int]:
    """Add taxon_id_update entries; return every merged taxonomy_id.

    A merged taxonomy_id is skipped when none of its organisms has genomes.
    Either way its scientific names are handled here, never as
    taxon_scientific_name_update entries.
    """
    merged_tids, skipped = set(), set()
    for tid, new_tid, current_sci, ncbi_sci, oids, has_genomes in con.sql(_SQL_MERGED_TIDS).fetchall():
        merged_tids.add(tid)
        if not has_genomes:
            skipped.add(tid)
            continue
        sci_change = (
            _str_change(current_sci, ncbi_sci) if ncbi_sci and not _same_name(current_sci, ncbi_sci) else None
        )
        updates[_next_update_key(updates, "taxon_id_update")] = TaxonIdUpdateEntry(
            taxonomy_id=tid,
            changes=IdChanges(taxonomy_id=IntChange(current=tid, new=new_tid), scientific_name=sci_change),
            targets=_build_targets(oids, context),
        )
    if skipped:
        logger.info("Skipped %d merged taxonomy_id values with no genomes", len(skipped))
    return merged_tids


def _add_species_id_updates(updates: Updates, con: duckdb.DuckDBPyConnection, context: ReportCtx) -> None:
    """Add species_id_update entries, which change only species_taxonomy_id.

    A merged species_taxonomy_id is skipped when none of its organisms has genomes.
    """
    skipped = 0
    for stid, new_stid, oids, has_genomes in con.sql(_SQL_MERGED_STIDS).fetchall():
        if not has_genomes:
            skipped += 1
            continue
        updates[_next_update_key(updates, "species_id_update")] = TaxonSpeciesIdUpdateEntry(
            species_taxonomy_id=stid,
            changes=SpeciesIdChanges(species_taxonomy_id=IntChange(current=stid, new=new_stid)),
            targets=_build_targets(oids, context),
        )
    if skipped:
        logger.info("Skipped %d merged species_taxonomy_id values with no genomes", skipped)


def _delete_entries(
    con: duckdb.DuckDBPyConnection,
    taxonomy_release: str,
    query: str,
    id_type: str,
) -> list[TaxonDelete]:
    rows = con.sql(query).fetchall()
    return [
        TaxonDelete(
            id_type=id_type,
            taxonomy_id=tid,
            species_taxonomy_id=stid,
            scientific_name=sci,
            common_name=common or None,
            missing_behavior=behavior,
            metadata=_report_metadata(taxonomy_release),
        )
        for tid, stid, behavior, sci, common in rows
    ]


def _report_metadata(taxonomy_release: str) -> Metadata:
    return Metadata(source=_SOURCE, taxonomy_release=taxonomy_release, detected_at=datetime.now(timezone.utc))


def build_reports(
    con: duckdb.DuckDBPyConnection,
    taxonomy_release: str,
    oid_to_genomes: dict[int, list[dict]],
    include_common_name: bool = False,
) -> tuple[TaxonUpdateReport, TaxonDeleteReport]:
    """Build the update and delete reports from the registered checks table.

    A merged taxonomy_id's scientific_name change rides in its taxon_id_update;
    taxon_scientific_name_update covers the rest. Common names of a merged
    taxonomy_id are still taxon_common_name_update entries, since they also
    depend on each organism's genebuild.
    """
    context = _load_ctx(oid_to_genomes, include_genebuild=include_common_name)
    updates: Updates = {}

    merged_tids = _add_taxon_id_updates(updates, con, context)
    _add_species_id_updates(updates, con, context)
    _add_sci_updates(
        updates,
        entries=_group_rows(con, _SQL_SCI_GROUPS),
        skip_tids=merged_tids,
        context=context,
    )
    if include_common_name:
        # Common names of merged taxonomy_ids skipped for having no genomes are left alone too.
        tids_with_id_update = {e.taxonomy_id for e in updates.values() if isinstance(e, TaxonIdUpdateEntry)}
        _add_common_updates(
            updates,
            entries=_group_rows(con, _SQL_COMMON_GROUPS),
            skip_tids=merged_tids - tids_with_id_update,
            context=context,
        )

    delete_entries = _delete_entries(con, taxonomy_release, _SQL_TAXON_DELETE_ROWS, "taxonomy_id")
    delete_entries += _delete_entries(con, taxonomy_release, _SQL_SPECIES_DELETE_ROWS, "species_taxonomy_id")

    return (
        TaxonUpdateReport(metadata=_report_metadata(taxonomy_release), updates=updates),
        TaxonDeleteReport(root=delete_entries),
    )


# ── Pipeline ─────────────────────────────────────────────────────────────────

def check_taxonomy(
    metadata_uri: str,
    taxonomy_path: Path,
    checks_path: Path | None = None,
    include_common_name: bool = False,
    release_name: str | None = None,
) -> tuple[TaxonUpdateReport, TaxonDeleteReport]:
    """Run the taxonomy check pipeline and return the update and delete reports.

    If release_name is given, first check that release's taxonomy_snapshot
    matches the TaxonDB snapshot; raises ValueError if not.
    """
    with TaxonDB(str(taxonomy_path)) as db:
        if release_name is not None:
            check_taxonomy_snapshot(metadata_uri, release_name, db.taxdump_id)

        rows = load_organism_rows(metadata_uri)
        logger.info("%d rows read from organism table", len(rows))

        taxonomy_release = db.import_date
        db_ids = set(db.taxon_ids())
        conn = db._conn  # pylint: disable=protected-access

        all_tids = {r["taxonomy_id"] for r in rows}
        all_stids = {r["species_taxonomy_id"] for r in rows if r["species_taxonomy_id"] is not None}
        absent_tids = all_tids - db_ids
        absent_stids = all_stids - db_ids
        logger.info("absent taxon_ids: %d  absent species_ids: %d", len(absent_tids), len(absent_stids))

        # Both need classifying — a species_taxonomy_id that's missing but
        # never itself a missing taxonomy_id elsewhere would otherwise have no
        # status to resolve against.
        statuses = classify_missing_ids(conn, sorted(absent_tids | absent_stids), taxonomy_path)

        merged_into_ids = sorted({new_id for status, new_id in statuses.values() if status == "MERGED"})
        merged_ncbi_sci_names = fetch_sci_names(conn, merged_into_ids)
        merged_ncbi_common_names = fetch_common_names(conn, merged_into_ids)

        present_tids = sorted(all_tids - absent_tids)
        present_sci_names = fetch_sci_names(conn, present_tids)
        ncbi_common = fetch_common_names(conn, present_tids)

    duck_con = duckdb.connect()
    checks_table = build_check_table(
        duck_con, rows, db_ids, statuses, present_sci_names, ncbi_common,
        merged_ncbi_sci_names, merged_ncbi_common_names,
    )
    if checks_path is not None:
        checks_table.df().to_csv(checks_path, index=False)
        logger.info("Written full checks table (%d rows) to %s", checks_table.shape[0], checks_path)

    oid_to_genomes = {r["organism_id"]: r["genomes"] for r in rows}
    return build_reports(duck_con, taxonomy_release, oid_to_genomes, include_common_name)


def main() -> None:
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Compare organism taxonomy in the metadata DB against NCBI Taxonomy"
    )
    parser.add_argument(
        "--metadata-uri",
        required=True,
        help="Database URI for the metadata database",
    )
    parser.add_argument(
        "--taxon-parquet-dir",
        required=True,
        type=Path,
        help="Directory holding the NCBI Taxonomy parquet files: ncbi_taxa_name.parquet, "
             "ncbi_taxa_node.parquet, and ncbi_taxa_past.parquet (or, for older snapshots, "
             "taxa_delnodes.parquet). For a TaxonCache, this is taxdumps/<year>/<taxdump id>/.",
    )
    parser.add_argument(
        "--output-path",
        default="taxonomy_updates.json",
        type=Path,
        help="Output path for the update JSON. Defaults to taxonomy_updates.json in the current directory.",
    )
    parser.add_argument(
        "--deletes-path",
        type=Path,
        help=f"Output path for the deletes JSON for human review. Defaults to {_DEFAULT_DELETES_NAME} "
             "next to --output-path.",
    )
    parser.add_argument(
        "--include-common-name",
        action="store_true",
        help="Also report common_name changes. Off by default.",
    )
    parser.add_argument(
        "--release-name",
        help="Ensembl release name. If given, the run stops unless that release's "
             "taxonomy_snapshot matches the TaxonDB snapshot (taxdump_id).",
    )
    parser.add_argument(
        "--checks-path",
        type=Path,
        help="Optional CSV path to write the per-organism checks table, for debugging.",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    deletes_path = args.deletes_path or args.output_path.with_name(_DEFAULT_DELETES_NAME)
    try:
        update_report, delete_report = check_taxonomy(
            args.metadata_uri, args.taxon_parquet_dir, args.checks_path, args.include_common_name,
            args.release_name,
        )
        args.output_path.write_text(update_report.model_dump_json(indent=2, exclude_none=True))
        print(f"Written {len(update_report.updates)} entries to {args.output_path}")
        deletes_path.write_text(delete_report.model_dump_json(indent=2, exclude_none=True))
        print(f"Written {len(delete_report.root)} entries to {deletes_path}")
    except Exception as e:  # pylint: disable=broad-except
        print(f"Error checking taxonomy: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
