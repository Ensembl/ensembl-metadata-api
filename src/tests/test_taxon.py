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

import re
from pathlib import Path

import duckdb
import pytest
from sqlalchemy import create_engine, insert, select, text

from ensembl.production.metadata.api.models.organism import Organism
from ensembl.production.metadata.api.taxon import compare, implement
from ensembl.production.metadata.api.taxon.schema import (
    ConfidenceLevel,
    TaxonCommonNameUpdateEntry,
    TaxonIdUpdateEntry,
    TaxonScientificNameUpdateEntry,
    TaxonSpeciesIdUpdateEntry,
    TaxonUpdateReport,
)

db_directory = (Path(__file__).parent / "databases").resolve()

pytestmark = pytest.mark.parametrize(
    "test_dbs",
    [[{"src": db_directory / "ensembl_genome_metadata"}, {"src": db_directory / "ncbi_taxonomy"}]],
    indirect=True,
)

# NCBI snapshot: (taxon_id, name, name_class). merged_taxon_id rows map the old
# id (name) to the id it was merged into (taxon_id). 300 is in delnodes.
NCBI_NAMES = [
    (1, "2026-06-29T16:39:01Z", "import date"),
    (1, "2026-06-28T09:00:00Z", "taxdump_id"),
    (100, "Mus musculus", "scientific name"), (100, "house mouse", "genbank common name"),
    (201, "New name", "scientific name"), (201, "200", "merged_taxon_id"),
    (500, "Foo bar", "scientific name"),
    (502, "Foo species", "scientific name"), (502, "501", "merged_taxon_id"),
    (600, "Homo sapiens", "scientific name"),
    (701, "Merged nogenome", "scientific name"), (701, "700", "merged_taxon_id"),
    (800, "Canis lupus", "scientific name"), (800, "dog", "genbank common name"),
    (900, "totally different", "scientific name"),  # lower case: proposed as "Totally different"
    (1000, "Myotis lucifugus", "scientific name"), (1000, "little brown bat", "genbank common name"),
]
NCBI_NODES = [1, 100, 201, 500, 502, 600, 701, 800, 900, 1000]
DELETED = [300]

# Metadata organisms:
# (organism_id, taxonomy_id, species_taxonomy_id, common_name, scientific_name,
#  genebuild_source, genebuild_method, genome_uuids). Each genome takes the
# organism's genebuild source/method unless GENOME_GENEBUILD overrides it.
# Genomes are in release 116, except UNRELEASED ones, which are in no release.
ORGANISMS = [
    (1, 100, 100, "mouse", "Mus musculus", "ensembl", "full_genebuild", ["u1"]),
    (2, 200, 200, None, "Old name", "ensembl", "full_genebuild", ["u2"]),
    (3, 200, 200, None, "Old name variant", "ensembl", "full_genebuild", ["u3"]),
    (4, 300, 300, None, "Deleted sp", None, None, ["u4"]),
    (5, 400, None, "NA", "Missing sp", None, None, []),
    (6, 500, 501, None, "Foo bar", "refseq", "import", ["u6"]),
    (7, 600, 600, None, "homo Sapiens", "ensembl", "full_genebuild", ["u7"]),
    (8, 700, 700, None, "Old nogenome", None, None, []),
    (9, 800, 800, None, "Canis lupus", "refseq", "external_annotation_import", ["u9"]),
    (10, 900, 900, None, "Something else", "ensembl", "full_genebuild", ["u10"]),
    # Same taxon, several genomes each: only 12 (all ensembl, no import) adopts NCBI's common name.
    (11, 1000, 1000, "bat", "Myotis lucifugus", "ensembl", "full_genebuild", ["u11a", "u11b"]),
    (12, 1000, 1000, "bat", "Myotis lucifugus", "ensembl", "full_genebuild", ["u12a", "u12b"]),
    (13, 1000, 1000, "bat", "Myotis lucifugus", "ensembl", "full_genebuild", ["u13a", "u13b"]),
    (14, 600, 600, None, "Homo sapien", "ensembl", "full_genebuild", ["u14"]),
    # Scientific names differing from NCBI's only in letter case: still reported
    # (exact match required), on a merged taxonomy_id (15) or not (7, 16).
    (15, 200, 200, None, "new name", "ensembl", "full_genebuild", ["u15"]),
    (16, 600, 600, None, "homo sapiens", "ensembl", "full_genebuild", ["u16"]),
]
GENOME_GENEBUILD = {"u11b": ("refseq", "full_genebuild"), "u13b": ("ensembl", "import")}
UNRELEASED = {"u14"}


@pytest.fixture
def taxonomy_dir(tmp_path: Path) -> Path:
    path = tmp_path / "taxonomy"
    path.mkdir()
    con = duckdb.connect()
    con.execute("CREATE TABLE ncbi_taxa_name(taxon_id INTEGER, name VARCHAR, name_class VARCHAR)")
    con.executemany("INSERT INTO ncbi_taxa_name VALUES (?, ?, ?)", NCBI_NAMES)
    con.execute("CREATE TABLE ncbi_taxa_node(taxon_id INTEGER, parent_id INTEGER, rank VARCHAR, root_id INTEGER)")
    con.executemany("INSERT INTO ncbi_taxa_node VALUES (?, 1, 'species', 1)", [[t] for t in NCBI_NODES])
    con.execute("CREATE TABLE taxa_delnodes(taxon_id INTEGER)")
    con.executemany("INSERT INTO taxa_delnodes VALUES (?)", [[t] for t in DELETED])
    for table in ("ncbi_taxa_name", "ncbi_taxa_node", "taxa_delnodes"):
        con.execute(f"COPY {table} TO '{path / table}.parquet' (FORMAT parquet)")
    con.close()
    return path


@pytest.fixture
def organism_rows(monkeypatch) -> list[dict]:
    rows = [
        {
            "organism_id": oid, "taxonomy_id": tid, "species_taxonomy_id": stid,
            "common_name": common, "scientific_name": sci,
            "genomes": [
                {
                    "genome_uuid": u,
                    "release_name": None if u in UNRELEASED else "116",
                    "release_version": None if u in UNRELEASED else 116.0,
                    "genebuild_source": GENOME_GENEBUILD.get(u, (source, method))[0],
                    "genebuild_method": GENOME_GENEBUILD.get(u, (source, method))[1],
                }
                for u in uuids
            ],
        }
        for oid, tid, stid, common, sci, source, method, uuids in ORGANISMS
    ]
    monkeypatch.setattr(compare, "load_organism_rows", lambda uri: rows)
    return rows


@pytest.fixture
def metadata_uri(tmp_path: Path) -> str:
    uri = f"sqlite:///{tmp_path / 'metadata.db'}"
    engine = create_engine(uri)
    Organism.__table__.create(engine)
    with engine.begin() as conn:
        for oid, tid, stid, common, sci, *_ in ORGANISMS:
            conn.execute(insert(Organism).values(
                organism_id=oid, organism_uuid=f"org-{oid}", biosample_id=f"bio-{oid}",
                taxonomy_id=tid, species_taxonomy_id=stid, common_name=common, scientific_name=sci,
            ))
    engine.dispose()
    return uri


def _organisms(uri: str) -> dict[int, tuple]:
    engine = create_engine(uri)
    with engine.connect() as conn:
        rows = conn.execute(select(
            Organism.organism_id, Organism.taxonomy_id, Organism.species_taxonomy_id,
            Organism.scientific_name, Organism.common_name,
        )).all()
    engine.dispose()
    return {row[0]: tuple(row[1:]) for row in rows}


def _entries_for(report: TaxonUpdateReport, organism_id: int) -> list:
    return [e for e in report.updates.values() if any(t.organism_id == organism_id for t in e.targets)]


@pytest.mark.parametrize(
    "given, ncbi, expected",
    [
        ("homo Sapiens", "Homo sapiens", ConfidenceLevel.CASE_DIFF),
        ("sapiens Homo", "Homo sapiens", ConfidenceLevel.ORDER_DIFF),
        ("Homo sapien", "Homo sapiens", ConfidenceLevel.SIMILAR),
        ("Mus musculus", "Canis lupus", ConfidenceLevel.DIFFERENT),
    ],
)
def test_classify_name_pair(given, ncbi, expected):
    assert compare.classify_name_pair(given, ncbi) == expected


ENS = ("ensembl", "full_genebuild")


@pytest.mark.parametrize(
    "current, ncbi, genebuilds, expected",
    [
        ("", "", [ENS], None),
        ("", "dog", [("refseq", "import")], "Dog"),  # D1: adopt NCBI (capitalised) when current is blank
        ("dog", "", [ENS], None),  # D2: keep current when NCBI is blank
        ("Dog", "dog", [ENS], None),  # D3: case-only differences are ignored
        ("Little Brown bat", "little brown bat", [ENS], None),
        ("mouse", "house mouse", [("ensembl", "external_annotation_import")], None),  # D4: import keeps current
        ("mouse", "house mouse", [ENS], "House mouse"),  # D5: ensembl adopts NCBI, capitalised
        ("mouse", "house mouse", [("refseq", "full_genebuild")], None),  # D5: non-ensembl keeps current
        # Several genomes: any import, or any non-ensembl source, keeps current.
        ("mouse", "house mouse", [ENS, ENS], "House mouse"),
        ("mouse", "house mouse", [ENS, ("ensembl", "import")], None),
        ("mouse", "house mouse", [ENS, ("refseq", "full_genebuild")], None),
        ("mouse", "house mouse", [ENS, (None, None)], None),
        ("mouse", "house mouse", [], None),  # no genomes
    ],
)
def test_common_name_decision(current, ncbi, genebuilds, expected):
    assert compare._common_name_decision(current, ncbi, genebuilds) == expected


@pytest.mark.parametrize(
    "name, expected",
    [
        ("little brown bat", "Little brown bat"),
        ("Homo sapiens", "Homo sapiens"),
        (" uncultured bacterium ", "Uncultured bacterium"),
        ("x", "X"),
        ("", ""),
    ],
)
def test_capitalise(name, expected):
    assert compare._capitalise(name) == expected


@pytest.mark.parametrize(
    "a, b, expected",
    [
        ("little brown bat", "Little brown bat", True),
        (" Homo sapiens ", "homo sapiens", True),
        ("homo Sapiens", "Homo sapiens", True),
        ("Little Brown Bat", "little brown bat", True),
        ("Homo sapiens", "Homo sapien", False),
    ],
)
def test_same_name(a, b, expected):
    assert compare._same_name(a, b) is expected


@pytest.mark.parametrize(
    "current, ncbi, expected",
    [
        ("Homo sapiens", "Homo sapiens", None),
        (" Homo sapiens ", "Homo sapiens", None),
        ("Homo sapiens", None, None),
        ("Homo sapiens", "", None),
        ("homo sapiens", "Homo sapiens", ("Homo sapiens", ConfidenceLevel.CASE_DIFF)),
        ("homo Sapiens", "Homo sapiens", ("Homo sapiens", ConfidenceLevel.CASE_DIFF)),
        # NCBI's name is proposed verbatim, never re-capitalised.
        ("Uncultured bacterium", "uncultured bacterium", ("uncultured bacterium", ConfidenceLevel.CASE_DIFF)),
        ("Homo sapien", "Homo sapiens", ("Homo sapiens", ConfidenceLevel.SIMILAR)),
    ],
)
def test_sci_change(current, ncbi, expected):
    change = compare._sci_change(current, ncbi)
    assert (None if change is None else (change.new, change.confidence)) == expected


def test_reports(taxonomy_dir, organism_rows):
    updates, deletes = compare.check_taxonomy("unused", taxonomy_dir, include_common_name=True)
    assert updates.metadata.taxonomy_release == "2026-06-29T16:39:01Z"

    # A taxonomy_id merge carries its scientific_name change, one entry per current name.
    id_entries = [e for e in updates.updates.values() if isinstance(e, TaxonIdUpdateEntry)]
    sci = lambda c: (c.current, c.new) if c else None  # noqa: E731
    assert [
        (e.taxonomy_id, e.changes.taxonomy_id.new, sci(e.changes.scientific_name), [t.organism_id for t in e.targets])
        for e in id_entries
    ] == [
        (200, 201, ("Old name", "New name"), [2]),
        (200, 201, ("Old name variant", "New name"), [3]),
        (200, 201, ("new name", "New name"), [15]),  # scientific names are case-sensitive
    ]
    # ...so merged organisms get no separate scientific_name update.
    for oid in (2, 3, 15):
        assert not [e for e in _entries_for(updates, oid) if isinstance(e, TaxonScientificNameUpdateEntry)]

    # Each entry kind has its own key prefix.
    prefixes = {
        TaxonIdUpdateEntry: "taxon_id_update_",
        TaxonSpeciesIdUpdateEntry: "species_id_update_",
        TaxonScientificNameUpdateEntry: "taxon_scientific_name_update_",
        TaxonCommonNameUpdateEntry: "taxon_common_name_update_",
    }
    assert {type(e) for e in updates.updates.values()} == set(prefixes)
    assert all(k.startswith(prefixes[type(e)]) for k, e in updates.updates.items())

    # Targets list each genome with its release; release fields are None when unreleased.
    assert [g.model_dump() for g in id_entries[0].targets[0].genomes] == [
        {"genome_uuid": "u2", "release_name": "116", "release_version": 116.0,
         "genebuild_source": "ensembl", "genebuild_method": "full_genebuild"}]
    unreleased_target = _entries_for(updates, 14)[0].targets[0]
    assert [(g.genome_uuid, g.release_name, g.release_version) for g in unreleased_target.genomes] == [
        ("u14", None, None)]

    # species_taxonomy_id merges are their own entries, changing only that id.
    species_entries = [e for e in updates.updates.values() if isinstance(e, TaxonSpeciesIdUpdateEntry)]
    assert [
        (e.species_taxonomy_id, e.changes.species_taxonomy_id.new, [t.organism_id for t in e.targets])
        for e in species_entries
    ] == [(200, 201, [2, 3, 15]), (501, 502, [6])]

    # Merged ids without genomes are left alone, names included.
    assert _entries_for(updates, 8) == []

    # Common names across several genomes: only organism 12 (all ensembl, no import) changes.
    bat, = [e for e in updates.updates.values() if isinstance(e, TaxonCommonNameUpdateEntry) and e.taxonomy_id == 1000]
    assert (bat.changes.common_name.current, bat.changes.common_name.new) == ("bat", "Little brown bat")
    assert [t.organism_id for t in bat.targets] == [12]
    assert [(g.genome_uuid, g.genebuild_source, g.genebuild_method) for g in bat.targets[0].genomes] == [
        ("u12a", "ensembl", "full_genebuild"), ("u12b", "ensembl", "full_genebuild")]

    # Scientific names must match NCBI exactly: case-only differences are reported.
    for oid, current in ((7, "homo Sapiens"), (16, "homo sapiens")):
        entry, = _entries_for(updates, oid)
        change = entry.changes.scientific_name
        assert (change.current, change.new, change.confidence) == (current, "Homo sapiens", ConfidenceLevel.CASE_DIFF)
    similar, = _entries_for(updates, 14)
    assert similar.changes.scientific_name.confidence == ConfidenceLevel.SIMILAR

    common = {
        oid: (e.changes.common_name.current, e.changes.common_name.new)
        for oid in (1, 9) for e in _entries_for(updates, oid) if isinstance(e, TaxonCommonNameUpdateEntry)
    }
    assert common == {1: ("mouse", "House mouse"), 9: ("", "Dog")}
    # Proposed scientific names are NCBI's verbatim (not re-capitalised).
    other, = _entries_for(updates, 10)
    assert (other.changes.scientific_name.current, other.changes.scientific_name.new) == (
        "Something else", "totally different")

    assert [(d.taxonomy_id, d.id_type, d.missing_behavior) for d in deletes.root] == [
        (300, "taxonomy_id", "DELETED"),
        (400, "taxonomy_id", "NOT FOUND"),
    ]


def test_genebuild_only_with_common_names(taxonomy_dir, organism_rows):
    updates, _ = compare.check_taxonomy("unused", taxonomy_dir)
    assert not any(isinstance(e, TaxonCommonNameUpdateEntry) for e in updates.updates.values())
    targets = [t for e in updates.updates.values() for t in e.targets]
    genomes = [g for t in targets for g in t.genomes]
    assert genomes and all(g.genebuild_source is None and g.genebuild_method is None for g in genomes)
    assert "genebuild" not in updates.model_dump_json(exclude_none=True)


def test_report_json_round_trip(taxonomy_dir, organism_rows):
    updates, _ = compare.check_taxonomy("unused", taxonomy_dir, include_common_name=True)
    parsed = TaxonUpdateReport.model_validate_json(updates.model_dump_json(exclude_none=True))
    assert {k: type(v) for k, v in parsed.updates.items()} == {k: type(v) for k, v in updates.updates.items()}
    assert parsed == updates


def test_implement(tmp_path, taxonomy_dir, organism_rows, metadata_uri):
    updates, _ = compare.check_taxonomy("unused", taxonomy_dir, include_common_name=True)
    json_path = tmp_path / "taxonomy_updates.json"
    json_path.write_text(updates.model_dump_json(exclude_none=True))
    revert_path = tmp_path / "revert.sql"
    before = _organisms(metadata_uri)

    implement.run(metadata_uri, json_path, update_common=False, confidences={"similar"},
                  dry_run=False, revert_path=revert_path, assume_yes=True)
    after = _organisms(metadata_uri)

    # Merged taxonomy_id and species_taxonomy_id: both ids and the name change for every organism.
    assert after[2] == (201, 201, "New name", None)
    assert after[3] == (201, 201, "New name", None)
    # Species-only merge changes only the species id.
    assert after[6] == (500, 502, "Foo bar", None)
    # A taxonomy_id merge applies its scientific_name change whatever the confidence.
    assert after[15] == (201, 201, "New name", None)
    assert after[16] == before[16]
    # Name-only changes are filtered by confidence: similar applied; case_diff and different skipped.
    assert after[7] == before[7]
    assert after[14][2] == "Homo sapiens"
    assert after[10] == before[10]
    # Common names need --update-common.
    assert after[1] == before[1]
    assert after[9] == before[9]

    # Re-running is a no-op.
    implement.run(metadata_uri, json_path, False, {"similar"}, False, tmp_path / "revert2.sql", True)
    assert _organisms(metadata_uri) == after
    assert not (tmp_path / "revert2.sql").exists()

    # The revert file restores the previous values.
    engine = create_engine(metadata_uri)
    with engine.begin() as conn:
        for statement in revert_path.read_text().splitlines():
            if statement.startswith("UPDATE"):
                conn.exec_driver_sql(statement)
    engine.dispose()
    assert _organisms(metadata_uri) == before


@pytest.fixture
def release_uri(metadata_uri: str) -> str:
    """metadata_uri plus an ensembl_release table holding taxonomy_snapshot values."""
    engine = create_engine(metadata_uri)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE ensembl_release (release_id INTEGER PRIMARY KEY, name VARCHAR(3), "
                          "taxonomy_snapshot VARCHAR(255))"))
        conn.execute(text("INSERT INTO ensembl_release VALUES "
                          "(1, '116', '2026-06-28T09:00:00Z'), (2, '117', '2026-09-01T00:00:00Z'), "
                          "(3, '118', NULL)"))
    engine.dispose()
    return metadata_uri


@pytest.fixture
def taxonomy_dir_past(tmp_path: Path) -> Path:
    """The same snapshot in the newer TaxonCache layout: retired ids are in
    ncbi_taxa_past.parquet instead of merged_taxon_id rows and taxa_delnodes."""
    path = tmp_path / "taxonomy_past"
    path.mkdir()
    merged = [(int(name), "merged", tid) for tid, name, cls in NCBI_NAMES if cls == "merged_taxon_id"]
    deleted = [(tid, "deleted", None) for tid in DELETED]
    con = duckdb.connect()
    con.execute("CREATE TABLE ncbi_taxa_name(taxon_id INTEGER, name VARCHAR, name_class VARCHAR)")
    con.executemany("INSERT INTO ncbi_taxa_name VALUES (?, ?, ?)",
                    [row for row in NCBI_NAMES if row[2] != "merged_taxon_id"])
    con.execute("CREATE TABLE ncbi_taxa_node(taxon_id INTEGER, parent_id INTEGER, rank VARCHAR, root_id INTEGER)")
    con.executemany("INSERT INTO ncbi_taxa_node VALUES (?, 1, 'species', 1)", [[t] for t in NCBI_NODES])
    con.execute("CREATE TABLE ncbi_taxa_past(taxon_id INTEGER, status VARCHAR, primary_id INTEGER)")
    con.executemany("INSERT INTO ncbi_taxa_past VALUES (?, ?, ?)", merged + deleted)
    for table in ("ncbi_taxa_name", "ncbi_taxa_node", "ncbi_taxa_past"):
        con.execute(f"COPY {table} TO '{path / table}.parquet' (FORMAT parquet)")
    con.close()
    return path


def test_past_parquet_layout(taxonomy_dir, taxonomy_dir_past, organism_rows):
    def dump(reports):
        updates, deletes = reports
        return (updates.model_dump(exclude={"metadata"}),
                [d.model_dump(exclude={"metadata"}) for d in deletes.root])

    old = dump(compare.check_taxonomy("unused", taxonomy_dir, include_common_name=True))
    new = dump(compare.check_taxonomy("unused", taxonomy_dir_past, include_common_name=True))
    assert new == old
    assert any(k.startswith("taxon_id_update_") for k in new[0]["updates"])
    assert [(d["taxonomy_id"], d["missing_behavior"]) for d in new[1]] == [(300, "DELETED"), (400, "NOT FOUND")]


def test_snapshot_check_passes(taxonomy_dir, organism_rows, release_uri):
    updates, _ = compare.check_taxonomy(release_uri, taxonomy_dir, release_name="116")
    assert updates.updates


@pytest.mark.parametrize(
    "release_name, message",
    [
        ("117", "mismatch for release '117': metadata DB has '2026-09-01T00:00:00Z', TaxonDB is '2026-06-28T09:00:00Z'"),
        ("118", "has no taxonomy_snapshot set"),
        ("999", "No ensembl_release named '999'"),
    ],
)
def test_snapshot_check_fails(taxonomy_dir, organism_rows, release_uri, release_name, message):
    with pytest.raises(ValueError, match=re.escape(message)):
        compare.check_taxonomy(release_uri, taxonomy_dir, release_name=release_name)


def test_snapshot_check_skipped_without_release_name(taxonomy_dir, organism_rows):
    # "unused" isn't a database: the check would fail if it ran.
    updates, _ = compare.check_taxonomy("unused", taxonomy_dir)
    assert updates.updates


def test_load_organism_rows(test_dbs):
    rows = compare.load_organism_rows(test_dbs["ensembl_genome_metadata"].dbc.url)
    assert rows
    assert {"organism_id", "taxonomy_id", "species_taxonomy_id", "scientific_name", "common_name",
            "genomes"} == set(rows[0])
    genomes = {g["genome_uuid"]: g for r in rows for g in r["genomes"]}
    assert {"genome_uuid", "release_name", "release_version", "genebuild_source", "genebuild_method"} == set(
        next(iter(genomes.values())))
    assert any(g["genebuild_source"] and g["genebuild_method"] for g in genomes.values())
    # Each genome carries the latest release it is in. In the fixture these two are
    # in releases 110.1 or 108.0, 110.2 and 115.0; 115.0 has no name.
    for uuid in ("9caa2cae-d1c8-4cfc-9ffd-2e13bc3e95b1", "a73351f7-93e7-11ec-a39d-005056b38ce3"):
        assert (genomes[uuid]["release_name"], genomes[uuid]["release_version"]) == (None, 115.0)
