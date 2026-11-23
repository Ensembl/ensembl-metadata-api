import itertools
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from ensembl.production.metadata.scripts.genome_taxonomy_counts import GenomeTaxonomyCountGenerator
from ensembl.production.metadata.scripts.genome_taxonomy_counts import _ensure_itertools_batched


class FakeTaxonDB:
    """TaxonDB fixture with the lineages represented in the metadata test DB."""

    stop_ids = {
        "metazoa": 33208,
        "archaea": 2157,
        "bacteria": 2,
        "fungi": 4751,
        "viridiplantae": 33090,
        "root": 1,
    }
    group_by_taxon = {
        9606: 33208,
        6239: 33208,
        4565: 33090,
        559292: 4751,
        511145: 2,
        36329: 2,
    }

    def __init__(self, _location):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        pass

    def primary_id(self, taxon_id):
        return None if taxon_id == 999999 else taxon_id

    def lineage_ids(self, taxon_id):
        group_id = self.group_by_taxon[taxon_id]
        return [1, group_id, taxon_id] if taxon_id != group_id else [1, taxon_id]

    def query_taxa_by_name(self, name, **_kwargs):
        return [self.stop_ids[name]]


db_directory = Path(__file__).parent / "databases"


@pytest.mark.parametrize(
    "test_dbs",
    [[
        {"src": db_directory / "ensembl_genome_metadata"},
        {"src": db_directory / "ncbi_taxonomy"},
    ]],
    indirect=True,
)
def test_supplies_itertools_batched_for_python_before_312(monkeypatch, test_dbs):
    del test_dbs  # The autouse database fixture is required by this test module.
    monkeypatch.delattr(itertools, "batched", raising=False)

    _ensure_itertools_batched()

    assert list(itertools.batched(range(5), 2)) == [(0, 1), (2, 3), (4,)]


@pytest.mark.parametrize(
    "test_dbs",
    [[
        {"src": db_directory / "ensembl_genome_metadata"},
        {"src": db_directory / "ncbi_taxonomy"},
    ]],
    indirect=True,
)
def test_generates_all_release_and_released_release_counts(test_dbs):
    generator = GenomeTaxonomyCountGenerator(
        test_dbs["ensembl_genome_metadata"].dbc.url,
        "unused-by-fake",
        taxon_db_factory=FakeTaxonDB,
    )

    report = generator.generate()
    rows = report.rows

    assert rows
    assert {row.release_id for row in rows}.issuperset({0, 1, 5, 7})
    assert sum(row.count for row in rows if row.release_id == 0) > 0
    assert {row.ensembl_taxon_name for row in rows}.issubset(
        {"Animals", "Archaea", "Bacteria", "Fungi", "Green Plants", "Others"}
    )
    assert all(row.label == "" for row in rows if row.release_id == 0)
    assert report.failed_taxonomy_ids == []


@pytest.mark.parametrize(
    "test_dbs",
    [[
        {"src": db_directory / "ensembl_genome_metadata"},
        {"src": db_directory / "ncbi_taxonomy"},
    ]],
    indirect=True,
)
def test_reports_unresolvable_taxonomy_ids_without_raising(test_dbs):
    metadata_uri = test_dbs["ensembl_genome_metadata"].dbc.url
    with create_engine(metadata_uri).begin() as connection:
        connection.execute(
            text("UPDATE organism SET taxonomy_id = 999999 WHERE organism_id = 1")
        )

    report = GenomeTaxonomyCountGenerator(
        metadata_uri,
        "unused-by-fake",
        taxon_db_factory=FakeTaxonDB,
    ).generate()

    assert report.failed_taxonomy_ids == [999999]
