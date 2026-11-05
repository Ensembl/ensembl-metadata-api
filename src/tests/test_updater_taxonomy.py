#  See the NOTICE file distributed with this work for additional information
#  regarding copyright ownership.
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#      http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Tests for :mod:`ensembl.production.metadata.updater.taxonomy` and the organism taxonomy fallbacks."""
from pathlib import Path
import sys

import pytest

from ensembl.production.metadata.api.exceptions import TaxonNotFoundException
from ensembl.production.metadata.updater.core import CoreMetaUpdater
from ensembl.production.metadata.updater.taxonomy import (
    SQLTaxonomyLookup,
    TaxonDBLookup,
    TaxonStatus,
    TaxonomyLookup,
    open_taxonomy,
)
from ensembl.production.metadata.updater.updater_utils import get_homology_reference_collection

# Applied to every test in this module: the autouse 'engine' fixture in conftest.py needs
# test_dbs['ensembl_genome_metadata'] even for tests that use no database themselves.
pytestmark = pytest.mark.parametrize(
    "test_dbs",
    [
        [
            {'src': Path(__file__).parent / "databases" / "updater_utils" / "ensembl_genome_metadata"},
            {'src': Path(__file__).parent / "databases" / "updater_utils" / "ncbi_taxonomy"},
        ]
    ],
    indirect=True,
)


class TestSQLTaxonomyLookup:
    """The legacy ncbi_taxonomy database backend, on the updater_utils fixture."""

    @pytest.fixture
    def lookup(self, test_dbs):
        lookup = open_taxonomy(test_dbs["ncbi_taxonomy"].dbc.url)
        yield lookup
        lookup.close()

    def test_backend_selected_by_uri(self, lookup):
        assert isinstance(lookup, SQLTaxonomyLookup)

    def test_status(self, lookup):
        assert lookup.status(3702) == TaxonStatus("current", 3702)
        assert lookup.status(3706) == TaxonStatus("merged", 3711)
        assert lookup.status(999999999) == TaxonStatus("unknown", None)

    def test_validate(self, lookup):
        assert lookup.validate("3702") == 3702
        with pytest.raises(TaxonNotFoundException, match="merged into 3711"):
            lookup.validate(3706)
        with pytest.raises(TaxonNotFoundException, match="unknown"):
            lookup.validate(999999999)
        with pytest.raises(TaxonNotFoundException, match="Invalid taxonomy ID"):
            lookup.validate("not-a-taxon")

    def test_names(self, lookup):
        assert lookup.scientific_name(3702) == "Arabidopsis thaliana"
        assert lookup.genbank_common_name(3702) == "thale cress"

    def test_lineage_and_species(self, lookup):
        assert lookup.lineage(3702)[0] == (3702, "species")
        assert lookup.species_taxonomy_id(3702) == 3702
        # Subspecies resolve to their species ancestor.
        assert lookup.species_taxonomy_id(10091) == 10090
        assert lookup.species_taxonomy_id(4567) == 4571
        # The root has no species in its lineage.
        assert lookup.species_taxonomy_id(1) is None

    def test_homology_reference_collection_with_open_lookup(self, test_dbs, lookup):
        with test_dbs["ensembl_genome_metadata"].dbc.session_scope() as session:
            assert get_homology_reference_collection(3702, lookup, session) == "Viridiplantae"


def _write_snapshot(snapshot_dir: Path) -> None:
    """Write a minimal TaxonDB Parquet snapshot, laid out as TaxonCache writes it."""
    pd = pytest.importorskip("pandas")
    # taxon_id: (parent_id, rank); the root's parent_id is 0, as in TaxonCache output.
    tree = {
        1: (0, "no rank"),
        2759: (1, "superkingdom"),
        33090: (2759, "kingdom"),
        3701: (33090, "genus"),
        3702: (3701, "species"),
        59689: (3701, "species"),
        81972: (59689, "subspecies"),
    }
    children = {}
    for taxon_id, (parent_id, _) in tree.items():
        children.setdefault(parent_id, []).append(taxon_id)
    indexes = {}
    counter = iter(range(1, 2 * len(tree) + 1))

    def index(taxon_id):
        left = next(counter)
        for child_id in sorted(children.get(taxon_id, [])):
            index(child_id)
        indexes[taxon_id] = (left, next(counter))

    index(1)
    nodes = pd.DataFrame(
        [
            (taxon_id, parent_id, rank, 0, *indexes[taxon_id], 1)
            for taxon_id, (parent_id, rank) in tree.items()
        ],
        columns=["taxon_id", "parent_id", "rank", "genbank_hidden_flag", "left_index", "right_index", "root_id"],
    )
    names = pd.DataFrame(
        [
            (1, "root", "scientific name"),
            (1, "2026-10-01T03:30:04Z", "taxdump_id"),
            (1, "2026-10-01", "archive_id"),
            (1, "2026-10-28T21:45:07Z", "import date"),
            (2759, "Eukaryota", "scientific name"),
            (33090, "Viridiplantae", "scientific name"),
            (3701, "Arabidopsis", "scientific name"),
            (3702, "Arabidopsis thaliana", "scientific name"),
            (3702, "thale cress", "genbank common name"),
            (59689, "Arabidopsis lyrata", "scientific name"),
            (81972, "Arabidopsis lyrata subsp. lyrata", "scientific name"),
        ],
        columns=["taxon_id", "name", "name_class"],
    )
    past = pd.DataFrame(
        [(12, "merged", 3702), (13, "deleted", None)],
        columns=["taxon_id", "status", "primary_id"],
    ).astype({"primary_id": "Int64"})
    snapshot_dir.mkdir(parents=True)
    nodes.to_parquet(snapshot_dir / "ncbi_taxa_node.parquet", index=False)
    names.to_parquet(snapshot_dir / "ncbi_taxa_name.parquet", index=False)
    past.to_parquet(snapshot_dir / "ncbi_taxa_past.parquet", index=False)


@pytest.mark.skipif(sys.version_info < (3, 12), reason="TaxonDB lineage queries use itertools.batched")
class TestTaxonDBLookup:
    """The TaxonDB (Parquet snapshot) backend, on a synthetic snapshot."""

    @pytest.fixture
    def lookup(self, tmp_path):
        pytest.importorskip("ensembl.taxonomy.db")
        snapshot_dir = tmp_path / "taxdumps" / "2026" / "20261001T033004Z"
        _write_snapshot(snapshot_dir)
        lookup = open_taxonomy(snapshot_dir.as_uri())
        yield lookup
        lookup.close()

    def test_backend_selected_by_uri(self, lookup):
        assert isinstance(lookup, TaxonDBLookup)
        assert lookup.snapshot_id == "2026-10-01T03:30:04Z"

    def test_status(self, lookup):
        assert lookup.status(3702) == TaxonStatus("current", 3702)
        assert lookup.status(12) == TaxonStatus("merged", 3702)
        assert lookup.status(13) == TaxonStatus("deleted", None)
        assert lookup.status(999999999) == TaxonStatus("unknown", None)

    def test_validate(self, lookup):
        assert lookup.validate("3702") == 3702
        with pytest.raises(TaxonNotFoundException, match="merged into 3702"):
            lookup.validate(12)
        with pytest.raises(TaxonNotFoundException, match="deleted"):
            lookup.validate(13)

    def test_names(self, lookup):
        assert lookup.scientific_name(3702) == "Arabidopsis thaliana"
        assert lookup.genbank_common_name(3702) == "thale cress"
        # TaxonDB raises for a missing name class; the lookup returns None instead.
        assert lookup.genbank_common_name(59689) is None

    def test_lineage_and_species(self, lookup):
        assert lookup.lineage(81972) == [
            (81972, "subspecies"), (59689, "species"), (3701, "genus"),
            (33090, "kingdom"), (2759, "superkingdom"), (1, "no rank"),
        ]
        assert lookup.ancestor_ids(3702) == [3701, 33090, 2759, 1]
        assert lookup.species_taxonomy_id(81972) == 59689
        assert lookup.species_taxonomy_id(3701) is None


class _FakeTaxonomy(TaxonomyLookup):
    """In-memory lookup for testing the organism fallbacks without a database."""

    def __init__(self):
        self.calls = []

    def status(self, taxon_id):
        return TaxonStatus("current", int(taxon_id)) if int(taxon_id) != 12 else TaxonStatus("merged", 3702)

    def scientific_name(self, taxon_id):
        self.calls.append("scientific_name")
        return "Arabidopsis lyrata subsp. lyrata"

    def genbank_common_name(self, taxon_id):
        self.calls.append("genbank_common_name")
        return "lyrate rockcress"

    def lineage(self, taxon_id):
        self.calls.append("lineage")
        return [(81972, "subspecies"), (59689, "species"), (3701, "genus"), (1, "no rank")]


class _FakeQuery:
    def __init__(self, result):
        self._result = result

    def filter(self, *_):
        return self

    def one_or_none(self):
        return self._result


class _FakeSession:
    def __init__(self, existing_organism=None):
        self.existing_organism = existing_organism
        self.added = []

    def query(self, model):
        return _FakeQuery(self.existing_organism if model.__name__ == "Organism" else None)

    def add(self, obj):
        self.added.append(obj)


def _updater(meta: dict, taxonomy: TaxonomyLookup) -> CoreMetaUpdater:
    updater = CoreMetaUpdater.__new__(CoreMetaUpdater)  # skip __init__: no core/metadata databases
    updater.meta_dict = {1: {key: [value] for key, value in meta.items()}}
    updater._taxonomy = taxonomy
    return updater


class TestOrganismTaxonomyFallbacks:
    """get_or_new_organism() fills names and the species taxonomy ID from the taxonomy when absent."""

    BASE_META = {"organism.biosample_id": "SAMN000001", "organism.taxonomy_id": "81972"}

    def test_core_with_only_taxonomy_id(self):
        taxonomy = _FakeTaxonomy()
        session = _FakeSession()
        organism = _updater(self.BASE_META, taxonomy).get_or_new_organism(1, session)
        assert session.added == [organism]
        assert organism.taxonomy_id == 81972
        assert organism.scientific_name == "Arabidopsis lyrata subsp. lyrata"
        assert organism.common_name == "Lyrate rockcress"
        assert organism.species_taxonomy_id == 59689

    def test_core_values_take_precedence(self):
        taxonomy = _FakeTaxonomy()
        meta = {
            **self.BASE_META,
            "organism.scientific_name": "Arabidopsis lyrata lyrata",
            "organism.common_name": "rockcress",
            "organism.species_taxonomy_id": "59689",
        }
        organism = _updater(meta, taxonomy).get_or_new_organism(1, _FakeSession())
        assert organism.scientific_name == "Arabidopsis lyrata lyrata"
        assert organism.common_name == "Rockcress"
        assert organism.species_taxonomy_id == "59689"
        assert taxonomy.calls == []

    def test_existing_organism_skips_taxonomy(self):
        taxonomy = _FakeTaxonomy()
        existing = object()
        assert _updater(self.BASE_META, taxonomy).get_or_new_organism(1, _FakeSession(existing)) is existing
        assert taxonomy.calls == []

    def test_merged_taxonomy_id_is_rejected(self):
        meta = {**self.BASE_META, "organism.taxonomy_id": "12"}
        with pytest.raises(TaxonNotFoundException, match="merged into 3702"):
            _updater(meta, _FakeTaxonomy()).get_or_new_organism(1, _FakeSession())
