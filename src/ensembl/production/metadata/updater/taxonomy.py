# See the NOTICE file distributed with this work for additional information
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
"""Taxonomy lookups used by the metadata updater.

Two backends sit behind the same interface, chosen by the taxonomy URI scheme:

* ``file://<snapshot dir>``: a TaxonDB snapshot (Parquet files) from ``ensembl-taxonomy``.
* any SQLAlchemy URI (``mysql://...``, ``sqlite://...``): the legacy ``ncbi_taxonomy`` database.

This lets the updater move from the MySQL database to the Parquet snapshot by changing
the URI only.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import NamedTuple, Optional
from urllib.parse import urlparse

from ensembl.production.metadata.api import exceptions

logger = logging.getLogger(__name__)

__all__ = ["TaxonStatus", "TaxonomyLookup", "SQLTaxonomyLookup", "TaxonDBLookup", "open_taxonomy"]

# Safety net against cycles in a malformed tree; real NCBI lineages are < 100 deep.
_MAX_LINEAGE_DEPTH = 500


class TaxonStatus(NamedTuple):
    """Lifecycle status of a taxonomy ID: ``current``, ``merged``, ``deleted`` or ``unknown``."""
    status: str
    primary_id: Optional[int]


def _to_taxon_id(taxon_id) -> int:
    try:
        return int(taxon_id)
    except (TypeError, ValueError) as exc:
        raise exceptions.TaxonNotFoundException(f"Invalid taxonomy ID: {taxon_id!r}") from exc


class TaxonomyLookup(ABC):
    """Read-only access to the NCBI taxonomy needed by the metadata updater."""

    @abstractmethod
    def status(self, taxon_id) -> TaxonStatus:
        """Return the lifecycle status of ``taxon_id``."""

    @abstractmethod
    def scientific_name(self, taxon_id) -> Optional[str]:
        """Return the scientific name of ``taxon_id``, or None if it has none."""

    @abstractmethod
    def genbank_common_name(self, taxon_id) -> Optional[str]:
        """Return the GenBank common name of ``taxon_id``, or None if it has none."""

    @abstractmethod
    def lineage(self, taxon_id) -> list[tuple[int, str]]:
        """Return ``(taxon_id, rank)`` pairs from ``taxon_id`` itself up to the root (nearest first)."""

    @property
    def snapshot_id(self) -> Optional[str]:
        """Identifier of the taxonomy snapshot, if the backend knows it."""
        return None

    def close(self) -> None:
        """Release backend resources."""

    def __enter__(self) -> "TaxonomyLookup":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def validate(self, taxon_id) -> int:
        """Return ``taxon_id`` as an int if it is current; otherwise raise TaxonNotFoundException.

        Merged IDs are reported with their replacement rather than silently remapped, so the
        core database meta can be corrected at source.
        """
        int_taxon_id = _to_taxon_id(taxon_id)
        taxon_status = self.status(int_taxon_id)
        if taxon_status.status == "current":
            return int_taxon_id
        snapshot = f" (taxonomy snapshot {self.snapshot_id})" if self.snapshot_id else ""
        if taxon_status.status == "merged":
            raise exceptions.TaxonNotFoundException(
                f"taxon id {int_taxon_id} has been merged into {taxon_status.primary_id}{snapshot}; "
                f"update organism.taxonomy_id in the core database"
            )
        raise exceptions.TaxonNotFoundException(
            f"taxon id {int_taxon_id} is {taxon_status.status} in the taxonomy database{snapshot}"
        )

    def ancestor_ids(self, taxon_id) -> list[int]:
        """Return the IDs of the strict ancestors of ``taxon_id``, nearest first, root last."""
        return [ancestor_id for ancestor_id, _ in self.lineage(taxon_id)[1:]]

    def species_taxonomy_id(self, taxon_id) -> Optional[int]:
        """Return the ID of the nearest taxon at rank ``species`` (``taxon_id`` itself included).

        Returns None if no taxon in the lineage has rank ``species``, e.g. for a genus.
        """
        for lineage_id, rank in self.lineage(taxon_id):
            if rank == "species":
                return lineage_id
        return None


class SQLTaxonomyLookup(TaxonomyLookup):
    """Lookup backed by the legacy ``ncbi_taxonomy`` relational database."""

    def __init__(self, uri: str) -> None:
        from ensembl.utils.database import DBConnection

        self._db = DBConnection(uri)

    def close(self) -> None:
        self._db.dispose()

    def status(self, taxon_id) -> TaxonStatus:
        from ensembl.ncbi_taxonomy.models import NCBITaxaName, NCBITaxaNode

        int_taxon_id = _to_taxon_id(taxon_id)
        with self._db.session_scope() as session:
            node = session.query(NCBITaxaNode.taxon_id).filter(NCBITaxaNode.taxon_id == int_taxon_id).first()
            if node is not None:
                return TaxonStatus("current", int_taxon_id)
            # The legacy schema records a merged ID as a 'merged_taxon_id' name of its replacement.
            merged_into = session.query(NCBITaxaName.taxon_id).filter(
                NCBITaxaName.name == str(int_taxon_id),
                NCBITaxaName.name_class == "merged_taxon_id",
            ).first()
            if merged_into is not None:
                return TaxonStatus("merged", int(merged_into[0]))
        # The legacy schema does not track deleted IDs, so they are reported as unknown.
        return TaxonStatus("unknown", None)

    def _name(self, taxon_id, name_class: str) -> Optional[str]:
        from ensembl.ncbi_taxonomy.models import NCBITaxaName

        with self._db.session_scope() as session:
            row = session.query(NCBITaxaName.name).filter(
                NCBITaxaName.taxon_id == _to_taxon_id(taxon_id),
                NCBITaxaName.name_class == name_class,
            ).order_by(NCBITaxaName.name).first()
            return row[0] if row is not None else None

    def scientific_name(self, taxon_id) -> Optional[str]:
        return self._name(taxon_id, "scientific name")

    def genbank_common_name(self, taxon_id) -> Optional[str]:
        return self._name(taxon_id, "genbank common name")

    def lineage(self, taxon_id) -> list[tuple[int, str]]:
        # Walk parent_id rather than using left/right indexes, which are not always populated.
        from ensembl.ncbi_taxonomy.models import NCBITaxaNode

        current_id = _to_taxon_id(taxon_id)
        lineage = []
        with self._db.session_scope() as session:
            for _ in range(_MAX_LINEAGE_DEPTH):
                node = session.query(
                    NCBITaxaNode.taxon_id, NCBITaxaNode.parent_id, NCBITaxaNode.rank, NCBITaxaNode.root_id
                ).filter(NCBITaxaNode.taxon_id == current_id).first()
                if node is None:
                    if not lineage:
                        raise exceptions.TaxonNotFoundException(
                            f"taxon id {current_id} not found in taxonomy database"
                        )
                    logger.warning(f"Lineage of taxon {taxon_id} is broken at missing parent {current_id}")
                    break
                lineage.append((int(node.taxon_id), (node.rank or "").strip()))
                if node.taxon_id == node.root_id or node.parent_id == node.taxon_id:
                    break
                current_id = node.parent_id
            else:
                raise exceptions.MetadataUpdateException(
                    f"Lineage of taxon {taxon_id} exceeds {_MAX_LINEAGE_DEPTH} levels; is the tree cyclic?"
                )
        return lineage


class TaxonDBLookup(TaxonomyLookup):
    """Lookup backed by an ``ensembl-taxonomy`` TaxonDB snapshot (Parquet files).

    Opening a snapshot loads it into an in-memory DuckDB database, so create one instance per
    updater run, not per genome.
    """

    def __init__(self, uri: str, threads: int = 1) -> None:
        try:
            from ensembl.taxonomy.db import TaxonDB
        except ImportError as exc:
            raise exceptions.UpdaterException(
                "Reading a taxonomy snapshot from a file:// URI requires the 'ensembl-taxonomy' package"
            ) from exc
        self._db = TaxonDB.from_uri(uri, threads=threads)

    def close(self) -> None:
        self._db._conn.close()  # TaxonDB exposes no public close(); its __exit__ does the same.

    @property
    def snapshot_id(self) -> Optional[str]:
        return self._db.taxdump_id

    def status(self, taxon_id) -> TaxonStatus:
        int_taxon_id = _to_taxon_id(taxon_id)
        primary_id = self._db.primary_id(int_taxon_id)
        if primary_id == int_taxon_id:
            return TaxonStatus("current", int_taxon_id)
        if primary_id is not None:
            return TaxonStatus("merged", int(primary_id))
        if self._db.is_deleted_id(int_taxon_id):
            return TaxonStatus("deleted", None)
        return TaxonStatus("unknown", None)

    def _name(self, method_name: str, taxon_id) -> Optional[str]:
        from ensembl.taxonomy.db import InvalidTaxaError

        try:
            return getattr(self._db, method_name)(_to_taxon_id(taxon_id))
        except InvalidTaxaError as exc:
            raise exceptions.TaxonNotFoundException(str(exc)) from exc
        except RuntimeError:
            # TaxonDB raises RuntimeError (not None) when a current taxon lacks the name class.
            return None

    def scientific_name(self, taxon_id) -> Optional[str]:
        return self._name("scientific_name", taxon_id)

    def genbank_common_name(self, taxon_id) -> Optional[str]:
        return self._name("genbank_common_name", taxon_id)

    def lineage(self, taxon_id) -> list[tuple[int, str]]:
        from ensembl.taxonomy.db import InvalidTaxaError

        int_taxon_id = _to_taxon_id(taxon_id)
        try:
            # TaxonDB orders the lineage root first; reverse it to nearest first.
            lineage_ids = [int(lineage_id) for lineage_id in reversed(self._db.lineage_ids(int_taxon_id))]
        except InvalidTaxaError as exc:
            raise exceptions.TaxonNotFoundException(str(exc)) from exc
        ranks = {}
        for chunk in self._db.query_taxa_attributes(["rank"], taxon_ids=lineage_ids):
            for chunk_taxon_id, rank in zip(chunk["taxon_id"], chunk["rank"]):
                ranks[int(chunk_taxon_id)] = str(rank)
        return [(lineage_id, ranks.get(lineage_id, "")) for lineage_id in lineage_ids]


def open_taxonomy(uri: str) -> TaxonomyLookup:
    """Open a taxonomy lookup for ``uri``: ``file://`` for a TaxonDB snapshot, else a SQL database."""
    if not uri:
        raise exceptions.UpdaterException("No taxonomy URI given")
    if urlparse(uri).scheme == "file":
        return TaxonDBLookup(uri)
    return SQLTaxonomyLookup(uri)
