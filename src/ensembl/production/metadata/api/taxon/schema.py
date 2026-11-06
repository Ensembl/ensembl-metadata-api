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
"""Pydantic models for the taxonomy update and delete JSON reports."""

from datetime import datetime
from enum import Enum
from typing import Literal

from pydantic import BaseModel, ConfigDict, RootModel


class ConfidenceLevel(str, Enum):
    """How a current name compares to NCBI's."""

    CASE_DIFF = "case_diff"
    ORDER_DIFF = "order_diff"
    SIMILAR = "similar"
    DIFFERENT = "different"


class IntChange(BaseModel):
    current: int
    new: int | None = None


class StrChange(BaseModel):
    current: str
    new: str | None = None
    confidence: ConfidenceLevel | None = None


class TargetGenome(BaseModel):
    """A genome of a target organism, with the latest release it is in
    (both None if the genome is in no release yet).

    genebuild_source and genebuild_method are only filled in when common names
    are compared, since only the common-name rules use them.
    """

    genome_uuid: str
    release_name: str | None = None
    release_version: float | None = None
    genebuild_source: str | None = None
    genebuild_method: str | None = None


class Target(BaseModel):
    """One organism affected by the update, with its genomes."""

    organism_id: int
    genomes: list[TargetGenome]


class _Changes(BaseModel):
    # Unknown fields are rejected so each entry kind parses back to its own class.
    model_config = ConfigDict(extra="forbid")


class IdChanges(_Changes):
    """A taxonomy_id merge, with the scientific_name change it brings.

    Organisms sharing a merged taxonomy_id but holding different current
    scientific names get one entry each, so `current` is exact per target.
    Common-name changes stay in separate taxon_common_name_update entries.
    """

    taxonomy_id: IntChange
    scientific_name: StrChange | None = None


class SpeciesIdChanges(_Changes):
    """A species_taxonomy_id merge. Only the id changes; names follow taxonomy_id."""

    species_taxonomy_id: IntChange


class ScientificNameChanges(_Changes):
    """A scientific_name change for organisms not covered by a taxonomy_id merge."""

    scientific_name: StrChange


class CommonNameChanges(_Changes):
    """A common_name change."""

    common_name: StrChange


class Metadata(BaseModel):
    source: str
    taxonomy_release: str
    detected_at: datetime


class TaxonIdUpdateEntry(BaseModel):
    """An id-merge update entry."""

    taxonomy_id: int
    changes: IdChanges
    targets: list[Target]


class TaxonSpeciesIdUpdateEntry(BaseModel):
    """A species_taxonomy_id merge update entry."""

    species_taxonomy_id: int
    changes: SpeciesIdChanges
    targets: list[Target]


class TaxonScientificNameUpdateEntry(BaseModel):
    """A standalone scientific_name update entry."""

    taxonomy_id: int
    changes: ScientificNameChanges
    targets: list[Target]


class TaxonCommonNameUpdateEntry(BaseModel):
    """A common_name update entry."""

    taxonomy_id: int
    changes: CommonNameChanges
    targets: list[Target]


class TaxonUpdateReport(BaseModel):
    """Top-level report for taxonomy updates."""

    schema_version: str = "1.0"
    metadata: Metadata
    updates: dict[
        str,
        TaxonIdUpdateEntry | TaxonSpeciesIdUpdateEntry | TaxonScientificNameUpdateEntry | TaxonCommonNameUpdateEntry,
    ]


class TaxonDelete(BaseModel):
    """A deleted or missing taxonomy identifier that needs human review."""

    type: Literal["delete"] = "delete"
    id_type: Literal["taxonomy_id", "species_taxonomy_id"]
    taxonomy_id: int
    species_taxonomy_id: int | None = None
    scientific_name: str
    common_name: str | None = None
    missing_behavior: Literal["DELETED", "NOT FOUND"]
    needs_review: Literal[True] = True
    metadata: Metadata


class TaxonDeleteReport(RootModel[list[TaxonDelete]]):
    """Taxa that are deleted or not found in NCBI Taxonomy."""
