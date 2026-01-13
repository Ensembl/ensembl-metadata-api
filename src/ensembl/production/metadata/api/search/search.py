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

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Tuple, Iterator, Union

from ensembl.utils.database import DBConnection
from pydantic import BaseModel
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import Session, joinedload

from ensembl.production.metadata.api.models import (
    Genome,
    Dataset,
    DatasetAttribute,
    EnsemblRelease,
    GenomeRelease,
    GenomeDataset,
    ReleaseStatus,
    DatasetStatus,
)

logger = logging.getLogger(__name__)


# ============================================================================
# EXCEPTIONS AND ERROR TRACKING
# ============================================================================


class MissingDatasetFieldError(Exception):
    """Raised when a required dataset field is missing"""

    pass


@dataclass
class GenomeIndexError:
    """Represents an error that occurred while indexing a genome"""

    genome_uuid: str
    release_label: str
    error_message: str
    exception: Exception


class IndexingErrorCollection:
    """Collects errors during indexing process"""

    def __init__(self):
        self.errors: List[GenomeIndexError] = []

    def add_error(self, genome_uuid: str, release_label: str, error_message: str, exception: Exception):
        """Add an error to the collection"""
        self.errors.append(
            GenomeIndexError(
                genome_uuid=genome_uuid,
                release_label=release_label,
                error_message=error_message,
                exception=exception,
            )
        )

    def has_errors(self) -> bool:
        """Check if any errors were collected"""
        return len(self.errors) > 0

    def get_summary(self) -> str:
        """Get a formatted summary of all errors"""
        if not self.errors:
            return "No errors occurred during indexing."

        summary = [f"\n{'=' * 80}"]
        summary.append(f"INDEXING ERRORS: {len(self.errors)} genome(s) failed to index")
        summary.append("=" * 80)

        for i, error in enumerate(self.errors, 1):
            summary.append(f"\n{i}. Genome: {error.genome_uuid} (Release: {error.release_label})")
            summary.append(f"   Error: {error.error_message}")

        summary.append("=" * 80 + "\n")
        return "\n".join(summary)

    def raise_if_errors(self):
        """Raise an exception if there are any errors"""
        if self.has_errors():
            raise MissingDatasetFieldError(self.get_summary())


# ============================================================================
# PYDANTIC SCHEMAS
# ============================================================================


class SearchField(BaseModel):
    """A single field in a search entry"""

    name: str
    value: Union[str, int, bool, List[int], List[str]]


class SearchEntry(BaseModel):
    """A single search entry with fields array"""

    fields: List[SearchField]


class SearchIndex(BaseModel):
    """The complete search index document"""

    name: str = "ensemblNext"
    release: str
    entry_count: int
    entries: List[SearchEntry]


class GenomeSearchDocument(BaseModel):
    """Internal schema for genome data before conversion to fields format"""

    # Direct fields from Genome/Organism/Assembly
    genome_uuid: str
    common_name: Optional[str] = None
    scientific_name: str
    strain_type: Optional[str] = None
    strain: Optional[str] = None
    assembly_name: str
    accession: str
    url_name: Optional[str] = None
    tol_id: Optional[str] = None
    is_reference: bool
    species_taxonomy_id: int
    taxonomy_id: int
    scientific_parlance_name: Optional[str] = None
    organism_uuid: str  # Changed from organism_id
    rank: int = 0

    # Additional taxonomy fields
    lineage_taxids: list[int]
    lineage_name: list[str]

    # Complex derived fields from datasets - REQUIRED
    contig_n50: int
    coding_genes: int
    has_variation: bool = False
    has_regulation: bool = False
    genebuild_provider: str
    genebuild_method_display: str

    # Release information
    first_release_name: str
    first_release_type: str
    latest_release_name: str
    latest_release_type: str
    is_latest_release_current: int
    releases: str  # comma-separated list of integrated releases

    class Config:
        from_attributes = True

    def to_search_entry(self) -> SearchEntry:
        """Convert to SearchEntry format with fields array"""
        # Strip version from accession (e.g., GCA_000005845.2 -> GCA_000005845)
        unversioned_accession = self.accession.rsplit(".", 1)[0] if "." in self.accession else self.accession

        fields = [
            SearchField(name="id", value=self.genome_uuid),
            SearchField(name="common_name", value=self.common_name or ""),
            SearchField(name="scientific_name", value=self.scientific_name),
            SearchField(name="assembly", value=self.assembly_name),
            SearchField(name="assembly_accession", value=self.accession),
            SearchField(name="unversioned_assembly_accession", value=unversioned_accession),
            SearchField(name="type_value", value=self.strain or ""),
            SearchField(name="parlance_name", value=self.scientific_parlance_name or ""),
            SearchField(name="type_type", value=self.strain_type or ""),
            SearchField(name="coding_genes", value=str(self.coding_genes)),
            SearchField(name="n50", value=str(self.contig_n50)),
            SearchField(name="has_variation", value=self.has_variation),
            SearchField(name="has_regulation", value=self.has_regulation),
            SearchField(name="annotation_method", value=self.genebuild_method_display),
            SearchField(name="annotation_provider", value=self.genebuild_provider),
            SearchField(name="genome_uuid", value=self.genome_uuid),
            SearchField(name="url_name", value=self.url_name or ""),
            SearchField(name="tol_id", value=self.tol_id or ""),
            SearchField(name="is_reference", value=self.is_reference),
            SearchField(name="species_taxonomy_id", value=self.species_taxonomy_id),
            SearchField(name="taxonomy_id", value=self.taxonomy_id),
            SearchField(name="lineage_taxids", value=self.lineage_taxids),
            SearchField(name="lineage_name", value=self.lineage_name),
            SearchField(name="organism_id", value=self.organism_uuid),
            SearchField(name="rank", value=self.rank),
            SearchField(name="first_release_name", value=self.first_release_name),
            SearchField(name="first_release_type", value=self.first_release_type),
            SearchField(name="latest_release_name", value=self.latest_release_name),
            SearchField(name="latest_release_type", value=self.latest_release_type),
            SearchField(name="is_latest_release_current", value=self.is_latest_release_current),
            SearchField(name="releases", value=self.releases),
        ]

        return SearchEntry(fields=fields)


# ============================================================================
# DATASET FIELD EXTRACTOR
# ============================================================================


class DatasetFieldExtractor:
    """
    Extracts dataset-related fields for a genome.
    Always uses is_current=1 datasets regardless of release.
    Release is kept for context in error messages.
    """

    def __init__(self, session: Session, genome: Genome, release: EnsemblRelease):
        self.session = session
        self.genome = genome
        self.release = release
        self._datasets_cache = None

    def _get_relevant_datasets(self) -> List[GenomeDataset]:
        """
        Get datasets relevant for this genome.

        Logic:
        - Always return datasets with is_current == 1
        - Filter to only Released datasets
        """
        if self._datasets_cache is not None:
            return self._datasets_cache

        self._datasets_cache = [
            gd
            for gd in self.genome.genome_datasets
            if gd.is_current == 1 and gd.dataset.status == DatasetStatus.RELEASED
        ]

        return self._datasets_cache

    def _get_dataset_attribute(self, dataset_type_name: str, attribute_name: str) -> Optional[str]:
        """Get a specific attribute value from a dataset of a given type"""
        relevant_datasets = self._get_relevant_datasets()

        for genome_dataset in relevant_datasets:
            dataset = genome_dataset.dataset

            # Check if this is the right dataset type
            if dataset.dataset_type.name != dataset_type_name:
                continue

            for dataset_attr in dataset.dataset_attributes:
                if dataset_attr.attribute.name == attribute_name:
                    return dataset_attr.value

        return None

    def _has_dataset_type(self, dataset_type_name: str) -> bool:
        """Check if a dataset of the given type exists"""
        relevant_datasets = self._get_relevant_datasets()

        return any(gd.dataset.dataset_type.name == dataset_type_name for gd in relevant_datasets)

    def get_contig_n50(self) -> int:
        """
        Get contig N50 from assembly dataset.

        Raises:
            MissingDatasetFieldError: If assembly.stats.contig_n50 is not found
        """
        value = self._get_dataset_attribute("assembly", "assembly.stats.contig_n50")

        if value is None:
            raise MissingDatasetFieldError(
                f"Missing required field 'assembly.stats.contig_n50' for genome "
                f"{self.genome.genome_uuid} in release {self.release.label}"
            )

        try:
            return int(value)
        except ValueError:
            raise MissingDatasetFieldError(
                f"Invalid value for 'assembly.stats.contig_n50' ('{value}') for genome "
                f"{self.genome.genome_uuid} in release {self.release.label}"
            )

    def get_coding_genes(self) -> int:
        """
        Get coding genes count from genebuild dataset.

        Raises:
            MissingDatasetFieldError: If genebuild.stats.coding_genes is not found
        """
        value = self._get_dataset_attribute("genebuild", "genebuild.stats.coding_genes")

        if value is None:
            raise MissingDatasetFieldError(
                f"Missing required field 'genebuild.stats.coding_genes' for genome "
                f"{self.genome.genome_uuid} in release {self.release.label}"
            )

        try:
            return int(value)
        except ValueError:
            raise MissingDatasetFieldError(
                f"Invalid value for 'genebuild.stats.coding_genes' ('{value}') for genome "
                f"{self.genome.genome_uuid} in release {self.release.label}"
            )

    def has_variation(self) -> bool:
        """Check if genome has variation data"""
        return self._has_dataset_type("variation")

    def has_regulation(self) -> bool:
        """Check if genome has regulatory features data"""
        return self._has_dataset_type("regulatory_features")

    def get_genebuild_provider(self) -> str:
        """
        Get genebuild provider name.
        First tries genebuild.provider_name_display attribute,
        falls back to genome.provider_name

        Raises:
            MissingDatasetFieldError: If neither source has a provider name
        """
        provider = self._get_dataset_attribute("genebuild", "genebuild.provider_name_display")

        if provider:
            return provider

        # Fallback to genome's provider_name
        if self.genome.provider_name:
            return self.genome.provider_name

        raise MissingDatasetFieldError(
            f"Missing required field 'genebuild_provider' for genome "
            f"{self.genome.genome_uuid} in release {self.release.label}. "
            f"Neither genebuild.provider_name_display nor genome.provider_name are set."
        )

    def get_genebuild_method_display(self) -> str:
        """
        Get genebuild method display from genebuild dataset.

        Raises:
            MissingDatasetFieldError: If genebuild.method_display is not found
        """
        value = self._get_dataset_attribute("genebuild", "genebuild.method_display")

        if value is None:
            raise MissingDatasetFieldError(
                f"Missing required field 'genebuild.method_display' for genome "
                f"{self.genome.genome_uuid} in release {self.release.label}"
            )

        return value


# ============================================================================
# RELEASE SELECTION HELPER
# ============================================================================


class ReleaseSelector:
    """Handles the logic of selecting the appropriate release for a genome"""

    @staticmethod
    def select_release_for_genome(genome: Genome) -> Optional[EnsemblRelease]:
        """
        Select the single appropriate release for a genome.

        Rules:
        1. If only partial release(s) exist: return it IF genome_release.is_current=1, else None
        2. If both partial and integrated exist: return the newest integrated (by label)
        """
        # Get all released genome_releases
        released_genome_releases = [
            gr
            for gr in genome.genome_releases
            if gr.ensembl_release and gr.ensembl_release.status == ReleaseStatus.RELEASED
        ]

        if not released_genome_releases:
            return None

        # Separate into integrated and partial
        integrated_grs = [
            gr for gr in released_genome_releases if gr.ensembl_release.release_type == "integrated"
        ]
        partial_grs = [gr for gr in released_genome_releases if gr.ensembl_release.release_type == "partial"]

        # If we have integrated releases, return the newest one
        if integrated_grs:
            newest_integrated = max(integrated_grs, key=lambda gr: gr.ensembl_release.label)
            return newest_integrated.ensembl_release

        # Only partial releases exist - check for is_current=1
        current_partial_grs = [gr for gr in partial_grs if gr.is_current == 1]

        if current_partial_grs:
            if len(current_partial_grs) > 1:
                # Data integrity issue - should never have multiple is_current partials
                partial_labels = [gr.ensembl_release.label for gr in current_partial_grs]
                raise ValueError(
                    f"Data integrity error: Genome {genome.genome_uuid} has multiple is_current=1 "
                    f"partial releases: {', '.join(partial_labels)}. Only one is_current partial should exist."
                )
            return current_partial_grs[0].ensembl_release

        return None

    @staticmethod
    def get_release_info(
            genome: Genome,
    ) -> Tuple[Optional[EnsemblRelease], Optional[EnsemblRelease], List[EnsemblRelease]]:
        """
        Get first release, latest release, and all integrated releases for a genome.

        Returns:
            (first_release, latest_release, all_integrated_releases)

        Rules:
        - first_release: earliest release (integrated preferred, partial if no integrated)
        - latest_release: most recent release (integrated preferred, partial if no integrated)
        - all_integrated_releases: list of ALL integrated releases sorted by label
        """
        released_genome_releases = [
            gr
            for gr in genome.genome_releases
            if gr.ensembl_release and gr.ensembl_release.status == ReleaseStatus.RELEASED
        ]

        if not released_genome_releases:
            return None, None, []

        integrated_grs = [
            gr for gr in released_genome_releases if gr.ensembl_release.release_type == "integrated"
        ]
        partial_grs = [gr for gr in released_genome_releases if gr.ensembl_release.release_type == "partial"]

        # Get all integrated releases sorted
        all_integrated = sorted([gr.ensembl_release for gr in integrated_grs], key=lambda r: r.label)

        # Determine first release
        if integrated_grs:
            first_release = min(integrated_grs, key=lambda gr: gr.ensembl_release.label).ensembl_release
        elif partial_grs:
            first_release = min(partial_grs, key=lambda gr: gr.ensembl_release.label).ensembl_release
        else:
            first_release = None

        # Determine latest release
        if integrated_grs:
            latest_release = max(integrated_grs, key=lambda gr: gr.ensembl_release.label).ensembl_release
        elif partial_grs:
            latest_release = max(partial_grs, key=lambda gr: gr.ensembl_release.label).ensembl_release
        else:
            latest_release = None

        return first_release, latest_release, all_integrated

    @staticmethod
    def get_is_latest_release_current(
            genome: Genome, selected_release: EnsemblRelease, latest_release: EnsemblRelease
    ) -> int:
        """
        Determine if the latest release is current.

        Rules:
        - For partial-only genomes: always 1
        - For genomes with integrated releases: 1 if the selected_release matches the latest_release
        """
        # Check if genome has any integrated releases
        has_integrated = any(
            gr.ensembl_release.release_type == "integrated"
            for gr in genome.genome_releases
            if gr.ensembl_release and gr.ensembl_release.status == ReleaseStatus.RELEASED
        )

        if not has_integrated:
            # Partial-only genome
            return 1
        else:
            # Has integrated releases - check if selected matches latest
            return 1 if selected_release.release_id == latest_release.release_id else 0


# ============================================================================
# MAIN SERVICE CLASS WITH BATCHING
# ============================================================================


class GenomeSearchIndexer:
    """Service for generating genome search documents"""

    def __init__(self, metadata_uri: str, taxonomy_uri: str, batch_size: int = 500):
        self.metadata_db = DBConnection(metadata_uri)
        self.taxonomy_db = DBConnection(taxonomy_uri)
        self.release_selector = ReleaseSelector()
        self.batch_size = batch_size

    def _get_genome_ids_to_process(self, session: Session) -> List[int]:
        """
        Get list of genome IDs that have released releases.
        This is a lightweight query to get just the IDs.
        """
        genome_ids = (
            session.query(Genome.genome_id)
            .join(GenomeRelease)
            .join(EnsemblRelease)
            .filter(EnsemblRelease.status == ReleaseStatus.RELEASED)
            .distinct()
            .all()
        )
        return [gid[0] for gid in genome_ids]

    def _get_genomes_batch(self, session: Session, genome_ids: List[int]) -> List[Genome]:
        """
        Get a batch of genomes with all necessary relationships eagerly loaded.
        """
        return (
            session.query(Genome)
            .filter(Genome.genome_id.in_(genome_ids))
            .options(
                joinedload(Genome.organism),
                joinedload(Genome.assembly),
                # Load all genome_releases with their releases for selection logic
                joinedload(Genome.genome_releases).joinedload(GenomeRelease.ensembl_release),
                # Load all genome_datasets with their releases and dataset info
                joinedload(Genome.genome_datasets)
                .joinedload(GenomeDataset.dataset)
                .joinedload(Dataset.dataset_type),
                joinedload(Genome.genome_datasets)
                .joinedload(GenomeDataset.dataset)
                .joinedload(Dataset.dataset_attributes)
                .joinedload(DatasetAttribute.attribute),
                joinedload(Genome.genome_datasets).joinedload(GenomeDataset.ensembl_release),
            )
            .all()
        )

    def get_genomes_with_releases_batched(
            self, session: Session
    ) -> Iterator[List[Tuple[Genome, EnsemblRelease]]]:
        """
        Get genomes with their selected releases in batches.
        Yields batches of (genome, release) tuples.
        """
        # Get all genome IDs (lightweight query)
        genome_ids = self._get_genome_ids_to_process(session)
        total_genomes = len(genome_ids)
        logger.info(f"Found {total_genomes} genomes to process")

        # Process in batches
        for i in range(0, total_genomes, self.batch_size):
            batch_ids = genome_ids[i: i + self.batch_size]
            logger.info(
                f"Processing batch {i // self.batch_size + 1}/{(total_genomes + self.batch_size - 1) // self.batch_size}: "
                f"genomes {i + 1}-{min(i + self.batch_size, total_genomes)}"
            )

            # Load this batch with all relationships
            genomes_batch = self._get_genomes_batch(session, batch_ids)

            # Select appropriate release for each genome
            genome_release_pairs = []
            for genome in genomes_batch:
                selected_release = self.release_selector.select_release_for_genome(genome)
                if selected_release:
                    genome_release_pairs.append((genome, selected_release))

            yield genome_release_pairs

            # Clear session to free memory
            session.expunge_all()

    def _get_newest_partial_release(self, session: Session) -> Optional[str]:
        """
        Get the newest partial release label from the database.
        This will be used as the top-level release in the search index.
        """
        newest_partial = (
            session.query(EnsemblRelease)
            .filter(EnsemblRelease.release_type == "partial")
            .filter(EnsemblRelease.status == ReleaseStatus.RELEASED)
            .order_by(EnsemblRelease.label.desc())
            .first()
        )

        return newest_partial.label if newest_partial else None

    def _extract_direct_fields(self, genome: Genome) -> dict:
        """Extract direct fields from Genome/Organism/Assembly"""
        return {
            "genome_uuid": genome.genome_uuid,
            "common_name": genome.organism.common_name,
            "scientific_name": genome.organism.scientific_name,
            "strain_type": genome.organism.strain_type,
            "strain": genome.organism.strain,
            "assembly_name": genome.assembly.name,
            "accession": genome.assembly.accession,
            "url_name": genome.url_name,
            "tol_id": genome.organism.tol_id,
            "is_reference": bool(genome.assembly.is_reference),
            "species_taxonomy_id": genome.organism.species_taxonomy_id,
            "taxonomy_id": genome.organism.taxonomy_id,
            "scientific_parlance_name": genome.organism.scientific_parlance_name,
            "organism_uuid": genome.organism.organism_uuid,
            "rank": genome.organism.rank or 0,
        }

    def _get_taxonomy_lineage(
            self, taxonomy_session: Session, taxonomy_id: int
    ) -> Tuple[List[int], List[str]]:
        """
        Get taxonomy lineage for a given taxonomy_id.

        Returns:
            - lineage_taxids: List of all taxon_ids from current to root
            - lineage_names: List of ALL names (all name_classes) for all taxids in lineage

        Args:
            taxonomy_session: Taxonomy database session
            taxonomy_id: The taxonomy ID to get lineage for

        Returns:
            (lineage_taxids, lineage_names) tuple
        """
        from ensembl.ncbi_taxonomy.api.utils import Taxonomy
        from ensembl.ncbi_taxonomy.models import NCBITaxonomy

        # Get all ancestors
        try:
            ancestors = Taxonomy.fetch_ancestors(taxonomy_session, taxonomy_id)
        except NoResultFound:
            # If no ancestors found, just use the current taxon
            ancestors = []

        # Build list of taxon_ids: current + all ancestors
        lineage_taxids = [taxonomy_id]
        for ancestor in ancestors:
            lineage_taxids.append(ancestor["taxon_id"])

        # Query for ALL names (all name_class values) for all taxon_ids in the lineage
        # exclude 'import date' entries.
        # TODO: Add more exclusions on here!
        all_names = (
            taxonomy_session.query(NCBITaxonomy.name)
            .filter(NCBITaxonomy.taxon_id.in_(lineage_taxids))
            .filter(NCBITaxonomy.name_class != "import date")
            .distinct()
            .all()
        )

        # Extract just the name strings from the query results
        lineage_names = [name[0] for name in all_names]

        return (lineage_taxids, lineage_names)

    def create_search_document(
            self, metadata_session: Session, taxonomy_session: Session, genome: Genome, release: EnsemblRelease
    ) -> GenomeSearchDocument:
        """
        Create search document for a genome/release pair.

        Note: Dataset fields always come from is_current=1 datasets,
        regardless of which release is being indexed.

        Args:
            metadata_session: Session for metadata database
            taxonomy_session: Session for taxonomy database
            genome: Genome object
            release: EnsemblRelease object (the selected release for indexing)

        Raises:
            MissingDatasetFieldError: If any required dataset fields are missing
        """

        # Extract direct fields
        doc_data = self._extract_direct_fields(genome)

        # Get release information
        first_release, latest_release, all_integrated = self.release_selector.get_release_info(genome)

        # Build comma-separated list of integrated release labels
        releases_str = ",".join([r.label for r in all_integrated]) if all_integrated else ""

        # Calculate is_latest_release_current
        is_current = self.release_selector.get_is_latest_release_current(genome, release, latest_release)

        doc_data.update(
            {
                "first_release_name": first_release.label if first_release else "",
                "first_release_type": first_release.release_type if first_release else "",
                "latest_release_name": latest_release.label if latest_release else "",
                "latest_release_type": latest_release.release_type if latest_release else "",
                "is_latest_release_current": is_current,
                "releases": releases_str,
            }
        )

        # Extract dataset fields - will raise exceptions if required fields are missing
        dataset_extractor = DatasetFieldExtractor(metadata_session, genome, release)
        doc_data.update(
            {
                "contig_n50": dataset_extractor.get_contig_n50(),
                "coding_genes": dataset_extractor.get_coding_genes(),
                "has_variation": dataset_extractor.has_variation(),
                "has_regulation": dataset_extractor.has_regulation(),
                "genebuild_provider": dataset_extractor.get_genebuild_provider(),
                "genebuild_method_display": dataset_extractor.get_genebuild_method_display(),
            }
        )

        # Add taxonomy lineage - use taxonomy session
        lineage_taxids, lineage_names = self._get_taxonomy_lineage(
            taxonomy_session, genome.organism.taxonomy_id
        )
        doc_data.update(
            {
                "lineage_taxids": lineage_taxids,
                "lineage_name": lineage_names,
            }
        )

        return GenomeSearchDocument(**doc_data)

    def export_to_json(self, output_path: str, raise_on_errors: bool = False, pretty_print: bool = True) -> int:
        """
        Generate search index and export to JSON file.
        Loads all documents in memory then writes to file.

        Best for smaller datasets or when you need the documents in memory anyway.

        Args:
            output_path: Path to output JSON file
            raise_on_errors: If True, raise exception if any errors occurred
            pretty_print: If True, format JSON with indentation (larger file)

        Returns:
            Number of successfully indexed documents

        Raises:
            MissingDatasetFieldError: If raise_on_errors=True and any errors occurred
        """
        logger.info(f"Generating search index and exporting to {output_path}")

        # Get all documents
        search_index = self.get_search_index(raise_on_errors=raise_on_errors)

        # Write to file
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, "w") as f:
            if pretty_print:
                json.dump(search_index.model_dump(), f, indent=2)
            else:
                json.dump(search_index.model_dump(), f)

        logger.info(f"Successfully exported {search_index.entry_count} documents to {output_path}")
        return search_index.entry_count

    def stream_to_json(self, output_path: str, pretty_print: bool = True) -> Tuple[int, IndexingErrorCollection]:
        """
        Generate search index and stream to JSON file.
        Writes documents as they're generated to minimize memory usage.

        Best for large datasets (20k+ genomes) to avoid loading everything in memory.

        Args:
            output_path: Path to output JSON file
            pretty_print: If True, format JSON with indentation (larger file)

        Returns:
            Tuple of (number of successful documents, error collection)
        """
        logger.info(f"Streaming search index to {output_path}")

        error_collection = IndexingErrorCollection()
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        total = 0

        with open(output_file, "w") as f:
            with self.metadata_db.session_scope() as metadata_session:
                with self.taxonomy_db.session_scope() as taxonomy_session:
                    # Get the newest partial release for top-level
                    newest_partial = self._get_newest_partial_release(metadata_session)
                    if not newest_partial:
                        raise ValueError("No partial releases found in database")

                    # Write opening with metadata
                    if pretty_print:
                        f.write('{\n  "name": "ensemblNext",\n')
                        f.write(f'  "release": "{newest_partial}",\n')
                        f.write('  "entry_count": 0,\n')  # Placeholder, will be updated
                        f.write('  "entries": [\n')
                    else:
                        f.write('{"name":"ensemblNext",')
                        f.write(f'"release":"{newest_partial}",')
                        f.write('"entry_count":0,')
                        f.write('"entries":[')

                    first = True

                    for batch in self.get_genomes_with_releases_batched(metadata_session):
                        for genome, release in batch:
                            try:
                                doc = self.create_search_document(
                                    metadata_session, taxonomy_session, genome, release
                                )
                                entry = doc.to_search_entry()

                                # Write comma before all but first document
                                if not first:
                                    f.write(",\n" if pretty_print else ",")
                                first = False

                                # Write entry
                                if pretty_print:
                                    # Add indentation for array items
                                    entry_json = json.dumps(entry.model_dump(), indent=2)
                                    # Indent each line by 4 spaces
                                    indented = "\n".join("    " + line for line in entry_json.split("\n"))
                                    f.write(indented)
                                else:
                                    json.dump(entry.model_dump(), f)

                                total += 1

                                # Log progress
                                if total % 100 == 0:
                                    logger.info(f"Streamed {total} documents...")

                            except MissingDatasetFieldError as e:
                                error_collection.add_error(
                                    genome_uuid=genome.genome_uuid,
                                    release_label=release.label,
                                    error_message=str(e),
                                    exception=e,
                                )
                            except Exception as e:
                                error_collection.add_error(
                                    genome_uuid=genome.genome_uuid,
                                    release_label=release.label,
                                    error_message=f"Unexpected error: {str(e)}",
                                    exception=e,
                                )

                    f.write("\n  ]\n}" if pretty_print else "]}")

        # Update entry_count in the file
        with open(output_file, "r") as f:
            content = f.read()
        content = content.replace('"entry_count": 0', f'"entry_count": {total}', 1)
        content = content.replace('"entry_count":0', f'"entry_count":{total}', 1)
        with open(output_file, "w") as f:
            f.write(content)

        logger.info(f"Successfully streamed {total} documents to {output_path}")

        if error_collection.has_errors():
            logger.warning(f"Failed to index {len(error_collection.errors)} genome(s)")
            print(error_collection.get_summary())

        return total, error_collection

    def export_to_json_auto(
            self,
            output_path: str,
            raise_on_errors: bool = False,
            pretty_print: bool = True,
            stream_threshold: int = 5000,
    ) -> int:
        """
        Automatically choose between regular export and streaming based on dataset size.

        Counts genomes first, then uses streaming if above threshold.

        Args:
            output_path: Path to output JSON file
            raise_on_errors: If True, raise exception if any errors occurred (non-streaming only)
            pretty_print: If True, format JSON with indentation
            stream_threshold: Use streaming if more than this many genomes

        Returns:
            Number of successfully indexed documents
        """
        # Count genomes first
        with self.metadata_db.session_scope() as session:
            genome_count = len(self._get_genome_ids_to_process(session))

        logger.info(f"Found {genome_count} genomes to process")

        if genome_count > stream_threshold:
            logger.info(f"Using streaming mode (>{stream_threshold} genomes)")
            total, error_collection = self.stream_to_json(output_path, pretty_print)

            if raise_on_errors and error_collection.has_errors():
                error_collection.raise_if_errors()

            return total
        else:
            logger.info(f"Using regular mode (<={stream_threshold} genomes)")
            return self.export_to_json(output_path, raise_on_errors, pretty_print)

    def get_search_index(self, raise_on_errors: bool = False) -> SearchIndex:
        """
        Main entry point to generate search index.

        Processes genomes in batches to manage memory usage.
        Collects all errors and reports them at the end.

        Args:
            raise_on_errors: If True, raise exception if any errors occurred.
                           If False, return successfully indexed documents and print errors.

        Returns:
            SearchIndex object with all successfully indexed documents

        Raises:
            MissingDatasetFieldError: If raise_on_errors=True and any errors occurred
        """
        error_collection = IndexingErrorCollection()
        search_entries = []
        total_processed = 0

        with self.metadata_db.session_scope() as metadata_session:
            with self.taxonomy_db.session_scope() as taxonomy_session:
                # Get the newest partial release for top-level
                newest_partial = self._get_newest_partial_release(metadata_session)
                if not newest_partial:
                    raise ValueError("No partial releases found in database")

                # Process genomes in batches
                for batch in self.get_genomes_with_releases_batched(metadata_session):
                    for genome, release in batch:
                        try:
                            doc = self.create_search_document(
                                metadata_session, taxonomy_session, genome, release
                            )
                            entry = doc.to_search_entry()
                            search_entries.append(entry)
                            total_processed += 1

                            # Log progress periodically
                            if total_processed % 100 == 0:
                                logger.info(f"Processed {total_processed} genomes...")

                        except MissingDatasetFieldError as e:
                            error_collection.add_error(
                                genome_uuid=genome.genome_uuid,
                                release_label=release.label,
                                error_message=str(e),
                                exception=e,
                            )
                        except Exception as e:
                            # Catch any other unexpected errors
                            error_collection.add_error(
                                genome_uuid=genome.genome_uuid,
                                release_label=release.label,
                                error_message=f"Unexpected error: {str(e)}",
                                exception=e,
                            )

                # Report results
                logger.info(f"Successfully indexed: {len(search_entries)} genome(s)")
                if error_collection.has_errors():
                    logger.warning(f"Failed to index: {len(error_collection.errors)} genome(s)")
                    print(error_collection.get_summary())

                    if raise_on_errors:
                        error_collection.raise_if_errors()

                return SearchIndex(
                    name="ensemblNext", release=newest_partial, entry_count=len(search_entries), entries=search_entries
                )


# ============================================================================
# USAGE
# ============================================================================


def main() -> None:
    """Main entry point for the script."""

    parser = argparse.ArgumentParser(description="Generate genome search index for Ensembl releases")
    parser.add_argument("--metadata-uri", required=True, help="Database URI for the metadata database")
    parser.add_argument("--taxonomy-uri", required=True, help="Database URI for the NCBI taxonomy database")
    parser.add_argument("--output-path", required=True, help="Output path for the search index JSON file")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Number of genomes to process per batch (default: 500). "
             "Smaller batches use less memory but more queries.",
    )
    parser.add_argument(
        "--stream-threshold",
        type=int,
        default=5000,
        help="Use streaming mode if genome count exceeds this threshold (default: 5000)",
    )
    parser.add_argument(
        "--no-pretty-print", action="store_true", help="Disable JSON pretty printing (creates smaller files)"
    )
    parser.add_argument(
        "--raise-on-errors",
        action="store_true",
        help="Raise exception and stop if any genomes fail indexing (default: continue and dump what succeeded)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: INFO)",
    )

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level), format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    try:
        indexer = GenomeSearchIndexer(
            metadata_uri=args.metadata_uri, taxonomy_uri=args.taxonomy_uri, batch_size=args.batch_size
        )

        count = indexer.export_to_json_auto(
            output_path=args.output_path,
            raise_on_errors=args.raise_on_errors,
            pretty_print=not args.no_pretty_print,
            stream_threshold=args.stream_threshold,
        )

        logger.info(f"Genome search index generated successfully: {count} documents")
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    except MissingDatasetFieldError as e:
        logger.error(f"Data quality error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error generating genome search index: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()