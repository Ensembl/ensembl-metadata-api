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

import logging
from dataclasses import dataclass
from typing import Optional, List, Tuple, Iterator

from ensembl.utils.database import DBConnection
from pydantic import BaseModel
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import Session, joinedload

from ensembl.production.metadata.api.models import (
    Genome, Dataset, DatasetAttribute,
    EnsemblRelease, GenomeRelease, GenomeDataset, ReleaseStatus, DatasetStatus
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
        self.errors.append(GenomeIndexError(
            genome_uuid=genome_uuid,
            release_label=release_label,
            error_message=error_message,
            exception=exception
        ))

    def has_errors(self) -> bool:
        """Check if any errors were collected"""
        return len(self.errors) > 0

    def get_summary(self) -> str:
        """Get a formatted summary of all errors"""
        if not self.errors:
            return "No errors occurred during indexing."

        summary = [f"\n{'=' * 80}"]
        summary.append(f"INDEXING ERRORS: {len(self.errors)} genome(s) failed to index")
        summary.append('=' * 80)

        for i, error in enumerate(self.errors, 1):
            summary.append(f"\n{i}. Genome: {error.genome_uuid} (Release: {error.release_label})")
            summary.append(f"   Error: {error.error_message}")

        summary.append('=' * 80 + '\n')
        return '\n'.join(summary)

    def raise_if_errors(self):
        """Raise an exception if there are any errors"""
        if self.has_errors():
            raise MissingDatasetFieldError(self.get_summary())


# ============================================================================
# PYDANTIC SCHEMAS
# ============================================================================

class GenomeSearchDocument(BaseModel):
    """Schema for genome search indexing"""

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
    organism_id: int
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
    release_type: str
    release_label: str
    release_id: int

    class Config:
        from_attributes = True


# ============================================================================
# DATASET FIELD EXTRACTOR
# ============================================================================

class DatasetFieldExtractor:
    """Extracts dataset-related fields for a genome/release pair"""

    def __init__(self, session: Session, genome: Genome, release: EnsemblRelease):
        self.session = session
        self.genome = genome
        self.release = release
        self._datasets_cache = None

    def _get_relevant_datasets(self) -> List[GenomeDataset]:
        """
        Get datasets relevant for this genome/release combination.

        Logic:
        - If integrated release: datasets with release_id matching the release
        - If partial release: datasets with is_current == 1
        - ALWAYS filter to only Released datasets
        """
        if self._datasets_cache is not None:
            return self._datasets_cache

        if self.release.release_type == 'integrated':
            self._datasets_cache = [
                gd for gd in self.genome.genome_datasets
                if gd.release_id == self.release.release_id
                   and gd.dataset.status == DatasetStatus.RELEASED
            ]
        else:  # partial
            self._datasets_cache = [
                gd for gd in self.genome.genome_datasets
                if gd.is_current == 1
                   and gd.dataset.status == DatasetStatus.RELEASED
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

            # Find the attribute in this dataset
            for dataset_attr in dataset.dataset_attributes:
                if dataset_attr.attribute.name == attribute_name:
                    return dataset_attr.value

        return None

    def _has_dataset_type(self, dataset_type_name: str) -> bool:
        """Check if a dataset of the given type exists"""
        relevant_datasets = self._get_relevant_datasets()

        return any(
            gd.dataset.dataset_type.name == dataset_type_name
            for gd in relevant_datasets
        )

    def get_contig_n50(self) -> int:
        """
        Get contig N50 from assembly dataset.

        Raises:
            MissingDatasetFieldError: If assembly.stats.contig_n50 is not found
        """
        value = self._get_dataset_attribute('assembly', 'assembly.stats.contig_n50')

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
        value = self._get_dataset_attribute('genebuild', 'genebuild.stats.coding_genes')

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
        return self._has_dataset_type('variation')

    def has_regulation(self) -> bool:
        """Check if genome has regulatory features data"""
        return self._has_dataset_type('regulatory_features')

    def get_genebuild_provider(self) -> str:
        """
        Get genebuild provider name.
        First tries genebuild.provider_name_display attribute,
        falls back to genome.provider_name

        Raises:
            MissingDatasetFieldError: If neither source has a provider name
        """
        provider = self._get_dataset_attribute('genebuild', 'genebuild.provider_name_display')

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
        value = self._get_dataset_attribute('genebuild', 'genebuild.method_display')

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
    """Handles the complex logic of selecting the appropriate release for a genome"""

    @staticmethod
    def select_release_for_genome(genome: Genome) -> Optional[EnsemblRelease]:
        """
        Select the single appropriate release for a genome.

        Rules for NON-SUPPRESSED genomes:
        1. If attached to integrated release(s):
           - Take the most recent Released integrated (by label: YYYY-MM)
           - BUT if any dataset is attached to a newer partial (Released, is_current),
             use that partial instead
        2. If no integrated releases but has a Released partial, use the partial
        3. Otherwise return None

        Rules for SUPPRESSED genomes:
        1. Never return a partial-only genome (return None)
        2. If part of integrated release(s), return the most recent integrated (by label)
        3. Even if there's newer data in a partial, stick with the integrated
        """
        # Get all released releases for this genome through genome_releases
        released_releases = [
            gr.ensembl_release
            for gr in genome.genome_releases
            if gr.ensembl_release and gr.ensembl_release.status == ReleaseStatus.RELEASED
        ]

        if not released_releases:
            return None

        # Separate into integrated and partial
        integrated_releases = [
            r for r in released_releases
            if r.release_type == 'integrated'
        ]
        partial_releases = [
            r for r in released_releases
            if r.release_type == 'partial' and r.is_current
        ]

        # SUPPRESSED GENOME LOGIC
        if genome.suppressed:
            # Rule 1: If no integrated releases, don't display (return None)
            if not integrated_releases:
                return None

            # Rule 2 & 3: Return most recent integrated by label, ignore any partial
            return max(integrated_releases, key=lambda r: r.label)

        # NON-SUPPRESSED GENOME LOGIC (original logic)
        # Case 1: No integrated releases - return partial if exists
        if not integrated_releases:
            return partial_releases[0] if partial_releases else None

        # Case 2: Has integrated releases
        # Get the most recent integrated by label (YYYY-MM format sorts correctly)
        most_recent_integrated = max(integrated_releases, key=lambda r: r.label)

        # Check if we should use a partial instead
        if partial_releases:
            partial_release = partial_releases[0]  # Should only be one current partial

            # Check if any RELEASED dataset is attached to this partial release
            partial_datasets = [
                gd for gd in genome.genome_datasets
                if gd.release_id == partial_release.release_id
                   and gd.dataset.status == DatasetStatus.RELEASED
            ]

            # If there are datasets attached to a partial that's newer than the integrated
            # Compare labels: partial label should be "greater" than integrated label
            if partial_datasets and partial_release.label > most_recent_integrated.label:
                return partial_release

        return most_recent_integrated


# ============================================================================
# MAIN SERVICE CLASS WITH BATCHING
# ============================================================================

class GenomeSearchIndexer:
    """Service for generating genome search documents"""

    def __init__(self, metadata_uri: str, batch_size: int = 500):
        self.metadata_db = DBConnection(metadata_uri)
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
            .filter(
                EnsemblRelease.status == ReleaseStatus.RELEASED
            )
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
                joinedload(Genome.genome_datasets).joinedload(GenomeDataset.ensembl_release)
            )
            .all()
        )

    def get_genomes_with_releases_batched(
            self,
            session: Session
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
            batch_ids = genome_ids[i:i + self.batch_size]
            logger.info(
                f"Processing batch {i // self.batch_size + 1}/{(total_genomes + self.batch_size - 1) // self.batch_size}: "
                f"genomes {i + 1}-{min(i + self.batch_size, total_genomes)}")

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

    def _extract_direct_fields(self, genome: Genome) -> dict:
        """Extract direct fields from Genome/Organism/Assembly"""
        return {
            'genome_uuid': genome.genome_uuid,
            'common_name': genome.organism.common_name,
            'scientific_name': genome.organism.scientific_name,
            'strain_type': genome.organism.strain_type,
            'strain': genome.organism.strain,
            'assembly_name': genome.assembly.name,
            'accession': genome.assembly.accession,
            'url_name': genome.url_name,
            'tol_id': genome.organism.tol_id,
            'is_reference': bool(genome.assembly.is_reference),
            'species_taxonomy_id': genome.organism.species_taxonomy_id,
            'taxonomy_id': genome.organism.taxonomy_id,
            'scientific_parlance_name': genome.organism.scientific_parlance_name,
            'organism_id': genome.organism_id,
            'rank': genome.organism.rank or 0,
        }

    def _get_taxonomy_lineage(self, session: Session, taxonomy_id: int) -> Tuple[List[int], List[str]]:
        """
        Get taxonomy lineage for a given taxonomy_id.

        Returns:
            - lineage_taxids: List of all taxon_ids from current to root
            - lineage_names: List of ALL names (all name_classes) for all taxids in lineage

        Args:
            session: Database session
            taxonomy_id: The taxonomy ID to get lineage for

        Returns:
            (lineage_taxids, lineage_names) tuple
        """
        from ensembl.ncbi_taxonomy.api.utils import Taxonomy
        from ensembl.ncbi_taxonomy.models import NCBITaxonomy

        # Get all ancestors
        try:
            ancestors = Taxonomy.fetch_ancestors(session, taxonomy_id)
        except NoResultFound:
            # If no ancestors found, just use the current taxon
            ancestors = []

        # Build list of taxon_ids: current + all ancestors
        lineage_taxids = [taxonomy_id]
        for ancestor in ancestors:
            lineage_taxids.append(ancestor['taxon_id'])

        # Query for ALL names (all name_class values) for all taxon_ids in the lineage
        all_names = (
            session.query(NCBITaxonomy.name)
            .filter(NCBITaxonomy.taxon_id.in_(lineage_taxids))
            .distinct()
            .all()
        )

        # Extract just the name strings from the query results
        lineage_names = [name[0] for name in all_names]

        return (lineage_taxids, lineage_names)

    def create_search_document(
            self,
            session: Session,
            genome: Genome,
            release: EnsemblRelease
    ) -> GenomeSearchDocument:
        """
        Create search document for a genome/release pair.

        Raises:
            MissingDatasetFieldError: If any required dataset fields are missing
        """

        # Extract direct fields
        doc_data = self._extract_direct_fields(genome)

        # Add release information
        doc_data.update({
            'release_type': release.release_type,
            'release_label': release.label,
            'release_id': release.release_id,
        })

        # Extract dataset fields - will raise exceptions if required fields are missing
        dataset_extractor = DatasetFieldExtractor(session, genome, release)
        doc_data.update({
            'contig_n50': dataset_extractor.get_contig_n50(),
            'coding_genes': dataset_extractor.get_coding_genes(),
            'has_variation': dataset_extractor.has_variation(),
            'has_regulation': dataset_extractor.has_regulation(),
            'genebuild_provider': dataset_extractor.get_genebuild_provider(),
            'genebuild_method_display': dataset_extractor.get_genebuild_method_display(),
        })

        # Add taxonomy lineage
        lineage_taxids, lineage_names = self._get_taxonomy_lineage(genome.organism.taxonomy_id)
        doc_data.update({
            'lineage_taxids': lineage_taxids,
            'lineage_name': lineage_names,
        })

        return GenomeSearchDocument(**doc_data)

    def get_search_index(self, raise_on_errors: bool = True) -> List[dict]:
        """
        Main entry point to generate search index.

        Processes genomes in batches to manage memory usage.
        Collects all errors and reports them at the end.

        Args:
            raise_on_errors: If True, raise exception if any errors occurred.
                           If False, return successfully indexed documents and print errors.

        Returns:
            List of successfully indexed genome documents as dicts

        Raises:
            MissingDatasetFieldError: If raise_on_errors=True and any errors occurred
        """
        error_collection = IndexingErrorCollection()
        search_documents = []
        total_processed = 0

        with self.metadata_db.session_scope() as session:
            # Process genomes in batches
            for batch in self.get_genomes_with_releases_batched(session):
                for genome, release in batch:
                    try:
                        doc = self.create_search_document(session, genome, release)
                        search_documents.append(doc.model_dump())
                        total_processed += 1

                        # Log progress periodically
                        if total_processed % 100 == 0:
                            logger.info(f"Processed {total_processed} genomes...")

                    except MissingDatasetFieldError as e:
                        error_collection.add_error(
                            genome_uuid=genome.genome_uuid,
                            release_label=release.label,
                            error_message=str(e),
                            exception=e
                        )
                    except Exception as e:
                        # Catch any other unexpected errors
                        error_collection.add_error(
                            genome_uuid=genome.genome_uuid,
                            release_label=release.label,
                            error_message=f"Unexpected error: {str(e)}",
                            exception=e
                        )

            # Report results
            logger.info(f"Successfully indexed: {len(search_documents)} genome(s)")
            if error_collection.has_errors():
                logger.warning(f"Failed to index: {len(error_collection.errors)} genome(s)")
                print(error_collection.get_summary())

                # if raise_on_errors:
                #     error_collection.raise_if_errors()

            return search_documents


# ============================================================================
# USAGE
# ============================================================================

def main():
    """Example usage"""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    metadata_uri = "mysql://user:pass@host/metadata_db"

    # You can adjust batch_size based on your memory constraints
    # Smaller batches = less memory, but more queries
    # Larger batches = more memory, but fewer queries
    indexer = GenomeSearchIndexer(metadata_uri, batch_size=500)

    # Option 1: Raise on errors (default)
    try:
        documents = indexer.get_search_index(raise_on_errors=True)
        logger.info(f"Generated {len(documents)} search documents")
        return documents
    except MissingDatasetFieldError as e:
        logger.error("Indexing failed due to data quality issues")
        return []
