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

"""
Generate changelogs for Ensembl releases.

This module provides functionality to generate release changelogs for both
partial and integrated Ensembl releases, exporting the data to CSV format.
"""
import csv
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set

from ensembl.utils.database import DBConnection
from sqlalchemy import select, func

from ensembl.production.metadata.api.models import (
    EnsemblRelease, Genome, GenomeDataset, GenomeRelease, Dataset, DatasetType,
    DatasetAttribute, Attribute, Organism, Assembly
)

# Constants
DATASET_TYPE_GENEBUILD = 'genebuild'
DATASET_TYPE_VARIATION = 'variation'
DATASET_TYPE_REGULATORY = 'regulatory_features'
DATASET_TYPES_OF_INTEREST = [DATASET_TYPE_GENEBUILD, DATASET_TYPE_VARIATION, DATASET_TYPE_REGULATORY]

RELEASE_TYPE_PARTIAL = 'partial'
RELEASE_TYPE_INTEGRATED = 'integrated'

ANNOTATION_SOURCE_ATTRIBUTE = 'genebuild.annotation_source'

STATUS_NEW = 'New'
STATUS_REMOVED = 'Removed'
STATUS_UPDATED = 'Updated'
STATUS_UNCHANGED = 'Unchanged'

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ChangelogGenerator:
    """
    Generate release changelogs for Ensembl releases.

    This class builds data structures capable of exporting both partial and
    integrated release information to CSV format. It queries the metadata
    database to gather genome, dataset, and release information.

    Attributes:
        metadata_db: Database connection to the Ensembl metadata database
        release_label: Label identifying the release (e.g., '2024-02' or '2025-05-25')
        output_path: Optional path for the output CSV file
    """

    def __init__(
            self,
            metadata_uri: str,
            release_label: str,
            output_path: Optional[str] = None
    ):
        """
        Initialize the changelog generator.

        Args:
            metadata_uri: Database URI for the metadata database
            release_label: Release label (e.g., '2024-02' or '2025-05-25')
            output_path: Optional output path for the changelog CSV file

        Raises:
            ValueError: If metadata_uri or release_label is invalid
        """
        if not metadata_uri or not isinstance(metadata_uri, str):
            raise ValueError("metadata_uri must be a non-empty string")

        if not release_label or not isinstance(release_label, str):
            raise ValueError("release_label must be a non-empty string")

        try:
            self.metadata_db = DBConnection(metadata_uri)
        except Exception as e:
            raise ValueError(f"Failed to connect to database: {e}") from e

        self.release_label = release_label
        self.output_path = output_path

    def generate(self) -> None:
        """
        Generate the changelog and export to CSV.

        Determines the release type (partial or integrated) and calls the
        appropriate data gathering method, then exports the results to CSV.

        Raises:
            ValueError: If release type is unknown
            Exception: If data generation or export fails
        """
        logger.info(f"Starting changelog generation for release: {self.release_label}")

        release_type = self.verify_release()
        logger.info(f"Verified release type: {release_type}")

        if release_type == RELEASE_TYPE_PARTIAL:
            data = self.gather_partial_data()
        elif release_type == RELEASE_TYPE_INTEGRATED:
            logger.warning(
                "Integrated release changelog generation has not been "
                "extensively tested across multiple releases"
            )
            data = self.gather_integrated_data()
        else:
            raise ValueError(f"Unknown release type: {release_type}")

        if not data:
            logger.warning(f"No changelog data generated for release {self.release_label}")
        else:
            logger.info(f"Generated {len(data)} changelog entries")

        self.export_to_csv(data)
        logger.info("Changelog generation completed successfully")

    def verify_release(self) -> str:
        """
        Verify that the release exists and return its type.

        Returns:
            Release type ('partial' or 'integrated')

        Raises:
            ValueError: If the release is not found in the database
        """
        release_query = select(EnsemblRelease.release_type).where(
            EnsemblRelease.label == self.release_label
        )

        with self.metadata_db.session_scope() as session:
            result = session.execute(release_query).one_or_none()
            if result is None:
                raise ValueError(
                    f"Release not found: {self.release_label}. "
                    "Please use a valid Release Label (e.g., '2024-02' or '2025-05-25')"
                )
            return result[0]

    def gather_partial_data(self) -> List[Dict]:
        """
        Gather changelog data for a partial release using optimized bulk queries.

        For partial releases, this method collects information about which genomes
        have updated datasets (genebuild, variation, or regulatory features) in
        the specified release.

        Returns:
            List of dictionaries containing changelog data with keys:
                - scientific_name: Species scientific name
                - common_name: Species common name
                - assembly_name: Assembly name
                - assembly_accession: Assembly accession
                - annotation_provider: Source of annotation
                - geneset_updated: 1 if genebuild updated, 0 otherwise
                - variation_updated: 1 if variation updated, 0 otherwise
                - regulation_updated: 1 if regulation updated, 0 otherwise
        """
        with self.metadata_db.session_scope() as session:
            # Get release ID
            release_query = select(EnsemblRelease.release_id).where(
                EnsemblRelease.label == self.release_label
            )
            release_id = session.execute(release_query).scalar_one()

            # Find all genomes with relevant datasets in this release
            genome_query = select(Genome).join(
                GenomeDataset, GenomeDataset.genome_id == Genome.genome_id
            ).join(
                Dataset, Dataset.dataset_id == GenomeDataset.dataset_id
            ).join(
                DatasetType, DatasetType.dataset_type_id == Dataset.dataset_type_id
            ).where(
                GenomeDataset.release_id == release_id,
                DatasetType.name.in_(DATASET_TYPES_OF_INTEREST)
            ).distinct()

            genomes = session.execute(genome_query).scalars().all()

            if not genomes:
                logger.warning(f"No genomes found for release {self.release_label}")
                return []

            genome_ids = [g.genome_id for g in genomes]

            # Bulk fetch dataset information
            datasets_by_genome = self._get_datasets_for_partial_bulk(
                session, genome_ids, release_id
            )

            # Bulk fetch annotation sources
            annotation_sources = self._get_annotation_sources_bulk(
                session, genome_ids
            )

            # Build results
            results = []
            for genome in genomes:
                organism = genome.organism
                assembly = genome.assembly

                if not organism or not assembly:
                    logger.warning(f"Skipping genome {genome.genome_id}: missing organism or assembly")
                    continue

                datasets = datasets_by_genome.get(genome.genome_id, set())
                annotation_source = annotation_sources.get(genome.genome_id)

                result = {
                    'scientific_name': organism.scientific_name,
                    'common_name': organism.common_name,
                    'assembly_name': assembly.name,
                    'assembly_accession': assembly.accession,
                    'annotation_provider': annotation_source,
                    'geneset_updated': 1 if DATASET_TYPE_GENEBUILD in datasets else 0,
                    'variation_updated': 1 if DATASET_TYPE_VARIATION in datasets else 0,
                    'regulation_updated': 1 if DATASET_TYPE_REGULATORY in datasets else 0
                }
                results.append(result)

            return results

    def gather_integrated_data(self) -> List[Dict]:
        """
        Gather changelog data for an integrated release.

        For integrated releases, this method compares the current release to the
        previous integrated release to determine which genomes are new, removed,
        updated, or unchanged. It includes the partial release labels for when
        each dataset was last updated.

        Returns:
            List of dictionaries containing changelog data with keys:
                - scientific_name: Species scientific name
                - common_name: Species common name
                - assembly_name: Assembly name
                - assembly_accession: Assembly accession
                - annotation_provider: Source of annotation
                - geneset_updated: Partial release label when geneset was updated
                - variation_updated: Partial release label when variation was updated
                - regulation_updated: Partial release label when regulation was updated
                - status: One of 'New', 'Removed', 'Updated', or 'Unchanged'
        """
        with self.metadata_db.session_scope() as session:
            # Get current integrated release information
            current_release_query = select(
                EnsemblRelease.release_id,
                EnsemblRelease.version
            ).where(
                EnsemblRelease.label == self.release_label
            )
            current_release = session.execute(current_release_query).one()
            current_release_id = current_release[0]

            # Get previous integrated release for comparison
            prev_release_id = self._get_previous_integrated_release_id(
                session, current_release_id
            )

            # Get current genomes
            current_genomes = self._get_genomes_for_release(session, current_release_id)
            current_genome_ids = [g.genome_id for g in current_genomes]

            # Bulk fetch data for current genomes
            current_datasets = self._get_datasets_for_integrated_bulk(
                session, current_genome_ids, current_release_id
            )
            annotation_sources = self._get_annotation_sources_bulk(
                session, current_genome_ids
            )

            # Get previous genomes and their data
            prev_genome_ids = []
            prev_datasets = {}
            if prev_release_id:
                prev_genome_ids = self._get_genome_ids_for_release(session, prev_release_id)
                prev_datasets = self._get_datasets_for_integrated_bulk(
                    session, prev_genome_ids, prev_release_id
                )

            prev_genome_ids_set = set(prev_genome_ids)
            current_genome_ids_set = set(current_genome_ids)

            results = []

            # Process current genomes
            for genome in current_genomes:
                if not genome.organism or not genome.assembly:
                    logger.warning(f"Skipping genome {genome.genome_id}: missing organism or assembly")
                    continue

                status = self._determine_genome_status(
                    genome.genome_id,
                    current_datasets.get(genome.genome_id, {}),
                    prev_datasets.get(genome.genome_id, {}),
                    prev_genome_ids_set
                )

                result = self._build_changelog_entry(
                    genome.organism,
                    genome.assembly,
                    annotation_sources.get(genome.genome_id),
                    current_datasets.get(genome.genome_id, {}),
                    status
                )
                results.append(result)

            # Process removed genomes
            if prev_release_id:
                removed_genome_ids = prev_genome_ids_set - current_genome_ids_set
                if removed_genome_ids:
                    removed_results = self._process_removed_genomes(
                        session,
                        list(removed_genome_ids),
                        prev_datasets,
                        annotation_sources
                    )
                    results.extend(removed_results)

            return results

    def _get_previous_integrated_release_id(self, session, current_release_id: int) -> Optional[int]:
        """
        Get the ID of the previous integrated release.

        Args:
            session: Database session
            current_release_id: Current release ID

        Returns:
            Previous release ID or None if no previous release exists
        """
        prev_release_query = select(EnsemblRelease.release_id).where(
            EnsemblRelease.release_type == RELEASE_TYPE_INTEGRATED,
            EnsemblRelease.release_id < current_release_id
        ).order_by(EnsemblRelease.release_id.desc()).limit(1)

        return session.execute(prev_release_query).scalar_one_or_none()

    def _get_genomes_for_release(self, session, release_id: int) -> List[Genome]:
        """
        Get all genomes for a specific release.

        Args:
            session: Database session
            release_id: Release ID

        Returns:
            List of Genome objects
        """
        genomes_query = select(Genome).join(
            GenomeRelease, GenomeRelease.genome_id == Genome.genome_id
        ).where(
            GenomeRelease.release_id == release_id
        ).distinct()

        return session.execute(genomes_query).scalars().all()

    def _get_genome_ids_for_release(self, session, release_id: int) -> List[int]:
        """
        Get all genome IDs for a specific release.

        Args:
            session: Database session
            release_id: Release ID

        Returns:
            List of genome IDs
        """
        genome_ids_query = select(Genome.genome_id).join(
            GenomeRelease, GenomeRelease.genome_id == Genome.genome_id
        ).where(
            GenomeRelease.release_id == release_id
        )

        return session.execute(genome_ids_query).scalars().all()

    def _determine_genome_status(
            self,
            genome_id: int,
            current_datasets: Dict[str, Optional[str]],
            prev_datasets: Dict[str, Optional[str]],
            prev_genome_ids: Set[int]
    ) -> str:
        """
        Determine the status of a genome (New, Updated, or Unchanged).

        Args:
            genome_id: Genome ID
            current_datasets: Current dataset information
            prev_datasets: Previous dataset information
            prev_genome_ids: Set of previous genome IDs

        Returns:
            Status string
        """
        if genome_id not in prev_genome_ids:
            return STATUS_NEW

        # Check if any dataset has been updated
        dataset_keys = ['geneset_updated', 'variation_updated', 'regulation_updated']
        for key in dataset_keys:
            if current_datasets.get(key) != prev_datasets.get(key):
                return STATUS_UPDATED

        return STATUS_UNCHANGED

    def _build_changelog_entry(
            self,
            organism: Organism,
            assembly: Assembly,
            annotation_source: Optional[str],
            datasets: Dict[str, Optional[str]],
            status: str
    ) -> Dict:
        """
        Build a changelog entry dictionary.

        Args:
            organism: Organism object
            assembly: Assembly object
            annotation_source: Annotation source string
            datasets: Dataset information dictionary
            status: Status string

        Returns:
            Changelog entry dictionary
        """
        return {
            'scientific_name': organism.scientific_name,
            'common_name': organism.common_name,
            'assembly_name': assembly.name,
            'assembly_accession': assembly.accession,
            'annotation_provider': annotation_source,
            'geneset_updated': datasets.get('geneset_updated'),
            'variation_updated': datasets.get('variation_updated'),
            'regulation_updated': datasets.get('regulation_updated'),
            'status': status
        }

    def _process_removed_genomes(
            self,
            session,
            removed_genome_ids: List[int],
            prev_datasets: Dict[int, Dict[str, Optional[str]]],
            annotation_sources: Dict[int, Optional[str]]
    ) -> List[Dict]:
        """
        Process removed genomes and create changelog entries.

        Args:
            session: Database session
            removed_genome_ids: List of removed genome IDs
            prev_datasets: Previous dataset information
            annotation_sources: Annotation sources mapping

        Returns:
            List of changelog entries for removed genomes
        """
        # Bulk fetch removed genomes
        removed_genomes_query = select(Genome).where(
            Genome.genome_id.in_(removed_genome_ids)
        )
        removed_genomes = session.execute(removed_genomes_query).scalars().all()

        # Update annotation sources for removed genomes if not already present
        missing_annotation_genome_ids = [
            gid for gid in removed_genome_ids if gid not in annotation_sources
        ]
        if missing_annotation_genome_ids:
            additional_sources = self._get_annotation_sources_bulk(
                session, missing_annotation_genome_ids
            )
            annotation_sources.update(additional_sources)

        results = []
        for genome in removed_genomes:
            if not genome.organism or not genome.assembly:
                logger.warning(f"Skipping removed genome {genome.genome_id}: missing organism or assembly")
                continue

            result = self._build_changelog_entry(
                genome.organism,
                genome.assembly,
                annotation_sources.get(genome.genome_id),
                prev_datasets.get(genome.genome_id, {}),
                STATUS_REMOVED
            )
            results.append(result)

        return results

    def _get_datasets_for_partial_bulk(
            self, session, genome_ids: List[int], release_id: int
    ) -> Dict[int, Set[str]]:
        """
        Get dataset types for multiple genomes in a partial release (bulk query).

        Args:
            session: Database session
            genome_ids: List of genome IDs
            release_id: Release ID

        Returns:
            Mapping of genome_id to set of dataset type names
        """
        bulk_query = select(
            GenomeDataset.genome_id,
            DatasetType.name
        ).join(
            Dataset, Dataset.dataset_id == GenomeDataset.dataset_id
        ).join(
            DatasetType, DatasetType.dataset_type_id == Dataset.dataset_type_id
        ).where(
            GenomeDataset.genome_id.in_(genome_ids),
            GenomeDataset.release_id == release_id,
            DatasetType.name.in_(DATASET_TYPES_OF_INTEREST)
        )

        results = session.execute(bulk_query).all()

        # Build mapping: genome_id -> set of dataset types
        datasets_by_genome = {}
        for genome_id, dataset_type in results:
            if genome_id not in datasets_by_genome:
                datasets_by_genome[genome_id] = set()
            datasets_by_genome[genome_id].add(dataset_type)

        return datasets_by_genome

    def _get_datasets_for_integrated_bulk(
            self, session, genome_ids: List[int], release_id: int
    ) -> Dict[int, Dict[str, Optional[str]]]:
        """
        Get dataset information for multiple genomes in bulk queries.

        This method retrieves partial release labels for when each dataset type
        (genebuild, variation, regulatory_features) was released for each genome.
        Uses optimized bulk queries to avoid N+1 query problems.

        Args:
            session: Database session
            genome_ids: List of genome IDs to query
            release_id: Release ID to check datasets against

        Returns:
            Mapping of genome_id to dataset information:
                {genome_id: {
                    'geneset_updated': partial_release_label or None,
                    'variation_updated': partial_release_label or None,
                    'regulation_updated': partial_release_label or None
                }}
        """
        result = {gid: {
            'geneset_updated': None,
            'variation_updated': None,
            'regulation_updated': None
        } for gid in genome_ids}

        if not genome_ids:
            return result

        # Bulk query: Get all datasets for all genomes in one query
        bulk_query = select(
            GenomeDataset.genome_id,
            DatasetType.name,
            Dataset.dataset_id
        ).join(
            Dataset, Dataset.dataset_id == GenomeDataset.dataset_id
        ).join(
            DatasetType, DatasetType.dataset_type_id == Dataset.dataset_type_id
        ).where(
            GenomeDataset.genome_id.in_(genome_ids),
            GenomeDataset.release_id == release_id,
            DatasetType.name.in_(DATASET_TYPES_OF_INTEREST)
        )

        dataset_results = session.execute(bulk_query).all()
        dataset_info = {}  # {(genome_id, dataset_type): dataset_id}
        dataset_ids_to_check = set()

        for genome_id, dataset_type, dataset_id in dataset_results:
            dataset_info[(genome_id, dataset_type)] = dataset_id
            dataset_ids_to_check.add(dataset_id)

        if not dataset_ids_to_check:
            return result

        # Get partial release labels for all datasets
        partial_release_query = select(
            GenomeDataset.genome_id,
            GenomeDataset.dataset_id,
            EnsemblRelease.label
        ).join(
            EnsemblRelease, EnsemblRelease.release_id == GenomeDataset.release_id
        ).where(
            GenomeDataset.dataset_id.in_(dataset_ids_to_check),
            GenomeDataset.genome_id.in_(genome_ids),
            EnsemblRelease.release_type == RELEASE_TYPE_PARTIAL
        )

        partial_results = session.execute(partial_release_query).all()

        # Build lookup: {(genome_id, dataset_id): label}
        partial_labels = {}
        for genome_id, dataset_id, label in partial_results:
            key = (genome_id, dataset_id)
            if key not in partial_labels:
                partial_labels[key] = label

        # Populate final results
        for (genome_id, dataset_type), dataset_id in dataset_info.items():
            label = partial_labels.get((genome_id, dataset_id))

            if dataset_type == DATASET_TYPE_GENEBUILD:
                result[genome_id]['geneset_updated'] = label
            elif dataset_type == DATASET_TYPE_VARIATION:
                result[genome_id]['variation_updated'] = label
            elif dataset_type == DATASET_TYPE_REGULATORY:
                result[genome_id]['regulation_updated'] = label

        return result

    def _get_annotation_sources_bulk(
            self, session, genome_ids: List[int]
    ) -> Dict[int, Optional[str]]:
        """
        Get annotation sources for multiple genomes in bulk queries.

        Retrieves the annotation source from the most recent genebuild dataset
        for each genome. Uses optimized bulk queries to minimize database calls.

        Args:
            session: Database session
            genome_ids: List of genome IDs to query

        Returns:
            Mapping of genome_id to annotation_source
        """
        if not genome_ids:
            return {}

        # Subquery to get the most recent genebuild dataset per genome
        subq = select(
            GenomeDataset.genome_id,
            func.max(Dataset.created).label('max_created')
        ).join(
            Dataset, Dataset.dataset_id == GenomeDataset.dataset_id
        ).join(
            DatasetType, DatasetType.dataset_type_id == Dataset.dataset_type_id
        ).where(
            GenomeDataset.genome_id.in_(genome_ids),
            DatasetType.name == DATASET_TYPE_GENEBUILD
        ).group_by(GenomeDataset.genome_id).subquery()

        # Get the dataset_ids for the most recent genebuilds
        dataset_query = select(
            GenomeDataset.genome_id,
            Dataset.dataset_id
        ).join(
            Dataset, Dataset.dataset_id == GenomeDataset.dataset_id
        ).join(
            DatasetType, DatasetType.dataset_type_id == Dataset.dataset_type_id
        ).join(
            subq,
            (GenomeDataset.genome_id == subq.c.genome_id) &
            (Dataset.created == subq.c.max_created)
        ).where(
            DatasetType.name == DATASET_TYPE_GENEBUILD
        )

        dataset_results = session.execute(dataset_query).all()
        genome_to_dataset = {genome_id: dataset_id for genome_id, dataset_id in dataset_results}

        if not genome_to_dataset:
            return {}

        # Get annotation sources for all datasets
        attr_query = select(
            DatasetAttribute.dataset_id,
            DatasetAttribute.value
        ).join(
            Attribute, Attribute.attribute_id == DatasetAttribute.attribute_id
        ).where(
            DatasetAttribute.dataset_id.in_(genome_to_dataset.values()),
            Attribute.name == ANNOTATION_SOURCE_ATTRIBUTE
        )

        attr_results = session.execute(attr_query).all()
        dataset_to_source = {dataset_id: value for dataset_id, value in attr_results}

        # Map annotation sources back to genome_ids
        result = {}
        for genome_id, dataset_id in genome_to_dataset.items():
            result[genome_id] = dataset_to_source.get(dataset_id)

        return result

    def export_to_csv(self, data: List[Dict]) -> None:
        """
        Export changelog data to CSV file with a commented header.

        Creates a CSV file with a comment line indicating the release, followed
        by the changelog data. The field names vary depending on whether this is
        a partial or integrated release (integrated includes a 'status' column).

        Args:
            data: List of dictionaries containing changelog data

        Raises:
            IOError: If file cannot be written
        """
        # Determine output path
        if self.output_path:
            output_file = Path(self.output_path)
        else:
            output_file = Path(f"{self.release_label}.csv")

        # Ensure parent directory exists
        try:
            output_file.parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            raise IOError(f"Cannot create output directory {output_file.parent}: {e}") from e

        # Define field order based on release type
        if data and 'status' in data[0]:
            # Integrated release includes status column
            fieldnames = [
                'scientific_name',
                'common_name',
                'assembly_name',
                'assembly_accession',
                'annotation_provider',
                'geneset_updated',
                'variation_updated',
                'regulation_updated',
                'status'
            ]
        else:
            # Partial release uses binary flags
            fieldnames = [
                'scientific_name',
                'common_name',
                'assembly_name',
                'assembly_accession',
                'annotation_provider',
                'geneset_updated',
                'variation_updated',
                'regulation_updated'
            ]

        # Write CSV file
        try:
            with open(output_file, 'w', newline='') as csvfile:
                # Write commented header line
                csvfile.write(f"# Changelog for release {self.release_label}\n")

                # Write CSV data
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(data)

            logger.info(f"Changelog exported to: {output_file.absolute()}")
        except Exception as e:
            raise IOError(f"Failed to write CSV file {output_file}: {e}") from e


def main() -> None:
    """Main entry point for the script."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate release changelog for Ensembl releases"
    )
    parser.add_argument(
        "--metadata-uri",
        required=True,
        help="Database URI for the metadata database"
    )
    parser.add_argument(
        "--release-label",
        required=True,
        help="Release label (e.g., 2024-02 or 2025-05-25)"
    )
    parser.add_argument(
        "--output-path",
        default=None,
        help="Optional output path for the changelog file. "
             "Defaults to '<release_label>.csv' in current directory."
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: INFO)"
    )

    args = parser.parse_args()

    # Set log level
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    try:
        generator = ChangelogGenerator(
            metadata_uri=args.metadata_uri,
            release_label=args.release_label,
            output_path=args.output_path
        )
        generator.generate()
        logger.info(f"Changelog generated successfully for release {args.release_label}")
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error generating changelog: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
