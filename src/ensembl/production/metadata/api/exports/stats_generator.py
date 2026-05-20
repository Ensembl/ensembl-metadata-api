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
Generate release statistics for Ensembl releases.

This module provides functionality to generate release statistics for both
partial and integrated Ensembl releases, exporting the data to CSV format.
"""
import argparse
import csv
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from ensembl.utils.database import DBConnection
from sqlalchemy import distinct

from ensembl.production.metadata.api.models import (
    EnsemblRelease, Genome, GenomeDataset, GenomeRelease, Dataset, DatasetType,
    Assembly, DatasetStatus, ReleaseStatus
)

# Constants
DATASET_TYPE_VARIATION = 'variation'
DATASET_TYPE_REGULATORY = 'regulatory_features'
RELEASE_TYPE_PARTIAL = 'partial'
RELEASE_TYPE_INTEGRATED = 'integrated'

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StatsGenerator:
    """
    Generate release statistics for Ensembl releases.

    This class builds data structures capable of exporting both partial and
    integrated release information to CSV format. It queries the metadata
    database to gather genome, dataset, and release information.

    Attributes:
        metadata_db: Database connection to the Ensembl metadata database
        output_path: Optional path for output files
    """

    def __init__(self, metadata_uri: str, output_path: Optional[str] = None):
        """
        Initialize the stats generator.

        Args:
            metadata_uri: Database URI for the metadata database
            output_path: Optional output path for CSV files

        Raises:
            ValueError: If metadata_uri is empty or invalid
        """
        if not metadata_uri or not isinstance(metadata_uri, str):
            raise ValueError("metadata_uri must be a non-empty string")

        self.metadata_db = DBConnection(metadata_uri)
        self.output_path = Path(output_path) if output_path else Path.cwd()

        if not self.output_path.exists():
            try:
                self.output_path.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                raise ValueError(f"Cannot create output directory {self.output_path}: {e}") from e

    def generate(self) -> None:
        """
        Generate the statistics and export to CSV.

        Determines the release type (partial or integrated) and calls the
        appropriate data gathering method, then exports the results to CSV.
        """
        logger.info("Starting statistics generation")

        try:
            partial_data = self.get_partial_data()
            logger.info(f"Generated statistics for {len(partial_data)} partial releases")

            integrated_data = self.get_integrated_data()
            logger.info(f"Generated statistics for {len(integrated_data)} integrated releases")

            self.export_to_csv(partial_data, integrated_data)
            logger.info("Statistics generation completed successfully")
        except Exception as e:
            logger.error(f"Error during statistics generation: {e}")
            raise

    def get_partial_data(self) -> List[Dict]:
        """
        Generate partial release statistics with optimized cumulative calculations.

        Returns:
            List of dictionaries containing partial release statistics
        """
        with self.metadata_db.session_scope() as session:
            # Get all partial releases ordered by label - ONLY RELEASED
            releases = session.query(EnsemblRelease).filter(
                EnsemblRelease.release_type == RELEASE_TYPE_PARTIAL,
                EnsemblRelease.status == ReleaseStatus.RELEASED
            ).order_by(EnsemblRelease.label).all()

            if not releases:
                logger.warning("No released partial releases found")
                return []

            partial_data = []

            # Track cumulative totals across releases
            cumulative_genome_ids = set()
            cumulative_assembly_ids = set()
            cumulative_variation_ids = set()
            cumulative_regulation_ids = set()

            for release in releases:
                # NEW GENOMES: Count genomes in this release
                new_genomes = session.query(GenomeRelease).filter(
                    GenomeRelease.release_id == release.release_id
                ).count()

                # Get genome IDs for cumulative tracking
                genome_ids = {gr.genome_id for gr in session.query(
                    GenomeRelease.genome_id
                ).filter(
                    GenomeRelease.release_id == release.release_id
                ).all()}
                cumulative_genome_ids.update(genome_ids)

                # NEW ASSEMBLIES: Count unique assemblies with genomes in this release
                new_assemblies = session.query(distinct(Assembly.assembly_id)).join(
                    Genome, Genome.assembly_id == Assembly.assembly_id
                ).join(
                    GenomeRelease, GenomeRelease.genome_id == Genome.genome_id
                ).filter(
                    GenomeRelease.release_id == release.release_id
                ).count()

                # Get assembly IDs for cumulative tracking
                assembly_ids = {aid[0] for aid in session.query(
                    distinct(Assembly.assembly_id)
                ).join(
                    Genome, Genome.assembly_id == Assembly.assembly_id
                ).join(
                    GenomeRelease, GenomeRelease.genome_id == Genome.genome_id
                ).filter(
                    GenomeRelease.release_id == release.release_id
                ).all()}
                cumulative_assembly_ids.update(assembly_ids)

                # NEW VARIATION DATASETS
                new_variation, variation_ids = self._count_and_get_dataset_ids(
                    session, release.release_id, DATASET_TYPE_VARIATION
                )
                cumulative_variation_ids.update(variation_ids)

                # NEW REGULATION DATASETS
                new_regulation, regulation_ids = self._count_and_get_dataset_ids(
                    session, release.release_id, DATASET_TYPE_REGULATORY
                )
                cumulative_regulation_ids.update(regulation_ids)

                partial_data.append({
                    'release': release.label,
                    'new_genomes': new_genomes,
                    'total_genomes': len(cumulative_genome_ids),
                    'new_assemblies': new_assemblies,
                    'total_assemblies': len(cumulative_assembly_ids),
                    'new_variation_datasets': new_variation,
                    'total_variation_datasets': len(cumulative_variation_ids),
                    'new_regulation_datasets': new_regulation,
                    'total_regulation_datasets': len(cumulative_regulation_ids),
                })

            return partial_data

    def get_integrated_data(self) -> List[Dict]:
        """
        Generate integrated release statistics using optimized queries.

        Returns:
            List of dictionaries containing integrated release statistics
        """
        with self.metadata_db.session_scope() as session:
            # Get all integrated releases - ONLY RELEASED
            releases = session.query(EnsemblRelease).filter(
                EnsemblRelease.release_type == RELEASE_TYPE_INTEGRATED,
                EnsemblRelease.status == ReleaseStatus.RELEASED
            ).order_by(EnsemblRelease.label).all()

            if not releases:
                logger.warning("No released integrated releases found")
                return []

            integrated_data = []

            for release in releases:
                # Count genomes
                genomes_count = session.query(GenomeRelease).filter(
                    GenomeRelease.release_id == release.release_id
                ).count()

                # Count unique assemblies
                assemblies_count = session.query(distinct(Assembly.assembly_id)).join(
                    Genome, Genome.assembly_id == Assembly.assembly_id
                ).join(
                    GenomeRelease, GenomeRelease.genome_id == Genome.genome_id
                ).filter(
                    GenomeRelease.release_id == release.release_id
                ).count()

                # Count variation datasets - ONLY RELEASED
                variation_count = self._count_datasets(
                    session, release.release_id, DATASET_TYPE_VARIATION
                )

                # Count regulation datasets - ONLY RELEASED
                regulation_count = self._count_datasets(
                    session, release.release_id, DATASET_TYPE_REGULATORY
                )

                integrated_data.append({
                    'release': release.label,
                    'genomes': genomes_count,
                    'assemblies': assemblies_count,
                    'variation_datasets': variation_count,
                    'regulation_datasets': regulation_count,
                })

            return integrated_data

    def _count_datasets(self, session, release_id: int, dataset_type_name: str) -> int:
        """
        Count datasets of a specific type for a release.

        Args:
            session: Database session
            release_id: Release ID to query
            dataset_type_name: Name of the dataset type (e.g., 'variation')

        Returns:
            Count of datasets
        """
        return session.query(GenomeDataset).join(
            Dataset, GenomeDataset.dataset_id == Dataset.dataset_id
        ).join(
            DatasetType, Dataset.dataset_type_id == DatasetType.dataset_type_id
        ).filter(
            GenomeDataset.release_id == release_id,
            DatasetType.name == dataset_type_name,
            Dataset.status == DatasetStatus.RELEASED
        ).count()

    def _count_and_get_dataset_ids(
            self, session, release_id: int, dataset_type_name: str
    ) -> Tuple[int, set]:
        """
        Count datasets and return their IDs for a specific type and release.

        Args:
            session: Database session
            release_id: Release ID to query
            dataset_type_name: Name of the dataset type

        Returns:
            Tuple of (count, set of dataset IDs)
        """
        results = session.query(GenomeDataset.dataset_id).join(
            Dataset, GenomeDataset.dataset_id == Dataset.dataset_id
        ).join(
            DatasetType, Dataset.dataset_type_id == DatasetType.dataset_type_id
        ).filter(
            GenomeDataset.release_id == release_id,
            DatasetType.name == dataset_type_name,
            Dataset.status == DatasetStatus.RELEASED
        ).all()

        dataset_ids = {result[0] for result in results}
        return len(dataset_ids), dataset_ids

    def export_to_csv(
            self, partial_data: List[Dict], integrated_data: List[Dict]
    ) -> None:
        """
        Export statistics data to CSV files.

        Creates two CSV files (partial and integrated) with the statistics data.

        Args:
            partial_data: List of dictionaries containing partial release statistics
            integrated_data: List of dictionaries containing integrated release statistics
        """
        # Define output files
        partial_output_file = self.output_path / 'stats.partial.csv'
        integrated_output_file = self.output_path / 'stats.integrated.csv'

        # Partial release columns
        partial_fieldnames = [
            'release',
            'new_genomes',
            'total_genomes',
            'new_assemblies',
            'total_assemblies',
            'new_variation_datasets',
            'total_variation_datasets',
            'new_regulation_datasets',
            'total_regulation_datasets',
        ]

        # Integrated release columns
        integrated_fieldnames = [
            'release',
            'genomes',
            'assemblies',
            'variation_datasets',
            'regulation_datasets',
        ]

        # Sort data by release
        partial_data_sorted = sorted(partial_data, key=lambda x: x['release'])
        integrated_data_sorted = sorted(integrated_data, key=lambda x: x['release'])

        # Write partial file
        try:
            with open(partial_output_file, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=partial_fieldnames)
                writer.writeheader()
                writer.writerows(partial_data_sorted)
            logger.info(f"Partial stats exported to: {partial_output_file.absolute()}")
        except Exception as e:
            logger.error(f"Failed to write partial stats file: {e}")
            raise

        # Write integrated file
        try:
            with open(integrated_output_file, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=integrated_fieldnames)
                writer.writeheader()
                writer.writerows(integrated_data_sorted)
            logger.info(f"Integrated stats exported to: {integrated_output_file.absolute()}")
        except Exception as e:
            logger.error(f"Failed to write integrated stats file: {e}")
            raise


def main() -> None:
    """Main entry point for the script."""

    parser = argparse.ArgumentParser(
        description="Generate release statistics for Ensembl releases"
    )
    parser.add_argument(
        "--metadata-uri",
        required=True,
        help="Database URI for the metadata database"
    )
    parser.add_argument(
        "--output-path",
        default=None,
        help="Optional output path for the stats files. Filenames will be: "
             "stats.partial.csv & stats.integrated.csv. Defaults to current directory."
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
        generator = StatsGenerator(
            metadata_uri=args.metadata_uri,
            output_path=args.output_path
        )
        generator.generate()
        logger.info("Release statistics generated successfully")
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error generating release statistics: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
