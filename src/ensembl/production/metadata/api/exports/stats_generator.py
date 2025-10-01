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
from pathlib import Path

from ensembl.utils.database import DBConnection
from sqlalchemy import distinct

from ensembl.production.metadata.api.models import (
    EnsemblRelease, Genome, GenomeDataset, GenomeRelease, Dataset, DatasetType,
    Assembly, DatasetStatus, ReleaseStatus
)


class StatsGenerator:
    """
    Generate release stats for Ensembl releases.

    This class builds data structures capable of exporting both partial and
    integrated release information to CSV format. It queries the metadata
    database to gather genome, dataset, and release information.

    Attributes:
        metadata_db: Database connection to the Ensembl metadata database
        output_path: Optional path for output
    """

    def __init__(self, metadata_uri, output_path=None):
        """
        Initialize the stats generator.

        Args:
            metadata_uri: Database URI for the metadata database
            output_path: Optional output path
        """
        self.metadata_db = DBConnection(metadata_uri)
        self.output_path = output_path

    def generate(self):
        """
        Generate the stats and export to CSV.

        Determines the release type (partial or integrated) and calls the
        appropriate data gathering method, then exports the results to CSV.
        """
        partial_data = self.get_partial_data()
        integrated_data = self.get_integrated_data()
        self.export_to_csv(partial_data, integrated_data)

    def get_partial_data(self):
        with self.metadata_db.session_scope() as session:
            # Get all partial releases ordered by label - ONLY RELEASED
            releases = session.query(EnsemblRelease).filter(
                EnsemblRelease.release_type == 'partial',
                EnsemblRelease.status == ReleaseStatus.RELEASED
            ).order_by(EnsemblRelease.label).all()

            partial_data = []

            for release in releases:
                # NEW GENOMES: Count genomes in this release
                new_genomes = session.query(GenomeRelease).filter(
                    GenomeRelease.release_id == release.release_id
                ).count()

                # TOTAL GENOMES: Count unique genomes in this and all previous partial releases
                total_genomes = session.query(distinct(GenomeRelease.genome_id)).join(
                    EnsemblRelease, GenomeRelease.release_id == EnsemblRelease.release_id
                ).filter(
                    EnsemblRelease.release_type == 'partial',
                    EnsemblRelease.status == ReleaseStatus.RELEASED,
                    EnsemblRelease.label <= release.label
                ).count()

                # NEW ASSEMBLIES: Count unique assemblies with genomes in this release
                new_assemblies = session.query(distinct(Assembly.assembly_id)).join(
                    Genome, Genome.assembly_id == Assembly.assembly_id
                ).join(
                    GenomeRelease, GenomeRelease.genome_id == Genome.genome_id
                ).filter(
                    GenomeRelease.release_id == release.release_id
                ).count()

                # TOTAL ASSEMBLIES: Count unique assemblies in this and all previous partial releases
                total_assemblies = session.query(distinct(Assembly.assembly_id)).join(
                    Genome, Genome.assembly_id == Assembly.assembly_id
                ).join(
                    GenomeRelease, GenomeRelease.genome_id == Genome.genome_id
                ).join(
                    EnsemblRelease, GenomeRelease.release_id == EnsemblRelease.release_id
                ).filter(
                    EnsemblRelease.release_type == 'partial',
                    EnsemblRelease.status == ReleaseStatus.RELEASED,
                    EnsemblRelease.label <= release.label
                ).count()

                # NEW VARIATION DATASETS: Count variation datasets in this release - ONLY RELEASED
                new_variation = session.query(GenomeDataset).join(
                    Dataset, GenomeDataset.dataset_id == Dataset.dataset_id
                ).join(
                    DatasetType, Dataset.dataset_type_id == DatasetType.dataset_type_id
                ).filter(
                    GenomeDataset.release_id == release.release_id,
                    DatasetType.name == 'variation',
                    Dataset.status == DatasetStatus.RELEASED
                ).count()

                # TOTAL VARIATION DATASETS: Count unique variation datasets in this and all previous partial releases
                total_variation = session.query(distinct(GenomeDataset.dataset_id)).join(
                    Dataset, GenomeDataset.dataset_id == Dataset.dataset_id
                ).join(
                    DatasetType, Dataset.dataset_type_id == DatasetType.dataset_type_id
                ).join(
                    EnsemblRelease, GenomeDataset.release_id == EnsemblRelease.release_id
                ).filter(
                    EnsemblRelease.release_type == 'partial',
                    EnsemblRelease.status == ReleaseStatus.RELEASED,
                    EnsemblRelease.label <= release.label,
                    DatasetType.name == 'variation',
                    Dataset.status == DatasetStatus.RELEASED
                ).count()

                # NEW REGULATION DATASETS: Count regulation datasets in this release - ONLY RELEASED
                new_regulation = session.query(GenomeDataset).join(
                    Dataset, GenomeDataset.dataset_id == Dataset.dataset_id
                ).join(
                    DatasetType, Dataset.dataset_type_id == DatasetType.dataset_type_id
                ).filter(
                    GenomeDataset.release_id == release.release_id,
                    DatasetType.name == 'regulatory_features',
                    Dataset.status == DatasetStatus.RELEASED
                ).count()

                # TOTAL REGULATION DATASETS: Count unique regulation datasets in this and all previous partial releases
                total_regulation = session.query(distinct(GenomeDataset.dataset_id)).join(
                    Dataset, GenomeDataset.dataset_id == Dataset.dataset_id
                ).join(
                    DatasetType, Dataset.dataset_type_id == DatasetType.dataset_type_id
                ).join(
                    EnsemblRelease, GenomeDataset.release_id == EnsemblRelease.release_id
                ).filter(
                    EnsemblRelease.release_type == 'partial',
                    EnsemblRelease.status == ReleaseStatus.RELEASED,
                    EnsemblRelease.label <= release.label,
                    DatasetType.name == 'regulatory_features',
                    Dataset.status == DatasetStatus.RELEASED
                ).count()

                partial_data.append({
                    'release': release.label,
                    'new_genomes': new_genomes,
                    'total_genomes': total_genomes,
                    'new_assemblies': new_assemblies,
                    'total_assemblies': total_assemblies,
                    'new_variation_datasets': new_variation,
                    'total_variation_datasets': total_variation,
                    'new_regulation_datasets': new_regulation,
                    'total_regulation_datasets': total_regulation,
                })
            return partial_data

    def get_integrated_data(self):
        with self.metadata_db.session_scope() as session:
            # Get all integrated releases - ONLY RELEASED
            releases = session.query(EnsemblRelease).filter(
                EnsemblRelease.release_type == 'integrated',
                EnsemblRelease.status == ReleaseStatus.RELEASED
            ).order_by(EnsemblRelease.label).all()

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
                variation_count = session.query(GenomeDataset).join(
                    Dataset, GenomeDataset.dataset_id == Dataset.dataset_id
                ).join(
                    DatasetType, Dataset.dataset_type_id == DatasetType.dataset_type_id
                ).filter(
                    GenomeDataset.release_id == release.release_id,
                    DatasetType.name == 'variation',
                    Dataset.status == DatasetStatus.RELEASED
                ).count()

                # Count regulation datasets - ONLY RELEASED
                regulation_count = session.query(GenomeDataset).join(
                    Dataset, GenomeDataset.dataset_id == Dataset.dataset_id
                ).join(
                    DatasetType, Dataset.dataset_type_id == DatasetType.dataset_type_id
                ).filter(
                    GenomeDataset.release_id == release.release_id,
                    DatasetType.name == 'regulatory_features',
                    Dataset.status == DatasetStatus.RELEASED
                ).count()

                integrated_data.append({
                    'release': release.label,
                    'genomes': genomes_count,
                    'assemblies': assemblies_count,
                    'variation_datasets': variation_count,
                    'regulation_datasets': regulation_count,
                })
        return integrated_data

    def export_to_csv(self, partial_data, integrated_data):
        """
        Export changelog data to CSV file with a commented header.

        Creates a CSV file with a comment line indicating the release, followed
        by the changelog data. The field names vary depending on whether this is
        a partial or integrated release (integrated includes a 'status' column).

        Args:
            partial_data: List of dictionaries containing partial changelog data
            integrated_data: List of dictionaries containing integrated changelog data
        """

        # Determine output path
        if self.output_path:
            output_dir = Path(self.output_path)
            partial_output_file = output_dir / 'stats.partial.csv'
            integrated_output_file = output_dir / 'stats.integrated.csv'
        else:
            partial_output_file = Path('stats.partial.csv')
            integrated_output_file = Path('stats.integrated.csv')

        # Ensure parent directory exists
        partial_output_file.parent.mkdir(parents=True, exist_ok=True)
        integrated_output_file.parent.mkdir(parents=True, exist_ok=True)

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
        with open(partial_output_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=partial_fieldnames)
            writer.writeheader()
            writer.writerows(partial_data_sorted)
        print(f"Partial stats exported to: {partial_output_file.absolute()}")

        # Write integrated file
        with open(integrated_output_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=integrated_fieldnames)
            writer.writeheader()
            writer.writerows(integrated_data_sorted)
        print(f"Integrated stats exported to: {integrated_output_file.absolute()}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate release stats for Ensembl releases"
    )
    parser.add_argument(
        "--metadata-uri",
        required=True,
        help="Database URI for the metadata database"
    )
    parser.add_argument(
        "--output-path",
        default=None,
        help="Optional output path for the stats files. Filenames will be: stats.partial.csv & stats.integrated.csv"
             "Defaults to current directory."
    )

    args = parser.parse_args()

    generator = StatsGenerator(
        metadata_uri=args.metadata_uri,
        output_path=args.output_path
    )

    try:
        generator.generate()
        print(f"Releases stats successfully")
    except Exception as e:
        print(f"Error generating release stats: {e}")
        exit(1)
