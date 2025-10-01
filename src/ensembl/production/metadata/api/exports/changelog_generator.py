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
from sqlalchemy import select, func

from ensembl.production.metadata.api.models import (
    EnsemblRelease, Genome, GenomeDataset, GenomeRelease, Dataset, DatasetType,
    DatasetAttribute, Attribute
)


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

    def __init__(self, metadata_uri, release_label, output_path=None):
        """
        Initialize the changelog generator.

        Args:
            metadata_uri: Database URI for the metadata database
            release_label: Release label (e.g., '2024-02' or '2025-05-25')
            output_path: Optional output path for the changelog CSV file
        """
        self.metadata_db = DBConnection(metadata_uri)
        self.release_label = release_label
        self.output_path = output_path

    def generate(self):
        """
        Generate the changelog and export to CSV.

        Determines the release type (partial or integrated) and calls the
        appropriate data gathering method, then exports the results to CSV.
        """
        release_type = self.verify_release()

        if release_type == 'partial':
            data = self.gather_partial_data()
        elif release_type == 'integrated':
            print("WARNING: Integrated release changelog generation has not been "
                  "extensively tested across multiple releases")
            data = self.gather_integrated_data()
        else:
            raise ValueError(f"Unknown release type: {release_type}")

        self.export_to_csv(data)

    def verify_release(self):
        """
        Verify that the release exists and return its type.

        Returns:
            str: Release type ('partial' or 'integrated')

        Raises:
            Exception: If the release is not found in the database
        """
        release_query = select(EnsemblRelease.release_type).where(
            EnsemblRelease.label == self.release_label
        )

        with self.metadata_db.session_scope() as session:
            result = session.execute(release_query).one_or_none()
            if result is None:
                raise Exception(
                    f"Release not found: {self.release_label}. "
                    "Please use a valid Release Label (e.g., '2024-02' or '2025-05-25')"
                )
            return result[0]

    def gather_partial_data(self):
        """
        Gather changelog data for a partial release.

        For partial releases, this method collects information about which genomes
        have updated datasets (genebuild, variation, or regulatory features) in
        the specified release.

        Returns:
            list: List of dictionaries containing changelog data with keys:
                - scientific_name: Species scientific name
                - common_name: Species common name
                - assembly_accession: Assembly accession
                - annotation_provider: Source of annotation
                - geneset_updated: 1 if genebuild updated, 0 otherwise
                - variation_updated: 1 if variation updated, 0 otherwise
                - regulation_updated: 1 if regulation updated, 0 otherwise
        """
        with self.metadata_db.session_scope() as session:
            # Get the release ID
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
                DatasetType.name.in_(['genebuild', 'variation', 'regulatory_features'])
            ).distinct()

            genomes = session.execute(genome_query).scalars().all()

            # Process each genome to build changelog entries
            results = []
            for genome in genomes:
                organism = genome.organism
                assembly = genome.assembly

                # Check which dataset types are present for this genome in this release
                dataset_check_query = select(
                    DatasetType.name,
                    Dataset.dataset_id
                ).join(
                    Dataset, Dataset.dataset_type_id == DatasetType.dataset_type_id
                ).join(
                    GenomeDataset, GenomeDataset.dataset_id == Dataset.dataset_id
                ).where(
                    GenomeDataset.genome_id == genome.genome_id,
                    GenomeDataset.release_id == release_id,
                    DatasetType.name.in_(['genebuild', 'variation', 'regulatory_features'])
                )

                dataset_results = session.execute(dataset_check_query).all()
                datasets_in_release = {name: dataset_id for name, dataset_id in dataset_results}

                # Get annotation source from genebuild dataset
                # NOTE: This section should be improved after schema changes
                annotation_source = None
                genebuild_query = select(Dataset.dataset_id).join(
                    GenomeDataset, GenomeDataset.dataset_id == Dataset.dataset_id
                ).join(
                    DatasetType, DatasetType.dataset_type_id == Dataset.dataset_type_id
                ).where(
                    GenomeDataset.genome_id == genome.genome_id,
                    DatasetType.name == 'genebuild',
                )

                genebuild_result = session.execute(genebuild_query).first()
                if genebuild_result:
                    genebuild_dataset_id = genebuild_result[0]
                    attr_query = select(DatasetAttribute.value).join(
                        Attribute, Attribute.attribute_id == DatasetAttribute.attribute_id
                    ).where(
                        DatasetAttribute.dataset_id == genebuild_dataset_id,
                        Attribute.name == 'genebuild.annotation_source'
                    )
                    annotation_source = session.execute(attr_query).scalar_one_or_none()

                # Build changelog entry
                result = {
                    'scientific_name': organism.scientific_name,
                    'common_name': organism.common_name,
                    'assembly_name': assembly.name,
                    'assembly_accession': assembly.accession,
                    'annotation_provider': annotation_source,
                    'geneset_updated': 1 if 'genebuild' in datasets_in_release else 0,
                    'variation_updated': 1 if 'variation' in datasets_in_release else 0,
                    'regulation_updated': 1 if 'regulatory_features' in datasets_in_release else 0
                }
                results.append(result)

        return results

    def gather_integrated_data(self):
        """
        Gather changelog data for an integrated release.

        For integrated releases, this method compares the current release to the
        previous integrated release to determine which genomes are new, removed,
        updated, or unchanged. It includes the partial release labels for when
        each dataset was last updated.

        Returns:
            list: List of dictionaries containing changelog data with keys:
                - scientific_name: Species scientific name
                - common_name: Species common name
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
            prev_release_query = select(EnsemblRelease.release_id).where(
                EnsemblRelease.release_type == 'integrated',
                EnsemblRelease.release_id < current_release_id
            ).order_by(EnsemblRelease.release_id.desc()).limit(1)

            prev_release_id = session.execute(prev_release_query).scalar_one_or_none()

            # Get all genomes in current integrated release
            current_genomes_query = select(Genome).join(
                GenomeRelease, GenomeRelease.genome_id == Genome.genome_id
            ).where(
                GenomeRelease.release_id == current_release_id
            ).distinct()

            current_genomes = session.execute(current_genomes_query).scalars().all()
            current_genome_ids = {g.genome_id for g in current_genomes}

            # Bulk fetch dataset information for current release
            current_datasets_bulk = self._get_all_genome_datasets_bulk(
                session, list(current_genome_ids), current_release_id
            )

            # Bulk fetch annotation sources for all genomes
            annotation_sources = self._get_all_annotation_sources_bulk(
                session, list(current_genome_ids)
            )

            # Build previous genome data for comparison (if previous release exists)
            prev_genomes = {}
            prev_genome_ids_list = []
            if prev_release_id:
                prev_genomes_query = select(Genome.genome_id).join(
                    GenomeRelease, GenomeRelease.genome_id == Genome.genome_id
                ).where(
                    GenomeRelease.release_id == prev_release_id
                )

                prev_genome_ids_list = session.execute(prev_genomes_query).scalars().all()

                # Bulk fetch dataset information for previous release
                prev_genomes = self._get_all_genome_datasets_bulk(
                    session, prev_genome_ids_list, prev_release_id
                )

            results = []

            # Process current genomes
            for genome in current_genomes:
                organism = genome.organism
                assembly = genome.assembly

                annotation_source = annotation_sources.get(genome.genome_id)
                current_datasets = current_datasets_bulk.get(genome.genome_id, {
                    'geneset_updated': None,
                    'variation_updated': None,
                    'regulation_updated': None
                })

                # Determine status by comparing to previous release
                if genome.genome_id not in prev_genomes:
                    status = "New"
                else:
                    prev_datasets = prev_genomes[genome.genome_id]
                    # Check if any dataset has been updated
                    if (current_datasets['geneset_updated'] != prev_datasets['geneset_updated'] or
                            current_datasets['variation_updated'] != prev_datasets['variation_updated'] or
                            current_datasets['regulation_updated'] != prev_datasets['regulation_updated']):
                        status = "Updated"
                    else:
                        status = "Unchanged"

                result = {
                    'scientific_name': organism.scientific_name,
                    'common_name': organism.common_name,
                    'assembly_name': assembly.name,
                    'assembly_accession': assembly.accession,
                    'annotation_provider': annotation_source,
                    'geneset_updated': current_datasets['geneset_updated'],
                    'variation_updated': current_datasets['variation_updated'],
                    'regulation_updated': current_datasets['regulation_updated'],
                    'status': status
                }
                results.append(result)

            # Add genomes that were removed (present in previous but not current)
            if prev_release_id:
                for genome_id in prev_genome_ids_list:
                    if genome_id not in current_genome_ids:
                        removed_genome = session.get(Genome, genome_id)
                        organism = removed_genome.organism
                        assembly = removed_genome.assembly
                        annotation_source = annotation_sources.get(genome_id)
                        prev_datasets = prev_genomes[genome_id]

                        result = {
                            'scientific_name': organism.scientific_name,
                            'common_name': organism.common_name,
                            'assembly_accession': assembly.accession,
                            'annotation_provider': annotation_source,
                            'geneset_updated': prev_datasets['geneset_updated'],
                            'variation_updated': prev_datasets['variation_updated'],
                            'regulation_updated': prev_datasets['regulation_updated'],
                            'status': "Removed"
                        }
                        results.append(result)

            return results

    def _get_all_genome_datasets_bulk(self, session, genome_ids, release_id):
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
            dict: Mapping of genome_id to dataset information:
                {genome_id: {
                    'geneset_updated': partial_release_label or None,
                    'variation_updated': partial_release_label or None,
                    'regulation_updated': partial_release_label or None
                }}
        """
        # Initialize result dictionary with None values
        result = {gid: {
            'geneset_updated': None,
            'variation_updated': None,
            'regulation_updated': None
        } for gid in genome_ids}

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
            DatasetType.name.in_(['genebuild', 'variation', 'regulatory_features'])
        )

        dataset_results = session.execute(bulk_query).all()

        # Build lookup structures
        dataset_info = {}  # {(genome_id, dataset_type): dataset_id}
        dataset_ids_to_check = set()

        for genome_id, dataset_type, dataset_id in dataset_results:
            dataset_info[(genome_id, dataset_type)] = dataset_id
            dataset_ids_to_check.add(dataset_id)

        # If no datasets found, return empty result
        if not dataset_ids_to_check:
            return result

        # Bulk query: Get partial release labels for all datasets
        partial_release_query = select(
            GenomeDataset.genome_id,
            GenomeDataset.dataset_id,
            EnsemblRelease.label
        ).join(
            EnsemblRelease, EnsemblRelease.release_id == GenomeDataset.release_id
        ).where(
            GenomeDataset.dataset_id.in_(dataset_ids_to_check),
            GenomeDataset.genome_id.in_(genome_ids),
            EnsemblRelease.release_type == 'partial'
        )

        partial_results = session.execute(partial_release_query).all()

        # Build lookup: {(genome_id, dataset_id): label}
        partial_labels = {}
        for genome_id, dataset_id, label in partial_results:
            key = (genome_id, dataset_id)
            # Keep only the first occurrence (should be most recent if ordered properly)
            if key not in partial_labels:
                partial_labels[key] = label

        # Populate final results
        for (genome_id, dataset_type), dataset_id in dataset_info.items():
            label = partial_labels.get((genome_id, dataset_id))

            if dataset_type == 'genebuild':
                result[genome_id]['geneset_updated'] = label
            elif dataset_type == 'variation':
                result[genome_id]['variation_updated'] = label
            elif dataset_type == 'regulatory_features':
                result[genome_id]['regulation_updated'] = label

        return result

    def _get_all_annotation_sources_bulk(self, session, genome_ids):
        """
        Get annotation sources for multiple genomes in bulk queries.

        Retrieves the annotation source from the most recent genebuild dataset
        for each genome. Uses optimized bulk queries to minimize database calls.

        Args:
            session: Database session
            genome_ids: List of genome IDs to query

        Returns:
            dict: Mapping of genome_id to annotation_source:
                {genome_id: annotation_source or None}
        """
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
            DatasetType.name == 'genebuild'
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
            DatasetType.name == 'genebuild'
        )

        dataset_results = session.execute(dataset_query).all()
        genome_to_dataset = {genome_id: dataset_id for genome_id, dataset_id in dataset_results}

        # If no genebuild datasets found, return empty dict
        if not genome_to_dataset:
            return {}

        # Bulk query: Get all annotation sources in one query
        attr_query = select(
            DatasetAttribute.dataset_id,
            DatasetAttribute.value
        ).join(
            Attribute, Attribute.attribute_id == DatasetAttribute.attribute_id
        ).where(
            DatasetAttribute.dataset_id.in_(genome_to_dataset.values()),
            Attribute.name == 'genebuild.annotation_source'
        )

        attr_results = session.execute(attr_query).all()
        dataset_to_source = {dataset_id: value for dataset_id, value in attr_results}

        # Map annotation sources back to genome_ids
        result = {}
        for genome_id, dataset_id in genome_to_dataset.items():
            result[genome_id] = dataset_to_source.get(dataset_id)

        return result

    def export_to_csv(self, data):
        """
        Export changelog data to CSV file with a commented header.

        Creates a CSV file with a comment line indicating the release, followed
        by the changelog data. The field names vary depending on whether this is
        a partial or integrated release (integrated includes a 'status' column).

        Args:
            data: List of dictionaries containing changelog data
        """

        # Determine output path
        if self.output_path:
            output_file = Path(self.output_path)
        else:
            output_file = Path(f"{self.release_label}.csv")

        # Ensure parent directory exists
        output_file.parent.mkdir(parents=True, exist_ok=True)

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
        with open(output_file, 'w', newline='') as csvfile:
            # Write commented header line
            csvfile.write(f"# Changelog for release {self.release_label}\n")

            # Write CSV data
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

        print(f"Changelog exported to: {output_file.absolute()}")


if __name__ == "__main__":
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

    args = parser.parse_args()

    generator = ChangelogGenerator(
        metadata_uri=args.metadata_uri,
        release_label=args.release_label,
        output_path=args.output_path
    )

    try:
        generator.generate()
        print(f"Changelog generated successfully for release {args.release_label}")
    except Exception as e:
        print(f"Error generating changelog: {e}")
        exit(1)