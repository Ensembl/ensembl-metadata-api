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
from pathlib import Path

from ensembl.utils.database import DBConnection
from sqlalchemy import select

from ensembl.production.metadata.api.models import *


class ChangelogGenerator:
    """
    Independent class for generating release changelog.
    Builds the Datastructure capable of exporting both partial and integrated data.
    """

    def __init__(self, metadata_uri, release_label, output_path=None):
        self.metadata_db = DBConnection(metadata_uri)
        self.release_label = release_label
        self.output_path = output_path

    def generate(self):
        release_type = self.verify_release()
        if release_type == 'partial':
            data = self.gather_partial_data()
            self.export_to_csv(data)
        elif release_type == 'integrated':
            data = self.gather_integrated_data()
            self.export_to_csv(data)

    def verify_release(self):
        release_query = select(EnsemblRelease.release_type
                               ).where(
            EnsemblRelease.label == self.release_label
        )

        with self.metadata_db.session_scope() as session:
            dataset_results = session.execute(release_query).one_or_none()
            if dataset_results is None:
                raise Exception("Release not found. Please use the Release Label. ex: 2024-02 or 2025-05-25")
            else:
                return dataset_results[0]

    def gather_partial_data(self):
        with self.metadata_db.session_scope() as session:
            release_query = select(EnsemblRelease.release_id).where(EnsemblRelease.label == self.release_label)
            release_id = session.execute(release_query).scalar_one()

            # First query: Find all genomes with genebuild, variation, or regulation datasets in this release
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

            # Second query: For each genome, gather the required data
            results = []
            for genome in genomes:
                # Get organism data (scientific_name, common_name)
                organism = genome.organism

                # Get assembly data (assembly_accession)
                assembly = genome.assembly

                # Check which dataset types are present in this release for this genome
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

                # Get annotation_provider from genebuild dataset attributes
                ###### THIS SECTION SHOULD BE IMPROVED POST SCHEMA CHANGES #####
                annotation_source = None
                genebuild_query = select(Dataset.dataset_id).join(
                    GenomeDataset, GenomeDataset.dataset_id == Dataset.dataset_id
                ).join(
                    DatasetType, DatasetType.dataset_type_id == Dataset.dataset_type_id
                ).where(
                    GenomeDataset.genome_id == genome.genome_id,
                    DatasetType.name == 'genebuild',
                )
                genebuild_dataset_id = session.execute(genebuild_query).first()[0]
                if genebuild_dataset_id:
                    attr_query = select(DatasetAttribute.value).join(
                        Attribute, Attribute.attribute_id == DatasetAttribute.attribute_id
                    ).where(
                        DatasetAttribute.dataset_id == genebuild_dataset_id,
                        Attribute.name == 'genebuild.annotation_source'
                    )
                    annotation_source = session.execute(attr_query).scalar_one_or_none()
                ###### ABOVE SHOULD BE IMPROVED POST SCHEMA CHANGES #####

                result = {
                    'scientific_name': organism.scientific_name,
                    'common_name': organism.common_name,
                    'assembly_accession': assembly.accession,  # Adjust field name if needed
                    'annotation_provider': annotation_source,
                    'geneset_updated': 1 if 'genebuild' in datasets_in_release else 0,
                    'variation_updated': 1 if 'variation' in datasets_in_release else 0,
                    'regulation_updated': 1 if 'regulatory_features' in datasets_in_release else 0
                }
                results.append(result)

        return results

    def gather_integrated_data(self):
        with self.metadata_db.session_scope() as session:
            # Get current integrated release
            current_release_query = select(
                EnsemblRelease.release_id,
                EnsemblRelease.version
            ).where(
                EnsemblRelease.label == self.release_label
            )
            current_release = session.execute(current_release_query).one()
            current_release_id = current_release[0]

            # Get previous integrated release
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

            # Build previous genome data for comparison (if exists)
            prev_genomes = {}
            if prev_release_id:
                prev_genomes_query = select(Genome.genome_id).join(
                    GenomeRelease, GenomeRelease.genome_id == Genome.genome_id
                ).where(
                    GenomeRelease.release_id == prev_release_id
                )

                prev_genome_ids = session.execute(prev_genomes_query).scalars().all()

                for genome_id in prev_genome_ids:
                    prev_genomes[genome_id] = self._get_genome_datasets(session, genome_id, prev_release_id)

            results = []

            # Process current genomes
            for genome in current_genomes:
                organism = genome.organism
                assembly = genome.assembly

                # Get annotation_provider
                annotation_source = self._get_annotation_source(session, genome.genome_id)

                # Get dataset release labels for this genome
                current_datasets = self._get_genome_datasets(session, genome.genome_id, current_release_id)

                # Determine status
                if genome.genome_id not in prev_genomes:
                    status = "New"
                else:
                    prev_datasets = prev_genomes[genome.genome_id]
                    if (current_datasets['geneset_updated'] != prev_datasets['geneset_updated'] or
                            current_datasets['variation_updated'] != prev_datasets['variation_updated'] or
                            current_datasets['regulation_updated'] != prev_datasets['regulation_updated']):
                        status = "Updated"
                    else:
                        status = "Unchanged"

                result = {
                    'scientific_name': organism.scientific_name,
                    'common_name': organism.common_name,
                    'assembly_accession': assembly.accession,
                    'annotation_provider': annotation_source,
                    'geneset_updated': current_datasets['geneset_updated'],
                    'variation_updated': current_datasets['variation_updated'],
                    'regulation_updated': current_datasets['regulation_updated'],
                    'status': status
                }
                results.append(result)

            # Add removed genomes
            if prev_release_id:
                for genome_id, prev_datasets in prev_genomes.items():
                    if genome_id not in current_genome_ids:
                        # Get genome info
                        removed_genome = session.get(Genome, genome_id)
                        organism = removed_genome.organism
                        assembly = removed_genome.assembly
                        annotation_source = self._get_annotation_source(session, genome_id)

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

    def _get_genome_datasets(self, session, genome_id, release_id):
        """
        Get the partial release labels for when each dataset type was released.
        """
        dataset_types = ['genebuild', 'variation', 'regulatory_features']
        result = {
            'geneset_updated': None,
            'variation_updated': None,
            'regulation_updated': None
        }

        for dataset_type in dataset_types:
            # Get the dataset attached to this genome in this release
            dataset_query = select(Dataset.dataset_id).join(
                GenomeDataset, GenomeDataset.dataset_id == Dataset.dataset_id
            ).join(
                DatasetType, DatasetType.dataset_type_id == Dataset.dataset_type_id
            ).where(
                GenomeDataset.genome_id == genome_id,
                GenomeDataset.release_id == release_id,
                DatasetType.name == dataset_type
            )

            dataset_id = session.execute(dataset_query).scalar_one_or_none()

            if dataset_id:
                # Find the partial release this dataset was released in
                partial_release_query = select(EnsemblRelease.label).join(
                    GenomeDataset, GenomeDataset.release_id == EnsemblRelease.release_id
                ).where(
                    GenomeDataset.genome_id == genome_id,
                    GenomeDataset.dataset_id == dataset_id,
                    EnsemblRelease.release_type == 'partial'
                ).order_by(EnsemblRelease.version.desc()).limit(1)

                partial_label = session.execute(partial_release_query).scalar_one_or_none()

                if dataset_type == 'genebuild':
                    result['geneset_updated'] = partial_label
                elif dataset_type == 'variation':
                    result['variation_updated'] = partial_label
                elif dataset_type == 'regulatory_features':
                    result['regulation_updated'] = partial_label

        return result

    def _get_annotation_source(self, session, genome_id):
        """
        Get annotation source for a genome (from most recent genebuild dataset).
        """
        annotation_source = None
        genebuild_query = select(Dataset.dataset_id).join(
            GenomeDataset, GenomeDataset.dataset_id == Dataset.dataset_id
        ).join(
            DatasetType, DatasetType.dataset_type_id == Dataset.dataset_type_id
        ).where(
            GenomeDataset.genome_id == genome_id,
            DatasetType.name == 'genebuild',
        ).order_by(Dataset.created.desc()).limit(1)

        genebuild_dataset_id = session.execute(genebuild_query).scalar_one_or_none()

        if genebuild_dataset_id:
            attr_query = select(DatasetAttribute.value).join(
                Attribute, Attribute.attribute_id == DatasetAttribute.attribute_id
            ).where(
                DatasetAttribute.dataset_id == genebuild_dataset_id,
                Attribute.name == 'genebuild.annotation_source'
            )
            annotation_source = session.execute(attr_query).scalar_one_or_none()

        return annotation_source

    def export_to_csv(self, data):
        """
        Export changelog data to CSV file with commented header.
        """
        import csv

        # Determine output path
        if self.output_path:
            output_file = Path(self.output_path)
        else:
            output_file = Path(f"{self.release_label}.csv")

        # Ensure parent directory exists
        output_file.parent.mkdir(parents=True, exist_ok=True)

        # Define the field order - check if status exists in data
        if data and 'status' in data[0]:
            # Integrated release
            fieldnames = [
                'scientific_name',
                'common_name',
                'assembly_accession',
                'annotation_provider',
                'geneset_updated',
                'variation_updated',
                'regulation_updated',
                'status'
            ]
        else:
            # Partial release
            fieldnames = [
                'scientific_name',
                'common_name',
                'assembly_accession',
                'annotation_provider',
                'geneset_updated',
                'variation_updated',
                'regulation_updated'
            ]

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
        help="Optional output path for the changelog file. Defaults to outputing the label in the cwd."
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
