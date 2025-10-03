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
Generate FTP metadata JSON and Parquet for Ensembl releases.

This module provides functionality to generate hierarchical FTP metadata
structures for genomic datasets, organized by species, assemblies, providers,
and releases. Supports both JSON and Parquet export formats.
"""
import json
import logging
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set

from ensembl.utils.database import DBConnection
from pandas import DataFrame
from sqlalchemy import select, func, case
from sqlalchemy.orm import selectinload

from ensembl.production.metadata.api.adaptors.genome import format_accession_path
from ensembl.production.metadata.api.exceptions import TypeNotFoundException
from ensembl.production.metadata.api.models import *

# Constants
DATASET_TYPE_GENEBUILD = 'genebuild'
DATASET_TYPE_ASSEMBLY = 'assembly'
DATASET_TYPE_HOMOLOGIES = 'homologies'
DATASET_TYPE_VARIATION = 'variation'

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FTPIndexGenerator:
    """
    Generate FTP metadata JSON and Parquet for Ensembl releases.

    This class builds hierarchical data structures organized by species,
    assemblies, genebuild providers, and releases. It queries the metadata
    database to gather genome, dataset, and release information, generating
    file paths for all available datasets.

    Attributes:
        metadata_db: Database connection to the Ensembl metadata database
        metadata_uri: URI for the metadata database
    """

    def __init__(self, metadata_uri: str):
        """
        Initialize the FTP metadata exporter.

        Args:
            metadata_uri: Database URI for the metadata database

        """
        self.metadata_db = DBConnection(metadata_uri)
        self.metadata_uri = metadata_uri

    def export_to_json(self, metadata, output_file: str) -> Optional[Dict]:
        """
        Export FTP metadata to JSON format.

        Args:
            metadata: Dictionary containing the metadata structure to export
            output_file: Optional file path to write JSON. If None, returns dict.

        Returns:
            Metadata structure if output_file is None, otherwise None

        Raises:
            IOError: If file cannot be written
        """

        try:
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            with open(output_path, 'w') as f:
                json.dump(metadata, f, indent=2, default=str)

            logger.info(f"FTP metadata exported to: {output_path.absolute()}")
            logger.info(f"Found {len(metadata['species'])} species with released datasets")
            return None
        except Exception as e:
            raise IOError(f"Failed to write JSON file {output_file}: {e}") from e

    def export_to_parquet(self, metadata, output_file: str) -> None:
        """
        Export FTP metadata to Parquet format.

        The hierarchical JSON structure is flattened into tabular format with
        separate tables for species, files, and dataset information.

        Args:
            output_file: File path to write Parquet file

        Raises:
            IOError: If file cannot be written
        """
        logger.info("Starting FTP metadata export to Parquet")

        df = self._flatten_metadata_to_dataframe(metadata)

        try:
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            df.to_parquet(output_path, index=False, engine='pyarrow')

            logger.info(f"FTP metadata exported to: {output_path.absolute()}")
            logger.info(f"Parquet file contains {len(df)} rows")
        except Exception as e:
            raise IOError(f"Failed to write Parquet file {output_file}: {e}") from e

    def export(self, output_base: str, formats: List[str] = None) -> None:
        """
        Export FTP metadata in the specified format(s).

        By default, exports both JSON and Parquet formats. The data is collected
        once and then exported to all requested formats for efficiency.
        """
        if formats is None:
            formats = ['json', 'parquet']  # Default to both

        formats = [f.lower() for f in formats]

        # Validate formats
        valid_formats = {'json', 'parquet'}
        invalid_formats = set(formats) - valid_formats
        if invalid_formats:
            raise ValueError(f"Unsupported format(s): {', '.join(invalid_formats)}...")

        # Build metadata ONCE
        logger.info("Building FTP metadata structure")
        metadata = self.build_ftp_metadata_json()
        logger.info(f"Found {len(metadata['species'])} species with released datasets")

        # Export to ALL requested formats
        output_path = Path(output_base)

        if 'json' in formats:
            json_path = output_path.with_suffix('.json')
            self.export_to_json(metadata, str(json_path))

        if 'parquet' in formats:
            parquet_path = output_path.with_suffix('.parquet')
            self.export_to_parquet(metadata, str(parquet_path))

    def _flatten_metadata_to_dataframe(self, metadata: Dict) -> DataFrame:
        """
        Flatten hierarchical metadata structure into a pandas DataFrame.

        Converts the nested JSON structure into a flat table suitable for
        Parquet format. Each row represents a file with all related metadata.

        Args:
            metadata: Hierarchical metadata dictionary from build_ftp_metadata_json

        Returns:
            pandas DataFrame with flattened metadata
        """
        rows = []

        for species_key, species_data in metadata['species'].items():
            for assembly_key, assembly_data in species_data['assemblies'].items():
                for provider, provider_data in assembly_data['genebuild_providers'].items():
                    for release, release_data in provider_data.items():
                        for dataset_type, dataset_info in release_data['paths'].items():
                            # Handle genebuild and assembly (direct files structure)
                            if dataset_type in [DATASET_TYPE_GENEBUILD, DATASET_TYPE_ASSEMBLY]:
                                for file_category, files in dataset_info['files'].items():
                                    for file_name, file_path in files.items():
                                        row = {
                                            # Species information
                                            'species_key': species_key,
                                            'taxid': species_data['taxid'],
                                            'species_taxonomy_id': species_data['species_taxonomy_id'],
                                            'scientific_name': species_data['scientific_name'],
                                            'common_name': species_data['common_name'],
                                            'strain': species_data['strain'],
                                            'strain_type': species_data['strain_type'],
                                            'biosample_id': species_data['biosample_id'],

                                            # Assembly information
                                            'assembly_accession': assembly_key,
                                            'assembly_name': assembly_data['name'],
                                            'assembly_level': assembly_data['level'],

                                            # Release information
                                            'genebuild_provider': provider,
                                            'release': release,
                                            'partial_release_label': None,

                                            # Dataset information
                                            'dataset_type': dataset_type,
                                            'file_category': file_category,
                                            'file_name': file_name,
                                            'file_path': file_path,

                                            # Metadata
                                            'last_updated': metadata['last_updated']
                                        }
                                        rows.append(row)

                            # Handle homologies and variation (nested by partial release label)
                            elif dataset_type in [DATASET_TYPE_HOMOLOGIES, DATASET_TYPE_VARIATION]:
                                for partial_label, partial_data in dataset_info.items():
                                    for file_category, files in partial_data['files'].items():
                                        for file_name, file_path in files.items():
                                            row = {
                                                # Species information
                                                'species_key': species_key,
                                                'taxid': species_data['taxid'],
                                                'species_taxonomy_id': species_data['species_taxonomy_id'],
                                                'scientific_name': species_data['scientific_name'],
                                                'common_name': species_data['common_name'],
                                                'strain': species_data['strain'],
                                                'strain_type': species_data['strain_type'],
                                                'biosample_id': species_data['biosample_id'],

                                                # Assembly information
                                                'assembly_accession': assembly_key,
                                                'assembly_name': assembly_data['name'],
                                                'assembly_level': assembly_data['level'],

                                                # Release information
                                                'genebuild_provider': provider,
                                                'release': release,
                                                'partial_release_label': partial_label,

                                                # Dataset information
                                                'dataset_type': dataset_type,
                                                'file_category': file_category,
                                                'file_name': file_name,
                                                'file_path': file_path,

                                                # Metadata
                                                'last_updated': metadata['last_updated']
                                            }
                                            rows.append(row)

        df = DataFrame(rows)

        # Convert to appropriate data types
        if not df.empty:
            df['taxid'] = df['taxid'].astype('Int64')
            df['species_taxonomy_id'] = df['species_taxonomy_id'].astype('Int64')

        logger.info(f"Flattened metadata into {len(df)} rows")
        return df

    def _get_partial_release_label(self, dataset) -> str:
        """
        Get the partial release label for a homology or variation dataset.

        Args:
            dataset: Dataset object

        Returns:
            Release label string, or 'unknown' if not found
        """
        # Find the GenomeDataset relationship with a partial release
        for genome_dataset in dataset.genome_datasets:
            if (genome_dataset.ensembl_release and
                    genome_dataset.ensembl_release.release_type == 'partial' and
                    genome_dataset.ensembl_release.label):
                return genome_dataset.ensembl_release.label

        # Fallback: look for any release with a label
        for genome_dataset in dataset.genome_datasets:
            if genome_dataset.ensembl_release and genome_dataset.ensembl_release.label:
                return genome_dataset.ensembl_release.label

        logger.warning(f"No partial release label found for dataset {dataset.dataset_uuid}")
        return 'unknown'

    def _generate_path_for_dataset_type(
            self,
            genebuild_metadata: Dict,
            dataset_type: str
    ) -> Dict[str, str]:
        """
        Generate public FTP path for a single dataset type (genebuild or assembly).

        Args:
            genebuild_metadata: Dictionary containing genebuild metadata
            dataset_type: Type of dataset

        Returns:
            Dictionary with 'dataset_type' and 'path' keys
        """
        scientific_name = genebuild_metadata.get('scientific_name')
        accession = genebuild_metadata.get('accession')
        genebuild_source_name = genebuild_metadata.get('genebuild_source_name')
        last_geneset_update = genebuild_metadata.get('last_geneset_update')

        # Validate required fields
        if not all([scientific_name, accession, genebuild_source_name, last_geneset_update]):
            raise ValueError("Required metadata fields are missing")

        # Format the release date
        match = re.match(r'^(\d{4}-\d{2})', last_geneset_update)
        if not match:
            raise ValueError(f"Invalid last_geneset_update format: {last_geneset_update}")
        formatted_release = match.group(1).replace('-', '_')

        # Build path
        genebuild_source_name = genebuild_source_name.lower()
        base_path = format_accession_path(accession)
        common_path = f"{base_path}/{genebuild_source_name}/{formatted_release}"

        path_templates = {
            DATASET_TYPE_GENEBUILD: f"{common_path}/geneset",
            DATASET_TYPE_ASSEMBLY: f"{common_path}/genome",
        }

        if dataset_type not in path_templates:
            raise ValueError(f"Unsupported dataset type: {dataset_type}")

        return {
            "dataset_type": dataset_type,
            "path": path_templates[dataset_type]
        }

    def _generate_path_for_homology_variation(
            self,
            genebuild_metadata: Dict,
            dataset_type: str,
            partial_release_label: str
    ) -> str:
        """
        Generate public FTP path for homology or variation dataset.

        Args:
            genebuild_metadata: Dictionary containing genebuild metadata
            dataset_type: Type of dataset (homologies or variation)
            partial_release_label: The label from the partial release

        Returns:
            Path string
        """
        accession = genebuild_metadata.get('accession')
        genebuild_source_name = genebuild_metadata.get('genebuild_source_name')
        last_geneset_update = genebuild_metadata.get('last_geneset_update')

        if not all([accession, genebuild_source_name, last_geneset_update]):
            raise ValueError("Required metadata fields are missing")

        # Format the genebuild release date
        match = re.match(r'^(\d{4}-\d{2})', last_geneset_update)
        if not match:
            raise ValueError(f"Invalid last_geneset_update format: {last_geneset_update}")
        formatted_genebuild_release = match.group(1).replace('-', '_')

        # Build path with both genebuild release and partial release label
        genebuild_source_name = genebuild_source_name.lower()
        base_path = format_accession_path(accession)
        common_path = f"{base_path}/{genebuild_source_name}/{formatted_genebuild_release}"

        path_templates = {
            DATASET_TYPE_HOMOLOGIES: f"{common_path}/homology/{partial_release_label}",
            DATASET_TYPE_VARIATION: f"{common_path}/variation/{partial_release_label}",
        }

        if dataset_type not in path_templates:
            raise ValueError(f"Unsupported dataset type: {dataset_type}")

        return path_templates[dataset_type]

    def build_ftp_metadata_json(self) -> Dict:
        """
        Build a hierarchical data structure for JSON export containing FTP metadata.

        Only includes released datasets. The structure is organized as:
        species -> assemblies -> genebuild_providers -> releases -> paths -> datasets

        Returns:
            Dictionary containing the complete metadata structure with keys:
                - last_updated: ISO format timestamp
                - species: Dictionary of species data keyed by normalized name
        """
        metadata_structure = {
            "last_updated": datetime.now().isoformat(),
            "species": {}
        }

        with self.metadata_db.session_scope() as session:
            genome_data = self._load_all_genome_data(session)
            logger.info(f"Loaded data for {len(genome_data)} genomes")

            for genome_uuid, data in genome_data.items():
                try:
                    self._process_genome_data(data, metadata_structure)
                except Exception as e:
                    logger.error(f"Error processing genome {genome_uuid}: {e}")
                    continue

        return metadata_structure

    def _load_all_genome_data(self, session) -> Dict[str, Dict]:
        """
        Load all genome data in bulk queries to minimize database round trips.

        Performs optimized bulk queries to fetch genomes, datasets, attributes,
        and genebuild metadata for all released genomes.

        Args:
            session: Database session

        Returns:
            Dictionary mapping genome_uuid to genome data containing:
                - genome: Genome object
                - datasets: List of dataset information
                - attributes: Dataset attributes
                - genebuild_metadata: Genebuild-specific metadata
        """
        logger.info("Loading genome data from database")

        # Query for genomes with released datasets
        genomes_query = select(Genome).options(
            selectinload(Genome.organism),
            selectinload(Genome.assembly),
            selectinload(Genome.genome_releases).selectinload(GenomeRelease.ensembl_release)
        ).outerjoin(GenomeRelease).outerjoin(EnsemblRelease).outerjoin(GenomeDataset).outerjoin(Dataset).where(
            (Dataset.status == DatasetStatus.RELEASED) |
            (EnsemblRelease.status == ReleaseStatus.RELEASED)
        ).distinct()

        genomes = session.execute(genomes_query).scalars().all()
        genome_uuids = [g.genome_uuid for g in genomes]

        if not genome_uuids:
            logger.warning("No genomes found with released datasets")
            return {}

        logger.info(f"Found {len(genome_uuids)} genomes with released datasets")

        # Bulk fetch datasets
        datasets_query = select(
            Genome.genome_uuid,
            Dataset,
            DatasetType.name.label('dataset_type_name'),
            DatasetSource.name.label('dataset_source_name')
        ).select_from(
            GenomeDataset
        ).join(
            Genome, GenomeDataset.genome_id == Genome.genome_id
        ).join(
            Dataset, GenomeDataset.dataset_id == Dataset.dataset_id
        ).join(
            DatasetType, Dataset.dataset_type_id == DatasetType.dataset_type_id
        ).join(
            DatasetSource, Dataset.dataset_source_id == DatasetSource.dataset_source_id
        ).where(
            Genome.genome_uuid.in_(genome_uuids),
            Dataset.status == DatasetStatus.RELEASED
        ).options(
            selectinload(Dataset.genome_datasets).selectinload(GenomeDataset.ensembl_release)
        )

        dataset_results = session.execute(datasets_query).all()
        dataset_ids = [r.Dataset.dataset_id for r in dataset_results]

        # Bulk fetch attributes
        attributes_query = select(
            DatasetAttribute.dataset_id,
            Attribute.name.label('attribute_name'),
            DatasetAttribute.value
        ).join(
            Attribute, DatasetAttribute.attribute_id == Attribute.attribute_id
        ).where(
            DatasetAttribute.dataset_id.in_(dataset_ids)
        ) if dataset_ids else select().where(False)

        attribute_results = session.execute(attributes_query).all()

        # Bulk fetch genebuild metadata
        genebuild_query = select(
            Genome.genome_uuid,
            Organism.scientific_name,
            Assembly.accession,
            func.max(case(
                (Attribute.name == 'genebuild.annotation_source', DatasetAttribute.value)
            )).label('genebuild_source_name'),
            func.max(case(
                (Attribute.name == 'genebuild.last_geneset_update', DatasetAttribute.value)
            )).label('last_geneset_update')
        ).select_from(
            Genome
        ).join(
            Organism, Genome.organism_id == Organism.organism_id
        ).join(
            Assembly, Genome.assembly_id == Assembly.assembly_id
        ).join(
            GenomeDataset, Genome.genome_id == GenomeDataset.genome_id
        ).join(
            Dataset, GenomeDataset.dataset_id == Dataset.dataset_id
        ).join(
            DatasetType, Dataset.dataset_type_id == DatasetType.dataset_type_id
        ).join(
            DatasetAttribute, Dataset.dataset_id == DatasetAttribute.dataset_id
        ).join(
            Attribute, DatasetAttribute.attribute_id == Attribute.attribute_id
        ).where(
            Genome.genome_uuid.in_(genome_uuids),
            DatasetType.name == DATASET_TYPE_GENEBUILD,
            Attribute.name.in_(['genebuild.annotation_source', 'genebuild.last_geneset_update'])
        ).group_by(
            Genome.genome_uuid,
            Organism.scientific_name,
            Assembly.accession
        )

        genebuild_results = session.execute(genebuild_query).all()

        # Organize data structures
        genome_data = {}
        for genome in genomes:
            genome_data[genome.genome_uuid] = {
                'genome': genome,
                'datasets': [],
                'attributes': {},
                'genebuild_metadata': None
            }

        # Map datasets to genomes
        for result in dataset_results:
            if result.genome_uuid in genome_data:
                genome_data[result.genome_uuid]['datasets'].append({
                    'dataset': result.Dataset,
                    'dataset_type_name': result.dataset_type_name,
                    'dataset_source_name': result.dataset_source_name
                })

        # Map attributes to datasets
        attributes_by_dataset = defaultdict(dict)
        for result in attribute_results:
            attributes_by_dataset[result.dataset_id][result.attribute_name] = result.value

        for genome_uuid, data in genome_data.items():
            for dataset_info in data['datasets']:
                dataset_id = dataset_info['dataset'].dataset_id
                dataset_info['attributes'] = attributes_by_dataset.get(dataset_id, {})

        # Map genebuild metadata to genomes
        for result in genebuild_results:
            if result.genome_uuid in genome_data:
                genome_data[result.genome_uuid]['genebuild_metadata'] = {
                    'scientific_name': result.scientific_name,
                    'accession': result.accession,
                    'genebuild_source_name': result.genebuild_source_name,
                    'last_geneset_update': result.last_geneset_update
                }

        return genome_data

    def _process_genome_data(self, data: Dict, metadata_structure: Dict) -> None:
        """
        Process a single genome's data using preloaded information.

        Adds the genome's organism and assembly information to the metadata
        structure and delegates dataset processing.

        Args:
            data: Genome data dictionary containing genome, datasets, and metadata
            metadata_structure: Target metadata structure to populate
        """
        genome = data['genome']
        organism = genome.organism
        assembly = genome.assembly

        if not organism or not assembly:
            logger.warning(f"Skipping genome {genome.genome_uuid}: missing organism or assembly")
            return

        species_key = self._normalize_species_name(organism.scientific_name)

        # Initialize species entry if not present
        if species_key not in metadata_structure["species"]:
            metadata_structure["species"][species_key] = {
                "taxid": organism.taxonomy_id,
                "species_taxonomy_id": organism.species_taxonomy_id,
                "scientific_name": organism.scientific_name,
                "common_name": organism.common_name,
                "strain": organism.strain,
                "strain_type": organism.strain_type,
                "biosample_id": organism.biosample_id,
                "assemblies": {}
            }

        assembly_key = assembly.accession

        # Initialize assembly entry if not present
        if assembly_key not in metadata_structure["species"][species_key]["assemblies"]:
            metadata_structure["species"][species_key]["assemblies"][assembly_key] = {
                "name": getattr(assembly, 'name', None),
                "level": getattr(assembly, 'level', None),
                "genebuild_providers": {}
            }

        assembly_data = metadata_structure["species"][species_key]["assemblies"][assembly_key]
        self._process_genome_datasets(data, assembly_data)

    def _process_genome_datasets(self, genome_data: Dict, assembly_data: Dict) -> None:
        """
        Process all datasets for a genome using preloaded data.

        Uses GenomeAdaptor to get public paths for all dataset types and
        generates file paths for each. Organizes data by provider and release.

        Args:
            genome_data: Dictionary containing genome, datasets, and metadata
            assembly_data: Target assembly data structure to populate
        """
        genome = genome_data['genome']
        datasets = genome_data['datasets']
        genebuild_metadata = genome_data['genebuild_metadata']

        if not genebuild_metadata or not genebuild_metadata.get('last_geneset_update'):
            logger.debug(f"Skipping genome {genome.genome_uuid}: missing genebuild metadata")
            return

        genome_uuid = genome.genome_uuid

        # Extract release and provider information
        genebuild_release_info = self._extract_genebuild_release_info(genebuild_metadata)
        provider_for_path = self._extract_provider_name(genebuild_metadata)
        release_key = genebuild_release_info["release"]

        # Initialize provider/release structure
        if provider_for_path not in assembly_data["genebuild_providers"]:
            assembly_data["genebuild_providers"][provider_for_path] = {}

        if release_key not in assembly_data["genebuild_providers"][provider_for_path]:
            assembly_data["genebuild_providers"][provider_for_path][release_key] = {
                "paths": {}
            }

        release_data = assembly_data["genebuild_providers"][provider_for_path][release_key]

        # Group datasets by type
        datasets_by_type = {}
        for dataset_info in datasets:
            dtype = dataset_info['dataset_type_name']
            if dtype not in datasets_by_type:
                datasets_by_type[dtype] = []
            datasets_by_type[dtype].append(dataset_info)

        # Process genebuild and assembly (one per genebuild release)
        for dataset_type in [DATASET_TYPE_GENEBUILD, DATASET_TYPE_ASSEMBLY]:
            if dataset_type not in datasets_by_type:
                continue

            try:
                path_info = self._generate_path_for_dataset_type(
                    genebuild_metadata, dataset_type
                )
                base_path = path_info['path']

                file_paths = self._get_dataset_file_paths(
                    base_path, dataset_type, genome, assembly_data
                )

                release_data["paths"][dataset_type] = {
                    "files": file_paths
                }
            except (ValueError, TypeNotFoundException) as e:
                logger.warning(f"Error generating path for {dataset_type} in genome {genome_uuid}: {e}")

        # Process homologies (can have multiple per genebuild, grouped by partial release)
        if DATASET_TYPE_HOMOLOGIES in datasets_by_type:
            homologies_by_release = {}

            for dataset_info in datasets_by_type[DATASET_TYPE_HOMOLOGIES]:
                dataset = dataset_info['dataset']
                partial_release_label = self._get_partial_release_label(dataset)

                if partial_release_label not in homologies_by_release:
                    homologies_by_release[partial_release_label] = []
                homologies_by_release[partial_release_label].append(dataset_info)

            # Create structure for each partial release
            if DATASET_TYPE_HOMOLOGIES not in release_data["paths"]:
                release_data["paths"][DATASET_TYPE_HOMOLOGIES] = {}

            for partial_label, dataset_list in homologies_by_release.items():
                try:
                    base_path = self._generate_path_for_homology_variation(
                        genebuild_metadata, DATASET_TYPE_HOMOLOGIES, partial_label
                    )

                    file_paths = self._get_dataset_file_paths(
                        base_path, DATASET_TYPE_HOMOLOGIES, genome, assembly_data
                    )

                    release_data["paths"][DATASET_TYPE_HOMOLOGIES][partial_label] = {
                        "files": file_paths
                    }
                except (ValueError, TypeNotFoundException) as e:
                    logger.warning(
                        f"Error generating homology path for genome {genome_uuid}, release {partial_label}: {e}")

        # Process variations (can have multiple per genebuild, grouped by partial release)
        if DATASET_TYPE_VARIATION in datasets_by_type:
            variations_by_release = {}

            for dataset_info in datasets_by_type[DATASET_TYPE_VARIATION]:
                dataset = dataset_info['dataset']
                partial_release_label = self._get_partial_release_label(dataset)

                if partial_release_label not in variations_by_release:
                    variations_by_release[partial_release_label] = []
                variations_by_release[partial_release_label].append(dataset_info)

            # Create structure for each partial release
            if DATASET_TYPE_VARIATION not in release_data["paths"]:
                release_data["paths"][DATASET_TYPE_VARIATION] = {}

            for partial_label, dataset_list in variations_by_release.items():
                try:
                    base_path = self._generate_path_for_homology_variation(
                        genebuild_metadata, DATASET_TYPE_VARIATION, partial_label
                    )

                    file_paths = self._get_dataset_file_paths(
                        base_path, DATASET_TYPE_VARIATION, genome, assembly_data
                    )

                    release_data["paths"][DATASET_TYPE_VARIATION][partial_label] = {
                        "files": file_paths
                    }
                except (ValueError, TypeNotFoundException) as e:
                    logger.warning(
                        f"Error generating variation path for genome {genome_uuid}, release {partial_label}: {e}")


    def _normalize_species_name(self, scientific_name: str) -> str:
        """
        Normalize species name for use as a key.

        Replaces spaces with underscores, dots with underscores, and merges
        multiple consecutive underscores into a single underscore.

        Args:
            scientific_name: Original scientific name

        Returns:
            Normalized species name suitable for use as a dictionary key
        """
        normalized = scientific_name.replace(' ', '_')
        normalized = normalized.replace('.', '_')
        normalized = re.sub(r'_+', '_', normalized)
        return normalized

    def _extract_provider_name(self, genebuild_metadata: Dict) -> str:
        """
        Extract the provider component from genebuild metadata.

        Args:
            genebuild_metadata: Dictionary containing genebuild metadata

        Returns:
            Lowercase provider name, or 'unknown' if not found
        """
        if not genebuild_metadata or not genebuild_metadata.get('genebuild_source_name'):
            return 'unknown'

        provider = genebuild_metadata['genebuild_source_name'].lower()
        return provider

    def _extract_genebuild_release_info(self, genebuild_metadata: Dict) -> Dict[str, str]:
        """
        Extract release information from genebuild metadata.

        Parses the last_geneset_update field to extract the release date
        in YYYY_MM format.

        Args:
            genebuild_metadata: Dictionary containing genebuild metadata

        Returns:
            Dictionary with 'release' key containing the formatted release date
            or 'unknown' if parsing fails
        """
        release_info = {
            "release": "unknown"
        }

        if genebuild_metadata and genebuild_metadata.get('last_geneset_update'):
            last_geneset_update = genebuild_metadata['last_geneset_update']
            match = re.match(r'^(\d{4}-\d{2})', last_geneset_update)
            if match:
                release_info["release"] = match.group(1).replace('-', '_')

        return release_info

    def _generate_paths_from_metadata(
            self,
            genebuild_metadata: Dict,
            dataset_types: Set[str]
    ) -> List[Dict[str, str]]:
        """
        Generate public FTP paths directly from preloaded metadata.

        This avoids additional database queries by using data that was already
        loaded in bulk. Replicates the logic from GenomeAdaptor.get_public_path
        but operates on preloaded data.

        Args:
            genebuild_metadata: Dictionary containing genebuild metadata
            dataset_types: Set of available dataset type names

        Returns:
            List of dictionaries with 'dataset_type' and 'path' keys

        Raises:
            ValueError: If required metadata fields are missing
        """
        scientific_name = genebuild_metadata.get('scientific_name')
        accession = genebuild_metadata.get('accession')
        genebuild_source_name = genebuild_metadata.get('genebuild_source_name')
        last_geneset_update = genebuild_metadata.get('last_geneset_update')

        # Validate required fields
        missing_fields = []
        if not scientific_name:
            missing_fields.append("scientific_name")
        if not accession:
            missing_fields.append("assembly.accession")
        if not genebuild_source_name:
            missing_fields.append("genebuild.annotation_source")
        if not last_geneset_update:
            missing_fields.append("genebuild.last_geneset_update")

        if missing_fields:
            raise ValueError(
                f"Required metadata fields are missing: {', '.join(missing_fields)}"
            )

        # Format the release date
        match = re.match(r'^(\d{4}-\d{2})', last_geneset_update)
        if not match:
            raise ValueError(f"Invalid last_geneset_update format: {last_geneset_update}")
        formatted_release = match.group(1).replace('-', '_')

        # Build paths
        genebuild_source_name = genebuild_source_name.lower()
        base_path = format_accession_path(accession)
        common_path = f"{base_path}/{genebuild_source_name}/{formatted_release}"

        path_templates = {
            DATASET_TYPE_GENEBUILD: f"{common_path}/geneset",
            DATASET_TYPE_ASSEMBLY: f"{common_path}/genome",
            DATASET_TYPE_HOMOLOGIES: f"{common_path}/homology/{formatted_release}",
            DATASET_TYPE_VARIATION: f"{common_path}/variation/{formatted_release}",
        }

        # Generate paths only for available dataset types
        paths = []
        for dataset_type in dataset_types:
            if dataset_type in path_templates:
                paths.append({
                    "dataset_type": dataset_type,
                    "path": path_templates[dataset_type]
                })

        return paths

    def _get_dataset_file_paths(
            self,
            base_path: str,
            dataset_type: str,
            genome: Genome,
            assembly_data: Dict
    ) -> Dict[str, Dict[str, str]]:
        """
        Generate specific file paths for a dataset type.

        Creates the complete file structure with .bgz extensions (except for
        .gff and .txt files) based on the dataset type.

        Args:
            base_path: Base path from GenomeAdaptor
            dataset_type: Type of dataset (genebuild, assembly, etc.)
            genome: Genome object
            assembly_data: Assembly data structure

        Returns:
            Dictionary of file categories with their respective file paths
        """
        file_paths = {}

        if dataset_type == DATASET_TYPE_GENEBUILD:
            file_paths = {
                "annotations": {
                    "cdna.fa.bgz": f"{base_path}/cdna.fa.bgz",
                    "genes.embl.bgz": f"{base_path}/genes.embl.bgz",
                    "genes.gff3.bgz": f"{base_path}/genes.gff3.bgz",
                    "genes.gtf.bgz": f"{base_path}/genes.gtf.bgz",
                    "pep.fa.bgz": f"{base_path}/pep.fa.bgz",
                    "xref.tsv.gz": f"{base_path}/xref.tsv.gz"
                }
            }

        elif dataset_type == DATASET_TYPE_ASSEMBLY:
            file_paths = {
                "genome_sequences": {
                    "chromosomes.tsv.gz": f"{base_path}/chromosomes.tsv.gz",
                    "hardmasked.fa.bgz": f"{base_path}/hardmasked.fa.bgz",
                    "softmasked.fa.bgz": f"{base_path}/softmasked.fa.bgz",
                    "unmasked.fa.bgz": f"{base_path}/unmasked.fa.bgz"
                }
            }

        elif dataset_type == DATASET_TYPE_HOMOLOGIES:

            file_paths = {
                "homology_data": {
                    "homology.txt.gz": f"{base_path}/homology.txt.gz"
                }
            }

        elif dataset_type == DATASET_TYPE_VARIATION:
            file_paths = {
                "variation_data": {
                    "variation.vcf.bgz": f"{base_path}/variation.vcf.bgz"
                }
            }

        return file_paths


def main() -> None:
    """Main entry point for the script."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate FTP metadata in JSON and/or Parquet format for Ensembl releases"
    )
    parser.add_argument(
        "--metadata-uri",
        required=True,
        help="Database URI for the metadata database"
    )
    parser.add_argument(
        "--output-path",
        default="species",
        help="Base output path for the metadata files (default: species). "
             "Extensions (.json, .parquet) will be added automatically."
    )
    parser.add_argument(
        "--formats",
        nargs='+',
        choices=["json", "parquet"],
        default=None,
        help="Output format(s): json and/or parquet. Default: both formats"
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
        exporter = FTPIndexGenerator(
            metadata_uri=args.metadata_uri,
        )
        exporter.export(args.output_path, formats=args.formats)

        formats_str = "both formats" if args.formats is None else " and ".join(args.formats)
        logger.info(f"FTP metadata export completed successfully in {formats_str}")
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error generating FTP metadata: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
