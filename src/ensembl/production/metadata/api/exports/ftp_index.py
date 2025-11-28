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
import argparse
import json
import re
import sys
from collections import defaultdict
from datetime import datetime

from ensembl.utils.database import DBConnection
from sqlalchemy import select, func, case
from sqlalchemy.orm import selectinload

from ensembl.production.metadata.api.exceptions import TypeNotFoundException
from ensembl.production.metadata.api.models.assembly import Assembly
from ensembl.production.metadata.api.models.dataset import Dataset, DatasetType, DatasetAttribute, DatasetSource, \
    DatasetStatus, Attribute
from ensembl.production.metadata.api.models.genome import Genome, GenomeDataset, GenomeRelease
from ensembl.production.metadata.api.models.organism import Organism
from ensembl.production.metadata.api.models.release import EnsemblRelease, ReleaseStatus


class FTPMetadataExporter:
    """
    Independent class for generating FTP metadata JSON structure.
    Builds hierarchical data organized by species -> assemblies -> providers -> releases.
    """

    def __init__(self, metadata_uri):
        self.metadata_db = DBConnection(metadata_uri)

    def export_to_json(self, output_file=None):
        """
        Export FTP metadata to JSON format.

        Args:
            output_file: Optional file path to write JSON. If None, returns dict.

        Returns:
            dict: Metadata structure if output_file is None
        """
        metadata = self.build_ftp_metadata_json()

        if output_file:
            with open(output_file, 'w') as f:
                json.dump(metadata, f, indent=2, default=str)
            return None
        else:
            return metadata

    def build_ftp_metadata_json(self):
        """
        Build a hierarchical data structure for JSON export containing FTP metadata.
        Only includes released datasets.
        """
        metadata_structure = {
            "last_updated": datetime.now().isoformat(),
            "species": {}
        }

        with self.metadata_db.session_scope() as session:
            genome_data = self._load_all_genome_data(session)

            for genome_uuid, data in genome_data.items():
                self._process_genome_data(data, metadata_structure)

        return metadata_structure

    def _load_all_genome_data(self, session):
        """Load all genome data in bulk queries to minimize database round trips."""

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
            return {}

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
            (Dataset.status == DatasetStatus.RELEASED)
        )

        dataset_results = session.execute(datasets_query).all()
        dataset_ids = [r.Dataset.dataset_id for r in dataset_results]

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

        genebuild_query = select(
            Genome.genome_uuid,
            Organism.scientific_name,
            Assembly.accession,
            func.max(case(
                (Attribute.name == 'genebuild.annotation_source', DatasetAttribute.value),
                else_=None
            )).label('genebuild_source_name'),
            func.max(case(
                (Attribute.name == 'genebuild.last_geneset_update', DatasetAttribute.value),
                else_=None
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
            DatasetType.name == 'genebuild',
            Attribute.name.in_(['genebuild.annotation_source', 'genebuild.last_geneset_update'])
        ).group_by(
            Genome.genome_uuid,
            Organism.scientific_name,
            Assembly.accession
        )

        genebuild_results = session.execute(genebuild_query).all()

        genome_data = {}

        for genome in genomes:
            genome_data[genome.genome_uuid] = {
                'genome': genome,
                'datasets': [],
                'attributes': {},
                'genebuild_metadata': None
            }

        for result in dataset_results:
            if result.genome_uuid in genome_data:
                genome_data[result.genome_uuid]['datasets'].append({
                    'dataset': result.Dataset,
                    'dataset_type_name': result.dataset_type_name,
                    'dataset_source_name': result.dataset_source_name
                })

        attributes_by_dataset = defaultdict(dict)
        for result in attribute_results:
            attributes_by_dataset[result.dataset_id][result.attribute_name] = result.value

        for genome_uuid, data in genome_data.items():
            for dataset_info in data['datasets']:
                dataset_id = dataset_info['dataset'].dataset_id
                dataset_info['attributes'] = attributes_by_dataset.get(dataset_id, {})

        for result in genebuild_results:
            if result.genome_uuid in genome_data:
                genome_data[result.genome_uuid]['genebuild_metadata'] = {
                    'scientific_name': result.scientific_name,
                    'accession': result.accession,
                    'genebuild_source_name': result.genebuild_source_name,
                    'last_geneset_update': result.last_geneset_update
                }

        return genome_data

    def _process_genome_data(self, data, metadata_structure):
        """Process a single genome's data using preloaded information."""

        genome = data['genome']
        organism = genome.organism
        assembly = genome.assembly

        species_key = self._normalize_species_name(organism.scientific_name)

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

        if assembly_key not in metadata_structure["species"][species_key]["assemblies"]:
            metadata_structure["species"][species_key]["assemblies"][assembly_key] = {
                "name": getattr(assembly, 'name', None),
                "level": getattr(assembly, 'level', None),
                "genebuild_providers": {},
                "assembly": None
            }

        assembly_data = metadata_structure["species"][species_key]["assemblies"][assembly_key]
        self._process_genome_datasets_bulk(data, assembly_data)

    def _process_genome_datasets_bulk(self, genome_data, assembly_data):
        """Process all datasets for a genome using preloaded data."""

        genome = genome_data['genome']
        datasets = genome_data['datasets']
        genebuild_metadata = genome_data['genebuild_metadata']

        genome_is_released = any(
            gr.ensembl_release and gr.ensembl_release.status == ReleaseStatus.RELEASED
            for gr in genome.genome_releases
        )

        if genebuild_metadata and genebuild_metadata.get('last_geneset_update'):
            genebuild_release_info = self._extract_genebuild_release_info(genebuild_metadata)
        else:
            genebuild_release_info = {"release": "unknown"}

        provider_datasets = defaultdict(lambda: defaultdict(list))
        assembly_dataset_info = None
        seen_homology_paths = set()

        for dataset_info in datasets:
            dataset = dataset_info['dataset']
            dataset_type = dataset_info['dataset_type_name']

            provider_for_path = self._extract_provider_from_path(genebuild_metadata)

            if dataset_type == 'assembly':
                assembly_dataset_info = dataset_info
            else:
                if provider_for_path not in assembly_data["genebuild_providers"]:
                    assembly_data["genebuild_providers"][provider_for_path] = {}

                release_key = genebuild_release_info["release"]
                if release_key not in assembly_data["genebuild_providers"][provider_for_path]:
                    assembly_data["genebuild_providers"][provider_for_path][release_key] = {
                        "release": genebuild_release_info["release"],
                        "paths": {}
                    }

                if dataset_type == 'homologies':
                    try:
                        if genebuild_metadata:
                            temp_paths = self._get_public_paths_bulk(genebuild_metadata, datasets, dataset_type)
                            if temp_paths:
                                homology_path = temp_paths[0]["path"]
                                if homology_path in seen_homology_paths:
                                    continue
                                seen_homology_paths.add(homology_path)
                    except:
                        pass

                provider_datasets[provider_for_path][release_key].append(dataset_info)

        if assembly_dataset_info and genebuild_metadata:
            try:
                assembly_paths = self._get_public_paths_bulk(genebuild_metadata, datasets, 'assembly')
                if assembly_paths:
                    assembly_path = assembly_paths[0]["path"]
                    file_paths = self._get_dataset_file_paths(
                        assembly_path, 'assembly', genome, assembly_data
                    )

                    assembly_data["assembly"] = {
                        "files": file_paths
                    }
            except Exception as e:
                print(f"Error generating assembly paths for genome {genome.genome_uuid}: {e}")

        for provider, releases in provider_datasets.items():
            for release_key, dataset_list in releases.items():
                release_data = assembly_data["genebuild_providers"][provider][release_key]

                try:
                    if genebuild_metadata:
                        paths = self._get_public_paths_bulk(genebuild_metadata, datasets)

                        for path_info in paths:
                            dataset_type = path_info["dataset_type"]

                            if dataset_type == 'assembly':
                                continue

                            if self._has_released_dataset_bulk(datasets, dataset_type):
                                file_paths = self._get_dataset_file_paths(
                                    path_info["path"], dataset_type, genome, assembly_data
                                )

                                release_data["paths"][dataset_type] = {
                                    "files": file_paths
                                }

                except Exception as e:
                    error_msg = f"Error generating paths for genome {genome.genome_uuid}: {e}"
                    if "Required metadata fields are missing" in str(e):
                        error_msg += f" (Provider: {provider}, Release: {release_key})"
                    print(error_msg)

    def _get_public_paths_bulk(self, genebuild_metadata, datasets, dataset_type='all'):
        """Generate public FTP paths using preloaded metadata."""

        if not genebuild_metadata:
            return []

        scientific_name = genebuild_metadata['scientific_name']
        accession = genebuild_metadata['accession']
        genebuild_source_name = genebuild_metadata['genebuild_source_name']
        last_geneset_update = genebuild_metadata['last_geneset_update']

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
                f"Required metadata fields are missing: {', '.join(missing_fields)}. Please check the database entries.")

        unique_dataset_types = list(set([
            'regulation' if d['dataset_type_name'] == 'regulatory_features'
            else d['dataset_type_name']
            for d in datasets
        ]))

        if dataset_type == 'regulatory_features':
            dataset_type = 'regulation'

        match = re.match(r'^(\d{4}-\d{2})', last_geneset_update)
        if match:
            last_geneset_update = match.group(1).replace('-', '_')

        scientific_name = self._normalize_species_name(scientific_name)
        genebuild_source_name = genebuild_source_name.lower()
        base_path = f"{scientific_name}/{accession}"
        common_path = f"{base_path}/{genebuild_source_name}"

        path_templates = {
            'genebuild': f"{common_path}/geneset/{last_geneset_update}",
            'assembly': f"{base_path}/genome",
            'homologies': f"{common_path}/homology/{last_geneset_update}",
            'regulation': f"{common_path}/regulation",
            'variation': f"{common_path}/variation/{last_geneset_update}",
        }

        paths = []

        if dataset_type not in unique_dataset_types and dataset_type != 'all':
            raise TypeNotFoundException(f"Dataset Type : {dataset_type} not found in metadata.")

        if dataset_type == 'all':
            for t in unique_dataset_types:
                if t in path_templates:
                    paths.append({
                        "dataset_type": t,
                        "path": path_templates[t]
                    })
        elif dataset_type in path_templates:
            paths.append({
                "dataset_type": dataset_type,
                "path": path_templates[dataset_type]
            })
        else:
            raise TypeNotFoundException(f"Dataset Type : {dataset_type} has no associated path.")

        return paths

    def _normalize_species_name(self, scientific_name):
        """Normalize species name by replacing dots with underscores and merging multiple underscores."""
        normalized = scientific_name.replace(' ', '_')
        normalized = normalized.replace('.', '_')
        normalized = re.sub(r'_+', '_', normalized)
        return normalized

    def _extract_provider_from_path(self, genebuild_metadata):
        """Extract the provider component from the genebuild metadata for use in paths."""
        if not genebuild_metadata or not genebuild_metadata.get('genebuild_source_name'):
            return 'unknown'

        provider = genebuild_metadata['genebuild_source_name'].lower()
        return provider

    def _extract_genebuild_release_info(self, genebuild_metadata):
        """Extract release information from genebuild metadata for use as release key/value."""
        release_info = {
            "release": "unknown"
        }

        if genebuild_metadata and genebuild_metadata.get('last_geneset_update'):
            last_geneset_update = genebuild_metadata['last_geneset_update']
            match = re.match(r'^(\d{4}-\d{2})', last_geneset_update)
            if match:
                release_info["release"] = match.group(1).replace('-', '_')

        return release_info

    def _extract_release_info_from_ensembl_release(self, genome):
        """Extract release information from ensembl_release table."""
        release_info = {
            "release": "unknown"
        }

        for genome_release in genome.genome_releases:
            if genome_release.ensembl_release and genome_release.ensembl_release.status == ReleaseStatus.RELEASED:
                ensembl_release = genome_release.ensembl_release
                if hasattr(ensembl_release, 'version'):
                    release_info["release"] = str(ensembl_release.version)
                elif hasattr(ensembl_release, 'release_number'):
                    release_info["release"] = str(ensembl_release.release_number)
                elif hasattr(ensembl_release, 'name'):
                    release_info["release"] = ensembl_release.name
                break

        return release_info

    def _has_released_dataset_bulk(self, datasets, dataset_type):
        """Check if there's a released dataset of the specified type using preloaded data."""

        type_mapping = {
            'regulation': 'regulatory_features',
            'genebuild': 'genebuild',
            'assembly': 'assembly',
            'homologies': 'homologies',
            'variation': 'variation'
        }

        mapped_type = type_mapping.get(dataset_type, dataset_type)

        return any(
            d['dataset_type_name'] == mapped_type
            for d in datasets
        )

    def _get_dataset_file_paths(self, base_path, dataset_type, genome, assembly_data):
        """Generate specific file paths for a dataset type."""

        file_paths = {}

        if dataset_type == 'genebuild':
            file_paths = {
                "annotations": {
                    "cdna.fa.gz": f"{base_path}/cdna.fa.gz",
                    "genes.embl.gz": f"{base_path}/genes.embl.gz",
                    "genes.gff3.gz": f"{base_path}/genes.gff3.gz",
                    "genes.gtf.gz": f"{base_path}/genes.gtf.gz",
                    "pep.fa.gz": f"{base_path}/pep.fa.gz",
                    "xref.tsv.gz": f"{base_path}/xref.tsv.gz"
                }
            }

            path_parts = base_path.split('/')
            if len(path_parts) >= 4:
                species = path_parts[0]
                assembly = path_parts[1]
                provider = path_parts[2]
                release = path_parts[4] if len(path_parts) > 4 else path_parts[3]
                vep_base = f"{species}/{assembly}/vep/{provider}/geneset/{release}"

                file_paths["vep"] = {
                    "genes.gff3.bgz": f"{vep_base}/genes.gff3.bgz",
                    "genes.gff3.bgz.csi": f"{vep_base}/genes.gff3.bgz.csi"
                }

        elif dataset_type == 'assembly':
            file_paths = {
                "genome_sequences": {
                    "chromosomes.tsv.gz": f"{base_path}/chromosomes.tsv.gz",
                    "hardmasked.fa.gz": f"{base_path}/hardmasked.fa.gz",
                    "softmasked.fa.gz": f"{base_path}/softmasked.fa.gz",
                    "unmasked.fa.gz": f"{base_path}/unmasked.fa.gz"
                }
            }

            path_parts = base_path.split('/')
            if len(path_parts) >= 2:
                species = path_parts[0]
                assembly = path_parts[1]
                vep_genome_base = f"{species}/{assembly}/vep/genome"

                file_paths["vep"] = {
                    "softmasked.fa.bgz": f"{vep_genome_base}/softmasked.fa.bgz",
                    "softmasked.fa.bgz.fai": f"{vep_genome_base}/softmasked.fa.bgz.fai",
                    "softmasked.fa.bgz.gzi": f"{vep_genome_base}/softmasked.fa.bgz.gzi"
                }

        elif dataset_type == 'homologies':
            organism = genome.organism
            assembly = genome.assembly
            species_name = self._normalize_species_name(organism.scientific_name)

            release = base_path.split('/')[-1] if '/' in base_path else "unknown"
            homology_filename = f"{species_name}-{assembly.accession}-{release}-homology.tsv.gz"

            file_paths = {
                "homology_data": {
                    homology_filename: f"{base_path}/{homology_filename}"
                }
            }

        elif dataset_type == 'variation':
            file_paths = {
                "variation_data": {
                    "variation.vcf.gz": f"{base_path}/variation.vcf.gz"
                }
            }

        elif dataset_type == 'regulation':
            file_paths = {
                "regulatory_features": {
                    "regulation.gff": f"{base_path}/regulation.gff"
                }
            }

        return file_paths


def main() -> None:
    """Main entry point for the script."""

    parser = argparse.ArgumentParser(
        description="Generate index files for the ftp"
    )
    parser.add_argument(
        "--metadata-uri",
        required=True,
        help="Database URI for the metadata database"
    )
    parser.add_argument(
        "--output-path",
        default="species.json",
        help="Optional output path for the stats files. Filenames will be: "
             "species.json Defaults to current directory."
    )
    args = parser.parse_args()

    try:
        exporter = FTPMetadataExporter(metadata_uri=args.metadata_uri)
        metadata = exporter.export_to_json(args.output_path)
        print(f"Metadata exported to {args.output_path}")
    except ValueError as e:
        print(e)
        sys.exit(1)
    except Exception as e:
        print(f"Error generating release statistics: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
