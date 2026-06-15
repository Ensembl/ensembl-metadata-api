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


def format_accession_path(accession):
    """
    Convert an assembly accession to its FTP directory path format.

    E.g. GCA_000001405.29 -> GCA/000/001/405/29

    The numeric portion (zero-padded to 9 digits) is split into three 3-digit
    chunks, and the version suffix becomes the final path component.
    """
    match = re.match(r'^(GC[AF])_(\d+)\.(\d+)$', accession)
    if not match:
        raise ValueError(f"Invalid accession format: {accession}")
    prefix = match.group(1)
    digits = match.group(2).zfill(9)  # ensure 9 digits for the 3-chunk split
    version = match.group(3)
    chunks = [digits[i:i + 3] for i in range(0, len(digits), 3)]
    return f"{prefix}/{'/'.join(chunks)}/{version}"


class FTPMetadataExporter:
    """
    Independent class for generating FTP metadata JSON structure.
    Builds hierarchical data organised by species -> assemblies -> providers -> releases.

    New FTP path structure
    ----------------------
    {accession_path}/{annotation_source}/{geneset_date}/
        geneset/          <- genebuild files
        genome/           <- assembly / repeat-masked genome files
        homology/{date}/  <- homology files  (partial release date)
        variation/{date}/ <- variation files (partial release date, if present)

    Index files (.fai, .gzi, .csi) and checksum files (md5sum.txt) are
    intentionally excluded from the JSON listing.
    """

    def __init__(self, metadata_uri):
        self.metadata_db = DBConnection(metadata_uri)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    def _load_all_genome_data(self, session):
        """Load all genome data in bulk queries to minimise database round trips."""

        # --- Genomes with at least one released dataset or released genome-release ---
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

        # --- Released datasets for all genomes ---
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
        )

        dataset_results = session.execute(datasets_query).all()
        dataset_ids = [r.Dataset.dataset_id for r in dataset_results]

        # --- Dataset attributes ---
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

        # --- Genebuild metadata (annotation source + geneset date) ---
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

        # --- Partial release labels for homologies and variation ---
        # These provide the sub-directory date component under homology/ and variation/.
        # Mirrors the logic in get_public_path: partial releases, is_current=True, released status.
        partial_releases_query = select(
            Genome.genome_uuid,
            DatasetType.name.label('dataset_type_name'),
            EnsemblRelease.label
        ).select_from(
            EnsemblRelease
        ).join(
            GenomeDataset
        ).join(
            Dataset, GenomeDataset.dataset_id == Dataset.dataset_id
        ).join(
            DatasetType, Dataset.dataset_type_id == DatasetType.dataset_type_id
        ).join(
            Genome, GenomeDataset.genome_id == Genome.genome_id
        ).where(
            Genome.genome_uuid.in_(genome_uuids),
            DatasetType.name.in_(['homologies', 'short_variants']),
            Dataset.status == DatasetStatus.RELEASED,
            GenomeDataset.is_current == True,
            EnsemblRelease.release_type == 'partial'
        )

        partial_release_results = session.execute(partial_releases_query).all()

        # --- Assemble per-genome partial release lookup ---
        # genome_uuid -> { 'homologies': label, 'short_variants': label }
        partial_releases_by_genome = defaultdict(dict)
        for result in partial_release_results:
            partial_releases_by_genome[result.genome_uuid][result.dataset_type_name] = result.label

        # --- Build genome_data dict ---
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
                genome_partial = partial_releases_by_genome.get(result.genome_uuid, {})
                genome_data[result.genome_uuid]['genebuild_metadata'] = {
                    'scientific_name': result.scientific_name,
                    'accession': result.accession,
                    'genebuild_source_name': result.genebuild_source_name,
                    'last_geneset_update': result.last_geneset_update,
                    # Partial release labels used as sub-directory names for homology/variation.
                    # None means the dataset type has no released partial release and will be omitted.
                    'homology_release': genome_partial.get('homologies'),
                    'variation_release': genome_partial.get('short_variants'),
                }

        return genome_data

    # ------------------------------------------------------------------
    # Processing
    # ------------------------------------------------------------------

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

        if genebuild_metadata and genebuild_metadata.get('last_geneset_update'):
            genebuild_release_info = self._extract_genebuild_release_info(genebuild_metadata)
        else:
            genebuild_release_info = {"release": "unknown"}

        provider_datasets = defaultdict(lambda: defaultdict(list))
        assembly_dataset_info = None
        seen_homology_paths = set()

        for dataset_info in datasets:
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
                    except Exception:
                        pass

                provider_datasets[provider_for_path][release_key].append(dataset_info)

        # --- Assembly / genome files ---
        if assembly_dataset_info and genebuild_metadata:
            try:
                assembly_paths = self._get_public_paths_bulk(genebuild_metadata, datasets, 'assembly')
                if assembly_paths:
                    file_paths = self._get_dataset_file_paths(assembly_paths[0]["path"], 'assembly')
                    assembly_data["assembly"] = {"files": file_paths}
            except Exception as e:
                print(f"Error generating assembly paths for genome {genome.genome_uuid}: {e}")

        # --- All other dataset types ---
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
                                    path_info["path"], dataset_type
                                )
                                release_data["paths"][dataset_type] = {"files": file_paths}

                except Exception as e:
                    error_msg = f"Error generating paths for genome {genome.genome_uuid}: {e}"
                    if "Required metadata fields are missing" in str(e):
                        error_msg += f" (Provider: {provider}, Release: {release_key})"
                    print(error_msg)

    # ------------------------------------------------------------------
    # Path construction
    # ------------------------------------------------------------------

    def _get_public_paths_bulk(self, genebuild_metadata, datasets, dataset_type='all'):
        """
        Generate public FTP paths using preloaded metadata.

        New structure:
            {accession_path}/{annotation_source}/{geneset_date}/geneset/
            {accession_path}/{annotation_source}/{geneset_date}/genome/
            {accession_path}/{annotation_source}/{geneset_date}/homology/{partial_release}/
            {accession_path}/{annotation_source}/{geneset_date}/variation/{partial_release}/
        """
        if not genebuild_metadata:
            return []

        accession = genebuild_metadata['accession']
        genebuild_source_name = genebuild_metadata['genebuild_source_name']
        last_geneset_update = genebuild_metadata['last_geneset_update']
        homology_release = genebuild_metadata.get('homology_release')
        variation_release = genebuild_metadata.get('variation_release')

        # Validate required fields
        missing_fields = []
        if not accession:
            missing_fields.append("assembly.accession")
        if not genebuild_source_name:
            missing_fields.append("genebuild.annotation_source")
        if not last_geneset_update:
            missing_fields.append("genebuild.last_geneset_update")
        if missing_fields:
            raise ValueError(
                f"Required metadata fields are missing: {', '.join(missing_fields)}. "
                "Please check the database entries."
            )

        # Normalise geneset date to YYYY_MM
        match = re.match(r'^(\d{4}-\d{2})', last_geneset_update)
        if not match:
            raise ValueError(f"Invalid last_geneset_update format: {last_geneset_update}")
        geneset_date = match.group(1).replace('-', '_')

        # Normalise partial release labels to YYYY_MM_DD (or whatever format they carry)
        if homology_release:
            homology_release = homology_release.replace('-', '_')
        if variation_release:
            variation_release = variation_release.replace('-', '_')

        # Collect the dataset types actually present for this genome
        unique_dataset_types = list(set([d["dataset_type_name"] for d in datasets]))

        # Drop homologies / variation if there is no partial release date available
        if not homology_release and 'homologies' in unique_dataset_types:
            unique_dataset_types = [t for t in unique_dataset_types if t != 'homologies']
        if not variation_release and "short_variants" in unique_dataset_types:
            unique_dataset_types = [t for t in unique_dataset_types if t != "short_variants"]

        # Build paths
        accession_path = format_accession_path(accession)
        genebuild_source_name = genebuild_source_name.lower()
        common_path = f"{accession_path}/{genebuild_source_name}/{geneset_date}"

        path_templates = {
            'genebuild': f"{common_path}/geneset",
            'assembly': f"{common_path}/genome",
            'homologies': f"{common_path}/homology/{homology_release}",
            'short_variants': f"{common_path}/variation/{variation_release}",
        }

        if dataset_type not in unique_dataset_types and dataset_type != 'all':
            raise TypeNotFoundException(f"Dataset Type : {dataset_type} not found in metadata.")

        paths = []
        if dataset_type == 'all':
            for t in unique_dataset_types:
                if t in path_templates:
                    paths.append({"dataset_type": t, "path": path_templates[t]})
        elif dataset_type in path_templates:
            paths.append({"dataset_type": dataset_type, "path": path_templates[dataset_type]})
        else:
            raise TypeNotFoundException(f"Dataset Type : {dataset_type} has no associated path.")

        return paths

    # ------------------------------------------------------------------
    # File listings
    # ------------------------------------------------------------------

    def _get_dataset_file_paths(self, base_path, dataset_type, *_, **__):
        """
        Return the explicit file listings for a dataset type under base_path.

        Index files (.fai, .gzi, .csi) and checksum files (md5sum.txt) are
        intentionally excluded.
        """

        if dataset_type == 'genebuild':
            # geneset/ directory
            filenames = [
                "cdna.fa.bgz",
                "genes.embl.gz",
                "genes.gff3.bgz",
                "genes.gff3.gz",
                "genes.gtf.bgz",
                "genes.gtf.gz",
                # The following are not alwalys present so we can't automatically generate them. No good solution for now.
                # "genes-including_alt.embl.gz",
                # "genes-including_alt.gff3.bgz",
                # "genes-including_alt.gff3.gz",
                # "genes-including_alt.gtf.bgz",
                # "genes-including_alt.gtf.gz", #
                "pep.fa.bgz",
                "xref.tsv.gz",
            ]
            return {
                "annotations": {f: f"{base_path}/{f}" for f in filenames}
            }

        elif dataset_type == 'assembly':
            # genome/ directory
            filenames = [
                "chromosomes.tsv.gz",
                "hardmasked.fa.bgz",
                "softmasked.fa.bgz",
                "unmasked.fa.bgz",
            ]
            return {
                "genome_sequences": {f: f"{base_path}/{f}" for f in filenames}
            }

        elif dataset_type == 'homologies':
            # homology/{partial_release}/ — single file
            return {
                "homology_data": {
                    "homology.tsv.gz": f"{base_path}/homology.tsv.gz"
                }
            }

        elif dataset_type == 'short_variants':
            # variation/{partial_release}/ — single file
            return {
                "variation_data": {
                    "variation.vcf.gz": f"{base_path}/variation.vcf.gz"
                }
            }
        return {}

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _normalize_species_name(self, scientific_name):
        """Normalise species name for use as a JSON key."""
        scientific_name = re.sub(r'[^a-zA-Z0-9]+', ' ', scientific_name)
        scientific_name = scientific_name.replace(' ', '_')
        scientific_name = re.sub(r'^_+|_+$', '', scientific_name)
        return scientific_name

    def _extract_provider_from_path(self, genebuild_metadata):
        """Return the annotation source component used as the provider key."""
        if not genebuild_metadata or not genebuild_metadata.get('genebuild_source_name'):
            return 'unknown'
        return genebuild_metadata['genebuild_source_name'].lower()

    def _extract_genebuild_release_info(self, genebuild_metadata):
        """Extract the YYYY_MM geneset date for use as the release key in the JSON."""
        release_info = {"release": "unknown"}
        if genebuild_metadata and genebuild_metadata.get('last_geneset_update'):
            match = re.match(r'^(\d{4}-\d{2})', genebuild_metadata['last_geneset_update'])
            if match:
                release_info["release"] = match.group(1).replace('-', '_')
        return release_info

    def _extract_release_info_from_ensembl_release(self, genome):
        """Extract release information from ensembl_release table (retained for reference)."""
        release_info = {"release": "unknown"}
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
        """Check if a released dataset of the given type exists in the preloaded data."""
        type_mapping = {
            "genebuild": "genebuild",
            "assembly": "assembly",
            "homologies": "homologies",
            "short_variants": "short_variants",
        }
        mapped_type = type_mapping.get(dataset_type, dataset_type)
        return any(d['dataset_type_name'] == mapped_type for d in datasets)


def main() -> None:
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Generate index files for the FTP"
    )
    parser.add_argument(
        "--metadata-uri",
        required=True,
        help="Database URI for the metadata database"
    )
    parser.add_argument(
        "--output-path",
        default="species.json",
        help="Output path for the JSON file. Defaults to species.json in the current directory."
    )
    args = parser.parse_args()

    try:
        exporter = FTPMetadataExporter(metadata_uri=args.metadata_uri)
        exporter.export_to_json(args.output_path)
        print(f"Metadata exported to {args.output_path}")
    except ValueError as e:
        print(e)
        sys.exit(1)
    except Exception as e:
        print(f"Error generating FTP metadata: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
