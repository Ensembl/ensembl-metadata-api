#!/usr/bin/env python3
"""
Fetch FTP paths for genomes from metadata database.

This script retrieves FTP relative paths for genome resources and optionally
validates that expected files exist at those paths.
"""
import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Optional, List, Dict, Any

from ensembl.production.metadata.api.adaptors.genome import GenomeAdaptor
from ensembl.production.metadata.api.factories.genomes import GenomeFactory

def get_ftp_paths(metadata_uri, taxonomy_uri, genome_uuid, dataset_type_filter=None):
    """
    Prepare FTP relative paths for the given genome uuid from metadata.

    Args:
        metadata_uri: Metadata database URI
        taxonomy_uri: Taxonomy database URI
        genome_uuid: Genome UUID
        dataset_type_filter: Optional dataset type to filter (e.g., 'assembly', 'genebuild')

    Returns:
        List of dicts with 'dataset_type' and 'path' keys, optionally filtered by dataset_type
    """
    paths = GenomeAdaptor(metadata_uri, taxonomy_uri).get_public_path(genome_uuid)

    # Filter by dataset_type if specified
    if dataset_type_filter and isinstance(paths, list):
        paths = [p for p in paths if p.get('dataset_type') == dataset_type_filter]

    return paths

def validate_expected_files(base_path, ftp_paths, expected_files, resource_label):
    """
    Validate that resource paths exist and contain all expected files.

    Args:
        base_path: Base path for validation
        ftp_paths: Can be:
            - List of dicts with 'dataset_type' and 'path' keys
            - List of strings
            - Single string
            - Dict
        expected_files: List of expected files to validate
        resource_label: Label for logging
    """
    if isinstance(ftp_paths, list):
        # Check if it's a list of dicts with 'path' key
        if ftp_paths and isinstance(ftp_paths[0], dict) and 'path' in ftp_paths[0]:
            for path_info in ftp_paths:
                dataset_type = path_info.get('dataset_type', 'unknown')
                path = path_info.get('path')
                validate_single_path(base_path, path, expected_files, f"{resource_label} ({dataset_type})")
        else:
            # List of strings
            for path in ftp_paths:
                validate_single_path(base_path, path, expected_files, resource_label)
    elif isinstance(ftp_paths, dict):
        # Handle dict format (for backwards compatibility)
        for label, path in ftp_paths.items():
            if isinstance(path, list):
                for p in path:
                    validate_single_path(base_path, p, expected_files, f"{resource_label} - {label}")
            else:
                validate_single_path(base_path, path, expected_files, f"{resource_label} - {label}")
    else:
        # Single string path
        validate_single_path(base_path, ftp_paths, expected_files, resource_label)


def validate_single_path(base_path, relative_path, expected_files, resource_label):
    """Validate that a single resource path exists and contains all expected files."""
    resource_path = Path(base_path) / relative_path
    assert resource_path.exists(), f"{resource_label} path does not exist: {resource_path}"

    missing_files = []
    for expected_file in expected_files:
        expected_path = resource_path / expected_file
        if any(char in expected_file for char in "*?[]"):
            if not list(resource_path.glob(expected_file)):
                missing_files.append(expected_file)
        elif not expected_path.exists():
            missing_files.append(expected_file)

    assert not missing_files, (
        f"Missing {resource_label} files in {resource_path}: {missing_files}"
    )


def setup_logging(verbose: bool = False) -> None:
    """Configure logging for the script."""
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Fetch FTP paths for genomes from metadata database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch paths for a single genome
  %(prog)s --metadata-uri mysql://user:pass@host/db --genome-uuid abc-123

  # Fetch paths for all genomes in the database
  %(prog)s --metadata-uri mysql://user:pass@host/db

  # Fetch paths for all genomes in a specific division
  %(prog)s --metadata-uri mysql://user:pass@host/db \\
           --division EnsemblVertebrates

  # Fetch paths for specific species
  %(prog)s --metadata-uri mysql://user:pass@host/db \\
           --species homo_sapiens mus_musculus

  # Fetch paths with dataset type filter (affects both genome selection and FTP paths)
  %(prog)s --metadata-uri mysql://user:pass@host/db \\
           --dataset-type variation \\
           --dataset-status Released

  # Get only variation FTP paths for a specific genome
  %(prog)s --metadata-uri mysql://user:pass@host/db \\
           --genome-uuid abc-123 \\
           --dataset-type variation

  # Fetch and validate files exist (validates all paths if --dataset-type not specified)
  %(prog)s --metadata-uri mysql://user:pass@host/db \\
           --genome-uuid abc-123 \\
           --base-path /ftp/ensembl \\
           --validate

  # Output as JSON for all genomes in a division
  %(prog)s --metadata-uri mysql://user:pass@host/db \\
           --division EnsemblPlants \\
           --output-format json
        """
    )

    # Required arguments
    parser.add_argument(
        '--metadata-uri',
        required=True,
        help='Metadata database URI (e.g., mysql://user:pass@host:port/dbname)'
    )

    parser.add_argument(
        '--genome-uuid',
        help='Genome UUID to fetch FTP paths for (if not provided, fetches all genomes)'
    )

    # Optional arguments
    parser.add_argument(
        '--taxonomy-uri',
        help='Taxonomy database URI (optional, uses metadata-uri if not specified)'
    )

    parser.add_argument(
        '--base-path',
        type=Path,
        help='Base FTP path for validation (required if --validate is used)'
    )

    # GenomeFactory filter arguments
    parser.add_argument(
        '--dataset-uuid',
        nargs='*',
        help='Filter by dataset UUIDs'
    )

    parser.add_argument(
        '--division',
        nargs='*',
        help='Filter by divisions (e.g., EnsemblVertebrates, EnsemblPlants)'
    )

    parser.add_argument(
        '--organism-group-type',
        help='Organism group type to filter (e.g., Division, Popular)'
    )

    parser.add_argument(
        '--species',
        nargs='*',
        help='Filter by species production names (e.g., homo_sapiens, mus_musculus)'
    )

    parser.add_argument(
        '--antispecies',
        nargs='*',
        help='Exclude species production names (e.g., homo_sapiens, mus_musculus)'
    )

    parser.add_argument(
        '--dataset-type',
        help='Filter by dataset type (e.g., assembly, genebuild, variation, homologies). '
             'Used for both GenomeFactory filtering AND FTP path filtering'
    )

    parser.add_argument(
        '--dataset-names',
        nargs='*',
        help='Filter by dataset names (e.g., genebuild, assembly)'
    )

    parser.add_argument(
        '--dataset-is-current',
        action='store_true',
        help='Filter datasets that are marked as current'
    )

    parser.add_argument(
        '--dataset-status',
        nargs='*',
        choices=['Submitted', 'Processing', 'Processed', 'Released'],
        help='Filter by dataset status'
    )

    parser.add_argument(
        '--release-id',
        type=int,
        nargs='*',
        help='Filter by genome release IDs'
    )

    parser.add_argument(
        '--dataset-release-id',
        type=int,
        nargs='*',
        help='Filter by dataset release IDs'
    )

    parser.add_argument(
        '--release-name',
        type=int,
        nargs='*',
        help='Filter by release names'
    )

    parser.add_argument(
        '--release-type',
        choices=['partial', 'integrated'],
        default='partial',
        help='Filter by release type (default: partial)'
    )

    parser.add_argument(
        '--batch-size',
        type=int,
        help='Number of results to retrieve per batch'
    )

    parser.add_argument(
        '--page',
        type=int,
        help='Page number for pagination'
    )

    parser.add_argument(
        '--run-all',
        action='store_true',
        help='Run for all divisions (sets division to all Ensembl divisions)'
    )

    parser.add_argument(
        '--validate',
        action='store_true',
        help='Validate that expected files exist at the FTP paths'
    )

    parser.add_argument(
        '--expected-files',
        nargs='*',
        help='List of expected files to validate (supports glob patterns)'
    )

    parser.add_argument(
        '--output-format',
        choices=['text', 'json'],
        default='text',
        help='Output format (default: text)'
    )

    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )

    parser.add_argument(
        '--version',
        action='version',
        version='%(prog)s 1.0.0'
    )

    args = parser.parse_args()

    # Validation
    if args.validate and not args.base_path:
        parser.error('--base-path is required when --validate is used')

    return args


def fetch_all_genome_uuids(metadata_uri: str, **filters) -> List[str]:
    """
    Fetch all genome UUIDs using GenomeFactory with optional filters.

    Args:
        metadata_uri: Metadata database URI
        **filters: Additional filters for GenomeFactory

    Returns:
        List of genome UUIDs
    """
    logger = logging.getLogger(__name__)
    genome_factory = GenomeFactory()
    genome_uuids = []

    logger.info("Fetching all genome UUIDs using GenomeFactory")

    # Build filters for GenomeFactory
    factory_filters = {
        'metadata_db_uri': metadata_uri,
    }

    # Map all optional filters if provided
    filter_mapping = {
        'dataset_uuid': 'dataset_uuid',
        'division': 'division',
        'organism_group_type': 'organism_group_type',
        'species': 'species',
        'antispecies': 'antispecies',
        'dataset_type': 'dataset_type',
        'dataset_names': 'dataset_names',
        'dataset_is_current': 'dataset_is_current',
        'dataset_status': 'dataset_status',
        'release_id': 'release_id',
        'dataset_release_id': 'dataset_release_id',
        'release_name': 'release_name',
        'release_type': 'release_type',
        'batch_size': 'batch_size',
        'page': 'page',
        'run_all': 'run_all',
    }

    for arg_key, filter_key in filter_mapping.items():
        value = filters.get(arg_key)
        if value is not None:
            # Convert boolean flags and handle special cases
            if isinstance(value, bool):
                factory_filters[filter_key] = 1 if value else 0
            else:
                factory_filters[filter_key] = value

    logger.debug(f"GenomeFactory filters: {factory_filters}")

    for genome_info in genome_factory.get_genomes(**factory_filters):
        genome_uuid = genome_info.get('genome_uuid')
        if genome_uuid and genome_uuid not in genome_uuids:
            genome_uuids.append(genome_uuid)

    logger.info(f"Found {len(genome_uuids)} unique genome(s)")
    return genome_uuids


def main() -> int:
    """Main entry point for the script."""
    args = parse_args()
    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)

    try:
        # Use metadata_uri for taxonomy if not specified
        taxonomy_uri = args.taxonomy_uri or args.metadata_uri

        # Determine genome UUIDs to process
        if args.genome_uuid:
            genome_uuids = [args.genome_uuid]
            logger.info(f"Processing single genome UUID: {args.genome_uuid}")
        else:
            logger.info("No genome UUID provided, fetching all genomes from metadata database")

            # Build filters for GenomeFactory from command-line arguments
            filters = {}

            # Add all filter parameters
            filter_params = [
                'dataset_uuid', 'division', 'organism_group_type', 'species',
                'antispecies', 'dataset_type', 'dataset_names', 'dataset_is_current',
                'dataset_status', 'release_id', 'dataset_release_id', 'release_name',
                'release_type', 'batch_size', 'page', 'run_all'
            ]

            for param in filter_params:
                # Convert hyphenated args to underscored filter names
                arg_name = param.replace('_', '_')
                value = getattr(args, param, None)
                if value is not None and (value != [] if isinstance(value, list) else True):
                    filters[param] = value

            genome_uuids = fetch_all_genome_uuids(args.metadata_uri, **filters)

            if not genome_uuids:
                logger.error("No genomes found with the specified filters")
                return 1

        logger.debug(f"Metadata URI: {args.metadata_uri}")
        logger.debug(f"Taxonomy URI: {taxonomy_uri}")

        # Process each genome UUID
        all_results = {}
        failed_genomes = []

        for genome_uuid in genome_uuids:
            logger.info(f"Fetching FTP paths for genome UUID: {genome_uuid}")

            try:
                # Fetch FTP paths with optional dataset type filter
                ftp_paths = get_ftp_paths(
                    args.metadata_uri,
                    taxonomy_uri,
                    genome_uuid,
                    dataset_type_filter=args.dataset_type
                )

                if not ftp_paths:
                    logger.warning(f"No FTP paths found for genome UUID: {genome_uuid}")
                    failed_genomes.append(genome_uuid)
                    continue

                logger.info(f"Successfully retrieved {len(ftp_paths) if isinstance(ftp_paths, list) else 1} FTP path(s) for {genome_uuid}")

                # Validate files if requested
                if args.validate:
                    logger.info(f"Validating file existence for {genome_uuid}...")
                    expected_files = args.expected_files or []
                    validation_failed = False

                    try:
                        validate_expected_files(
                            args.base_path,
                            ftp_paths,
                            expected_files,
                            genome_uuid
                        )

                        # Log success based on format
                        if isinstance(ftp_paths, list) and ftp_paths and isinstance(ftp_paths[0], dict):
                            for path_info in ftp_paths:
                                dataset_type = path_info.get('dataset_type', 'unknown')
                                logger.info(f"✓ {genome_uuid} ({dataset_type}): validation passed")
                        else:
                            logger.info(f"✓ {genome_uuid}: validation passed")
                    except AssertionError as e:
                        logger.error(f"✗ {genome_uuid}: {e}")
                        validation_failed = True

                    if validation_failed:
                        failed_genomes.append(genome_uuid)
                        continue

                # Store results
                all_results[genome_uuid] = ftp_paths

            except Exception as e:
                logger.error(f"Error processing genome {genome_uuid}: {e}", exc_info=args.verbose)
                failed_genomes.append(genome_uuid)

        # Output results
        if not all_results:
            logger.error("No FTP paths retrieved successfully")
            return 1

        if args.output_format == 'json':
            print(json.dumps(all_results, indent=2, default=str))
        else:
            for genome_uuid, ftp_paths in all_results.items():
                print(f"\n{'='*60}")
                print(f"Genome UUID: {genome_uuid}")
                print(f"{'='*60}")

                # Handle list of dicts with 'dataset_type' and 'path'
                if isinstance(ftp_paths, list) and ftp_paths and isinstance(ftp_paths[0], dict):
                    for path_info in ftp_paths:
                        dataset_type = path_info.get('dataset_type', 'unknown')
                        path = path_info.get('path', 'N/A')
                        print(f"  {dataset_type}: {path}")
                elif isinstance(ftp_paths, dict):
                    for label, path in ftp_paths.items():
                        if isinstance(path, list):
                            print(f"  {label}:")
                            for p in path:
                                print(f"    - {p}")
                        else:
                            print(f"  {label}: {path}")
                elif isinstance(ftp_paths, list):
                    print("  Paths:")
                    for path in ftp_paths:
                        print(f"    - {path}")
                else:
                    print(f"  {ftp_paths}")

        # Summary
        logger.info(f"\nSummary: {len(all_results)} genome(s) processed successfully")
        if failed_genomes:
            logger.warning(f"{len(failed_genomes)} genome(s) failed: {', '.join(failed_genomes)}")

        return 0 if not failed_genomes else 1

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=args.verbose)
        return 1


if __name__ == '__main__':
    sys.exit(main())