#!/usr/bin/env python3
#  See the NOTICE file distributed with this work for additional information
#  regarding copyright ownership.
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#      http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""
FTP Metadata Export Wrapper Script

This script provides a command-line interface for exporting FTP metadata
from the Ensembl metadata database to JSON format.

Usage:
    python ftp_export.py --database-url mysql://user:pass@host:port/database  # Output to stdout
    python ftp_export.py -d mysql://user:pass@host:port/database --outfile species.json
    python ftp_export.py -d mysql://user:pass@host:port/database -o species.json
"""

import argparse
import json
import sys
import time
from pathlib import Path

from ensembl.production.metadata.api.exports.ftp_index import FTPMetadataExporter


def main():
    parser = argparse.ArgumentParser(
        description="Export FTP metadata from Ensembl metadata database to JSON (defaults to stdout)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s -d mysql://user:pass@host:3306/ensembl_metadata
  %(prog)s -d mysql://user:pass@host:3306/ensembl_metadata --outfile species.json
  %(prog)s -d mysql://user:pass@host:3306/ensembl_metadata -o /path/to/output.json -v
  %(prog)s -d mysql://user:pass@host:3306/ensembl_metadata | jq '.species | keys'
        """
    )

    parser.add_argument(
        '-d', '--database-url',
        required=True,
        help='Database connection URL (e.g., mysql://user:pass@host:port/database)'
    )

    parser.add_argument(
        '-o', '--outfile',
        help='Output JSON file path. If not specified, output goes to stdout'
    )

    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose output (sent to stderr when using stdout)'
    )

    parser.add_argument(
        '--indent',
        type=int,
        default=2,
        help='JSON indentation level (default: 2)'
    )

    args = parser.parse_args()

    # Validate output directory if writing to file
    if args.outfile:
        output_path = Path(args.outfile)
        output_dir = output_path.parent

        if not output_dir.exists():
            print(f"Error: Output directory '{output_dir}' does not exist", file=sys.stderr)
            sys.exit(1)

        if not output_dir.is_dir():
            print(f"Error: '{output_dir}' is not a directory", file=sys.stderr)
            sys.exit(1)

    # Initialize exporter
    if args.verbose:
        print(f"Connecting to database: {args.database_url}", file=sys.stderr)

    try:
        exporter = FTPMetadataExporter(args.database_url)
    except Exception as e:
        print(f"Error connecting to database: {e}", file=sys.stderr)
        sys.exit(1)

    # Export metadata
    if args.verbose:
        print("Exporting FTP metadata...", file=sys.stderr)
        start_time = time.time()

    try:
        if args.outfile:
            # Export directly to file
            exporter.export_to_json(args.outfile)

            if args.verbose:
                # Load the file to get species count for verbose output
                try:
                    with open(args.outfile, 'r') as f:
                        metadata = json.load(f)
                    species_count = len(metadata.get('species', {}))
                    print(f"Successfully exported {species_count} species to '{args.outfile}'", file=sys.stderr)
                except:
                    print(f"Successfully exported metadata to '{args.outfile}'", file=sys.stderr)
            else:
                print(f"Metadata exported to '{args.outfile}'")
        else:
            # Export to dictionary and output to stdout
            metadata = exporter.export_to_json()
            json.dump(metadata, sys.stdout, indent=args.indent, default=str)
            print()  # Add newline at end

            if args.verbose:
                species_count = len(metadata.get('species', {}))
                print(f"Successfully exported {species_count} species to stdout", file=sys.stderr)

    except Exception as e:
        print(f"Error during export: {e}", file=sys.stderr)
        sys.exit(1)

    if args.verbose:
        elapsed_time = time.time() - start_time
        print(f"Export completed in {elapsed_time:.2f} seconds", file=sys.stderr)


if __name__ == "__main__":
    main()
