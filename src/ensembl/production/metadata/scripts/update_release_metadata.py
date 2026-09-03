#!/usr/bin/env python3
"""
This script updates the metadata for a genome release in the Ensembl production database. It allows you to set a release as partially released, which is useful for tracking the status of genome releases during the production process. The script takes the release name and database connection details, and it includes logging for better traceability of the operations performed.
"""
from __future__ import annotations

import argparse
import logging
import sys
from typing import Optional

from ensembl.production.metadata.api.factories.release import ReleaseFactory


def setup_logging(level: str = "INFO", log_file: Optional[str] = None) -> None:
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    handlers = []
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    else:
        handlers.append(logging.StreamHandler(sys.stdout))

    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=handlers,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Update genome release metadata.")
    parser.add_argument("--release-name", type=str, required=True, help="Release name (e.g. 14)")
    parser.add_argument(
        "--metadata-db-uri",
        type=str,
        required=True,
        help="Metadata database URI (e.g. mysql://user:pass@host:port/dbname)",
    )
    parser.add_argument("--log-file", type=str, help="Optional path to a log file")
    parser.add_argument(
        "--log-level",
        type=str,
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        default="INFO",
        help="Logging level",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    setup_logging(level=args.log_level, log_file=args.log_file)

    logger = logging.getLogger(__name__)
    logger.info("Starting release update: name=%s", args.release_name)

    try:
        rf = ReleaseFactory(args.metadata_db_uri)
    except Exception:
        logger.exception("Failed to initialise ReleaseFactory with URI: %s", args.metadata_db_uri)
        return 1

    try:
        release = rf.set_partial_released(release_name=args.release_name)
    except Exception:
        logger.exception("Failed to set release as partially released (name=%s)", args.release_name)
        return 2

    logger.info(
        "Release update finished successfully for name=%s (id=%s)",
        release.name,
        release.release_id,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
