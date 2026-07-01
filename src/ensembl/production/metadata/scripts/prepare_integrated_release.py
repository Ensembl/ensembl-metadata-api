#!/usr/bin/env python3
"""Prepare a new integrated release from current partial release state."""
from __future__ import annotations

import argparse
import logging
import sys
from decimal import Decimal
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
    parser = argparse.ArgumentParser(description="Prepare a new integrated release.")
    parser.add_argument(
        "--release-name",
        type=str,
        required=True,
        help="Release name for the new integrated release (e.g. I2)",
    )
    parser.add_argument(
        "--release-version",
        type=str,
        required=True,
        help="Release version for the new integrated release (e.g. 200.0)",
    )
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
    logger.info(
        "Preparing integrated release: name=%s version=%s",
        args.release_name,
        args.release_version
    )

    try:
        rf = ReleaseFactory(args.metadata_db_uri)
    except Exception:
        logger.exception("Failed to initialise ReleaseFactory with URI: %s", args.metadata_db_uri)
        return 1

    try:
        version = Decimal(args.release_version)
    except Exception:
        logger.exception("Invalid release version: %s", args.release_version)
        return 2

    try:
        release = rf.prepare_integrated_release(
            version=version,
            name=args.release_name,
        )
    except Exception:
        logger.exception("Failed to prepare integrated release %s", args.release_name)
        return 3

    logger.info("Prepared integrated release %s with id=%s", release.name, release.release_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
