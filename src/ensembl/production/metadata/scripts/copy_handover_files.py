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
import logging
import os
import shutil
import sys
from pathlib import Path
from sqlalchemy.orm import joinedload
from ensembl.production.metadata.api.adaptors.genome import GenomeAdaptor

from ensembl.utils.database import DBConnection
from ensembl.production.metadata.api.factories.datasets import DatasetFactory
from ensembl.production.metadata.api.models import (
    Dataset,
    Genome,
    GenomeDataset,
    EnsemblRelease,
    Attribute,
    DatasetAttribute,
)

# Configure root logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Set minimum level for all handlers
logger.handlers.clear()
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"
)

# ERROR log handler
error_file_handler = logging.FileHandler("error.log")
error_file_handler.setLevel(logging.ERROR)
error_file_handler.setFormatter(formatter)
logger.addHandler(error_file_handler)

# Console handler (shows INFO and above)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

logger = logging.getLogger(__name__)


def check_directory(paths: str) -> list:
    paths = paths.split(",")
    paths = [p.strip() for p in paths]
    print(paths)
    for path in paths:
        if not os.path.isdir(path):
            raise argparse.ArgumentTypeError(f"The directory '{path}' does not exist.")
    return paths


def variation_tracks(json_input, release_id, destinations):
    try:
        with open(json_input, "r") as f:
            data = json.load(f)
    except Exception as e:
        logger.error(e)
        raise e
    try:
        for item in data:
            genome_uuid = item
            source_files = data[item]["datafiles"].values()
            for destination in destinations:
                dest_dir = f"{destination}{genome_uuid}/"
                dest_dir = Path(dest_dir)
                dest_dir.mkdir(parents=True, exist_ok=True)
                for source_file in source_files:
                    shutil.copy2(source_file, dest_dir)
    except:
        logger.error(e)
        raise e


def ftp_copy(json_input, destinations, metadata_db):
    try:
        with open(json_input, "r") as f:
            data = json.load(f)
    except Exception as e:
        logger.error(e)
        raise e
    try:
        print(metadata_db)
        for item in data:
            genome_uuid = item["genome_uuid"]
            dataset_source = item["dataset_source"]["name"]
            source_type = item["dataset_source"]["type"]
            dataset_type = item["dataset_type"]
            name = item["name"]
            label = item["label"]
            version = item.get("version", None)
            source = Path(item["dataset_source"]["name"])

            genome_public_paths = GenomeAdaptor(metadata_db, metadata_db).get_public_path(genome_uuid)
            destination_postfix = next(
                (item["path"] for item in genome_public_paths if item["dataset_type"] == dataset_type), None
            )
            for destination in destinations:
                dest_dir = f"{destination}{destination_postfix}/"
                dest_dir = Path(dest_dir)
                dest_dir.mkdir(parents=True, exist_ok=True)
                shutil.copy2(item["dataset_source"]["name"], dest_dir)
                print(f"Copied files from {source} to {dest_dir}.")
    except Exception as e:
        logger.error("An Error occurred:")
        logger.error(e)


def regulation_copy(json_input, release_id, destinations, rename_files=None):
    try:
        with open(json_input, "r") as f:
            data = json.load(f)
    except Exception as e:
        logger.error(e)
        raise e
    try:
        for item in data:
            genome_uuid = item["genome_uuid"]
            dataset_source = item["dataset_source"]["name"]
            source_type = item["dataset_source"]["type"]
            dataset_type = item["dataset_type"]
            dataset_attributes = {attr["name"]: attr["value"] for attr in item["dataset_attribute"]}
            name = item["name"]
            label = item["label"]
            version = item.get("version", None)
            source = Path(item["dataset_source"]["name"])
            for destination in destinations:
                dest_dir = f"{destination}{genome_uuid}/"
                dest_dir = Path(dest_dir)
                dest_dir.mkdir(parents=True, exist_ok=True)
                # Rename to standard track file name EX: regulatory-features.bb
                dest_dir = f"{dest_dir}/regulatory-features{source.suffix}"
                shutil.copy2(item["dataset_source"]["name"], dest_dir)

                print(f"Copied files from {source} to {dest_dir}.")
    except Exception as e:
        logger.error("An Error occurred:")
        logger.error(e)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="File handover script.")
    parser.add_argument(
        "--release_id",
        type=str,
        required=True,
        help="Release id.",
    )

    parser.add_argument(
        "--rename_files",
        type=str,
        required=False,
        help="Rename files into the provided string.",
    )
    parser.add_argument(
        "--dataset_type",
        type=str,
        required=True,
        help="Fetch Dataset Based on dataset type Ex: variation_tracks, vep, regulation",
    )
    parser.add_argument(
        "--json_file_path",
        type=str,
        required=True,
        help="Path to JSON file handed over by teams ",
    )

    parser.add_argument(
        "--destinations",
        type=check_directory,
        required=True,
        help="Datafiles destination directory(s). You can seprate directories with EX:dir1,dir2",
    )

    parser.add_argument(
        "--metadata_db_uri",
        type=str,
        required=False,
        help="metadata db mysql uri, ex: mysql://ensro@localhost:3366/ensembl_genome_metadata",
    )

    ARGS = parser.parse_args()
    logger.info(f"Provided Arguments  {ARGS} ")
    if ARGS.dataset_type == "variation_tracks":
        variation_tracks(
            json_input=ARGS.json_file_path, release_id=ARGS.release_id, destinations=ARGS.destinations
        )
    elif ARGS.dataset_type == "regulation":
        regulation_copy(
            json_input=ARGS.json_file_path,
            release_id=ARGS.release_id,
            destinations=ARGS.destinations,
            rename_files=ARGS.rename_files,
        )
    elif ARGS.dataset_type in ["vep", "variation"]:
        ftp_copy(
            json_input=ARGS.json_file_path, destinations=ARGS.destinations, metadata_db=ARGS.metadata_db_uri
        )
    else:
        raise ("Please spesify a proper dataset type. variation_tracks, vep, variation or regulation")
