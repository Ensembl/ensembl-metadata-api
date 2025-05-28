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
    paths = paths.split(',')
    paths = [p.strip() for p in paths]
    print(paths)
    for path in paths:
        if not os.path.isdir(path):
            raise argparse.ArgumentTypeError(f"The directory '{path}' does not exist.")
    return paths


def main(json_input, release_id, destinations, rename_files=None, status="Submitted"):
    try:
        with open(json_input, 'r') as f:
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
                    if name == "regulatory_features":
                        dest_dir = f"{dest_dir}/regulatory-features{source.suffix}"
                    shutil.copy2(item["dataset_source"]["name"], dest_dir)

                    print(f"Copied files from {source} to {dest_dir}.")
    except Exception as e:
        logger.error("An Error occurred:")
        logger.error(e)
        # logger.error()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="File handover script."
    )
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
        help="Fetch Dataset Based on dataset type Ex: genebuild",
    )
    parser.add_argument(
        "--json_file_path",
        type=str,
        required=True,
        help="Path to JSON file handed over by teams ",
    )

    parser.add_argument(
        "--dest_dirs",
        type=check_directory,
        required=True,
        help="Datafiles destination directory(s). You can seprate directories with "," EX:dir1,dir2",
    )

    ARGS = parser.parse_args()
    logger.info(f"Provided Arguments  {ARGS} ")

    main(json_input=ARGS.json_file_path, release_id=ARGS.release_id, destinations=ARGS.destinations, rename_files=ARGS.rename_files)

