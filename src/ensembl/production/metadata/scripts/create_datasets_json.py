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

from ensembl.utils.database import DBConnection

from ensembl.production.metadata.api.factories.datasets import DatasetFactory

from ensembl.production.metadata.api.models import Dataset, Genome, GenomeDataset, EnsemblRelease, Attribute, \
    DatasetAttribute

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


def check_directory(path: str) -> str:
    if not os.path.isdir(path):
        raise argparse.ArgumentTypeError(f"The directory '{path}' does not exist.")
    return path


def main(json_input, release_id, conn_uri, destination, status="Submitted"):
    try:
        with open(json_input, 'r') as f:
            data = json.load(f)
        metadata_db = DBConnection(conn_uri)
    except Exception as e:
        logger.error(e)
        raise e
    try:
        with metadata_db.session_scope() as session:
            for item in data:
                genome_uuid = item["genome_uuid"]
                dataset_source = item["dataset_source"]["name"]
                source_type = item["dataset_source"]["type"]
                dataset_type = item["dataset_type"]
                dataset_attributes = {attr["name"]: attr["value"] for attr in item["dataset_attribute"]}
                name = item["name"]
                label = item["label"]
                version = item.get("version", None)
                dataset_factory = DatasetFactory(conn_uri)
                
                try:
                    release = session.query(EnsemblRelease).filter(EnsemblRelease.name == release_id).one_or_none()
                    print(release)
                except Exception as e:
                    session.rollback()
                    logger.error("An Error occurred:")
                    logger.error(e)

                # Create the main dataset
                try:
                    old_genome_datasets = session.query(GenomeDataset) \
                        .join(Genome, GenomeDataset.genome_id == Genome.genome_id) \
                        .join(Dataset, GenomeDataset.dataset_id == Dataset.dataset_id) \
                        .join(DatasetAttribute, DatasetAttribute.dataset_id == Dataset.dataset_id) \
                        .join(Attribute, DatasetAttribute.attribute_id == Attribute.attribute_id) \
                        .filter(Genome.genome_uuid == genome_uuid) \
                        .filter(Attribute.name.in_(list(dataset_attributes.keys()))) \
                        .filter(GenomeDataset.is_current==1) \
                        .all()
                    print(old_genome_datasets)
                except Exception as e:
                    session.rollback()
                    logger.error("An Error occurred:")
                    logger.error(e)

                for old_genome_dataset in old_genome_datasets:
                    children = session.query(GenomeDataset) \
                            .join(Dataset,Dataset.dataset_id==GenomeDataset.dataset_id) \
                            .filter(Dataset.parent_id == old_genome_dataset.dataset_id) \
                            .filter(GenomeDataset.is_current == 1) \
                            .all()
                    print(children)
                    for child in children:
                        # child_child = session.query(GenomeDataset).join(Dataset,Dataset.parent_id==child.dataset_id).filter(GenomeDataset.is_current == 1).all()
                        child_child = session.query(GenomeDataset) \
                            .join(Dataset,Dataset.dataset_id==GenomeDataset.dataset_id) \
                            .filter(Dataset.parent_id == child.dataset_id) \
                            .filter(GenomeDataset.is_current == 1) \
                            .all()
                        print(child)
                        print(child_child)
                        child.is_current = 0
                     #   if child:
                        for ch in child_child:
                            ch.is_current = 0
                    old_genome_dataset.is_current = 0

                dataset_uuid, new_dataset, new_dataset_attributes, new_genome_dataset = dataset_factory.create_dataset(
                    session=session,
                    genome_input=genome_uuid,
                    dataset_source=dataset_source,
                    dataset_type=dataset_type,
                    dataset_attributes=dataset_attributes,
                    name=name,
                    label=label,
                    version=version,
                    status=status,
                    source_type=source_type,
                    release=release,
                    is_current=True
                )
                print(dataset_uuid)

                # Populate child datasets

                children = dataset_factory.create_all_child_datasets(
                    dataset_uuid=dataset_uuid,
                    session=session,
                    topic="production_process",
                    status=status,
                    release=release
                )
                print(children)
                session.commit()
                dest_dir = f"{destination}{genome_uuid}/"
                source = Path(item["dataset_source"]["name"])
                #dest_dir = Path(dest_dir)
                #dest_dir.mkdir(parents=True, exist_ok=True)
                #if name == "regulatory_features":
                #    dest_dir = f"{dest_dir}/regulatory-features{source.suffix}"
                #shutil.copy2(item["dataset_source"]["name"], dest_dir)

                print(f"Created dataset UUID: {dataset_uuid} with children")
    except Exception as e:
        session.rollback()
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
        "--metadata_db_uri",
        type=str,
        required=True,
        help="metadata db mysql uri, ex: mysql://ensro@localhost:3366/ensembl_genome_metadata",
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
        "--dest_dir",
        type=check_directory,
        required=True,
        help="Datafiles destination directory.",
    )

    ARGS = parser.parse_args()
    logger.info(f"Provided Arguments  {ARGS} ")

    main(ARGS.json_file_path, ARGS.release_id, ARGS.metadata_db_uri, ARGS.dest_dir)

