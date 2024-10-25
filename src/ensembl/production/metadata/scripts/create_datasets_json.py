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


import json
import sys

from ensembl.utils.database import DBConnection

from ensembl.production.metadata.api.factories.datasets import DatasetFactory


def main(json_input, release_id, conn_uri, status="Submitted"):
    with open(json_input, 'r') as f:
        data = json.load(f)
    release = release_id
    metadata_db = DBConnection(conn_uri)

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
            # Create the main dataset
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
            session.commit()
            # Populate child datasets
            children = dataset_factory.create_all_child_datasets(
                dataset_uuid=dataset_uuid,
                session=session,
                topic="production_process",
                status=None,
                release=release
            )
            session.commit()

            print(f"Created dataset UUID: {dataset_uuid} with children")


if __name__ == "__main__":
    # Expecting JSON input, release id, and connection URI as command-line arguments
    if len(sys.argv) < 4:
        print("Usage: python create_datasets.py <json_input> <release_id> <conn_uri> <status(optional)>")
        sys.exit(1)

    json_input = sys.argv[1]
    release_id = sys.argv[2]
    conn_uri = sys.argv[3]

    main(json_input, release_id, conn_uri)
