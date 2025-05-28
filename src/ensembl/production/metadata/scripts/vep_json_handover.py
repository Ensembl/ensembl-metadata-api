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
from ensembl.production.metadata.api.models import EnsemblRelease, Genome, Dataset, GenomeDataset, \
    DatasetType, DatasetSource


def main(json_input, conn_uri, release_id=None, status="Submitted"):
    with open(json_input, 'r') as f:
        data = json.load(f)
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
            if release_id is None:
                # Attach the same way as the handover service.
                release = session.query(EnsemblRelease).filter(EnsemblRelease.status == "Planned").one_or_none()
            else:
                release = session.query(EnsemblRelease).filter(EnsemblRelease.release_id == release_id).one_or_none()

            if dataset_type == "vep_assembly_feature":
                # Get the assembly dataset for loading as a parent
                assembly_dataset = session.query(Dataset).join(GenomeDataset).join(Genome).join(DatasetType).filter(
                    DatasetType.name == "assembly",
                    Genome.genome_uuid == genome_uuid
                ).one_or_none()
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
                    parent=assembly_dataset,
                    release=release,
                    is_current=True
                )
                print(f"Created dataset UUID: {dataset_uuid}")
                # Check if an existing dataset exists for this genome and dataset name
                result_datasets = session.query(Dataset).join(GenomeDataset).join(Genome).filter(
                    Dataset.name == name,
                    Genome.genome_uuid == genome_uuid
                ).all()
                if len(result_datasets) > 1:
                    # Set the other datasets to not current
                    for ds in result_datasets:
                        if ds.dataset_uuid != dataset_uuid:
                            ds.is_current = False
                session.commit()

            elif dataset_type == "vep_genome_feature":
                # Check to see if a vep dataset already exists for this genome
                vep_dataset = session.query(Dataset).join(GenomeDataset).join(Genome).join(DatasetType).filter(
                    Genome.genome_uuid == genome_uuid).filter(DatasetType.name == "vep"
                                                              ).one_or_none()

                if not vep_dataset:
                    # Create a new VEP dataset
                    # Use the same source as the genebuild dataset
                    genebuild_source = session.query(DatasetSource).join(Dataset).join(GenomeDataset).join(
                        Genome).join(DatasetType).filter(
                        Genome.genome_uuid == genome_uuid,
                        DatasetType.name == "genebuild"
                    ).one_or_none()
                    dataset_uuid, vep_dataset, new_dataset_attributes, new_genome_dataset = dataset_factory.create_dataset(
                        session=session,
                        genome_input=genome_uuid,
                        dataset_source=genebuild_source.name,
                        source_type=genebuild_source.type,
                        dataset_type="vep",
                        name="vep",
                        label="vep",
                        version="1.0",
                        status=status,
                        release=release,
                        dataset_attributes=None,
                        is_current=True
                    )

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
                    parent=vep_dataset,
                    release=release,
                    is_current=True
                )
                print(f"Created dataset UUID: {dataset_uuid} with parent VEP dataset {vep_dataset.dataset_uuid}")
                result_datasets = session.query(Dataset).join(GenomeDataset).join(Genome).filter(
                    Dataset.name == name,
                    Genome.genome_uuid == genome_uuid
                ).all()
                if len(result_datasets) > 1:
                    # Set the other datasets to not current
                    for ds in result_datasets:
                        if ds.dataset_uuid != dataset_uuid:
                            ds.is_current = False
                session.commit()


if __name__ == "__main__":
    # Expecting JSON input, release id, and connection URI as command-line arguments
    if len(sys.argv) < 4:
        print("Usage: python create_datasets.py <json_input> <release_id> <conn_uri> <status(optional)>")
        sys.exit(1)

    json_input = sys.argv[1]
    release_id = sys.argv[2]
    conn_uri = sys.argv[3]
    main(json_input, release_id, conn_uri)
