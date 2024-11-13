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
#   limitations under the License.`

from ensembl.utils.database import DBConnection

from ensembl.production.metadata.api.models import DatasetSource, Genome, GenomeDataset, Dataset


def GetSourceFromGenomeUUID(meta_url, genome_uuid, dataset_name="genebuild"):
    metadata_db = DBConnection(meta_url)
    with metadata_db.session_scope() as session:
        query = (
            session.query(DatasetSource.name)
            .select_from(Genome)
            .join(GenomeDataset, Genome.genome_id == GenomeDataset.genome_id)
            .join(Dataset, GenomeDataset.dataset_id == Dataset.dataset_id)
            .join(DatasetSource, Dataset.dataset_source_id == DatasetSource.dataset_source_id)
            .filter(Genome.genome_uuid == genome_uuid)
            .filter(Dataset.name == dataset_name)
        )

        result = query.one_or_none()
    if result is None:
        raise ValueError(f"No source found for dataset named {dataset_name}, attached to genome {genome_uuid}")
    else:
        return str(result[0])


def GetGenomeUUIDFromSource(meta_url, dataset_source_name, dataset_name="genebuild"):
    metadata_db = DBConnection(meta_url)
    with metadata_db.session_scope() as session:
        query = (
            session.query(Genome.genome_uuid)
            .select_from(DatasetSource)
            .join(Dataset, DatasetSource.dataset_source_id == Dataset.dataset_source_id)
            .join(GenomeDataset, Dataset.dataset_id == GenomeDataset.dataset_id)
            .join(Genome, GenomeDataset.genome_id == Genome.genome_id)
            .filter(DatasetSource.name == dataset_source_name)
            .filter(Dataset.name == dataset_name)
        )

        result = query.one_or_none()
    if result is None:
        raise ValueError(f"No genome found for source named {dataset_source_name} with dataset {dataset_name}")
    else:
        return str(result[0])
