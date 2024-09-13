#!/usr/bin/env python
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

'''
Fetch Genome Info From New Metadata Database
'''

import argparse
import json
import logging
import re
from dataclasses import dataclass, field
from typing import List

from ensembl.utils.database import DBConnection
from sqlalchemy import select

from ensembl.production.metadata.api.factories.datasets import DatasetFactory
from ensembl.production.metadata.api.models.dataset import DatasetType, Dataset, DatasetSource, DatasetStatus
from ensembl.production.metadata.api.models.genome import Genome, GenomeDataset, GenomeRelease
from ensembl.production.metadata.api.models.organism import Organism, OrganismGroup, OrganismGroupMember

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class GenomeInputFilters:
    metadata_db_uri: str
    genome_uuid: List[str] = field(default_factory=list)
    dataset_uuid: List[str] = field(default_factory=list)
    division: List[str] = field(default_factory=list)
    dataset_type: str = "assembly"
    species: List[str] = field(default_factory=list)
    antispecies: List[str] = field(default_factory=list)
    dataset_status: List[str] = field(default_factory=lambda: ["Submitted"])
    release_id: int = 0
    batch_size: int = 50
    page: int = 1
    organism_group_type: str = ''
    run_all: int = 0
    update_dataset_status: str = ""
    update_dataset_attribute: dict = field(default_factory=lambda: {})
    columns: List = field(default_factory=lambda: [Genome.genome_uuid.label('genome_uuid'),
                                                   Genome.production_name.label('species'),
                                                   Dataset.dataset_uuid.label('dataset_uuid'),
                                                   Dataset.status.label('dataset_status'),
                                                   DatasetSource.name.label('dataset_source'),
                                                   DatasetType.name.label('dataset_type')
                                                   ])


@dataclass
class GenomeFactory:
    @staticmethod
    def _apply_filters(query, filters):

        if filters.organism_group_type:
            query = query.filter(OrganismGroup.type == filters.organism_group_type)

        if filters.run_all:
            filters.division = [
                'EnsemblBacteria',
                'EnsemblVertebrates',
                'EnsemblPlants',
                'EnsemblProtists',
                'EnsemblMetazoa',
                'EnsemblFungi',
            ]

        if filters.genome_uuid:
            query = query.filter(Genome.genome_uuid.in_(filters.genome_uuid))

        if filters.dataset_uuid:
            query = query.filter(Dataset.dataset_uuid.in_(filters.dataset_uuid))

        if filters.division:
            ensembl_divisions = filters.division

            if filters.organism_group_type == 'DIVISION':
                pattern = re.compile(r'^(ensembl)?', re.IGNORECASE)
                ensembl_divisions = ['Ensembl' + pattern.sub('', d).capitalize() for d in ensembl_divisions if d]

            query = query.filter(OrganismGroup.name.in_(ensembl_divisions))

        if filters.species:
            species = set(filters.species) - set(filters.antispecies)

            if species:
                query = query.filter(Genome.production_name.in_(filters.species))
            else:
                query = query.filter(~Genome.production_name.in_(filters.antispecies))

        elif filters.antispecies:
            query = query.filter(~Genome.production_name.in_(filters.antispecies))

        if filters.release_id:
            query = query.join(Genome.genome_releases)
            query = query.filter(GenomeDataset.release_id==filters.release_id)
            query = query.filter(GenomeRelease.release_id==filters.release_id)

        if filters.dataset_type:
            query = query.filter(Genome.genome_datasets.any(DatasetType.name.in_([filters.dataset_type])))

        if filters.dataset_status:
            query = query.filter(Dataset.status.in_(filters.dataset_status))

        if filters.batch_size:
            filters.page = filters.page if filters.page > 0 else 1
            query = query.offset((filters.page - 1) * filters.batch_size).limit(filters.batch_size)
        logger.debug(f"Filter Query {query}")
        return query

    def _build_query(self, filters):
        query = select(*filters.columns) \
            .select_from(Genome) \
            .join(Genome.assembly) \
            .join(Genome.organism) \
            .join(Organism.organism_group_members) \
            .join(OrganismGroupMember.organism_group) \
            .join(Genome.genome_datasets) \
            .join(GenomeDataset.dataset) \
            .join(Dataset.dataset_source) \
            .join(Dataset.dataset_type) \
            .group_by(Genome.genome_id, Dataset.dataset_id) \
            .order_by(Genome.genome_uuid)

        return self._apply_filters(query, filters)

    def get_genomes(self, **filters: GenomeInputFilters):

        filters = GenomeInputFilters(**filters)
        logger.info(f'Get Genomes with filters {filters}')

        with DBConnection(filters.metadata_db_uri).session_scope() as session:
            query = self._build_query(filters)
            logger.info(f'Executing SQL query: {query}')
            for genome in session.execute(query).fetchall():
                genome_info = genome._asdict()
                dataset_uuid = genome_info.get('dataset_uuid', None)

                # convert status enum object to string value
                dataset_status = genome_info.get('dataset_status', None)
                if dataset_status and isinstance(dataset_status, DatasetStatus):
                    genome_info['dataset_status'] = dataset_status.value

                if not dataset_uuid:
                    logger.warning(
                        f"No dataset uuid found for genome {genome_info} skipping this genome "
                    )
                    continue

                if filters.update_dataset_status:
                    _, status = DatasetFactory(filters.metadata_db_uri) \
                        .update_dataset_status(dataset_uuid,
                                               filters.update_dataset_status,
                                               session=session)
                    if filters.update_dataset_status == status.value:
                        logger.info(
                            f"Updated Dataset status for dataset uuid: {dataset_uuid} from "
                            f"{filters.update_dataset_status} to {status} for genome {genome_info['genome_uuid']}"
                        )
                        genome_info['updated_dataset_status'] = status.value

                    else:
                        logger.warning(
                            f"Cannot update status for dataset uuid: {dataset_uuid} "
                            f"{filters.update_dataset_status} to {status}  for genome {genome['genome_uuid']}"
                        )
                        genome_info['updated_dataset_status'] = None
                session.flush()
                yield genome_info


def main():
    parser = argparse.ArgumentParser(
        prog='genomes.py',
        description='Fetch Ensembl genome info from the new metadata database'
    )
    parser.add_argument('--genome_uuid', type=str, nargs='*', default=[], required=False,
                        help='List of genome UUIDs to filter the query. Default is an empty list.')
    parser.add_argument('--dataset_uuid', type=str, nargs='*', default=[], required=False,
                        help='List of dataset UUIDs to filter the query. Default is an empty list.')
    parser.add_argument('--organism_group_type', type=str, default='DIVISION', required=False,
                        help='Organism group type to filter the query. Default is "DIVISION"')
    parser.add_argument('--division', type=str, nargs='*', default=[], required=False,
                        help='List of organism group names to filter the query. Default is an empty list.')
    parser.add_argument('--dataset_type', type=str, default="assembly", required=False,
                        help='List of dataset types to filter the query. Default is an empty list.')
    parser.add_argument('--species', type=str, nargs='*', default=[], required=False,
                        help='List of Species Production names to filter the query. Default is an empty list.')
    parser.add_argument('--antispecies', type=str, nargs='*', default=[], required=False,
                        help='List of Species Production names to exclude from the query. Default is an empty list.')
    parser.add_argument('--release_id', type=int, default=0, required=False,
                        help='Genome_dataset release_id to filter the query. Default is 0 (no filter).')
    parser.add_argument('--dataset_status', nargs='*', default=["Submitted"],
                        choices=['Submitted', 'Processing', 'Processed', 'Released'], required=False,
                        help='List of dataset statuses to filter the query. Default is an empty list.')
    parser.add_argument('--update_dataset_status', type=str, default="", required=False,
                        choices=['Submitted', 'Processing', 'Processed', 'Released', ''],
                        help='Update the status of the selected datasets to the specified value. ')
    parser.add_argument('--batch_size', type=int, default=50, required=False,
                        help='Number of results to retrieve per batch. Default is 50.')
    parser.add_argument('--page', default=1, required=False,
                        type=lambda x: int(x) if int(x) > 0 else argparse.ArgumentTypeError(
                            "{x} is not a positive integer"),
                        help='The page number for pagination. Default is 1.')
    parser.add_argument('--metadata_db_uri', type=str, required=True,
                        help='metadata db mysql uri, ex: mysql://ensro@localhost:3366/ensembl_genome_metadata')
    parser.add_argument('--output', type=str, required=True, help='output file ex: genome_info.json')

    args = parser.parse_args()

    meta_details = re.match(r"mysql:\/\/.*:?(.*?)@(.*?):\d+\/(.*)", args.metadata_db_uri)
    with open(args.output, 'w') as json_output:
        logger.info(f'Connecting Metadata Database with  host:{meta_details.group(2)} & dbname:{meta_details.group(3)}')

        genome_fetcher = GenomeFactory()

        logger.info(f'Writing Results to {args.output}')
        for genome in genome_fetcher.get_genomes(
                metadata_db_uri=args.metadata_db_uri,
                update_dataset_status=args.update_dataset_status,
                genome_uuid=args.genome_uuid,
                dataset_uuid=args.dataset_uuid,
                organism_group_type=args.organism_group_type,
                division=args.division,
                dataset_type=args.dataset_type,
                species=args.species,
                antispecies=args.antispecies,
                batch_size=args.batch_size,
                release_id=args.release_id,
                dataset_status=args.dataset_status,
        ) or []:
            json.dump(genome, json_output)
            json_output.write("\n")

        logger.info(f'Completed !')


if __name__ == "__main__":
    logger.info('Fetching Genome Information From New Metadata Database')
    main()
