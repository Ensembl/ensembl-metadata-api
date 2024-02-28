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
from ensembl.database import DBConnection
from ensembl.production.metadata.api.hive.dataset_factory import DatasetFactory
from ensembl.production.metadata.api.models.assembly import Assembly
from ensembl.production.metadata.api.models.dataset import DatasetType, Dataset, DatasetSource
from ensembl.production.metadata.api.models.genome import Genome, GenomeDataset
from ensembl.production.metadata.api.models.organism import Organism, OrganismGroup, OrganismGroupMember
from sqlalchemy import JSON
from sqlalchemy import select, text
from typing import List

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class GenomeInputFilter:
    genome_uuid: List[str] = field(default_factory=list)
    dataset_uuid: List[str] = field(default_factory=list)
    division: List[str] = field(default_factory=list)
    dataset_type: str = "assembly"
    species: List[str] = field(default_factory=list)
    antispecies: List[str] = field(default_factory=list)
    dataset_status: List[str] = field(default_factory=lambda: ["Submitted"])
    batch_size: int = 50
    organism_group_type: str = "DIVISION"
    run_all: int = 0
    columns: List = field(default_factory=lambda: [Genome.genome_uuid,
                                                   Genome.production_name.label('species'),
                                                   DatasetType.name.label('dataset_type'),
                                                   Dataset.dataset_uuid])


class GenomeFactory:

    @staticmethod
    def _get_query(filters):

        query = select(filters.columns).select_from(Genome).group_by(Genome.genome_id)

        query = query.join(Genome.organism).join(Organism.organism_group_members) \
            .join(OrganismGroupMember.organism_group) \
            .outerjoin(Genome.genome_datasets).join(GenomeDataset.dataset) \
            .join(Dataset.dataset_source).join(Dataset.dataset_type)

        # default filter with organism group type to DIVISION
        query = query.filter(OrganismGroup.type == filters.organism_group_type)
        if filters.run_all:
            filters.division = ['EnsemblBacteria',
                                'EnsemblVertebrates',
                                'EnsemblPlants',
                                'EnsemblProtists',
                                'EnsemblMetazoa',
                                'EnsemblFungi',
                                ]
        if filters:

            if filters.genome_uuid:
                query = query.filter(Genome.genome_uuid.in_(filters.genome_uuid))

            if filters.division:
                ensembl_divisions = filters.division

                if filters.organism_group_type == 'DIVISION':
                    pattern = re.compile(r'^(ensembl)?', re.IGNORECASE)
                    ensembl_divisions = ['Ensembl' + pattern.sub('', d).capitalize() for d in ensembl_divisions if d]

                query = query.filter(OrganismGroup.name.in_(ensembl_divisions))

            if filters.species:
                species = set(filters.species) - set(filters.anti_species)

                if species:
                    query = query.filter(Genome.production_name.in_(filters.species))
                else:
                    query = query.filter(~Genome.production_name.in_(filters.anti_species))

            elif filters.antispecies:
                query = query.filter(~Genome.production_name.in_(filters.anti_species))

            if filters.dataset_type:
                query = query.filter(Genome.genome_datasets.any(DatasetType.name.in_([filters.dataset_type])))

            if filters.dataset_status:
                query = query.filter(Dataset.status.in_(filters.dataset_status))

            if filters.batch_size:
                query = query.limit(filters.batch_size)

        return query

    def get_genomes(self,
                    metadata_db_uri,
                    update_dataset_status="",
                    **filters: GenomeInputFilter
                    ):

        # validate the params
        logger.info(f'Get Genomes with filters {filters}')
        filters = GenomeInputFilter(**filters)
        metadata_db = DBConnection(metadata_db_uri)
        dataset_factory = DatasetFactory()

        # fetch genome results
        with metadata_db.session_scope() as session:
            try:
                query = self._get_query(filters)
                logger.info(f'executing sql query  {query}')
                for genome_info in session.execute(query).fetchall():
                    genome_info = genome_info._asdict()
                    dataset_uuid = genome_info.get('dataset_uuid', None)
                    if not dataset_uuid:
                        logger.warn(
                            f"No dataset uuid found for genome {genome_info} skipping this genome "
                        )
                        continue

                    if update_dataset_status:
                        _, status = dataset_factory.update_dataset_status(dataset_uuid, session, update_dataset_status)

                        if update_dataset_status == status:

                            logger.info(
                                f"Updated Dataset status for dataset uuid: {dataset_uuid} from {update_dataset_status} to {status}  for genome {genome_info['genome_uuid']}"
                            )
                            yield genome_info
                        else:
                            logger.warn(
                                f"Cannot update status for dataset uuid: {dataset_uuid} {update_dataset_status} to {status}  for genome {genome['genome_uuid']}"
                            )

                    # NOTE: when update_dataset_status is empty, returns all the genome irrespective of its dependencies and status
                    else:
                        yield genome_info

            except Exception as e:
                raise ValueError(str(e))


def main():
    parser = argparse.ArgumentParser(
        prog='genome.py',
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
    parser.add_argument('--dataset_status',  nargs='*', default=["Submitted"],
                        choices=['Submitted', 'Processing', 'Processed', 'Released'], required=False,
                        help='List of dataset statuses to filter the query. Default is an empty list.')
    parser.add_argument('--update_dataset_status', type=str, default="", required=False,
                        choices=['Submitted', 'Processing', 'Processed', 'Released', ''],
                        help='Update the status of the selected datasets to the specified value. ')
    parser.add_argument('--batch_size', type=int, default=50, required=False,
                        help='Number of results to retrieve per batch. Default is 50.')
    parser.add_argument('--metadata_db_uri', type=str, required=True,
                        help='metadata db mysql uri, ex: mysql://ensro@localhost:3366/ensembl_genome_metadata')
    parser.add_argument('--output', type=str, required=True, help='output file ex: genome_info.json')

    args = parser.parse_args()

    meta_details = re.match(r"mysql:\/\/.*:(.*?)@(.*?):\d+\/(.*)", args.metadata_db_uri)
    with open(args.output, 'w') as json_output:
        logger.info(f'Connecting Metadata Database with  host:{meta_details.group(2)} & dbname:{meta_details.group(3)}')

        genome_fetcher = GenomeFactory()

        logger.info(f'Writing Results to {args.output}')
        print(args)
        for genome in genome_fetcher.get_genomes(
                metadata_db_uri=args.metadata_db_uri,
                update_dataset_status=args.update_dataset_status,
                genome_uuid=args.genome_uuid,
                organism_group_type=args.organism_group_type,
                division=args.division,
                dataset_type=args.dataset_type,
                species=args.species,
                antispecies=args.antispecies,
                batch_size=args.batch_size,
                dataset_status=args.dataset_status,
        ) or []:
            json.dump(genome, json_output)
            json_output.write("\n")

        logger.info(f'Completed !')


if __name__ == "__main__":
    logger.info('Fetching Genome Information From New Metadata Database')
    main()
