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

import argparse
import logging
import os
import random
from datetime import datetime, timedelta
from typing import List

from ensembl.utils.database import DBConnection
from sqlalchemy.engine import make_url

from ensembl.production.metadata.api.factories.datasets import DatasetFactory
from ensembl.production.metadata.api.factories.genomes import GenomeFactory
from ensembl.production.metadata.api.factories.release import ReleaseFactory
from ensembl.production.metadata.api.models import *

logger = logging.getLogger(__name__)

gen_factory = GenomeFactory()


class MetadataUpdater:

    def __init__(self, metadata_uri):
        super().__init__()
        self.metadata_uri = metadata_uri

    def check(self):
        metadata_db = DBConnection(self.metadata_uri)
        with metadata_db.session_scope() as session:
            releases = session.query(EnsemblRelease).order_by(EnsemblRelease.version).all()
            factory = ReleaseFactory(self.metadata_uri)
            [factory.check_release(rel) for rel in releases]

    def wipe(self):
        metadata_db = DBConnection(self.metadata_uri)
        with metadata_db.session_scope() as session:
            dataset_types = session.query(DatasetType.dataset_type_id).filter(
                DatasetType.topic.in_(['production_process', 'production_preparation', 'production_publication']))
            delete = session.query(Dataset).filter(Dataset.dataset_type_id.in_(dataset_types)).delete()
            session.execute(delete)

    def create_release_ds(self):
        metadata_db = DBConnection(self.metadata_uri)
        with metadata_db.session_scope() as session:
            ds_factory = DatasetFactory(self.metadata_uri)
            releases: List[EnsemblRelease] = session.query(EnsemblRelease).all()
            for release in releases:
                if release.status == ReleaseStatus.RELEASED:
                    dataset_status = DatasetStatus.RELEASED
                    topic = None
                elif release.status == ReleaseStatus.PREPARING:
                    dataset_status = DatasetStatus.PROCESSED
                    topic = ['production_process']
                elif release.status == ReleaseStatus.PREPARED:
                    dataset_status = DatasetStatus.PROCESSED
                    topic = ['production_process', 'production_preparation']
                for genome_dataset in release.genome_datasets:
                    if topic is not None:
                        for top in topic:
                            ds_factory.create_all_child_datasets(dataset_uuid=genome_dataset.dataset.dataset_uuid,
                                                                 topic=top,
                                                                 session=session,
                                                                 status=dataset_status,
                                                                 release=release)
                    else:
                        ds_factory.create_all_child_datasets(dataset_uuid=genome_dataset.dataset.dataset_uuid,
                                                             topic=None,
                                                             session=session,
                                                             status=dataset_status,
                                                             release=release)
            # Randomly assign dates for production datasets expected attributes
            datasets = session.query(Dataset, EnsemblRelease).select_from(Dataset).join(
                DatasetType.datasets).join(GenomeDataset, GenomeDataset.dataset_id == Dataset.dataset_id).outerjoin(
                EnsemblRelease, EnsemblRelease.release_id == GenomeDataset.release_id).filter(
                DatasetType.topic.in_(('production_process', 'production_preparation'))).order_by(Dataset.dataset_uuid)
            # attribute_id IN(183, 182)
            for dataset in datasets.all():
                end = None
                start = None
                if dataset.Dataset.status == DatasetStatus.RELEASED and dataset.EnsemblRelease.release_date is not None:
                    logger.info(f"Dataset {dataset.Dataset.dataset_uuid} is released")
                    start = dataset.EnsemblRelease.release_date - timedelta(weeks=3)
                    end = dataset.EnsemblRelease.release_date
                elif dataset.Dataset.status == DatasetStatus.PROCESSED:
                    logger.info(f"Dataset {dataset.Dataset.dataset_uuid} is processed")
                    start = datetime.now() - timedelta(weeks=1)
                    end = datetime.now() - timedelta(days=1)
                elif dataset.Dataset.status == DatasetStatus.PROCESSING:
                    logger.info(f"Dataset {dataset.Dataset.dataset_uuid} is processing")
                    start = datetime.now() - timedelta(weeks=1)
                    end = None
                if end:
                    start_build = start + (end - start) * random.random()
                    end_build = start_build + timedelta(days=1)

                    session.add(DatasetAttribute(dataset_id=dataset.Dataset.dataset_id,
                                                 attribute_id=183,
                                                 value=datetime.strftime(end_build, "%y/%m/%d")))
                if start:
                    if not end:
                        end = datetime.now()
                    start_build = start + (end - start) * random.random()
                    session.add(DatasetAttribute(dataset_id=dataset.Dataset.dataset_id,
                                                 attribute_id=182,
                                                 value=datetime.strftime(start_build, "%y/%m/%d")))

    def create_submitted_ds(self):
        metadata_db = DBConnection(self.metadata_uri)
        with metadata_db.session_scope() as session:
            ds_factory = DatasetFactory(self.metadata_uri)
            datasets = session.query(Dataset).join(GenomeDataset.dataset).filter(GenomeDataset.release_id == None).all()
            for dataset in datasets:
                ds_factory.create_all_child_datasets(dataset_uuid=dataset.dataset_uuid,
                                                     topic='production_process',
                                                     session=session,
                                                     status=dataset.status)


def main():
    parser = argparse.ArgumentParser(
        prog='update_test_set.py',
        description='Some potential useful methods to update the test set on host'
    )
    parser.add_argument('-m', '--metadata_db_uri', type=str,
                        default="mysql://ensembl@localhost:3306/marco_ensembl_genome_metadata",
                        required=False, help='Target metadata uri')

    parser.add_argument('--action', type=str, help="Action method to call (check|wipe|create)",
                        required=False, default='create_submitted_ds')
    args = parser.parse_args()
    meta_details = make_url(args.metadata_db_uri)
    logger.info(f'Connecting Metadata Database with  host:{meta_details.host} & dbname:{meta_details.database}')
    meta_updater = MetadataUpdater(args.metadata_db_uri)
    getattr(meta_updater, args.action)()


if __name__ == "__main__":
    logger.info('Updating metadata content')
    main()
