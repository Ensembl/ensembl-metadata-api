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

""" Manage Release processing within dedicated Factory """
from __future__ import annotations

import logging

from ensembl.database import DBConnection
from sqlalchemy import update, select, or_, and_
from sqlalchemy.engine import make_url

from ensembl.production.metadata.api.models import ReleaseStatus, EnsemblRelease, EnsemblSite, Genome, GenomeDataset, \
    DatasetStatus, Dataset, DatasetType, GenomeRelease
from .datasets import DatasetFactory
from .genomes import GenomeInputFilters, GenomeFactory

logger = logging.getLogger(__name__)


#    PLANNED = "Planned"
#    PREPARING = "Preparing"
#    PREPARED = "Prepared"
#    RELEASED = "Released"
class ReleaseFactory:

    def __init__(self, conn_uri):
        super().__init__()
        self.metadata_uri = conn_uri
        self.gen_factory = GenomeFactory()
        self.ds_factory = DatasetFactory(conn_uri)

    def init_release(self, site: EnsemblSite):
        # Init a new release
        # not needed for now
        pass

    def prepare(self, release: int | EnsemblRelease):
        # Move a release from "planned" to "preparing"
        db = DBConnection(self.metadata_uri)
        with db.session_scope() as session:
            if isinstance(release, int):
                release = session.query(EnsemblRelease).filter(EnsemblRelease.release_id == release).one()
            self.__check_for_status(release, ReleaseStatus.PREPARING)
            # get all genome with status "Processed" and assign them to this release
            genomes = self.gen_factory.get_genomes(
                metadata_db_uri=self.metadata_uri,
                update_dataset_status=None,
                dataset_topic=['genebuild_annotation', 'variation_annotation', 'compara_annotation',
                               'regulation_annotation'],
                dataset_type=None,  # get all of them
                dataset_status=[DatasetStatus.PROCESSED.value, ],
                organism_group_type=None,
                dataset_unreleased=True,
                root_dataset=True,  # only fetch first level datasets
                batch_size=1000  # set to highest to make sure we get all of them.
            )
            release.status = ReleaseStatus.PREPARING
            logger.info("Release %s", release)
            logger.info("Genome(s): %s", genomes)
            genome_uuids = []
            dataset_uuids = []
            # get all processed dataset
            for genome_info in genomes:
                # for each: check all children are processed ??
                # Create datasets for release preparation
                logger.info("Genome %s / Dataset %s", genome_info['genome_uuid'], genome_info['dataset_uuid'])
                if genome_info['release_id'] is not None:
                    # Do not update datasets which are already attached to a release
                    logger.info("Skipping %s", genome_info)
                    continue
                else:
                    assert DatasetStatus(genome_info['dataset_status']) == DatasetStatus.PROCESSED
                    assert genome_info['parent_id'] is None
                    # Create all children datasets for release_preparation
                    self.ds_factory.create_all_child_datasets(dataset_uuid=genome_info['dataset_uuid'],
                                                              session=session,
                                                              topic='production_preparation',
                                                              status=DatasetStatus.SUBMITTED)
                    dataset_uuids.append(genome_info['dataset_uuid'])
                    genome_uuids.append(genome_info['genome_uuid'])

            logger.debug(f"Genome uuids {genome_uuids}")
            genome_uuids = list(dict.fromkeys(genome_uuids))
            logger.debug(f"Genome uuids {genome_uuids}")
            genomes = session.query(Genome).filter(Genome.genome_uuid.in_(genome_uuids)).all()
            # Attach them to release but not set as is_current for now (will be done when Releasing)
            [session.add(GenomeRelease(release_id=release.release_id,
                                       genome_id=genome.genome_id,
                                       is_current=0)) for genome in genomes]
            # bulk update and attach all to release
            processed_select = select(GenomeDataset.genome_dataset_id).join(Dataset).join(
                DatasetType).where(
                GenomeDataset.release_id == None,
                or_(and_(Dataset.status == DatasetStatus.PROCESSED, DatasetType.topic != 'production_preparation'),
                    and_(DatasetType.topic == 'production_preparation')))
            logger.debug("Processed Datasets %s", processed_select)
            update_processed = update(GenomeDataset).where(
                GenomeDataset.genome_dataset_id.in_(processed_select)).values(
                {GenomeDataset.release_id: release.release_id}).execution_options(synchronize_session='fetch')
            logger.debug("Processed Update %s", update_processed)
            session.execute(update_processed)
            return genome_uuids

    def prepared(self, release_id):
        # check everything is ok to set release status to "Prepared"
        pass

    def release(self, release_id):
        # mark a "prepared" release as "release"
        # set all genome as is_current (and remove the is_current from all previous ones)
        pass

    def __check_for_status(self, release: EnsemblRelease, status: ReleaseStatus):
        # Check if status is coherent for this release
        status_map = {
            ReleaseStatus.PLANNED: ReleaseStatus.PREPARING,
            ReleaseStatus.PREPARING: ReleaseStatus.PREPARED,
            ReleaseStatus.PREPARED: ReleaseStatus.RELEASED
        }
        if status_map[release.status] != status:
            raise ValueError(f"Wrong Release status attempt {release.status} -> {status}, no update possible")
