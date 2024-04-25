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

import argparse
import datetime
import logging

from ensembl.database import DBConnection
from sqlalchemy import update, select, or_, and_

from ensembl.production.metadata.api.exceptions import *
from ensembl.production.metadata.api.models import ReleaseStatus, EnsemblRelease, EnsemblSite, Genome, GenomeDataset, \
    DatasetStatus, Dataset, DatasetType, GenomeRelease
from .datasets import DatasetFactory
from .genomes import GenomeFactory

logger = logging.getLogger(__name__)


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

    def prepare(self, release: int | EnsemblRelease) -> list[str]:
        """
        Prepare a release:
        - Attach all Processed dataset from '*.annotation' topics to release
        - Create all 'release_preparation' related dataset and attach them to the release
        - Update Release status to 'Preparing'
        Args:
            release: the Release to prepare

        Returns:
            list[genome_uuid]: the list of genomes attached to release
        """
        # Move a release from "planned" to "preparing"
        db = DBConnection(self.metadata_uri)
        with db.session_scope() as session:
            release = self.__get_release(release)
            self.check_for_status(release, ReleaseStatus.PREPARING)
            # get all genomes with "genebuild" Processed
            #   - get all others "variation/homologies/regulation" Processed
            # get all genomes with "genebuild" Released having some "variation/homologies/regulation" Processed
            # With all those genomes, create the sub datasets for release preparation
            # Attach all those genomes/dataset to release
            # Update Release status to Preparing
            genebuild_ds = session.query(Genome.genome_id) \
                .join(GenomeDataset, GenomeDataset.genome_id == Genome.genome_id) \
                .join(Dataset, Dataset.dataset_id == GenomeDataset.dataset_id) \
                .join(DatasetType, DatasetType.dataset_type_id == Dataset.dataset_type_id) \
                .where(DatasetType.name == 'genebuild',
                       Dataset.status.in_([DatasetStatus.PROCESSED, DatasetStatus.RELEASED]))
            # for each genome retrieve all related available Processed datasets
            logger.debug("Release %s", release)
            logger.debug("Initial GBs %s", genebuild_ds)
            # get all others processed for those
            other_ds = session.query(GenomeDataset, Dataset, Genome).select_from(Dataset) \
                .join(GenomeDataset, Dataset.dataset_id == GenomeDataset.dataset_id) \
                .join(Genome, Genome.genome_id == GenomeDataset.genome_id) \
                .join(DatasetType, DatasetType.dataset_type_id == Dataset.dataset_type_id) \
                .where(Dataset.parent_id == None,
                       Dataset.status == DatasetStatus.PROCESSED,
                       GenomeDataset.release_id == None) \
                .filter(GenomeDataset.genome_id.in_(genebuild_ds))
            logger.debug(f"GB Ds {genebuild_ds}")
            logger.debug(f"Other Ds {other_ds}")
            genome_uuids = []
            dataset_uuids = []
            genomes = []
            # get all processed dataset
            for ds in other_ds.all():
                # for each: check all children are processed ??
                # Create datasets for release preparation
                logger.info("Genome %s / Dataset %s", ds.Genome.genome_uuid, ds.Dataset.dataset_uuid)
                assert ds.Dataset.status == DatasetStatus.PROCESSED
                assert ds.Dataset.parent_id is None
                # Create all children datasets for release_preparation
                self.ds_factory.create_all_child_datasets(dataset_uuid=ds.Dataset.dataset_uuid,
                                                          session=session,
                                                          topic='production_preparation',
                                                          status=DatasetStatus.SUBMITTED,
                                                          release=release)
                genome_uuids.append(ds.Genome.genome_uuid)
                ds.GenomeDataset.release_id = release.release_id
                dataset_uuids.append(ds.Dataset.dataset_uuid)

            genome_uuids = list(dict.fromkeys(genome_uuids))
            genomes_release = session.query(Genome).filter(Genome.genome_uuid.in_(genome_uuids)).all()
            logger.info(f"Adding {genomes} to release")
            # Attach them to release but not set as is_current for now (will be done when Releasing)
            [session.add(GenomeRelease(release_id=release.release_id,
                                       genome_id=genome.genome_id,
                                       is_current=0)) for genome in genomes_release]
            # bulk update and attach all to release
            logger.debug("Marked Release as Preparing for datasets: %s", )
            release.status = ReleaseStatus.PREPARING
            self.check_release(release)
            return genome_uuids

    def prepared(self, release: int | EnsemblRelease) -> list[str]:
        """
        Mark a released as prepared
        - Check that all release 'production_preparation' datasets associated with release are Processed
        - Update the Release status to Prepared
        Args:
            release: identifier or EnsemblRelease object

        Returns:
            list[genome_uuid]: the list of genomes attached to release
        """
        # check everything is ok to set release status to "Prepared"
        db = DBConnection(self.metadata_uri)
        with db.session_scope() as session:
            release = self.__get_release(release)
            self.check_for_status(release, ReleaseStatus.PREPARED)
            # get all release datasets
            preparation_datasets = session.query(GenomeDataset) \
                .join(Dataset, Dataset.dataset_id == GenomeDataset.dataset_id).join(
                DatasetType, DatasetType.dataset_type_id == Dataset.dataset_type_id) \
                .where(DatasetType.topic == 'release_preparation')
            logger.debug(preparation_datasets)
            datasets = preparation_datasets.all()
            logger.debug(datasets)
            release.status = ReleaseStatus.PREPARED
            self.check_release(release)
            return [gr.genome.genome_uuid for gr in release.genome_releases]

    def release(self, release: int | EnsemblRelease, release_date: str | datetime.date) -> list[str]:
        """
        Mark a release as Released
        - Check release status: must be Prepared
        - Mark all related datasets as "Released"
        - TODO Mark dataset as current / unmark others are current
        - TODO Check Genome `is_best` whether to keep this one as current
        - Mark Release as Release and set date
        Args:
            release_date: The actual release date to set
            release: The release to release
        Returns:
            list[genome_uuid]: the list of genomes which has been released
        """
        # mark a "prepared" release as "release"
        # set all genome as is_current (and remove the is_current from all previous ones)
        db = DBConnection(self.metadata_uri)
        with db.session_scope() as session:
            release = self.__get_release(release)
            self.check_for_status(release, ReleaseStatus.RELEASED)
            released_datasets = session.query(GenomeDataset).join(Dataset.genome_datasets).join(
                DatasetType.datasets).where(GenomeDataset.release_id == release.release_id).all()
            assert [dataset.status == DatasetStatus.PROCESSED for dataset in released_datasets]
            self.check_release(release)
            release.status = ReleaseStatus.RELEASED
            release.release_date = release_date
            for dataset in release.genome_datasets:
                dataset.status = DatasetStatus.RELEASED
            return [genome.genome_uuid for genome in release.genome_releases]

    def __get_release(self, release: int | EnsemblRelease) -> EnsemblRelease:
        db = DBConnection(self.metadata_uri)
        with db.session_scope() as session:
            if isinstance(release, int):
                release = session.query(EnsemblRelease).filter(EnsemblRelease.release_id == release).one()
            assert isinstance(release, EnsemblRelease)
            return release

    def check_release(self, release: int | EnsemblRelease) -> bool:
        """
        Check Release content for inconsistencies
         - all datasets in release should have there genome as well associated to the same release
         - check that all genomes are associated only once with Release
         - TODO more to come!
        Args:
            release: the release to check

        Returns:
            bool or Raise MissingMetaException
        """

        release = self.__get_release(release)
        errors = []
        logger.debug("Checking checking %s / %s", release.version, release.status)
        for gd in release.genome_datasets:
            gd_rel = next((gd for gd in gd.genome.genome_releases if gd.ensembl_release == release), None)
            if gd_rel is None:
                errors.append(f"Dataset [{gd.dataset.dataset_uuid}/{gd.dataset.name}] in {release.version}, genome "
                              f"[{gd.genome.genome_uuid}] is not")
            if release.status in (ReleaseStatus.PREPARING, ReleaseStatus.PREPARED):
                ds_type = 'production_process' if release.status == ReleaseStatus.PREPARING else 'production_preparation'
                if gd.dataset.dataset_type == ds_type and gd.dataset.status != DatasetStatus.PROCESSED:
                    errors.append(f"Dataset [{gd.dataset.dataset_uuid}/{gd.dataset.name}] is not Processed [{gd.dataset.status.value}]")
        if errors:
            raise ReleaseDataException(f"Inconsistent {release.version}: \n{errors}")
        return True

    def check_for_status(self, release: EnsemblRelease, status: ReleaseStatus):
        # Check if status is coherent for this release
        # TODO check that no other release is at the same status when wanting to move to RELEASED
        # TODO check that no less numbered release are not released either.
        status_map = {
            ReleaseStatus.PLANNED: ReleaseStatus.PREPARING,
            ReleaseStatus.PREPARING: ReleaseStatus.PREPARED,
            ReleaseStatus.PREPARED: ReleaseStatus.RELEASED
        }
        if status_map[release.status] != status:
            raise ValueError(f"Wrong Release status attempt {release.status} -> {status}, no update possible")
        self.check_release(release)


def main():
    parser = argparse.ArgumentParser(
        prog='genomes.py',
        description='Fetch Ensembl genome info from the new metadata database'
    )
    parser.add_argument('--genome_uuid', type=str, nargs='*', default=[], required=False,
                        help='List of genome UUIDs to filter the query. Default is an empty list.')
