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
from __future__ import annotations

import logging
from typing import List

import sqlalchemy as db

from ensembl.production.metadata.grpc.adaptors.base import check_parameter, BaseAdaptor
from ensembl.production.metadata.api.models import EnsemblRelease, EnsemblSite, GenomeRelease, Genome, GenomeDataset, \
    Dataset, ReleaseStatus

logger = logging.getLogger(__name__)


class ReleaseAdaptor(BaseAdaptor):

    def fetch_releases(self,
                       release_id: int | List[int] = None,
                       release_version: float | List[float] = None,
                       current_only: bool = False,
                       release_type: str = None):
        """
        Fetches releases based on the provided parameters.

        Args:
            release_id: release internal id (int or list[int])
            release_version (float or list or None): Release version(s) to filter by.
            current_only (bool): Flag indicating whether to fetch only current releases.
            release_type (str): Release type to filter by.

        Returns:
            list: A list of fetched releases.
        """
        release_select = db.select(EnsemblRelease, EnsemblSite).join(EnsemblRelease.ensembl_site).order_by(
            EnsemblRelease.version)

        releases_id = check_parameter(release_id)
        if releases_id is not None:
            release_select = release_select.filter(
                EnsemblRelease.release_id.in_(releases_id)
            )

        release_version = check_parameter(release_version)
        # WHERE ensembl_release.version < version
        if release_version is not None:
            release_select = release_select.filter(
                EnsemblRelease.version <= release_version
            )
        # WHERE ensembl_release.is_current =:is_current_1
        if current_only:
            release_select = release_select.filter(
                EnsemblRelease.is_current == 1
            )

        # WHERE ensembl_release.release_type = :release_type_1
        if release_type is not None:
            release_select = release_select.filter(
                EnsemblRelease.release_type.in_(release_type)
            )

        release_select = release_select.filter(
            EnsemblSite.site_id == self.config.ensembl_site_id
        )
        logger.debug("Query: %s ", release_select)
        logger.debug(f"Allowed unreleased {self.config.allow_unreleased}")
        if not self.config.allow_unreleased:
            release_select = release_select.filter(EnsemblRelease.status == ReleaseStatus.RELEASED)
        with self.metadata_db.session_scope() as session:
            session.expire_on_commit = False
            return session.execute(release_select).all()

    def fetch_releases_for_genome(self, genome_uuid):
        select_released = db.select(EnsemblRelease, EnsemblSite) \
            .join(GenomeRelease) \
            .join(Genome) \
            .join(EnsemblSite) \
            .where(Genome.genome_uuid == genome_uuid)
        if not self.config.allow_unreleased:
            select_released = select_released.filter(EnsemblRelease.status == ReleaseStatus.RELEASED)
        with self.metadata_db.session_scope() as session:
            session.expire_on_commit = False
            releases = session.execute(select_released).all()
            return releases

    def fetch_releases_for_dataset(self, dataset_uuid):
        select_released = db.select(EnsemblRelease, EnsemblSite) \
            .join(GenomeDataset) \
            .join(Dataset) \
            .join(EnsemblSite) \
            .where(Dataset.dataset_uuid == dataset_uuid)
        if not self.config.allow_unreleased:
            select_released = select_released.filter(EnsemblRelease.status == ReleaseStatus.RELEASED)
        with self.metadata_db.session_scope() as session:
            session.expire_on_commit = False
            releases = session.execute(select_released).all()
            return releases


class NewReleaseAdaptor(BaseAdaptor):

    def __init__(self, metadata_uri=None):
        super().__init__(metadata_uri)
        # Get current release ID from ensembl_release
        with self.metadata_db.session_scope() as session:
            self.current_release_id = (
                session.execute(db.select(EnsemblRelease.release_id).filter(EnsemblRelease.is_current == 1)).one()[0])
        if self.current_release_id == "":
            raise Exception("Current release not found")
        logger.debug(f'Release ID: {self.current_release_id}')

        # Get last release ID from ensembl_release
        with self.metadata_db.session_scope() as session:
            ############### Refactor this once done. It is messy.
            current_version = int(session.execute(
                db.select(EnsemblRelease.version).filter(EnsemblRelease.release_id == self.current_release_id)).one()[
                                      0])
            past_versions = session.execute(
                db.select(EnsemblRelease.version).filter(EnsemblRelease.version < current_version)).all()
            sorted_versions = []
            # Do I have to account for 1.12 and 1.2
            for version in past_versions:
                sorted_versions.append(float(version[0]))
            sorted_versions.sort()
            self.previous_release_id = (session.execute(
                db.select(EnsemblRelease.release_id).filter(EnsemblRelease.version == sorted_versions[-1])).one()[0])
            if self.previous_release_id == "":
                raise Exception("Previous release not found")

    #     new_genomes (list of new genomes in the new release)
    def fetch_new_genomes(self):
        # TODO: this code must be never called yet, because it would never work!!!!
        with self.metadata_db.session_scope() as session:
            genome_selector = db.select(
                EnsemblRelease, EnsemblSite
            ).join(EnsemblRelease.ensembl_site)
            old_genomes = session.execute(
                db.select(EnsemblRelease.version).filter(EnsemblRelease.version < current_version)).all()
            new_genomes = []
            novel_old_genomes = []
            novel_new_genomes = []
            return session.execute(release_select).all()
