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

from ensembl.production.metadata.grpc.adaptors.base import check_parameter, BaseAdaptor, cfg
from ensembl.production.metadata.api.models import EnsemblRelease, EnsemblSite, GenomeRelease, Genome, GenomeDataset, \
    Dataset, ReleaseStatus

logger = logging.getLogger(__name__)


def filter_release_status(query,
                          release_status: str | ReleaseStatus = None):
    logger.debug(f"Allowed unreleased {cfg.allow_unreleased}")
    query = query.add_columns(EnsemblSite)
    if not cfg.allow_unreleased:
        query = query.join(EnsemblSite,
                           EnsemblSite.site_id == EnsemblRelease.site_id &
                           EnsemblSite.site_id == cfg.ensembl_site_id) \
            .filter(EnsemblRelease.status == ReleaseStatus.RELEASED)
    else:
        query = query.outerjoin(EnsemblSite,
                                EnsemblSite.site_id == EnsemblRelease.site_id &
                                EnsemblSite.site_id == cfg.ensembl_site_id)
        # Release status filter only work when unreleased are allowed
        if release_status:
            if isinstance(release_status, str):
                release_status = ReleaseStatus(release_status)
            query = query.filter(EnsemblRelease.status == release_status)
    return query


class ReleaseAdaptor(BaseAdaptor):

    def fetch_releases(self,
                       release_id: int | List[int] = None,
                       release_version: float | List[float] = None,
                       current_only: bool = False,
                       release_type: str = None,
                       release_status: str | ReleaseStatus = None):
        """
        Fetches releases based on the provided parameters.

        Args:
            release_id: release internal id (int or list[int])
            release_version (float or list or None): Release version(s) to filter by.
            current_only (bool): Flag indicating whether to fetch only current releases.
            release_type (str): Release type to filter by.
            release_status: whether to filter particular release status

        Returns:
            list: A list of fetched releases.
        """
        release_select = db.select(EnsemblRelease).order_by(EnsemblRelease.version)

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
            EnsemblSite.site_id == cfg.ensembl_site_id
        )
        release_select = filter_release_status(release_select, release_status)
        logger.debug("Query: %s ", release_select)
        with self.metadata_db.session_scope() as session:
            session.expire_on_commit = False
            return session.execute(release_select).all()

    def fetch_releases_for_genome(self, genome_uuid):
        select_released = db.select(EnsemblRelease)
        if cfg.allow_unreleased:
            select_released = select_released.outerjoin(GenomeRelease)
        else:
            select_released = select_released.join(GenomeRelease)
        select_released = select_released.join(Genome) \
            .where(Genome.genome_uuid == genome_uuid)
        select_released = filter_release_status(select_released)

        logger.debug("Query: %s ", select_released)
        with self.metadata_db.session_scope() as session:
            session.expire_on_commit = False
            releases = session.execute(select_released).all()
            return releases

    def fetch_releases_for_dataset(self, dataset_uuid):
        select_released = db.select(EnsemblRelease) \
            .select_from(Dataset) \
            .join(GenomeDataset) \
            .where(Dataset.dataset_uuid == dataset_uuid)

        if cfg.allow_unreleased:
            select_released = select_released.outerjoin(EnsemblRelease)
        else:
            select_released = select_released.join(EnsemblRelease)
        select_released = filter_release_status(select_released)
        logger.debug("Query: %s ", select_released)
        with self.metadata_db.session_scope() as session:
            session.expire_on_commit = False
            releases = session.execute(select_released).all()
            return releases
