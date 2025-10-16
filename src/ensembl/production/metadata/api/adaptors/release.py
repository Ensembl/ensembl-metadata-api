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
from sqlalchemy import and_

from ensembl.production.metadata.api.adaptors.base import check_parameter, BaseAdaptor, cfg
from ensembl.production.metadata.api.models import (
    EnsemblRelease,
    EnsemblSite,
    GenomeRelease,
    Genome,
    GenomeDataset,
    Dataset,
    ReleaseStatus,
)

logger = logging.getLogger(__name__)


def filter_release_status(query, release_status: str | ReleaseStatus = None):
    """
    Adds EnsemblSite join and filters based on release status and configuration.

    Args:
        query: The SQLAlchemy query to filter
        release_status: Optional release status to filter by

    Returns:
        Modified query with site join and status filters applied
    """
    logger.debug(f"Allowed unreleased {cfg.allow_unreleased}")
    query = query.add_columns(EnsemblSite)

    if not cfg.allow_unreleased:
        # For released only: use inner join and filter
        query = query.join(
            EnsemblSite,
            and_(EnsemblSite.site_id == EnsemblRelease.site_id, EnsemblSite.site_id == cfg.ensembl_site_id),
        ).filter(EnsemblRelease.status == ReleaseStatus.RELEASED)
    else:
        # For unreleased allowed: use outer join
        query = query.outerjoin(
            EnsemblSite,
            and_(EnsemblSite.site_id == EnsemblRelease.site_id, EnsemblSite.site_id == cfg.ensembl_site_id),
        )
        # Release status filter only works when unreleased are allowed
        if release_status:
            if isinstance(release_status, str):
                release_status = ReleaseStatus(release_status)
            query = query.filter(EnsemblRelease.status == release_status)

    return query


def _ensure_scalar(value):
    """
    Ensures a parameter is a scalar value, unwrapping single-element lists.
    Handles pytest parametrization edge cases.

    Args:
        value: The value to check

    Returns:
        Scalar value or None
    """
    if value is None:
        return None

    # Unwrap single-element lists/tuples (pytest parametrization edge case)
    if isinstance(value, (list, tuple)) and len(value) == 1:
        value = value[0]

    # If still a list/tuple, return as-is for IN clause handling
    return value


class ReleaseAdaptor(BaseAdaptor):

    def fetch_releases(
            self,
            release_id: int | List[int] = None,
            release_version: float | List[float] = None,
            current_only: bool = False,
            site_name: str = None,
            release_type: str = None,
            release_label: str = None,
            release_status: str | ReleaseStatus = None,
    ):
        """
        Fetches releases based on the provided parameters.

        Args:
            release_id: release internal id (int or list[int])
            release_version (float or list or None): Release version(s) to filter by.
            current_only (bool): Flag indicating whether to fetch only current releases.
            site_name (str): Site name to filter by.
            release_type (str): Release type to filter by.
            release_label (str): Release label to filter by.
            release_status: whether to filter particular release status

        Returns:
            list: A list of fetched releases.
        """
        release_select = db.select(EnsemblRelease).order_by(EnsemblRelease.version)

        # Handle release_id parameter
        releases_id = check_parameter(release_id)
        if releases_id is not None:
            release_select = release_select.filter(EnsemblRelease.release_id.in_(releases_id))

        # Handle release_version parameter
        # Ensure it's a scalar for <= comparison, or list for IN clause
        release_version = _ensure_scalar(check_parameter(release_version))
        if release_version is not None:
            if isinstance(release_version, (list, tuple)):
                # Multiple versions: use IN clause
                release_select = release_select.filter(EnsemblRelease.version.in_(release_version))
            else:
                # Single version: use <= comparison
                # Convert to float to ensure type compatibility with SQLite
                release_version = float(release_version)
                release_select = release_select.filter(EnsemblRelease.version <= release_version)

        # Filter for current releases only
        if current_only:
            release_select = release_select.filter(EnsemblRelease.is_current == 1)

        # Filter by release type
        if release_type is not None:
            release_type = check_parameter(release_type)
            release_select = release_select.filter(EnsemblRelease.release_type.in_(release_type))

        # Filter by release label
        if release_label is not None:
            release_label = check_parameter(release_label)
            release_select = release_select.filter(EnsemblRelease.label.in_(release_label))

        # Filter by site name (requires site join, so must come before filter_release_status)
        if site_name is not None:
            site_name = check_parameter(site_name)
            release_select = release_select.filter(EnsemblSite.name.in_(site_name))

        # Add site join and status filters
        # NOTE: This already handles the site_id == cfg.ensembl_site_id filter
        release_select = filter_release_status(release_select, release_status)

        logger.debug("Query: %s ", release_select)

        with self.metadata_db.session_scope() as session:
            session.expire_on_commit = False
            return session.execute(release_select).all()

    def fetch_releases_for_genome(self, genome_uuid):
        """
        Fetches releases associated with a specific genome.

        Args:
            genome_uuid: The UUID of the genome

        Returns:
            list: A list of releases for the genome
        """
        select_released = db.select(EnsemblRelease).join(GenomeRelease)

        if not cfg.allow_unreleased:
            select_released = select_released.filter(EnsemblRelease.status == ReleaseStatus.RELEASED)

        select_released = select_released.join(Genome).where(Genome.genome_uuid == genome_uuid)
        select_released = filter_release_status(select_released)

        logger.debug("Query: %s ", select_released)

        with self.metadata_db.session_scope() as session:
            session.expire_on_commit = False
            releases = session.execute(select_released).all()
            return releases

    def fetch_releases_for_dataset(self, dataset_uuid):
        """
        Fetches releases associated with a specific dataset.

        Args:
            dataset_uuid: The UUID of the dataset

        Returns:
            list: A list of releases for the dataset
        """
        select_released = (
            db.select(EnsemblRelease)
            .select_from(Dataset)
            .join(GenomeDataset)
            .join(EnsemblRelease)
            .where(Dataset.dataset_uuid == dataset_uuid)
        )

        if not cfg.allow_unreleased:
            select_released = select_released.filter(EnsemblRelease.status == ReleaseStatus.RELEASED)

        select_released = filter_release_status(select_released)
        logger.debug("Query: %s ", select_released)

        with self.metadata_db.session_scope() as session:
            session.expire_on_commit = False
            releases = session.execute(select_released).all()
            return releases
