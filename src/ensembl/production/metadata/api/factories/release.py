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
from datetime import datetime
from decimal import Decimal
from typing import Optional

from ensembl.utils.database import DBConnection
from sqlalchemy import insert, update, select, func, cast, Integer

from ensembl.production.metadata.api.exceptions import *
from ensembl.production.metadata.api.factories.datasets import DatasetFactory
from ensembl.production.metadata.api.factories.genomes import GenomeFactory
from ensembl.production.metadata.api.models import *

logger = logging.getLogger(__name__)


class ReleaseFactory:

    def __init__(self, conn_uri):
        self.metadata_uri = conn_uri
        self.gen_factory = GenomeFactory()
        self.ds_factory = DatasetFactory(conn_uri)

    def init_release(
            self,
            version: Optional[Decimal] = None,
            release_date: Optional[str] = None,
            label: Optional[str] = None,
            site: str = "Ensembl",
            release_type: str = "partial",
            status: str = "Planned",
            name: str = None
    ) -> EnsemblRelease:
        """
        Creates a new Ensembl release entry.

        This method generates a new `EnsemblRelease` record and assigns it to the specified site.
        If no version is provided, the latest version is incremented by 0.1.
        The release must have either a `release_date` or a `label`.


        Args:
            version (Decimal, optional): The release version. If not provided, it increments the last version by 0.1.
            release_date (str, optional): The release date in 'YYYY-MM-DD' format or None.
            label (str, optional): A label for the release. Defaults to `release_date` if not provided.
            site (str): The site name to associate with the release. Defaults to "Ensembl".
            release_type (str): The type of release, must be either "partial" or "integrated". Defaults to "partial".
            status (str): The release status, must be either "planned" or "released". Defaults to "Planned".

        Returns:
            EnsemblRelease: The newly created release object.

        Raises:
            MissingMetaException: If the specified site does not exist.
            ValueError: If an invalid `release_date` is not provided.
        """
        db = DBConnection(self.metadata_uri)
        with db.session_scope() as session:
            # Validate site
            site_obj = session.query(EnsemblSite).filter(EnsemblSite.name == site).one_or_none()
            if site_obj is None:
                raise MissingMetaException(f"Site '{site}' not found.")

            # Determine version
            if version is None:
                last_release = session.query(EnsemblRelease).order_by(EnsemblRelease.version.desc()).first()
                version = last_release.version + Decimal("0.1") if last_release else Decimal("1.0")
                version = round(version, 1)

            # Validate release date only if provided
            release_date_obj = None
            if release_date:
                try:
                    release_date_obj = datetime.strptime(release_date, "%Y-%m-%d").date()
                except ValueError:
                    raise ValueError("Invalid release_date format. Expected YYYY-MM-DD.")
            else:
                if label:
                    try:
                        release_date_obj = datetime.strptime(label, "%Y-%m-%d").date()
                        release_date = label  # Store the string for later label assignment
                    except ValueError:
                        raise ValueError("Invalid label format. Expected YYYY-MM-DD when used as date.")
                else:
                    raise ValueError("Either release_date or label must be specified.")

            # Create a name if not provided. It should be one higher than any existing partial release.
            if not name and release_type == "partial":
                name = session.scalar(
                    select(func.max(cast(EnsemblRelease.name, Integer)))
                ) + 1

            # Ensure label is defined
            if label is None:
                label = release_date

            # Validate release type
            if release_type not in {"partial", "integrated"}:
                raise ValueError("Invalid release_type. Must be 'partial' or 'integrated'.")

            # Validate status
            if status not in {"Planned", "Released"}:
                raise ValueError("Invalid status. Must be 'Planned' or 'Released'.")

            # Create and store the new release
            release = EnsemblRelease(
                version=version,
                release_date=release_date_obj,
                label=label,
                ensembl_site=site_obj,
                release_type=release_type,
                status=status,
                name=name
            )
            session.add(release)
            session.commit()
            session.refresh(release)
            return release

    def set_partial_released(
        self,
        release_name: str,
        release_date: str = None,
        site_name: str = "Ensembl",
        exclude_genomes: list[str] = None,
        exclude_datasets: list[str] = None,
    ) -> EnsemblRelease:
        """

        Finalize a partial release by release name.

        - datasets or genomes can be excluded by providing a list of their UUIDs.
        - Processes faulty datasets in a separate transaction before release finalization.
        - Scopes all subsequent work to genomes already attached to the named release.
        - Attaches all non-faulty datasets for those genomes to the same release.
        - Marks all associated datasets as 'Released'.
        - Marks all associated genomes as current and unmarks older partial releases where needed.
        - Ensures only one 'current' dataset per dataset type exists.
        - Marks the release as 'Released' and sets the release date and label.
        """
        exclude_datasets = exclude_datasets or []
        exclude_genomes = exclude_genomes or []
        if not release_name:
            raise ValueError("release_name must be provided.")

        # Process faulty datasets in a separate transaction before the release transaction starts.
        self.ds_factory.process_faulty()

        db = DBConnection(self.metadata_uri)

        with db.session_scope() as session:
            site = session.query(EnsemblSite).filter_by(name=site_name).one_or_none()
            if site is None:
                raise MissingMetaException(f"Site '{site_name}' not found.")
            site_id = site.site_id

            release = (
                session.query(EnsemblRelease)
                .filter_by(
                    name=release_name,
                    site_id=site_id,
                    release_type="partial",
                )
                .one()
            )
            release_id = release.release_id
            already_released = release.status == ReleaseStatus.RELEASED

            logger.info(
                "Starting atomic partial release process for release_name=%s, release_id=%s",
                release.name,
                release_id,
            )

            excluded_genome_ids = {
                genome_id
                for (genome_id,) in session.query(Genome.genome_id)
                .filter(Genome.genome_uuid.in_(exclude_genomes))
                .all()
            }

            scoped_genome_ids = {
                genome_id
                for (genome_id,) in session.query(GenomeRelease.genome_id)
                .filter(GenomeRelease.release_id == release_id)
                .all()
            }
            scoped_genome_ids.difference_update(excluded_genome_ids)

            if not scoped_genome_ids:
                raise ValueError(f"Release '{release.name}' has no genomes attached to it.")

            existing_genome_release_ids = {
                genome_id
                for (genome_id,) in session.query(GenomeRelease.genome_id)
                .filter(GenomeRelease.release_id == release_id)
                .all()
            }
            missing_genome_release_ids = scoped_genome_ids.difference(existing_genome_release_ids)
            for genome_id in missing_genome_release_ids:
                session.add(GenomeRelease(genome_id=genome_id, release_id=release_id, is_current=0))

            candidate_links = (
                session.query(GenomeDataset)
                .join(Dataset)
                .filter(GenomeDataset.genome_id.in_(scoped_genome_ids))
                .filter(Dataset.status != DatasetStatus.FAULTY)
                .filter(Dataset.dataset_uuid.notin_(exclude_datasets) if exclude_datasets else True)
                .all()
            )

            logger.info(
                "Found %s non-faulty genome_dataset links across %s scoped genomes.",
                len(candidate_links),
                len(scoped_genome_ids),
            )
            genome_uuid_by_id = {
                genome_id: genome_uuid
                for genome_id, genome_uuid in session.query(Genome.genome_id, Genome.genome_uuid)
                .filter(Genome.genome_id.in_(scoped_genome_ids))
                .all()
            }

            release_links = session.query(GenomeDataset).filter(GenomeDataset.release_id == release_id).all()
            release_link_map = {
                (genome_dataset.genome_id, genome_dataset.dataset_id): genome_dataset
                for genome_dataset in release_links
            }
            null_release_links = {
                (genome_dataset.genome_id, genome_dataset.dataset_id): genome_dataset
                for genome_dataset in session.query(GenomeDataset)
                .filter(GenomeDataset.genome_id.in_(scoped_genome_ids))
                .filter(GenomeDataset.release_id.is_(None))
                .all()
            }

            for genome_dataset in candidate_links:
                link_key = (genome_dataset.genome_id, genome_dataset.dataset_id)
                release_link = release_link_map.get(link_key)
                if release_link is None:
                    null_release_link = null_release_links.get(link_key)
                    if null_release_link is not None:
                        null_release_link.release_id = release_id
                        release_link = null_release_link
                    else:
                        release_link = GenomeDataset(
                            genome_id=genome_dataset.genome_id,
                            dataset_id=genome_dataset.dataset_id,
                            release_id=release_id,
                            is_current=0,
                        )
                        session.add(release_link)
                    release_link_map[link_key] = release_link

            session.flush()

            release_genome_datasets = (
                session.query(GenomeDataset).filter(GenomeDataset.release_id == release_id).all()
            )

            for genome_dataset in release_genome_datasets:
                if genome_dataset.genome_id not in scoped_genome_ids:
                    continue
                if genome_dataset.dataset.dataset_uuid in exclude_datasets:
                    continue
                if genome_dataset.dataset.status == DatasetStatus.FAULTY:
                    continue
                genome_dataset.dataset.status = DatasetStatus.RELEASED
                genome_dataset.is_current = 1
                logger.info("Dataset %s has been marked as released.", genome_dataset.dataset.dataset_uuid)

            self.ds_factory.is_current_datasets_resolve(release_id, session, genome_ids=scoped_genome_ids)

            genome_releases = (
                session.query(GenomeRelease)
                .filter(
                    GenomeRelease.release_id == release_id,
                    GenomeRelease.genome_id.in_(scoped_genome_ids),
                )
                .all()
            )
            for genome_release in genome_releases:
                genome_release.is_current = 1
                logger.info("Genome %s has been marked as current.", genome_release.genome.genome_uuid)

            touched_groups = (
                session.query(Genome.assembly_id, Genome.provider_name)
                .filter(Genome.genome_id.in_(scoped_genome_ids))
                .distinct()
                .all()
            )
            assembly_uuid_by_id = {
                assembly_id: assembly_uuid
                for assembly_id, assembly_uuid in session.query(Assembly.assembly_id, Assembly.assembly_uuid)
                .filter(Assembly.assembly_id.in_([assembly_id for assembly_id, _ in touched_groups]))
                .all()
            }
            for assembly_id, provider in touched_groups:
                assembly_genome_releases = (
                    session.query(GenomeRelease)
                    .join(Genome, GenomeRelease.genome_id == Genome.genome_id)
                    .join(EnsemblRelease, GenomeRelease.release_id == EnsemblRelease.release_id)
                    .filter(Genome.assembly_id == assembly_id)
                    .filter(Genome.provider_name == provider)
                    .filter(EnsemblRelease.release_type == "partial")
                    .all()
                )

                if not assembly_genome_releases:
                    continue

                winner = max(
                    assembly_genome_releases,
                    key=lambda gr: (
                        gr.genome.genebuild_date or "",
                        1 if gr.release_id == release_id else 0,
                        gr.release_id,
                    ),
                )

                for genome_release in assembly_genome_releases:
                    genome_release.is_current = (
                        1 if genome_release.genome_release_id == winner.genome_release_id else 0
                    )

                logger.info(
                    "Genome releases for assembly %s and provider %s have been updated.",
                    assembly_uuid_by_id.get(assembly_id, assembly_id),
                    provider,
                )

            errors = self.pre_release_check(release_id, session=session)
            if errors:
                raise ValueError(f"Release '{release.name}' has errors: {errors}")

            release.status = ReleaseStatus.RELEASED
            release.is_current = 1
            if not already_released:
                if release_date is None:
                    release.release_date = datetime.now().date()
                    logger.info("Release date set to current date: %s.", release.release_date)
                else:
                    release.release_date = datetime.strptime(release_date, "%Y-%m-%d").date()
                    logger.info("Release date set to specified date: %s.", release.release_date)
                release.label = release.release_date.isoformat()
            else:
                logger.info(
                    "Release %s was already released; keeping existing release date %s.",
                    release.name,
                    release.release_date,
                )

            other_releases = (
                session.query(EnsemblRelease)
                .filter(EnsemblRelease.release_id != release.release_id)
                .filter(EnsemblRelease.site_id == site_id)
                .filter(EnsemblRelease.release_type == "partial")
                .all()
            )
            for other_release in other_releases:
                other_release.is_current = 0
                logger.info("Release %s has been marked as not current.", other_release.name)

            session.flush()
            session.refresh(release)
            session.expunge(release)
            return release

    def prepare_integrated_release(self, version: Decimal, name: str) -> EnsemblRelease:
        """Prepare a new integrated release from current partial release state."""
        db = DBConnection(self.metadata_uri)
        with db.session_scope() as session:
            self._archive_existing_integrated_releases(session)
            release = self._insert_integrated_release(session, version, name)
            self._insert_genome_release_rows(session, release.release_id)
            self._insert_genome_dataset_rows(session, release.release_id)
            self._insert_genome_group_member_rows(session, release.release_id)
            session.commit()
            session.refresh(release)
            session.expunge(release)
            return release

    def _archive_existing_integrated_releases(self, session) -> int:
        """Archive any existing integrated releases before creating a new one."""
        result = session.execute(
            update(EnsemblRelease)
            .where(EnsemblRelease.release_type == "integrated")
            .values(status=ReleaseStatus.ARCHIVED, is_current=0)
        )
        archived = result.rowcount if hasattr(result, "rowcount") else 0
        logger.info("Archived %s existing integrated release(s).", archived)
        return archived

    def _insert_integrated_release(self, session, version: Decimal, name: str) -> EnsemblRelease:
        """Create and return a new integrated release record."""
        release_date = datetime.now().date()
        label = release_date.strftime("%Y-%m")
        release = EnsemblRelease(
            version=version,
            release_date=release_date,
            label=label,
            is_current=1,
            release_type="integrated",
            status=ReleaseStatus.RELEASED,
            name=name,
        )
        session.add(release)
        session.flush()
        session.refresh(release)
        logger.info("Created new integrated release %s (%s) with id %s.", name, version, release.release_id)
        return release

    def _insert_genome_release_rows(self, session, release_id: int) -> int:
        """Insert genome_release rows for genomes currently attached to partial releases."""
        genome_ids = (
            session.query(Genome.genome_id)
            .join(GenomeRelease, Genome.genome_id == GenomeRelease.genome_id)
            .join(EnsemblRelease, GenomeRelease.release_id == EnsemblRelease.release_id)
            .filter(GenomeRelease.is_current == 1)
            .filter(EnsemblRelease.release_type == "partial")
            .filter(Genome.suppressed == 0)
            .distinct()
            .all()
        )
        for (genome_id,) in genome_ids:
            session.add(GenomeRelease(genome_id=genome_id, release_id=release_id, is_current=1))
        count = len(genome_ids)
        logger.info("Inserted %s genome_release row(s) for the new integrated release.", count)
        return count

    def _insert_genome_dataset_rows(self, session, release_id: int) -> int:
        """Insert genome_dataset rows for datasets currently attached to partial releases."""
        dataset_pairs = (
            session.query(GenomeDataset.dataset_id, GenomeDataset.genome_id)
            .join(Genome, GenomeDataset.genome_id == Genome.genome_id)
            .join(EnsemblRelease, GenomeDataset.release_id == EnsemblRelease.release_id)
            .filter(GenomeDataset.is_current == 1)
            .filter(EnsemblRelease.release_type == "partial")
            .filter(Genome.suppressed == 0)
            .distinct()
            .all()
        )
        for dataset_id, genome_id in dataset_pairs:
            session.add(
                GenomeDataset(
                    is_current=1,
                    dataset_id=dataset_id,
                    genome_id=genome_id,
                    release_id=release_id,
                )
            )
        count = len(dataset_pairs)
        logger.info("Inserted %s genome_dataset row(s) for the new integrated release.", count)
        return count

    def _insert_genome_group_member_rows(self, session, release_id: int) -> int:
        """Insert genome_group_member rows for group assignments currently attached to partial releases."""
        group_members = (
            session.query(GenomeGroupMember.is_reference, GenomeGroupMember.genome_id, GenomeGroupMember.genome_group_id)
            .join(Genome, GenomeGroupMember.genome_id == Genome.genome_id)
            .join(EnsemblRelease, GenomeGroupMember.release_id == EnsemblRelease.release_id)
            .filter(GenomeGroupMember.is_current == 1)
            .filter(EnsemblRelease.release_type == "partial")
            .filter(Genome.suppressed == 0)
            .distinct()
            .all()
        )

        if not group_members:
            logger.info("No current genome group member rows found to copy for the new integrated release.")
            return 0

        rows = [
            {
                "is_current": 1,
                "is_reference": is_reference,
                "genome_id": genome_id,
                "genome_group_id": genome_group_id,
                "release_id": release_id,
            }
            for is_reference, genome_id, genome_group_id in group_members
        ]

        dialect_name = session.bind.dialect.name
        if dialect_name == "mysql":
            stmt = insert(GenomeGroupMember).prefix_with("IGNORE")
        elif dialect_name == "sqlite":
            stmt = insert(GenomeGroupMember).prefix_with("OR IGNORE")
        else:
            stmt = insert(GenomeGroupMember)

        session.execute(stmt, rows)
        inserted = len(rows)
        logger.info("Inserted %s genome_group_member row(s) for the new integrated release.", inserted)
        return inserted

    def pre_release_check(self, release: int | EnsemblRelease, session=None) -> list[str]:
        """
        Perform pre-checks on a given release to identify inconsistencies.

        This method verifies the following:
        1. Every dataset in the release should have an associated genome.
        2. Each genome must be associated with the release only once.
        3. All datasets attached to a genome should have a status of either 'Processed' or 'Released'.
           - Some dataset types are exceptions and can remain unprocessed.

        TODO:
        - Extend logic to validate variation, VEP, and regulation datasets.

        Args:
            release (int | EnsemblRelease): The release ID or `EnsemblRelease` instance to check.

        Returns:
            list[str]: A list of error messages indicating inconsistencies found in the release.
        """
        errors = []
        if session is None:
            db = DBConnection(self.metadata_uri)
            with db.session_scope() as db_session:
                return self.pre_release_check(release, session=db_session)

        if isinstance(release, EnsemblRelease):
            release_id = release.release_id
        else:
            release_id = release
        release = session.query(EnsemblRelease).filter(EnsemblRelease.release_id == release_id).one()

        genome_datasets = (
            session.query(GenomeDataset)
            .join(Dataset, GenomeDataset.dataset_id == Dataset.dataset_id)
            .join(DatasetType, Dataset.dataset_type_id == DatasetType.dataset_type_id)
            .join(Genome, GenomeDataset.genome_id == Genome.genome_id)
            .filter(GenomeDataset.release_id == release.release_id)
            .all()
        )

        allowed_unprocessed_type_names = {
            "vcf_handover",
            "short_variants",
            "regulation_tracks",
            "vep",
            "vep_assembly_feature",
            "vep_genome_feature",
            "variation_ftp_web",
            "regulation_handover",
            "variation_register_track",
        }

        processed_or_released_pairs = {
            (genome_dataset.genome_id, genome_dataset.dataset.dataset_type_id)
            for genome_dataset in genome_datasets
            if genome_dataset.dataset.status in (DatasetStatus.PROCESSED, DatasetStatus.RELEASED)
        }

        for genome_dataset in genome_datasets:
            dataset = genome_dataset.dataset
            if dataset.status not in (DatasetStatus.PROCESSED, DatasetStatus.RELEASED):
                if dataset.dataset_type.name not in allowed_unprocessed_type_names:
                    has_processed_alternative = (
                        genome_dataset.genome_id,
                        dataset.dataset_type_id,
                    )
                    has_processed_alternative = has_processed_alternative in processed_or_released_pairs

                    if not has_processed_alternative:
                        errors.append(f"Dataset [{dataset.dataset_uuid}] is neither processed nor released.")

        return errors
