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
from sqlalchemy import update

from ensembl.production.metadata.api.exceptions import *
from ensembl.production.metadata.api.factories.datasets import DatasetFactory
from ensembl.production.metadata.api.factories.genomes import GenomeFactory
from ensembl.production.metadata.api.factories.utils import get_genome_sets_by_assembly_and_provider
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
            status: str = "Planned"
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
            if release_date:
                try:
                    datetime.strptime(release_date, "%Y-%m-%d").date()
                except ValueError:
                    raise ValueError("Invalid release_date format. Expected YYYY-MM-DD.")

            # Ensure label is defined
            if label is None:
                if release_date is None:
                    raise ValueError("Either release_date or label must be specified.")
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
                release_date=release_date,  # Will be stored as NULL if None
                label=label,
                ensembl_site=site_obj,
                release_type=release_type,
                status=status
            )
            session.add(release)
            session.commit()
            session.refresh(release)
            return release

    def set_partial_released(self, version: Decimal = None, release_id=None, release_date: str = None,
                             site_name: str = "Ensembl",
                             force: bool = False) -> EnsemblRelease:
        """

        To use this factory, just call it with a release. It should work as intended, but  has not been fully tested
        for a non-forced release.
        For a forced release all genome_release entries must be created and set to the proper release.

        - Processes faulty datasets
        - Attaches and updates non-released datasets of specific types (vep, variation, etc.)
        - if not forced, checks to see each dataset can be updated to processed
        - Runs final checks before releasing any data or changing is_current status
        - Updates all associated datasets to 'Released' status and attaches them to the release.
        - Marks all associated genomes as current and unmarks outdated genomes.
        - Ensures only one 'current' dataset per dataset type exists.
        - Marks the release as 'Released' and sets the release date and label.
        """
        if version is None and release_id is None:
            raise ValueError("Either version or release_id must be provided.")
        db = DBConnection(self.metadata_uri)
        df = DatasetFactory()

        with db.session_scope() as session:
            # Validate site existence
            site = session.query(EnsemblSite).filter_by(name=site_name).one_or_none()
            if site is None:
                raise MissingMetaException(f"Site '{site_name}' not found.")
            site_id = site.site_id

            # Retrieve the release
            if release_id is None:
                # TODO: Check that it is partial a valid release and is not released.
                # Remove the version
                release = session.query(EnsemblRelease).filter_by(version=version).one()
                release_id = release.release_id
            else:
                release = session.query(EnsemblRelease).filter_by(release_id=release_id).one()

            # Process faulty datasets to prevent errors
            df.process_faulty(session)

            # Attach and update non-released datasets of specific types (vep, variation, etc.)
            df.attach_misc_datasets(release_id, session, force)

            # Update dataset statuses based on the force flag
            if force:
                # Update only genomes attached to this release
                #TODO: DOUBLE CHECK!!
                datasets = (session.query(Dataset)
                            .join(GenomeDataset)
                            .join(DatasetType)
                            .filter(GenomeDataset.release_id == release_id)
                            .filter(DatasetType.parent.is_(None))
                            .all())
            else:
                # Update all top-level datasets that are not released/faulty
                datasets = (session.query(Dataset)
                            .join(DatasetType)
                            .filter(Dataset.status.notin_([DatasetStatus.RELEASED, DatasetStatus.FAULTY]))
                            .filter(DatasetType.parent.is_(None))
                            .all())

            for dataset in datasets:
                df.update_parent_and_children_status(dataset_uuid=dataset.dataset_uuid,
                                                     session=session,
                                                     force=force,
                                                     status="Processed")

            # Attach genomes to the release if they have a processed genebuild dataset
            genomes = (session.query(Genome)
                       .join(GenomeDataset)
                       .join(Dataset)
                       .join(DatasetType)
                       .filter(DatasetType.name == "genebuild")
                       .filter(Dataset.status == DatasetStatus.PROCESSED)
                       .all())

            for genome in genomes:
                if not session.query(GenomeRelease).filter_by(genome_id=genome.genome_id,
                                                              release_id=release_id).count():
                    session.add(GenomeRelease(genome_id=genome.genome_id, release_id=release_id, is_current=0))
                    session.commit()

            # Attach all datasets linked to genomes in this release
            datasets = (session.query(Dataset)
                        .join(GenomeDataset)
                        .join(Genome)
                        .join(GenomeRelease)
                        .filter(GenomeRelease.release_id == release_id)
                        .all())
            # non forced
            if force is False:
                for dataset in datasets:
                    if dataset.status == "Processed":
                        for genome_dataset in dataset.genome_datasets:
                            if genome_dataset.release_id is None:
                                genome_dataset.release_id = release_id
            else:
                for dataset in datasets:
                    for genome_dataset in dataset.genome_datasets:
                        if genome_dataset.release_id is None:
                            genome_dataset.release_id = release_id
            session.commit()

            # Final check before committing changes
            errors = self.pre_release_check(release_id)

            if errors:
                if not force:
                    raise ValueError(f"Release {version} has errors: {errors}")
                else:
                    print(f"Release {version} has errors: {errors}")
                    input("Are you sure you want to continue? Press any key to continue or Ctrl+C to exit.")
                    print("You are continuing. Good luck with that.")
            # Mark datasets as released and set them as current
            genome_datasets = session.query(GenomeDataset).filter_by(release_id=release_id).all()
            for genome_dataset in genome_datasets:
                genome_dataset.dataset.status = "Released"
                genome_dataset.is_current = 1
                session.commit()

            # Ensure only one current dataset per dataset type
            df.is_current_datasets_resolve(release_id, session)

            # Mark all genome releases as current
            genome_releases = session.query(GenomeRelease).filter_by(release_id=release_id).all()
            for genome_release in genome_releases:
                genome_release.is_current = 1

            # Adjust older genome releases to is_current=0
            genome_sets = get_genome_sets_by_assembly_and_provider(session)
            for (assembly_uuid, provider), genomes in genome_sets.items():
                genome_releases = (session.query(GenomeRelease)
                                   .join(Genome, GenomeRelease.genome_id == Genome.genome_id)
                                   .join(EnsemblRelease, GenomeRelease.release_id == EnsemblRelease.release_id)
                                   .filter(Genome.genome_uuid.in_([g[0] for g in genomes]))
                                   .filter(EnsemblRelease.release_type == "partial")
                                   .all())

                is_current_releases = [gr for gr in genome_releases if gr.is_current == 1]
                if len(is_current_releases) > 1:
                    is_current_releases.sort(
                        key=lambda gr: next(g[1] for g in genomes if g[0] == gr.genome.genome_uuid))
                    for gr in is_current_releases[:-1]:  # Unmark older ones
                        session.execute(
                            update(GenomeRelease).where(GenomeRelease.genome_release_id == gr.genome_release_id).values(
                                is_current=0))

            session.commit()

            # Update release information
            if release.status != "Planned" and not force:
                raise ValueError(f"Release {version} is not in 'Planned' status.")

            release.status = "Released"
            release.is_current = 1
            if release_date is None:
                release.release_date = datetime.now().date()
            else:
                release.release_date = datetime.strptime(release_date, "%Y-%m-%d").date()
            release.label = release.release_date
            session.commit()

            # Mark all other partial releases from the same site as not current
            other_releases = (session.query(EnsemblRelease)
                              .filter(EnsemblRelease.release_id != release.release_id)
                              .filter(EnsemblRelease.site_id == site_id)
                              .filter(EnsemblRelease.release_type == "partial")
                              .all())
            #TODO:not integrated
            for other_release in other_releases:
                other_release.is_current = 0

            return release

    def pre_release_check(self, release: int | EnsemblRelease) -> list[str]:
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
        db = DBConnection(self.metadata_uri)

        with db.session_scope() as session:
            # Ensure we have an EnsemblRelease instance
            release = session.query(EnsemblRelease).filter(EnsemblRelease.release_id == release).one()

            # Retrieve all genome datasets associated with this release
            genome_datasets = (
                session.query(GenomeDataset)
                .filter(GenomeDataset.release_id == release.release_id)
                .all()
            )

            for genome_dataset in genome_datasets:
                # Retrieve the genome linked to this dataset
                genome = session.query(Genome).filter(Genome.genome_id == genome_dataset.genome_id).one()

                # Fetch all datasets attached to this genome
                dataset = genome_dataset.dataset
                # Define dataset types that are allowed to be unprocessed
                allowed_unprocessed_types = {
                    "vcf_handover",
                    "variation",
                    "regulatory_features",
                    "vep",
                    "vep_feature",
                    "variation_ftp_web",
                    "regulation_handover",
                    "variation_register_track",
                }

                # Validate dataset statuses
                if dataset.status not in ("Processed", "Released"):
                    if dataset.dataset_type not in allowed_unprocessed_types:
                        # Check if another dataset of the same type is processed
                        has_processed_alternative = session.query(Dataset).join(GenomeDataset).filter(
                            GenomeDataset.genome_id == genome.genome_id,
                            Dataset.dataset_type == dataset.dataset_type,
                            Dataset.status == "Processed"
                        ).count() > 0

                        if not has_processed_alternative:
                            errors.append(
                                f"Dataset [{dataset.dataset_uuid}] is neither processed nor released."
                            )

        return errors
