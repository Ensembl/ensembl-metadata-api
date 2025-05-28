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

import logging
import uuid
from collections import defaultdict

import sqlalchemy.orm
from ensembl.utils.database.dbconnection import DBConnection
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy.sql import func

from ensembl.production.metadata.api.exceptions import *
from ensembl.production.metadata.api.models import Dataset, Genome, GenomeDataset, \
    DatasetType, DatasetStatus, EnsemblRelease, DatasetSource, GenomeRelease
from ensembl.production.metadata.updater.updater_utils import update_attributes

logger = logging.getLogger(__name__)


class DatasetFactory:

    def __init__(self, conn_uri=None):
        self.conn_uri = conn_uri

    def __get_db_connexion(self):
        if self.conn_uri:
            return DBConnection(self.conn_uri)
        else:
            raise ValueError("No connection URI provided")

    def simple_update_dataset_status(self, dataset_uuid: str, status: DatasetStatus, session=None):
        """
        Update the status of a dataset.

        If no session is provided, a new database session is created.

        Args:
            dataset_uuid (str): The UUID of the dataset to update.
            status (DatasetStatus): The new status to set.
            session (Session, optional): SQLAlchemy session object. If None, a new session is created.

        Returns:
            Tuple[str, DatasetStatus]: The dataset UUID and its updated status.
        """
        if session is None:
            with self.__get_db_connexion().session_scope() as db_session:
                return self.simple_update_dataset_status(dataset_uuid, status, session=db_session)

        dataset = self.__get_dataset(session, dataset_uuid)
        dataset.status = status
        session.commit()
        return dataset.dataset_uuid, dataset.status

    def create_all_child_datasets(self, dataset_uuid: str,
                                  session: sqlalchemy.orm.Session = None,
                                  topic: str = 'production_process',
                                  status: DatasetStatus = None,
                                  release: EnsemblRelease = None):
        # CURRENTLY BROKEN FOR STATUS AND RELEASE. Marc broke it with his last update. Trace back to fix.
        # Retrieve the top-level dataset
        # Will not work on datasets that are tied to multiple genomes!
        # !!!! WILL CREATE THE DATASETS EVEN IF THEY ALREADY EXIST
        if not session:
            with self.__get_db_connexion().session_scope() as db_session:
                return self.create_all_child_datasets(dataset_uuid, db_session, topic, status, release)
        top_level_dataset = self.__get_dataset(session, dataset_uuid)
        self.__create_child_datasets_recursive(session=session,
                                               parent_dataset=top_level_dataset,
                                               topic=topic,
                                               status=status,
                                               release=release)
        return self.query_all_child_datasets(dataset_uuid, session)

    def create_dataset(self, session, genome_input, dataset_source, dataset_type, dataset_attributes, name, label,
                       version, status=DatasetStatus.SUBMITTED, parent=None, release=None, source_type=None,
                       is_current=False):
        # Check if genome_input is a UUID (string) or a Genome object
        if isinstance(status, str):
            status = DatasetStatus(status)

        if isinstance(genome_input, str):
            genome = session.query(Genome).filter(Genome.genome_uuid == genome_input).one()
        elif isinstance(genome_input, Genome):
            genome = genome_input
        elif genome_input is None:
            genome = None
        else:
            raise ValueError("Invalid genome input. Must be either a UUID string or a Genome object. "
                             f"Got {genome_input}/{genome_input.__class__}")
        # Create Dataset source if it does not exist
        if isinstance(dataset_source, str):
            if source_type is None or dataset_source is None:
                raise ValueError(
                    "Invalid Source input. Must be either a string and source_type or DatasetSource object. "
                    f"Got {dataset_source}/{dataset_source.__class__} for dataset_source and "
                    f"{source_type}/{source_type.__class__} for source_type")
            test = session.query(DatasetSource).filter(DatasetSource.name == dataset_source).one_or_none()
            if test is None:
                dataset_source = DatasetSource(type=source_type, name=dataset_source)
            else:
                dataset_source = test
        # Query Dataset type
        if isinstance(dataset_type, str):
            dataset_type = session.query(DatasetType).filter(DatasetType.name == dataset_type).one()

        new_dataset = Dataset(
            dataset_uuid=str(uuid.uuid4()),
            dataset_type=dataset_type,  # Must be an object returned from the current session
            name=name,
            version=version,
            label=label,
            created=func.now(),
            dataset_source=dataset_source,  # Must
            status=status,
            parent_id=parent.dataset_id if parent else None
        )
        if dataset_attributes is not None:
            new_dataset_attributes = update_attributes(new_dataset, dataset_attributes, session)
        else:
            new_dataset_attributes = None
        dataset_uuid = new_dataset.dataset_uuid

        if genome is not None:
            new_genome_dataset = GenomeDataset(
                genome=genome,
                dataset=new_dataset,
                is_current=is_current,
            )
            if release is not None:
                if isinstance(release, str):
                    release = session.query(EnsemblRelease).filter(EnsemblRelease.version == release).one()
                logger.debug(f"Attaching {new_dataset.dataset_uuid} to release {release.release_id}")
                new_genome_dataset.release_id = release.release_id
            session.add(new_genome_dataset)
            session.commit()
            return dataset_uuid, new_dataset, new_dataset_attributes, new_genome_dataset
        else:
            return dataset_uuid, new_dataset, new_dataset_attributes, None

    def get_parent_datasets(self, dataset_uuid, **kwargs):
        session = kwargs.get('session')
        if not session:
            with self.__get_db_connexion().session_scope() as db_session:
                return self.get_parent_datasets(dataset_uuid, session=db_session)
        return self.__query_parent_datasets(session, dataset_uuid)

    def update_dataset_status(self, dataset_uuid, status, **kwargs):
        if isinstance(status, str):
            status = DatasetStatus(status)
        updated_datasets = [(dataset_uuid, status)]
        session = kwargs.get('session')
        attribute_dict = kwargs.get('attribute_dict')
        if not session:
            with self.__get_db_connexion().session_scope() as db_session:
                return self.update_dataset_status(dataset_uuid, status, session=db_session)
        updated_datasets = self.__update_status(session, dataset_uuid, status)
        if attribute_dict:
            self.update_dataset_attributes(dataset_uuid, attribute_dict, session=session)
        return updated_datasets

    def update_parent_and_children_status(self, dataset_uuid: str, status: DatasetStatus = None,
                                          session: Session = None,
                                          force: bool = False):
        if not session:
            with self.__get_db_connexion().session_scope() as db_session:
                return self.update_parent_and_children_status(dataset_uuid=dataset_uuid, session=db_session,
                                                              status=status, force=force)

        dataset = self.__get_dataset(session, dataset_uuid)

        if dataset.status in [DatasetStatus.FAULTY, DatasetStatus.RELEASED]:
            print(f"Dataset {dataset_uuid} is FAULTY or RELEASED and will not be updated.")
            return

        hierarchy_levels = defaultdict(list)
        terminals = []

        def gather_children(ds, level=0):
            if ds.children:
                hierarchy_levels[level].append(ds)
                for child in ds.children:
                    gather_children(child, level + 1)
            else:
                terminals.append(ds)

        gather_children(dataset)

        def force_update(ds, new_status):
            if ds.status not in [DatasetStatus.FAULTY, DatasetStatus.RELEASED]:
                ds.status = new_status
                for child in ds.children:
                    force_update(child, new_status)

        if force and status:
            force_update(dataset, status)

        elif status:
            for terminal_ds in terminals:
                if terminal_ds.status not in [DatasetStatus.FAULTY, DatasetStatus.RELEASED]:
                    terminal_ds.status = status

        # Update parents starting from deepest level
        for level in sorted(hierarchy_levels.keys(), reverse=True):
            for parent_ds in hierarchy_levels[level]:
                child_statuses = {child.status for child in parent_ds.children}

                if DatasetStatus.PROCESSING in child_statuses:
                    parent_ds.status = DatasetStatus.PROCESSING
                elif all(s == DatasetStatus.SUBMITTED for s in child_statuses):
                    parent_ds.status = DatasetStatus.SUBMITTED
                elif all(s in [DatasetStatus.PROCESSED, DatasetStatus.RELEASED] for s in child_statuses):
                    if status == DatasetStatus.RELEASED:
                        parent_ds.status = DatasetStatus.RELEASED
                    else:
                        parent_ds.status = DatasetStatus.PROCESSED

        try:
            session.commit()
            print(f"Dataset {dataset_uuid} statuses updated successfully.")
        except IntegrityError as e:
            session.rollback()
            raise RuntimeError(f"Failed to update dataset statuses: {e}")

    def is_current_datasets_resolve(self, release_id, session=None, logger=None):
        """
        Ensures that for each (genome_id, dataset_type_id) combination,
        only one GenomeDataset has is_current=1, prioritizing the dataset with the given release_id.

        :param session: SQLAlchemy session object
        :param release_id: The release_id to prioritize
        :param logger: Optional logging.Logger instance
        :return: List of altered GenomeDataset objects
        """
        if not session:
            with self.__get_db_connexion().session_scope() as db_session:
                return self.is_current_datasets_resolve(release_id=release_id, session=db_session, logger=logger)

        log = logger.info if logger else print
        # TODO: Reimplement this method to manage with unreleased datasets and dataset_types that may have multiple is_current=1 entries correctly
        # # Step 1: Identify problem pairs
        # log("Scanning for (genome_id, dataset_type_id) combinations with multiple is_current=1 GenomeDatasets...")
        # genome_type_pairs = (
        #     session.query(GenomeDataset.genome_id, Dataset.dataset_type_id)
        #     .join(Dataset, GenomeDataset.dataset_id == Dataset.dataset_id)
        #     .filter(GenomeDataset.is_current == 1)
        #     .group_by(GenomeDataset.genome_id, Dataset.dataset_type_id)
        #     .having(func.count(GenomeDataset.genome_dataset_id) > 1)
        #     .all()
        # )
        #
        # if not genome_type_pairs:
        #     log("No duplicates found. Nothing to fix.")
        #     return []
        #
        # altered_datasets = []
        #
        # for genome_id, dataset_type_id in genome_type_pairs:
        #     log(f"Fixing genome_id={genome_id}, dataset_type_id={dataset_type_id}")
        #
        #     gds = (
        #         session.query(GenomeDataset)
        #         .join(Dataset, GenomeDataset.dataset_id == Dataset.dataset_id)
        #         .filter(
        #             GenomeDataset.genome_id == genome_id,
        #             Dataset.dataset_type_id == dataset_type_id
        #         )
        #         .all()
        #     )
        #
        #     if not gds:
        #         log(f"  [WARN] No GenomeDataset found for genome_id={genome_id}, dataset_type_id={dataset_type_id}")
        #         continue
        #
        #     current_gds = [gd for gd in gds if gd.is_current]
        #     log(f"  Found {len(current_gds)} is_current=1 entries")
        #
        #     # Log current entries
        #     for gd in current_gds:
        #         log(f"    - GD_ID={gd.genome_dataset_id}, dataset_id={gd.dataset_id}, release_id={gd.release_id}")
        #
        #     # Reset all to is_current=0
        #     for gd in current_gds:
        #         altered_datasets.append(gd)
        #         gd.is_current = 0
        #
        #     # Set is_current=1 for matching release_id
        #     matching = [gd for gd in gds if gd.release_id == release_id]
        #     if len(matching) == 1:
        #         matching[0].is_current = 1
        #         log(f"  => Marked GD_ID={matching[0].genome_dataset_id} as current (release_id={release_id})")
        #     elif len(matching) == 0:
        #         log(f"  [WARN] No GenomeDataset found with release_id={release_id}")
        #     else:
        #         log(f"  [ERROR] Multiple GenomeDatasets found with release_id={release_id}, skipping setting current!")
        #
        # session.commit()
        # log(f"Finished resolving is_current flags. {len(altered_datasets)} entries modified.")
        # return altered_datasets

    def attach_misc_datasets(self, release_id, session=None, force=False):
        """
        Attaches top-level non-genebuild and non-assembly datasets to a release if they are in a PROCESSED state.
        If a dataset has child datasets that are FAULTY, PROCESSING, or SUBMITTED, its release should be removed.
        If force=True, it overrides the removal check, treating SUBMITTED, PROCESSING, and PROCESSED as equivalent.

        - Ensures only one dataset of each type per parent is considered.
        - If all required child datasets are PROCESSED (or equivalent if force=True), the genome is attached.
        - If multiple datasets of the same type exist, only PROCESSED ones are updated.
        """
        if session is None:
            with self.__get_db_connexion().session_scope() as db_session:
                return self.attach_misc_datasets(release_id=release_id, session=db_session, force=force)

        valid_statuses = {DatasetStatus.PROCESSED}
        if force:
            valid_statuses.update({DatasetStatus.SUBMITTED, DatasetStatus.PROCESSING})

        # Get all top-level datasets that are NOT Faulty, NOT Released, and NOT genebuild/assembly
        datasets = (
            session.query(Dataset)
            .join(DatasetType, Dataset.dataset_type_id == DatasetType.dataset_type_id)
            .filter(Dataset.status.notin_([DatasetStatus.RELEASED, DatasetStatus.FAULTY]))
            .filter(DatasetType.name.notin_(['genebuild', 'assembly']))
            .filter(DatasetType.parent.is_(None))
            .all()
        )

        for dataset in datasets:
            self.update_parent_and_children_status(dataset.dataset_uuid, session=session)
            # Get child datasets and ensure only one per type
            dataset_type_map = {}
            has_valid_status = False
            has_faulty = False

            for child_uuid, child_status in self.__query_child_datasets(session=session,
                                                                        dataset_uuid=dataset.dataset_uuid):
                child_dataset = session.query(Dataset).filter(Dataset.dataset_uuid == child_uuid).one()
                dataset_type_id = child_dataset.dataset_type_id

                # Track Faulty status
                if child_dataset.status == DatasetStatus.FAULTY:
                    has_faulty = True
                    continue  # Ignore if other valid datasets exist

                # Store one dataset per type, preferring PROCESSED
                if dataset_type_id not in dataset_type_map or dataset_type_map[dataset_type_id][
                    1] not in valid_statuses:
                    dataset_type_map[dataset_type_id] = (child_dataset, child_status)

                if child_status in valid_statuses:
                    has_valid_status = True

            if has_faulty and not has_valid_status:
                # Remove dataset from release
                all_child_datasets = self.query_all_child_datasets(dataset.dataset_uuid, session)
                all_child_datasets.append((dataset.dataset_uuid, None))
                child_uuids = [child_uuid for child_uuid, _ in all_child_datasets]

                session.query(GenomeDataset).filter(GenomeDataset.dataset_id.in_(child_uuids)).update(
                    {"release_id": None}, synchronize_session=False
                )
                logger.info(f"Removed release from dataset {dataset.dataset_uuid} and {len(child_uuids)} children")
                continue  # Skip further processing for this dataset
            if has_valid_status or (dataset.status in valid_statuses and not has_faulty):
                # Check if it is attached to a genebuild that is processed.

                genome_id = dataset.genome_datasets[0].genome_id
                genebuild_dataset = session.query(Dataset).join(GenomeDataset).filter(
                    GenomeDataset.genome_id == genome_id).filter(Dataset.name == "genebuild").one()

                if (
                        genebuild_dataset.status != DatasetStatus.PROCESSED and genebuild_dataset.status != DatasetStatus.RELEASED):
                    continue

                # Get all child datasets including the parent dataset
                all_child_datasets = self.query_all_child_datasets(dataset.dataset_uuid, session)
                all_child_datasets.append((dataset.dataset_uuid, dataset.status))
                child_uuids = [child_uuid for child_uuid, _ in all_child_datasets]

                for child_uuid in child_uuids:
                    dataset_obj = session.query(Dataset).filter(Dataset.dataset_uuid == child_uuid).one()

                    # Skip if dataset is FAULTY or RELEASED
                    if dataset_obj.status in (DatasetStatus.FAULTY, DatasetStatus.RELEASED):
                        continue  # ✅ Skip updating or inserting for this dataset

                    # Check if GenomeDataset exists for this dataset & genome
                    genome_dataset = session.query(GenomeDataset).filter(
                        GenomeDataset.dataset_id == dataset_obj.dataset_id,
                        GenomeDataset.genome_id == genome_id
                    ).one_or_none()

                    if genome_dataset:
                        # ✅ Update release_id even if it was attached to a previous release
                        genome_dataset.release_id = release_id
                    else:
                        # ✅ If it doesn’t exist, create a new one
                        new_gd = GenomeDataset(
                            genome_id=genome_id,
                            dataset=dataset_obj,
                            is_current=True,
                            release_id=release_id,
                        )
                        session.add(new_gd)

                session.commit()

    def process_faulty(self, session=None):
        """
        Process all datasets marked as FAULTY and handle their relationships.
        If no session is provided, a new database session is created.

        Steps:
        1. Identify all FAULTY datasets.
        2. Traverse upwards to mark all parent datasets as FAULTY.
        3. Retrieve all child datasets from the top-level parent and remove their release association.
        4. If any dataset in the chain has dataset_type.name of 'genebuild' or 'assembly':
           - Remove all genome_dataset.release_id values for the associated genome.
           - Delete all GenomeRelease entries for the affected genomes.
           - Don't remove any assembly datasets if they are attached to multiple genomes.

        Args:
            session (Session): SQLAlchemy session object for database operations.
        """
        if session is None:
            with self.__get_db_connexion().session_scope() as db_session:
                return self.process_faulty(session=db_session)

        faulty_datasets = session.query(Dataset).filter(Dataset.status == DatasetStatus.FAULTY).all()
        if not faulty_datasets:
            logger.info("No faulty datasets found.")
            return

        logger.info(f"Processing {len(faulty_datasets)} faulty datasets.")

        updated_datasets = set()
        genomes_to_remove_release = set()

        for dataset in faulty_datasets:
            # Find the top-level parent dataset
            top_level_uuid = self.__query_top_level_parent(session, dataset.dataset_uuid)

            # Traverse upwards and mark all parent datasets as FAULTY
            current_uuid = dataset.dataset_uuid
            while current_uuid:
                parent_uuid, _ = self.__query_parent_datasets(session, current_uuid)
                if parent_uuid is None:
                    break
                parent_dataset = session.query(Dataset).filter(Dataset.dataset_uuid == parent_uuid).one()
                if parent_dataset.status != DatasetStatus.FAULTY:
                    parent_dataset.status = DatasetStatus.FAULTY
                    updated_datasets.add(parent_dataset.dataset_uuid)
                current_uuid = parent_uuid

            # Get all child datasets including the top-level parent itself
            all_child_datasets = self.query_all_child_datasets(top_level_uuid, session)
            all_child_datasets.append((top_level_uuid, None))

            # Remove release IDs where applicable
            for child_uuid, _ in all_child_datasets:
                genome_datasets = (
                    session.query(GenomeDataset)
                    .join(Dataset)
                    .filter(Dataset.dataset_uuid == child_uuid)
                    .all()
                )
                for genome_dataset in genome_datasets:
                    if genome_dataset.release_id:
                        logger.info(f"Removing release from dataset {child_uuid}")
                        genome_dataset.release_id = None
                        updated_datasets.add(child_uuid)

                    # Track genomes that need full release removal if dataset is 'genebuild' or 'assembly'
                    if genome_dataset.dataset.dataset_type.name in {"genebuild", "assembly"}:
                        assembly_datasets = session.query(Dataset).join(GenomeDataset).join(DatasetType).filter(
                            GenomeDataset.genome_id == genome_dataset.genome_id).filter(
                            Dataset.status != DatasetStatus.FAULTY).filter(DatasetType.name == "assembly").all()
                        if len(assembly_datasets) == 0:
                            genomes_to_remove_release.add(genome_dataset.genome_id)
                            continue
                            # The following section would reomve the assembly from good genomes with multiple releases.
                            # Need to combine the two with integrated releases.
                        # genebuild_datasets = session.query(Dataset).join(GenomeDataset).join(DatasetType).filter(
                        #     GenomeDataset.genome_id == genome_dataset.genome_id).filter(
                        #     Dataset.status != DatasetStatus.FAULTY).filter(DatasetType.name == "genebuild").all()
                        # if len(genebuild_datasets) == 0:
                        #     genomes_to_remove_release.add(genome_dataset.genome_id)

        # Remove genome releases if necessary
        if genomes_to_remove_release:
            logger.info(f"Removing genome releases for {len(genomes_to_remove_release)} genomes.")

            # Remove release associations from all datasets linked to affected genomes
            genome_datasets = (
                session.query(GenomeDataset)
                .filter(GenomeDataset.genome_id.in_(genomes_to_remove_release))
                .all()
            )
            for genome_dataset in genome_datasets:
                if genome_dataset.release_id:
                    logger.info(
                        f"Removing release from dataset {genome_dataset.dataset.dataset_uuid} "
                        f"(linked to genome {genome_dataset.genome.genome_uuid})"
                    )
                    genome_dataset.release_id = None

            # Delete all GenomeRelease entries for affected genomes
            genome_releases = (
                session.query(GenomeRelease)
                .filter(GenomeRelease.genome_id.in_(genomes_to_remove_release))
                .all()
            )
            for genome_release in genome_releases:
                logger.info(f"Removing GenomeRelease entry for genome {genome_release.genome.genome_uuid}")
                session.delete(genome_release)

        session.commit()
        logger.info(f"Updated {len(updated_datasets)} datasets as FAULTY and removed releases where applicable.")

    def update_dataset_attributes(self, dataset_uuid, attribute_dict, **kwargs):
        session = kwargs.get('session')
        if not isinstance(attribute_dict, dict):
            raise TypeError("attribute_dict must be a dictionary")
        if not session:
            with self.__get_db_connexion().session_scope() as db_session:
                return self.update_dataset_attributes(dataset_uuid, attribute_dict, session=db_session)
        dataset = self.__get_dataset(session, dataset_uuid)
        dataset_attributes = update_attributes(dataset, attribute_dict, session)
        return dataset_attributes

    def get_genomes_by_status_and_type(self, status, dataset_type, **kwargs):
        if isinstance(status, str):
            status = DatasetStatus(status)
        session = kwargs.get('session')
        if not session:
            with self.__get_db_connexion().session_scope() as db_session:
                return self.get_genomes_by_status_and_type(status, dataset_type, session=db_session)
        genome_data = self.__query_genomes_by_status_and_type(session, status, dataset_type)
        return genome_data

    def __create_child_datasets_recursive(self, session, parent_dataset, topic=None, status=None, release=None):
        parent_dataset_type = parent_dataset.dataset_type

        # Find child dataset types for the parent dataset type
        child_dataset_types = session.query(DatasetType).filter(
            DatasetType.parent == parent_dataset_type.dataset_type_id)
        if topic is not None:
            child_dataset_types = child_dataset_types.filter(DatasetType.topic == topic)
        status = status or DatasetStatus.SUBMITTED
        for child_type in child_dataset_types.all():
            # Check if a dataset with the same type and genome exists
            existing_datasets = session.query(Dataset).join(GenomeDataset).filter(
                Dataset.dataset_type_id == child_type.dataset_type_id,
                GenomeDataset.genome_id.in_([gd.genome_id for gd in parent_dataset.genome_datasets])
            ).all()
            exist_ds = next((
                d for d in existing_datasets if d.status in [DatasetStatus.SUBMITTED, DatasetStatus.PROCESSING]), None)
            logger.debug(f"Skipped creation {exist_ds.name} is Submitted/Processing") if exist_ds else None
            if len(parent_dataset.genome_datasets) > 1:
                raise ValueError("More than one genome linked to a genome_dataset")

            # Get the first genome's UUID
            genome_uuid = parent_dataset.genome_datasets[0].genome.genome_uuid
            dataset_source = parent_dataset.dataset_source
            dataset_type = child_type
            dataset_attributes = {}  # Populate with appropriate attributes
            name = dataset_type.name
            label = f"From {parent_dataset.dataset_uuid}"
            version = parent_dataset.version
            # Create the child dataset
            if not exist_ds:
                # logger.debug(f"Creating dataset {dataset_type.name}/{dataset_source.name}/{status.value}/{release}")
                child_uuid, dataset, attributes, g_dataset = self.create_dataset(session=session,
                                                                                 genome_input=genome_uuid,
                                                                                 dataset_source=dataset_source,
                                                                                 dataset_type=dataset_type,
                                                                                 dataset_attributes=dataset_attributes,
                                                                                 name=name,
                                                                                 label=label,
                                                                                 version=version,
                                                                                 parent=parent_dataset,
                                                                                 status=status,
                                                                                 release=release)
            else:
                child_uuid = exist_ds.dataset_uuid

            session.commit()
            # Recursively create children of this new child dataset
            child_dataset = self.__get_dataset(session, child_uuid)
            self.__create_child_datasets_recursive(session=session,
                                                   parent_dataset=child_dataset,
                                                   topic=topic,
                                                   status=status,
                                                   release=release)

    def __query_parent_datasets(self, session, dataset_uuid):
        dataset = self.__get_dataset(session, dataset_uuid)
        dataset_type = session.query(DatasetType).filter(
            DatasetType.dataset_type_id == dataset.dataset_type_id).one()
        if dataset_type.parent is None:
            return None, None
        parent_dataset_type = dataset_type.parent
        genome_id = next((gd.genome_id for gd in dataset.genome_datasets), None)
        if not genome_id:
            raise ValueError("No associated Genome found for the given dataset UUID")

        parent_genome_dataset = session.query(GenomeDataset).join(Dataset).join(DatasetType).filter(
            GenomeDataset.genome_id == genome_id,
            DatasetType.dataset_type_id == parent_dataset_type).one()
        parent_uuid = parent_genome_dataset.dataset.dataset_uuid
        parent_status = parent_genome_dataset.dataset.status
        return parent_uuid, parent_status

    def __query_top_level_parent(self, session, dataset_uuid):
        current_uuid = dataset_uuid
        while True:
            parent_data, parent_status = self.__query_parent_datasets(session, current_uuid)
            if parent_data is None:
                return current_uuid
            current_uuid = parent_data

    def __query_related_genome_by_type(self, session, dataset_uuid, dataset_type):
        dataset = self.__get_dataset(session, dataset_uuid)
        genome_id = next((gd.genome_id for gd in dataset.genome_datasets), None)
        if not genome_id:
            raise ValueError("No associated Genome found for the given dataset UUID")

        # Determine if dataset_type is an ID or a name
        if isinstance(dataset_type, int) or (isinstance(dataset_type, str) and dataset_type.isdigit()):
            filter_condition = (GenomeDataset.genome_id == genome_id, Dataset.dataset_type_id == dataset_type)
        else:
            filter_condition = (GenomeDataset.genome_id == genome_id, DatasetType.name == dataset_type)

        related_genome_dataset = session.query(GenomeDataset).join(Dataset).join(DatasetType).filter(
            *filter_condition).one()
        related_uuid = related_genome_dataset.dataset.dataset_uuid
        related_status = related_genome_dataset.dataset.status
        return related_uuid, related_status

    def __query_child_datasets(self, session, dataset_uuid):
        parent_dataset = self.__get_dataset(session, dataset_uuid)
        parent_dataset_type = session.query(DatasetType).filter(
            DatasetType.dataset_type_id == parent_dataset.dataset_type_id).one()
        child_dataset_types = session.query(DatasetType).filter(
            DatasetType.parent == parent_dataset_type.dataset_type_id).all()
        if not child_dataset_types:
            return []  # Return an empty list if no child types are found
        # This will break if we have multiple genome datasets for a single dataset, which is not currently the case.
        genome_id = parent_dataset.genome_datasets[0].genome_id
        if not genome_id:
            raise ValueError("No associated Genome found for the given parent dataset UUID")

        child_datasets = []
        for child_type in child_dataset_types:
            child_datasets.extend(session.query(GenomeDataset).join(Dataset).join(DatasetType).filter(
                GenomeDataset.genome_id == genome_id,
                DatasetType.dataset_type_id == child_type.dataset_type_id
            ).all())

        child_data = [(ds.dataset.dataset_uuid, ds.dataset.status) for ds in child_datasets]

        return child_data

    def query_all_child_datasets(self, parent_dataset_uuid, session=None):
        if not session:
            with self.__get_db_connexion().session_scope() as db_session:
                return self.query_all_child_datasets(parent_dataset_uuid, db_session)
        # This method returns the child datasets for a given dataset
        child_datasets = self.__query_child_datasets(session, parent_dataset_uuid)

        all_child_datasets = []
        for child_uuid, child_status in child_datasets:
            all_child_datasets.append((child_uuid, child_status))
            sub_children = self.query_all_child_datasets(child_uuid, session)
            all_child_datasets.extend(sub_children)
        return all_child_datasets

    def __query_depends_on(self, session, dataset_uuid):
        dataset = session.query(Dataset).filter(Dataset.dataset_uuid == dataset_uuid).one_or_none()
        dataset_type = dataset.dataset_type
        dependent_types = dataset_type.depends_on.split(',') if dataset_type.depends_on else []
        dependent_datasets_info = []
        for dtype in dependent_types:
            new_uuid, new_status = self.__query_related_genome_by_type(session, dataset_uuid, dtype)
            dependent_datasets_info.append((new_uuid, new_status))
        return dependent_datasets_info

    def __update_status(self, session, dataset_uuid, status):
        # Processed to Released. Only accept top level. Check that all assembly and genebuild datsets (all the way down) are processed.
        # Then convert all to "Released".
        # Add a blocker and warning in here.
        current_dataset = session.query(Dataset).filter(Dataset.dataset_uuid == dataset_uuid).one()
        updated_datasets = (dataset_uuid, current_dataset.status)
        # if released
        if isinstance(status, str):
            status = DatasetStatus(status)
        if status == DatasetStatus.SUBMITTED:  # "Submitted":
            # Update to SUBMITTED and all parents.
            # Do not touch the children.
            # This should only be called in times of strife and error.
            current_dataset.status = DatasetStatus.SUBMITTED  # "Submitted"
            parent_uuid, parent_status = self.__query_parent_datasets(session, dataset_uuid)
            if parent_uuid is not None:
                self.__update_status(session, parent_uuid, DatasetStatus.SUBMITTED)  # "Submitted")

        elif status == DatasetStatus.PROCESSING:  # "Processing":
            # Update to PROCESSING and all parents.
            # Do not touch the children.
            if current_dataset.status == DatasetStatus.RELEASED:  # "Released":  # and it is not top level.
                return updated_datasets
            # Check the dependents
            dependents = self.__query_depends_on(session, dataset_uuid)
            for uuid, dep_status in dependents:
                if dep_status not in (DatasetStatus.PROCESSED, DatasetStatus.RELEASED):  # ("Processed", "Released"):
                    return updated_datasets
            current_dataset.status = DatasetStatus.PROCESSING  # "Processing"
            parent_uuid, parent_status = self.__query_parent_datasets(session, dataset_uuid)
            if parent_uuid is not None:
                self.__update_status(session, parent_uuid, DatasetStatus.PROCESSING)  # "Processing")

        elif status == DatasetStatus.PROCESSED:  # "Processed":
            if current_dataset.status == DatasetStatus.RELEASED:  # "Released":  # and it is not top level.
                return updated_datasets
            # Get children
            children_uuid = self.__query_child_datasets(session, dataset_uuid)
            # Check to see if any are still processing or submitted
            for child, child_status in children_uuid:
                if child_status in (DatasetStatus.PROCESSING, DatasetStatus.SUBMITTED):  # ("Processing", "Submitted"):
                    return updated_datasets
            # Update current dataset if all the children are updated.
            current_dataset.status = DatasetStatus.PROCESSED  # "Processed"
            # Check if parent needs to be updated
            parent_uuid, parent_status = self.__query_parent_datasets(session, dataset_uuid)
            if parent_uuid is not None:
                self.__update_status(session, parent_uuid, DatasetStatus.PROCESSED)  # "Processed")

        elif status == DatasetStatus.RELEASED:  # "Released":
            # Get current datasets chain top level.
            top_level_uuid = self.__query_top_level_parent(session, dataset_uuid)
            # Check that all children and sub children etc
            top_level_children = self.query_all_child_datasets(top_level_uuid, session)
            genebuild_uuid, genebuild_status = self.__query_related_genome_by_type(session, dataset_uuid, "genebuild")
            top_level_children.extend(self.query_all_child_datasets(genebuild_uuid, session))
            assembly_uuid, assembly_status = self.__query_related_genome_by_type(session, dataset_uuid, "assembly")
            top_level_children.extend(self.query_all_child_datasets(assembly_uuid, session))

            # Update if all datasets in it's chain are processed, all genebuild and assembly are processed. Else return error.
            for child_uuid, child_status in top_level_children:
                # if child_status != "Released" and child_status != "Processed":
                if child_status not in (DatasetStatus.RELEASED, DatasetStatus.PROCESSED):  #
                    child_dataset = session.query(Dataset).filter(Dataset.dataset_uuid == child_uuid).one()
                    raise DatasetFactoryException(
                        f"Dataset {child_uuid} is not released or processed. It is {child_status}")
            top_level_children = self.query_all_child_datasets(top_level_uuid, session)
            for child_uuid, child_status in top_level_children:
                child_dataset = session.query(Dataset).filter(Dataset.dataset_uuid == child_uuid).one()
                child_dataset.status = DatasetStatus.RELEASED  # "Released"
            current_dataset.status = DatasetStatus.RELEASED  # "Released"
        else:
            raise DatasetFactoryException(f"Dataset status: {status} is not a valid status")
        updated_datasets = (current_dataset.dataset_uuid, current_dataset.status)
        logger.debug(f"Updated Datasets {updated_datasets}")
        return updated_datasets

    def __get_dataset(self, session, dataset_uuid):
        query = session.query(Dataset).filter(Dataset.dataset_uuid == dataset_uuid)
        return query.one()

    def __query_genomes_by_status_and_type(self, session, status, dataset_type):
        if session is None:
            raise ValueError("Session is not provided")
        # Filter by Dataset status and DatasetType name
        if isinstance(status, str):
            status = DatasetStatus(status)
        query = session.query(
            Genome.genome_uuid,
            Genome.production_name,
            Dataset.dataset_uuid
        ).join(
            GenomeDataset, Genome.genome_id == GenomeDataset.genome_id
        ).join(
            Dataset, GenomeDataset.dataset_id == Dataset.dataset_id
        ).join(
            DatasetType, Dataset.dataset_type_id == DatasetType.dataset_type_id
        ).filter(
            Dataset.status == status,
            DatasetType.name == dataset_type
        ).all()

        # Execute query and fetch results
        results = query
        return results

