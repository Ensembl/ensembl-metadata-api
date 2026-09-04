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
from sqlalchemy.orm import Session, selectinload
from sqlalchemy.sql import func

from ensembl.production.metadata.api.exceptions import *
from ensembl.production.metadata.api.models import (
    Dataset,
    Genome,
    GenomeDataset,
    DatasetType,
    DatasetStatus,
    EnsemblRelease,
    DatasetSource,
    GenomeRelease,
)
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

    def create_all_child_datasets(
        self,
        dataset_uuid: str,
        session: sqlalchemy.orm.Session = None,
        topic: str = None,
        status: DatasetStatus = None,
        release: EnsemblRelease = None,
    ):
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
        session.add(new_dataset)
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

    def is_current_datasets_resolve(self, release_id, session=None, logger=None, genome_ids=None):
        """
        Ensure GenomeDataset.is_current is consistent for a release.

        Rules:
        - For dataset types with multiple_current=0, keep exactly one current GenomeDataset
          per (genome_id, dataset_type_id), preferring the specified release_id.
        - For dataset types with multiple_current=1, allow one current GenomeDataset per
          (genome_id, dataset_type_id, dataset.name), again preferring the specified release_id.
        - Different dataset names may therefore each remain current for the same genome/type
          when multiple_current=1.

        :param session: SQLAlchemy session object
        :param release_id: The release_id to prioritize
        :param logger: Optional logging.Logger instance
        :param genome_ids: Optional iterable of genome_id values to limit the reconciliation scope
        :return: List of altered GenomeDataset objects
        """
        if not session:
            with self.__get_db_connexion().session_scope() as db_session:
                return self.is_current_datasets_resolve(
                    release_id=release_id,
                    session=db_session,
                    logger=logger,
                    genome_ids=genome_ids,
                )

        log = logger.info if logger else print
        genome_ids = set(genome_ids or [])
        query = (
            session.query(GenomeDataset)
            .join(Dataset, GenomeDataset.dataset_id == Dataset.dataset_id)
            .join(DatasetType, Dataset.dataset_type_id == DatasetType.dataset_type_id)
            .outerjoin(EnsemblRelease, GenomeDataset.release_id == EnsemblRelease.release_id)
            .filter(
                (GenomeDataset.release_id == release_id)
                | (EnsemblRelease.release_type == "partial")
                | GenomeDataset.release_id.is_(None)
            )
        )
        if genome_ids:
            query = query.filter(GenomeDataset.genome_id.in_(genome_ids))
        genome_datasets = query.all()
        if not genome_datasets:
            log("No GenomeDataset rows found. Nothing to fix.")
            return []

        grouped = defaultdict(list)
        for genome_dataset in genome_datasets:
            dataset = genome_dataset.dataset
            dataset_type = dataset.dataset_type
            if dataset_type.multiple_current:
                group_key = (genome_dataset.genome_id, dataset.dataset_type_id, dataset.name)
            else:
                group_key = (genome_dataset.genome_id, dataset.dataset_type_id)
            grouped[group_key].append(genome_dataset)

        altered_datasets = []

        def sort_key(genome_dataset):
            dataset = genome_dataset.dataset
            return (
                1 if genome_dataset.release_id == release_id else 0,
                1 if genome_dataset.is_current else 0,
                1 if dataset.status == DatasetStatus.RELEASED else 0,
                1 if dataset.status == DatasetStatus.PROCESSED else 0,
                genome_dataset.release_id or -1,
                genome_dataset.genome_dataset_id,
            )

        for group_key, group_rows in grouped.items():
            winner = max(group_rows, key=sort_key)
            for genome_dataset in group_rows:
                desired_is_current = 1 if genome_dataset.genome_dataset_id == winner.genome_dataset_id else 0
                if genome_dataset.is_current != desired_is_current:
                    genome_dataset.is_current = desired_is_current
                    altered_datasets.append(genome_dataset)

        if altered_datasets:
            log(
                f"Resolved is_current on {len(altered_datasets)} GenomeDataset rows for release_id={release_id}"
            )

        return altered_datasets

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

                session.query(GenomeDataset).filter(
                    GenomeDataset.dataset_id.in_(
                        session.query(Dataset.dataset_id).filter(Dataset.dataset_uuid.in_(child_uuids))
                    )
                ).update({"release_id": None}, synchronize_session=False)
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

                    if dataset_obj.status in (DatasetStatus.FAULTY, DatasetStatus.RELEASED):
                        continue

                    genome_dataset = session.query(GenomeDataset).outerjoin(
                        EnsemblRelease, GenomeDataset.release_id == EnsemblRelease.release_id
                    ).filter(
                        GenomeDataset.dataset_id == dataset_obj.dataset_id,
                        GenomeDataset.genome_id == genome_id,
                        (EnsemblRelease.release_type != "integrated") | (GenomeDataset.release_id.is_(None))
                    ).one_or_none()

                    if genome_dataset:
                        genome_dataset.release_id = release_id
                    else:
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
        3. Retrieve all child datasets from the top-level parent; downgrade any
           RELEASED children to FAULTY, and remove their release association.
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

        datasets = (
            session.query(Dataset)
            .options(
                selectinload(Dataset.dataset_type),
                selectinload(Dataset.genome_datasets),
            )
            .all()
        )
        dataset_by_id = {dataset.dataset_id: dataset for dataset in datasets}
        children_by_parent_id = defaultdict(list)
        datasets_by_genome_and_type = defaultdict(list)
        has_non_faulty_assembly_by_genome = defaultdict(bool)
        for dataset in datasets:
            if dataset.parent_id is not None:
                children_by_parent_id[dataset.parent_id].append(dataset)
            for genome_dataset in dataset.genome_datasets:
                datasets_by_genome_and_type[(genome_dataset.genome_id, dataset.dataset_type_id)].append(
                    dataset
                )
            if dataset.dataset_type.name == "assembly" and dataset.status != DatasetStatus.FAULTY:
                for genome_dataset in dataset.genome_datasets:
                    has_non_faulty_assembly_by_genome[genome_dataset.genome_id] = True

        child_type_ids_by_parent_type_id = defaultdict(list)
        for dataset_type in session.query(DatasetType).all():
            if dataset_type.parent is not None:
                child_type_ids_by_parent_type_id[dataset_type.parent].append(dataset_type.dataset_type_id)

        faulty_datasets = [dataset for dataset in datasets if dataset.status == DatasetStatus.FAULTY]
        if not faulty_datasets:
            logger.info("No faulty datasets found.")
            return

        logger.info(f"Processing {len(faulty_datasets)} faulty datasets.")

        updated_datasets = set()
        genomes_to_remove_release = set()
        released_genomes_marked_faulty = set()
        impacted_top_level_ids = set()
        parent_ids_cache = {}
        child_ids_cache = {}

        def get_parent_ids(dataset):
            cached_parent_ids = parent_ids_cache.get(dataset.dataset_id)
            if cached_parent_ids is not None:
                return cached_parent_ids

            parent_ids = []
            seen_ids = set()

            if dataset.parent_id is not None and dataset.parent_id in dataset_by_id:
                parent_ids.append(dataset.parent_id)
                seen_ids.add(dataset.parent_id)
            elif dataset.dataset_type.parent is not None:
                for genome_dataset in dataset.genome_datasets:
                    for parent_dataset in datasets_by_genome_and_type.get(
                        (genome_dataset.genome_id, dataset.dataset_type.parent),
                        [],
                    ):
                        if parent_dataset.dataset_id == dataset.dataset_id:
                            continue
                        if parent_dataset.dataset_id in seen_ids:
                            continue
                        seen_ids.add(parent_dataset.dataset_id)
                        parent_ids.append(parent_dataset.dataset_id)

            parent_ids_cache[dataset.dataset_id] = parent_ids
            return parent_ids

        def get_child_ids(dataset):
            cached_child_ids = child_ids_cache.get(dataset.dataset_id)
            if cached_child_ids is not None:
                return cached_child_ids

            child_ids = []
            seen_ids = set()

            for child_dataset in children_by_parent_id.get(dataset.dataset_id, []):
                if child_dataset.dataset_id not in seen_ids:
                    seen_ids.add(child_dataset.dataset_id)
                    child_ids.append(child_dataset.dataset_id)

            for child_type_id in child_type_ids_by_parent_type_id.get(dataset.dataset_type_id, []):
                for genome_dataset in dataset.genome_datasets:
                    for child_dataset in datasets_by_genome_and_type.get(
                        (genome_dataset.genome_id, child_type_id), []
                    ):
                        if child_dataset.dataset_id == dataset.dataset_id:
                            continue
                        if child_dataset.dataset_id in seen_ids:
                            continue
                        seen_ids.add(child_dataset.dataset_id)
                        child_ids.append(child_dataset.dataset_id)

            child_ids_cache[dataset.dataset_id] = child_ids
            return child_ids

        for dataset in faulty_datasets:
            stack = [(dataset.dataset_id, {dataset.dataset_id})]

            while stack:
                current_dataset_id, path_ids = stack.pop()
                current_dataset = dataset_by_id[current_dataset_id]
                if current_dataset.status != DatasetStatus.FAULTY:
                    current_dataset.status = DatasetStatus.FAULTY
                    updated_datasets.add(current_dataset.dataset_uuid)

                parent_ids = get_parent_ids(current_dataset)
                if not parent_ids:
                    impacted_top_level_ids.add(current_dataset.dataset_id)
                    continue

                for parent_id in parent_ids:
                    if parent_id in path_ids:
                        raise ValueError(
                            f"Cycle detected while traversing parents for faulty dataset {dataset.dataset_uuid}"
                        )
                    stack.append((parent_id, path_ids | {parent_id}))

        processed_dataset_ids = set()
        for top_level_id in impacted_top_level_ids:
            stack = [(top_level_id, {top_level_id})]

            while stack:
                current_dataset_id, path_ids = stack.pop()
                if current_dataset_id in processed_dataset_ids:
                    continue
                processed_dataset_ids.add(current_dataset_id)
                chain_dataset = dataset_by_id[current_dataset_id]
                if chain_dataset.status == DatasetStatus.RELEASED:
                    logger.info(
                        "Downgrading dataset %s from RELEASED to FAULTY (parent chain is FAULTY)",
                        chain_dataset.dataset_uuid,
                    )
                    chain_dataset.status = DatasetStatus.FAULTY
                    updated_datasets.add(chain_dataset.dataset_uuid)

                for genome_dataset in chain_dataset.genome_datasets:
                    if genome_dataset.release_id:
                        logger.info("Removing release from dataset %s", chain_dataset.dataset_uuid)
                        genome_dataset.release_id = None
                        updated_datasets.add(chain_dataset.dataset_uuid)

                    if chain_dataset.dataset_type.name == "genebuild":
                        genomes_to_remove_release.add(genome_dataset.genome_id)
                    elif chain_dataset.dataset_type.name == "assembly":
                        if not has_non_faulty_assembly_by_genome[genome_dataset.genome_id]:
                            genomes_to_remove_release.add(genome_dataset.genome_id)

                for child_id in get_child_ids(chain_dataset):
                    if child_id in path_ids:
                        raise ValueError(
                            f"Cycle detected while traversing children for top-level dataset {chain_dataset.dataset_uuid}"
                        )
                    stack.append((child_id, path_ids | {child_id}))

        # Remove genome releases if necessary
        if genomes_to_remove_release:
            logger.info(f"Removing genome releases for {len(genomes_to_remove_release)} genomes.")

            released_genomes_marked_faulty.update(
                genome_uuid
                for (genome_uuid,) in session.query(Genome.genome_uuid)
                .join(GenomeRelease, GenomeRelease.genome_id == Genome.genome_id)
                .filter(Genome.genome_id.in_(genomes_to_remove_release))
                .distinct()
                .all()
            )

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
        if released_genomes_marked_faulty:
            released_genome_list = ", ".join(sorted(released_genomes_marked_faulty))
            raise DatasetFactoryException(
                f"Released genomes were marked faulty and removed from release: {released_genome_list}"
            )

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
            DatasetType.dataset_type_id == parent_dataset_type).first() # quick fix , if a genome is in multiple releases, this returns the first one, will not have impact as both genome dataset linked to one dataset 
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
        genome_id = next((gd.genome_id for gd in parent_dataset.genome_datasets), None)
        if not genome_id:
            logger.warning(
                "Skipping child dataset lookup for dataset %s because it has no associated GenomeDataset rows",
                dataset_uuid,
            )
            return []

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
        all_child_datasets = []
        visited_dataset_uuids = {parent_dataset_uuid}
        stack = [parent_dataset_uuid]

        while stack:
            current_parent_uuid = stack.pop()
            child_datasets = self.__query_child_datasets(session, current_parent_uuid)

            for child_uuid, child_status in child_datasets:
                if child_uuid in visited_dataset_uuids:
                    logger.warning(
                        "Skipping already visited child dataset %s while traversing descendants of %s",
                        child_uuid,
                        parent_dataset_uuid,
                    )
                    continue

                visited_dataset_uuids.add(child_uuid)
                all_child_datasets.append((child_uuid, child_status))
                stack.append(child_uuid)

        return all_child_datasets

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
