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

from ensembl.database import DBConnection
from sqlalchemy.engine import make_url
from sqlalchemy.sql import func

from ensembl.production.metadata.api.exceptions import *
from ensembl.production.metadata.api.models import Dataset, Genome, GenomeDataset, \
    DatasetType, DatasetStatus
from ensembl.production.metadata.updater.updater_utils import update_attributes

logger = logging.getLogger(__name__)


class DatasetFactory:

    def __init__(self, conn_uri):
        super().__init__()
        if isinstance(conn_uri, str):
            conn_uri = make_url(conn_uri)
        self.db = DBConnection(conn_uri)

    # TODO: Multiple genomes for a single dataset are not incorporated
    def create_all_child_datasets(self, dataset_uuid, session=None):
        # Retrieve the top-level dataset
        # Will not work on datasets that are tied to multiple genomes!
        # !!!! WILL CREATE THE DATASETS EVEN IF THEY ALREADY EXIST
        if session is None:
            with self.db.session_scope() as db_session:
                top_level_dataset = self.__get_dataset(db_session, dataset_uuid)
                self.__create_child_datasets_recursive(db_session, top_level_dataset)
        else:
            top_level_dataset = self.__get_dataset(session, dataset_uuid)
            self.__create_child_datasets_recursive(session, top_level_dataset)
        # TODO add return code for errors?
        return True

    def create_dataset(self, session, genome_input, dataset_source, dataset_type, dataset_attributes, name, label,
                       version, status=DatasetStatus.SUBMITTED, parent=None):
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
                is_current=False,
            )
            session.add(new_genome_dataset)
            return dataset_uuid, new_dataset, new_dataset_attributes, new_genome_dataset
        else:
            return dataset_uuid, new_dataset, new_dataset_attributes, None

    def get_parent_datasets(self, dataset_uuid, **kwargs):
        session = kwargs.get('session')
        if session:
            return self.__query_parent_datasets(session, dataset_uuid)
        elif self.db:
            with self.db.session_scope() as session:
                return self.__query_parent_datasets(session, dataset_uuid)
        else:
            raise DatasetFactoryException("session or metadata_uri are required")

    def update_dataset_status(self, dataset_uuid, status, **kwargs):
        if isinstance(status, str):
            status = DatasetStatus(status)
        updated_datasets = [(dataset_uuid, status)]
        session = kwargs.get('session')
        attribute_dict = kwargs.get('attribute_dict')
        if session:
            updated_datasets = self.__update_status(session, dataset_uuid, status)
            if attribute_dict:
                self.update_dataset_attributes(dataset_uuid, attribute_dict, session=session)
        else:
            with self.db.session_scope() as session:
                updated_datasets = self.__update_status(session, dataset_uuid, status)
                if attribute_dict:
                    self.update_dataset_attributes(dataset_uuid, attribute_dict, session=session)
        return updated_datasets

    def update_dataset_attributes(self, dataset_uuid, attribute_dict, **kwargs):
        # TODO ADD DELETE opiton to kwargs to redo dataset_attributes.
        session = kwargs.get('session')
        if not isinstance(attribute_dict, dict):
            raise TypeError("attribute_dict must be a dictionary")
        if session:
            dataset = self.__get_dataset(session, dataset_uuid)
            dataset_attributes = update_attributes(dataset, attribute_dict, session)
            return dataset_attributes
        else:
            with self.db.session_scope() as session:
                dataset = self.__get_dataset(session, dataset_uuid)
                dataset_attributes = update_attributes(dataset, attribute_dict, session)
                return dataset_attributes

    def get_genomes_by_status_and_type(self, status, dataset_type, **kwargs):
        if isinstance(status, str):
            status = DatasetStatus(status)
        session = kwargs.get('session')
        if session:
            genome_data = self.__query_genomes_by_status_and_type(session, status, dataset_type)
            return genome_data
        else:
            with self.db.session_scope() as session:
                genome_data = self.__query_genomes_by_status_and_type(session, status, dataset_type)
                return genome_data

    def __create_child_datasets_recursive(self, session, parent_dataset):
        parent_dataset_type = session.query(DatasetType).filter(
            DatasetType.dataset_type_id == parent_dataset.dataset_type_id).one()

        # Find child dataset types for the parent dataset type
        child_dataset_types = session.query(DatasetType).filter(
            DatasetType.parent == parent_dataset_type.dataset_type_id).all()

        for child_type in child_dataset_types:
            # Check if a dataset with the same type and genome exists
            existing_datasets = session.query(Dataset).join(GenomeDataset).filter(
                Dataset.dataset_type_id == child_type.dataset_type_id,
                GenomeDataset.genome_id.in_([gd.genome_id for gd in parent_dataset.genome_datasets])
            ).all()
            if any(d.status in [DatasetStatus.SUBMITTED, DatasetStatus.PROCESSING] for d in existing_datasets):
                logger.debug("Skipped creation some DS are processed or Released")
                continue  # Skip creation if any dataset is already Processed or Released

            if len(parent_dataset.genome_datasets) > 1:
                raise ValueError("More than one genome linked to a genome_dataset")

                # Get the first genome's UUID
            genome_uuid = parent_dataset.genome_datasets[0].genome.genome_uuid
            dataset_source = parent_dataset.dataset_source
            dataset_type = child_type
            dataset_attributes = {}  # Populate with appropriate attributes
            name = dataset_type.name
            label = f"Child of {parent_dataset.name}"
            version = None
            # Create the child dataset
            child_dataset_uuid, new_dataset, new_dataset_attributes, new_genome_dataset = self.create_dataset(session,
                                                                                                              genome_uuid,
                                                                                                              dataset_source,
                                                                                                              dataset_type,
                                                                                                              dataset_attributes,
                                                                                                              name,
                                                                                                              label,
                                                                                                              version,
                                                                                                              parent=parent_dataset,
                                                                                                              status=parent_dataset.status)
            session.commit()
            # Recursively create children of this new child dataset
            child_dataset = self.__get_dataset(session, child_dataset_uuid)
            self.__create_child_datasets_recursive(session, child_dataset)

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

    def __query_all_child_datasets(self, session, parent_dataset_uuid):
        # This method returns the child datasets for a given dataset
        child_datasets = self.__query_child_datasets(session, parent_dataset_uuid)

        all_child_datasets = []
        for child_uuid, child_status in child_datasets:
            all_child_datasets.append((child_uuid, child_status))
            sub_children = self.__query_all_child_datasets(session, child_uuid)
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
        # Then convert all to released. #Add a blocker and warning in here.
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
            # TODO: Check that you are top level. Then check all children are ready to release.
            # Get current datasets chain top level.
            top_level_uuid = self.__query_top_level_parent(session, dataset_uuid)
            # Check that all children and sub children etc
            top_level_children = self.__query_all_child_datasets(session, top_level_uuid)
            genebuild_uuid, genebuild_status = self.__query_related_genome_by_type(session, dataset_uuid, "genebuild")
            top_level_children.extend(self.__query_all_child_datasets(session, genebuild_uuid))
            assembly_uuid, assembly_status = self.__query_related_genome_by_type(session, dataset_uuid, "assembly")
            top_level_children.extend(self.__query_all_child_datasets(session, assembly_uuid))

            # Update if all datasets in it's chain are processed, all genebuild and assembly are processed. Else return error.
            for child_uuid, child_status in top_level_children:
                # if child_status != "Released" and child_status != "Processed":
                if child_status not in (DatasetStatus.RELEASED, DatasetStatus.PROCESSED):  #
                    child_dataset = session.query(Dataset).filter(Dataset.dataset_uuid == child_uuid).one()
                    raise DatasetFactoryException(
                        f"Dataset {child_uuid} is not released or processed. It is {child_status}")
            top_level_children = self.__query_all_child_datasets(session, top_level_uuid)
            for child_uuid, child_status in top_level_children:
                child_dataset = session.query(Dataset).filter(Dataset.dataset_uuid == child_uuid).one()
                child_dataset.status = DatasetStatus.RELEASED  # "Released"
            current_dataset.status = DatasetStatus.RELEASED  # "Released"
        else:
            raise DatasetFactoryException(f"Dataset status: {status} is not a vallid status")
        updated_datasets = (current_dataset.dataset_uuid, current_dataset.status)
        return updated_datasets

    def __get_dataset(self, session, dataset_uuid):
        query = session.query(Dataset).filter(Dataset.dataset_uuid == dataset_uuid)
        return query.one()

    def __query_genomes_by_status_and_type(self, session, status, dataset_type):
        if session is None:
            raise ValueError("Session is not provided")
        # TODO: NO NEED for session here (execute then add result to session)
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
