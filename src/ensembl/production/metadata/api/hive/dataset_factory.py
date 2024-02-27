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

from ensembl.database import DBConnection

from ensembl.production.metadata.api.exceptions import *
from ensembl.production.metadata.api.models import Dataset, Genome, GenomeDataset, \
    DatasetType, DatasetStatus
from sqlalchemy.sql import func
import uuid
from ensembl.production.metadata.updater.updater_utils import update_attributes


class DatasetFactory:

    def create_all_child_datasets(self, session, dataset_uuid):
        # Retrieve the top-level dataset
        top_level_dataset = self._get_dataset(session, dataset_uuid)
        self.__create_child_datasets_recursive(session, top_level_dataset)

    def create_dataset(self, session, genome_uuid, dataset_source, dataset_type, dataset_attributes, name, label,
                       version):
        new_dataset = Dataset(
            dataset_uuid=str(uuid.uuid4()),
            dataset_type=dataset_type,  # Must be an object returned from the current session
            name=name,
            version=version,
            label=label,
            created=func.now(),
            dataset_source=dataset_source,  # Must
            status='Submitted',
        )
        genome = session.query(Genome).filter(Genome.genome_uuid == genome_uuid).one()
        new_genome_dataset = GenomeDataset(
            genome=genome,
            dataset=new_dataset,
            is_current=False,
        )
        new_dataset_attributes = update_attributes(new_dataset, dataset_attributes, session)
        session.add(new_genome_dataset)
        dataset_uuid = new_dataset.dataset_uuid
        return dataset_uuid, new_dataset_attributes, new_genome_dataset

    def get_parent_datasets(self, dataset_uuid, **kwargs):
        session = kwargs.get('session')
        metadata_uri = kwargs.get('metadata_uri')
        if session:
            return self.__query_parent_datasets(session, dataset_uuid)
        elif metadata_uri:
            metadata_db = DBConnection(metadata_uri)
            with metadata_db.session_scope() as session:
                return self.__query_parent_datasets(session, dataset_uuid)
        else:
            raise DatasetFactoryException("session or metadata_uri are required")

    def update_dataset_status(self, dataset_uuid, status, **kwargs):
        updated_datasets = [(dataset_uuid, status)]
        session = kwargs.get('session')
        metadata_uri = kwargs.get('metadata_uri')
        attribute_dict = kwargs.get('attribut_dict')
        if session:
            dataset = session.query(Dataset).filter(Dataset.dataset_uuid == dataset_uuid).one()
            updated_datasets = self._update_status(dataset, status)
            if attribute_dict:
                updated_datasets = self._update_status(dataset, status)
        elif metadata_uri:
            metadata_db = DBConnection(metadata_uri)
            with metadata_db.session_scope() as session:
                dataset = session.query(Dataset).filter(Dataset.dataset_uuid == dataset_uuid).one()
                if attribute_dict:
                    updated_datasets = self._update_status(dataset, status)
        else:
            raise DatasetFactoryException("session or metadata_uri are required")
        return updated_datasets

    def update_dataset_attributes(self, dataset_uuid, attribut_dict, **kwargs):
        session = kwargs.get('session')
        metadata_uri = kwargs.get('metadata_uri')
        if not isinstance(attribut_dict, dict):
            raise TypeError("attribut_dict must be a dictionary")
        if session:
            dataset = self._get_dataset(session, dataset_uuid)
            dataset_attributes = update_attributes(dataset, attribut_dict, session)
            return dataset_attributes
        else:
            metadata_db = DBConnection(metadata_uri)
            with metadata_db.session_scope() as session:
                dataset = self._get_dataset(session, dataset_uuid)
                dataset_attributes = update_attributes(dataset, attribut_dict, session)
                return dataset_attributes

    def get_genomes_by_status_and_type(self, status, type, **kwargs):
        session = kwargs.get('session')
        metadata_uri = kwargs.get('metadata_uri')
        if session:
            genome_data = self._query_genomes_by_status_and_type(session, status, type)
            return genome_data
        else:
            metadata_db = DBConnection(metadata_uri)
            with metadata_db.session_scope() as session:
                genome_data = self._query_genomes_by_status_and_type(session, status, type)
                return genome_data

    def __create_child_datasets_recursive(self, session, parent_dataset):
        parent_dataset_type = session.query(DatasetType).filter(
            DatasetType.dataset_type_id == parent_dataset.dataset_type_id).one()

        # Find child dataset types for the parent dataset type
        child_dataset_types = session.query(DatasetType).filter(
            DatasetType.parent == parent_dataset_type.id).all()

        for child_type in child_dataset_types:
            # Example placeholders for dataset properties
            genome_uuid = parent_dataset.genome_datasets.genome_id
            dataset_source = parent_dataset.source
            dataset_type = child_type
            dataset_attributes = {}  # Populate with appropriate attributes
            name = dataset_type.name
            label = f"Child of {parent_dataset.name}"
            version = None

            # Create the child dataset
            child_dataset_uuid = self.create_dataset(session, genome_uuid, dataset_source, dataset_type,
                                                     dataset_attributes, name, label, version)

            # Recursively create children of this new child dataset
            child_dataset = self._get_dataset(session, child_dataset_uuid)
            self.__create_child_datasets_recursive(session, child_dataset)

    def __query_parent_datasets(self, session, dataset_uuid):
        dataset = self._get_dataset(session, dataset_uuid)
        dataset_type = session.query(DatasetType).filter(
            DatasetType.dataset_type_id == dataset.dataset_type_id).one()
        if dataset_type.parent is None:
            return None
        parent_dataset_type = dataset_type.parent
        genome_id = next((gd.genome_id for gd in dataset.genome_datasets), None)
        if not genome_id:
            raise ValueError("No associated Genome found for the given dataset UUID")

        parent_genome_dataset = session.query(GenomeDataset).join(Dataset).join(DatasetType).filter(
            GenomeDataset.genome_id == genome_id,
            DatasetType.id == parent_dataset_type).one()
        parent_uuid = parent_genome_dataset.dataset.dataset_uuid
        parent_status = parent_genome_dataset.dataset.status
        return parent_uuid, parent_status

    def __query_top_level_parent(self, session, dataset_uuid):
        current_uuid = dataset_uuid
        while True:
            parent_data = self.__query_parent_datasets(session, current_uuid)
            if parent_data is None:
                return current_uuid
            current_uuid = parent_data[0]

    def __query_related_genome_by_type(self, session, dataset_uuid, dataset_type):
        dataset = self._get_dataset(session, dataset_uuid)
        genome_id = next((gd.genome_id for gd in dataset.genome_datasets), None)
        if not genome_id:
            raise ValueError("No associated Genome found for the given dataset UUID")

        # Determine if dataset_type is an ID or a name
        if isinstance(dataset_type, int) or (isinstance(dataset_type, str) and dataset_type.isdigit()):
            # dataset_type is treated as an ID
            filter_condition = (GenomeDataset.genome_id == genome_id, Dataset.dataset_type_id == dataset_type)
        else:
            # dataset_type is treated as a name
            filter_condition = (GenomeDataset.genome_id == genome_id, DatasetType.name == dataset_type)

        related_genome_dataset = session.query(GenomeDataset).join(Dataset).join(DatasetType).filter(
            *filter_condition).one()
        related_uuid = related_genome_dataset.dataset.dataset_uuid
        related_status = related_genome_dataset.dataset.status
        return related_uuid, related_status

    def _query_child_datasets(self, session, dataset_uuid):
        parent_dataset = self._get_dataset(session, dataset_uuid)
        parent_dataset_type = session.query(DatasetType).filter(
            DatasetType.dataset_type_id == parent_dataset.dataset_type_id).one()
        child_dataset_types = session.query(DatasetType).filter(
            DatasetType.parent == parent_dataset_type.name).all()
        if not child_dataset_types:
            return []  # Return an empty list if no child types are found
        # This will break if we have multiple genome datasets for a single dataset, which is not currently the case.
        genome_id = parent_dataset.genome_datasets.genome_id
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

    def _query_all_child_datasets(self, session, parent_dataset_uuid):
        # This method returns the child datasets for a given dataset
        child_datasets = self._query_child_datasets(session, parent_dataset_uuid)

        all_child_datasets = []
        for child_uuid, child_status in child_datasets:
            all_child_datasets.append((child_uuid, child_status))
            sub_children = self._query_all_child_datasets(session, child_uuid)
            all_child_datasets.extend(sub_children)

        return all_child_datasets

    def _query_depends_on(self, session, dataset_uuid):
        dataset = session.query(Dataset).filter(Dataset.dataset_uuid == dataset_uuid).one_or_none()
        dataset_type = dataset.dataset_type
        dependent_types = dataset_type.depends_on.split(',') if dataset_type.depends_on else []
        dependent_datasets_info = []
        for dtype in dependent_types:
            new_uuid, new_status = self.__query_related_genome_by_type(session, dataset_uuid, dtype)
            dependent_datasets_info.append((new_uuid, new_status))
        return dependent_datasets_info

    def _update_status(self, session, dataset_uuid, status):
        updated_datasets = []
        # Processed to Released. Only accept top level. Check that all assembly and genebuild datsets (all the way down) are processed.
        # Then convert all to released. #Add a blocker and warning in here.
        current_dataset = session.query(Dataset).filter(Dataset.dataset_uuid == dataset_uuid).one()
        if status == DatasetStatus.SUBMITTED:
            # Update to SUBMITTED and all parents.
            # Do not touch the children.
            # This should only be called in times of strife and error.
            current_dataset.status = DatasetStatus.SUBMITTED
            parent_uuid, parent_status = self.__query_parent_datasets(session, dataset_uuid)
            if parent_uuid is not None:
                self._update_status(session, parent_uuid, DatasetStatus.SUBMITTED)

        elif status == DatasetStatus.PROCESSING:
            # Update to PROCESSING and all parents.
            # Do not touch the children.

            # Check the dependents
            dependents = self._query_depends_on(session, dataset_uuid)
            for uuid, dep_status in dependents:
                if dep_status != DatasetStatus.PROCESSED or dep_status != DatasetStatus.RELEASED:
                    return dataset_uuid, status
            current_dataset.status = DatasetStatus.PROCESSING
            parent_uuid, parent_status = self.__query_parent_datasets(session, dataset_uuid)
            if parent_uuid is not None:
                self._update_status(session, parent_uuid, DatasetStatus.PROCESSING)

        elif status == DatasetStatus.PROCESSED:
            # Get children
            children_uuid = self._query_child_datasets(session, dataset_uuid)
            new_status = DatasetStatus.PROCESSED
            # Check to see if any are still processing or submitted
            for child, child_status in children_uuid:
                # Not positive on the buisness rule here. Should we limit processed to the parents that have all children finished?
                # if child_status == DatasetStatus.PROCESSING or child_status == DatasetStatus.SUBMITTED:
                if child_status == DatasetStatus.PROCESSING:
                    new_status = DatasetStatus.PROCESSING
            # Update current dataset if all the children are updated.
            if new_status == DatasetStatus.PROCESSED:
                current_dataset.status = DatasetStatus.PROCESSED
                # Check if parent needs to be updated
                parent_uuid = self.__query_parent_datasets(session, dataset_uuid)
                if parent_uuid is not None:
                    self._update_status(session, parent_uuid, DatasetStatus.PROCESSED)

        elif status == DatasetStatus.RELEASED:
            # Get current datasets chain top level.
            top_level_uuid = self.__query_top_level_parent(dataset_uuid)
            # Check that all children and sub children etc
            top_level_children = self._query_all_child_datasets(top_level_uuid)
            genebuild_uuid = self.__query_related_genome_by_type(session, dataset_uuid, "genebuild")
            top_level_children.extend(self._query_all_child_datasets(genebuild_uuid))
            assembly_uuid = self.__query_related_genome_by_type(session, dataset_uuid, "assembly")
            top_level_children.extend(self._query_all_child_datasets(assembly_uuid))

            # Update if all datasets in it's chain are processed, all genebuild and assembly are processed. Else return error.
            for child_uuid, child_status in top_level_children:
                if child_status is not DatasetStatus.RELEASED or child_status is not DatasetStatus.PROCESSED:
                    raise DatasetFactoryException(
                        f"Dataset {child_uuid} is not released or processed. It is {child_status}")
            top_level_children = self._query_all_child_datasets(top_level_uuid)
            for child_uuid, child_status in top_level_children:
                child_dataset = session.query(Dataset).filter(Dataset.dataset_uuid == child_uuid).one()
                child_dataset.status = DatasetStatus.RELEASED
        else:
            raise DatasetFactoryException(f"Dataset status: {status} is not a vallid status")

        updated_datasets.append((current_dataset.dataset_uuid, current_dataset.status))
        return updated_datasets

    def _get_dataset(self, session, dataset_uuid):
        return session.query(Dataset).filter(Dataset.dataset_uuid == dataset_uuid).one()

    def _query_genomes_by_status_and_type(self, session, status, type):
        if session is None:
            raise ValueError("Session is not provided")

        # Filter by Dataset status and DatasetType name
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
            DatasetType.name == type
        )

        # Execute query and fetch results
        results = query.all()
        return results
