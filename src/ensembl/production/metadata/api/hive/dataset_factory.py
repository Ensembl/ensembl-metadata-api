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
from ensembl.production.metadata.api.models import Dataset, Attribute, DatasetAttribute, Genome, GenomeDataset, \
    DatasetType, DatasetSource, DatasetStatus
from sqlalchemy.sql import func
import uuid
from ensembl.production.metadata.updater.updater_utils import update_attributes


class DatasetFactory:
    """
    A class for interacting with the ensembl_genome_metadata database, specifically for modifying the dataset and
    dataset attribute tables.

    Attributes
    ----------
    session : SQLAlchemy session
        An active SQLAlchemy session for database operations. If not provided, a new session is created using metadata_uri.
    metadata_db : DBConnection
        A database connection object used when a new session is created.
    owns_session : bool
        Flag to indicate whether this class instance owns the session (True) or if it was provided by the user (False).

    Methods
    -------
    create_child_datasets(dataset_uuid, parent_type, child_type, dataset_attributes, genome_uuid):
        Creates child datasets based on various parameters like dataset_uuid, parent_type, child_type, etc.
    get_parent_datasets(dataset_uuid):
        Retrieves parent datasets for a given dataset UUID.
    create_dataset(session, genome_uuid, dataset_source, dataset_type, dataset_attributes, name, label, version):
        Creates a new dataset and associates it with a genome.
    update_dataset_status(dataset_uuid, status):
        Updates the status of a dataset identified by its UUID.
    update_dataset_attributes(dataset_uuid, attribut_dict):
        Updates the attributes of a dataset identified by its UUID.
    get_dataset(session, dataset_uuid):
        Retrieves a dataset by its UUID.
    """



    def create_all_child_datasets(self, session, dataset_uuid):
        # Retrieve the top-level dataset
        top_level_dataset = self.get_dataset(session, dataset_uuid)
        self._create_child_datasets_recursive(session, top_level_dataset)

    def _create_child_datasets_recursive(self, session, parent_dataset):
        parent_dataset_type = session.query(DatasetType).filter(
            DatasetType.dataset_type_id == parent_dataset.dataset_type_id).one()

        # Find child dataset types for the parent dataset type
        child_dataset_types = session.query(DatasetType).filter(
            DatasetType.parent == parent_dataset_type.name).all()

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
            child_dataset = self.get_dataset(session, child_dataset_uuid)
            self._create_child_datasets_recursive(session, child_dataset)



    def create_dataset(self, session, genome_uuid, dataset_source, dataset_type, dataset_attributes, name, label,
                       version):
        """
        Creates a new dataset record and associates it with a specific genome. The new dataset is added to the database session.

        Parameters:
        session (SQLAlchemy session): An active database session.
        genome_uuid (str): UUID of the genome to associate the dataset with.
        dataset_source (DatasetSource): The source of the dataset.
        dataset_type (DatasetType): The type of the dataset.
        dataset_attributes (dict): Attributes to assign to the dataset.
        name (str): Name of the dataset.
        label (str): Label for the dataset.
        version (str): Version of the dataset.

        Returns:
        tuple: Dataset UUID, dataset attributes, and the new genome-dataset association.
        """
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

    def _query_parent_datasets(self, session, dataset_uuid):
        dataset = self.get_dataset(session, dataset_uuid)
        dataset_type = session.query(DatasetType).filter(
            DatasetType.dataset_type_id == dataset.dataset_type_id).one()
        if dataset_type.parent is None:
            return None
        parent_dataset_types = dataset_type.parent.split(';')
        genome_id = next((gd.genome_id for gd in dataset.genome_datasets), None)
        if not genome_id:
            raise ValueError("No associated Genome found for the given dataset UUID")

        parent_genome_dataset = session.query(GenomeDataset).join(Dataset).join(DatasetType).filter(
            GenomeDataset.genome_id == genome_id,
            DatasetType.name.in_(parent_dataset_types)
        ).one()
        parent_uuid = parent_genome_dataset.dataset.dataset_uuid
        parent_status = parent_genome_dataset.dataset.status
        return parent_uuid, parent_status




    def get_parent_datasets(self, dataset_uuid, session=None, metadata_uri=None):
        """
        Retrieves the parent datasets of a specified dataset. If the dataset does not have a parent,
        it returns the dataset itself and marks it as 'top_level'.

        Parameters:
        dataset_uuid (str): UUID of the dataset for which the parent datasets are to be found.

        Returns:
        tuple: Two lists containing UUIDs and types of the parent datasets.
        """
        parent_uuid = ''
        if session:
            dataset = session.query(Dataset).filter(Dataset.dataset_uuid == dataset_uuid).one()
            return self.query_parent_datasets(self.session, dataset_uuid)
        elif metadata_uri:
            metadata_db = DBConnection(metadata_uri)
            with metadata_db.session_scope() as session:
                return self.query_parent_datasets(self.session, dataset_uuid)
        else:
            raise DatasetFactoryException("session or metadata_uri are required")

    def _query_top_level_parent(self, session, dataset_uuid):
        current_uuid = dataset_uuid
        while True:
            parent_data = self._query_parent_datasets(session, current_uuid)
            if parent_data is None:
                return current_uuid
            current_uuid = parent_data[0]


    def _query_related_genome_by_type(self, session, dataset_uuid, dataset_type):
        dataset = self.get_dataset(session, dataset_uuid)
        genome_id = next((gd.genome_id for gd in dataset.genome_datasets), None)
        if not genome_id:
            raise ValueError("No associated Genome found for the given dataset UUID")
        related_genome_dataset = session.query(GenomeDataset).join(Dataset).join(DatasetType).filter(
            GenomeDataset.genome_id == genome_id,
            DatasetType.name == dataset_type
        ).one()
        related_uuid = related_genome_dataset.dataset.dataset_uuid
        related_status = related_genome_dataset.dataset.status
        return related_uuid, related_status

    def _query_child_datasets(self, session, dataset_uuid):
        parent_dataset = self.get_dataset(session, dataset_uuid)
        parent_dataset_type = session.query(DatasetType).filter(
            DatasetType.dataset_type_id == parent_dataset.dataset_type_id).one()
        child_dataset_types = session.query(DatasetType).filter(
            DatasetType.parent == parent_dataset_type.name).all()
        if not child_dataset_types:
            return []  # Return an empty list if no child types are found
        #This will break if we have multiple genome datasets for a single dataset, which is not currently the case.
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

    def _query_depends_on(self,session, dataset_uuid):
        dataset = session.query(Dataset).filter(Dataset.dataset_uuid == dataset_uuid).one_or_none()
        dataset_type = dataset.dataset_type
        dependent_types = dataset_type.depends_on.split(',') if dataset_type.depends_on else []
        dependent_datasets_info = []
        for dtype in dependent_types:
            new_uuid, new_status = self._query_related_genome_by_type(session,dataset_uuid,dtype)
            dependent_datasets_info.append((new_uuid, new_status))
        return dependent_datasets_info


    def _update_status(self, session, dataset_uuid, status):
        #TODO: Return UUID, status
        #Processed to Released. Only accept top level. Check that all assembly and genebuild datsets (all the way down) are processed.
        # Then convert all to released. #Add a blocker and warning in here.
        current_dataset = session.query(Dataset).filter(Dataset.dataset_uuid == dataset_uuid).one()
        if status == DatasetStatus.SUBMITTED:
            #Update to SUBMITTED and all parents.
            #Do not touch the children.
            #This should only be called in times of strife and error.
            current_dataset.status = DatasetStatus.SUBMITTED
            parent_uuid, parent_status = self._query_parent_datasets(session,dataset_uuid)
            if parent_uuid is not None:
                self._update_status(session, parent_uuid, DatasetStatus.SUBMITTED)

        elif status == DatasetStatus.PROCESSING:
            #Update to PROCESSING and all parents.
            #Do not touch the children.

            #TODO:Add check the depending


            current_dataset.status = DatasetStatus.PROCESSING
            parent_uuid, parent_status = self._query_parent_datasets(session,dataset_uuid)
            if parent_uuid is not None:
                self._update_status(session, parent_uuid, DatasetStatus.PROCESSING)

        elif status == DatasetStatus.PROCESSED:
            #Get children
            children_uuid = self._query_child_datasets(session, dataset_uuid)
            new_status = DatasetStatus.PROCESSED
            #Check to see if any are still processing or submitted
            for child, child_status in children_uuid:
                if child_status == DatasetStatus.PROCESSING or child_status == DatasetStatus.SUBMITTED:
                    new_status = DatasetStatus.PROCESSING
            #Update current dataset if all the children are updated.
            if new_status == DatasetStatus.PROCESSED:
                current_dataset.status = DatasetStatus.PROCESSED
                #Check if parent needs to be updated
                parent_uuid = self._query_parent_datasets(session,dataset_uuid)
                if parent_uuid is not None:
                    self._update_status(session,parent_uuid,DatasetStatus.PROCESSED)

        elif status == DatasetStatus.RELEASED:
            #Get current datasets chain top level.
            top_level_uuid = self._query_top_level_parent(dataset_uuid)
            #Check that all children and sub children etc
            top_level_children = self._query_all_child_datasets(top_level_uuid)
            genebuild_uuid = self._query_related_genome_by_type(session, dataset_uuid, "genebuild")
            top_level_children.extend(self._query_all_child_datasets(genebuild_uuid))
            assembly_uuid = self._query_related_genome_by_type(session, dataset_uuid, "assembly")
            top_level_children.extend(self._query_all_child_datasets(assembly_uuid))

            # Update if all datasets in it's chain are processed, all genebuild and assembly are processed. Else return error.
            for child_uuid, child_status in top_level_children:
                if child_status is not DatasetStatus.RELEASED or child_status is not DatasetStatus.PROCESSED:
                    raise DatasetFactoryException(f"Dataset {child_uuid} is not released or processed. It is {child_status}")
            top_level_children = self._query_all_child_datasets(top_level_uuid)
            for child_uuid, child_status in top_level_children:
                child_dataset = session.query(Dataset).filter(Dataset.dataset_uuid == child_uuid).one()
                child_dataset.status = DatasetStatus.RELEASED
        else:
            raise DatasetFactoryException(f"Dataset status: {status} is not a vallid status")



    def update_dataset_status(self, dataset_uuid, status, session=None, metadata_uri=None):
        # TODO: Check parent for progress and update parent if child
        """
        Updates the status of a dataset identified by its UUID. The status is updated to the next logical state unless
        a specific state is provided.

        Parameters:
        dataset_uuid (str): UUID of the dataset to update.
        status (str, optional): The new status to set for the dataset. If not provided, status is advanced to the next logical state.

        Returns:
        tuple: Dataset UUID and the updated status.
        """
        if session:
            dataset = session.query(Dataset).filter(Dataset.dataset_uuid == dataset_uuid).one()
            updated_status, parent_status = self._update_status(dataset, status)
        elif metadata_uri:
            metadata_db = DBConnection(metadata_uri)
            with metadata_db.session_scope() as session:
                dataset = session.query(Dataset).filter(Dataset.dataset_uuid == dataset_uuid).one()
                updated_status, parent_status = self._update_status(dataset, status)
        else:
            raise DatasetFactoryException("session or metadata_uri are required")
        return dataset_uuid, updated_status, parent_status

    def update_dataset_attributes(self, dataset_uuid, attribut_dict):
        """
        Updates the attributes of a dataset identified by its UUID. The attributes to be updated are provided as a dictionary.

        Parameters:
        dataset_uuid (str): UUID of the dataset to update.
        attribute_dict (dict): A dictionary containing attribute names and their new values.

        Returns:
        list: Updated dataset attributes.
        """
        if not isinstance(attribut_dict, dict):
            raise TypeError("attribut_dict must be a dictionary")
        if self.owns_session:
            with self.metadata_db.session_scope() as session:
                dataset = self.get_dataset(session, dataset_uuid)
                dataset_attributes = update_attributes(dataset, attribut_dict, session)
                return dataset_attributes
        else:
            dataset = self.get_dataset(self.session, dataset_uuid)
            dataset_attributes = update_attributes(dataset, attribut_dict, self.session)
            return dataset_attributes

    def get_dataset(self, session, dataset_uuid):
        """
        Retrieves a dataset by its UUID using an active database session.

        Parameters:
        session (SQLAlchemy session): An active database session.
        dataset_uuid (str): UUID of the dataset to retrieve.

        Returns:
        Dataset: The dataset object corresponding to the provided UUID.
        """
        return session.query(Dataset).filter(Dataset.dataset_uuid == dataset_uuid).one()
