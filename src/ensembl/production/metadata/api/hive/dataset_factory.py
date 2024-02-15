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
    DatasetType, DatasetSource
from sqlalchemy.sql import func
import uuid
from ensembl.production.metadata.updater.updater_utils import update_attributes

class DatasetFactory():
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
    def __init__(self, session=None, metadata_uri=None):
        """
        Initializes the DatasetFactory instance.

        Parameters:
        session (SQLAlchemy session, optional): An active database session.
        metadata_uri (str, optional): URI for the metadata database.

        Raises:
        DatasetFactoryException: If neither session nor metadata_uri is provided.
        """
        if session:
            self.session = session
            self.owns_session = False
        else:
            if metadata_uri is None:
                raise DatasetFactoryException("session or metadata_uri are required")
            self.owns_session = True
            self.metadata_db = DBConnection(metadata_uri)

    #     #TODO: Determine how to implement genome_uuid when we can have multiples of each dataset type per genome
    def create_child_datasets(self, dataset_uuid=None, parent_type=None, child_type=None, dataset_attributes={},
                              genome_uuid=None):
        """
        Creates child datasets based on the provided parameters. Child datasets are created based on the type of parent
        dataset, child dataset type, or associated genome UUID. The method enforces rules to prevent conflict in parameters.

        Parameters:
        dataset_uuid (str, optional): UUID of the parent dataset.
        parent_type (str, optional): Type of the parent dataset.
        child_type (str, optional): Type of the child dataset to be created.
        dataset_attributes (dict, optional): Attributes to be assigned to the child dataset.
        genome_uuid (str, optional): UUID of the genome associated with the datasets.

        Returns:
        list: UUIDs of the created child datasets.
        """
        if dataset_uuid and genome_uuid:
            raise ValueError("Please only provide genome_uuid or dataset_uuid")
        if parent_type and child_type:
            raise ValueError("Please only provide child_type or parent_type")
        def fetch_parent_datasets(session):
            parent_dataset_types = set()
            potential_parent_datasets = []
            if dataset_uuid:
                parent_dataset = session.query(Dataset).filter(Dataset.dataset_uuid == dataset_uuid).first()
                if parent_dataset:
                    parent_dataset_types.add(parent_dataset.dataset_type.name)
                    potential_parent_datasets.append(parent_dataset)
            elif parent_type:
                parent_dataset_types.add(parent_type)
                potential_parent_datasets = session.query(Dataset).filter(
                    Dataset.dataset_type.has(name=parent_type),
                    Dataset.status != 'Released'
                ).all()
            elif genome_uuid:
                genome = session.query(Genome).filter(Genome.genome_uuid == genome_uuid).first()
                if not genome:
                    raise ValueError("No genome found with the provided genome_uuid")
                if not parent_type and not child_type:
                    raise ValueError("Genome_uuid requires either child type or parent type.")
                if child_type:
                    #Alwalys go for the first one as dependencies will check the rest later.
                    new_type = session.query(DatasetType).filter(DatasetType.name == child_type).one()
                    parent_dataset_types.add(new_type.parent.split(';')[0])
                for genome_dataset in genome.genome_datasets:
                    if genome_dataset.dataset.status != 'Released' and genome_dataset.dataset.dataset_type.name in parent_dataset_types:
                        potential_parent_datasets.append(genome_dataset.dataset)
            else:
                raise ValueError("Either dataset_uuid, parent_type, or genome_uuid must be provided")
            return parent_dataset_types, potential_parent_datasets

        def process_datasets(session, parent_dataset_types, potential_parent_datasets):

            child_datasets = []
            if child_type:
                potential_child_types = [session.query(DatasetType).filter(DatasetType.name == child_type).first()]
            else:
                potential_child_types = session.query(DatasetType).filter(
                    DatasetType.parent.in_(parent_dataset_types)).all()

            for parent_dataset in potential_parent_datasets:
                # I thought this was a good idea, but we would need different logic
                # if parent_dataset.status == 'Processed':
                for child_dataset_type in potential_child_types:
                    if check_existing_and_dependencies(session, parent_dataset, child_dataset_type):
                        parent_genome_uuid = parent_dataset.genome_datasets[0].genome.genome_uuid
                        parent_dataset_source = parent_dataset.dataset_source
                        new_dataset_uuid, new_dataset_attributes, new_genome_dataset = self.create_dataset(
                            session, parent_genome_uuid, parent_dataset_source, child_dataset_type,
                            dataset_attributes, child_dataset_type.name, child_dataset_type.name, None
                        )
                        child_datasets.append(new_dataset_uuid)
            return child_datasets

        def check_existing_and_dependencies(session, parent_dataset, child_dataset_type):
            existing_datasets = session.query(Dataset).filter(
                Dataset.dataset_type == child_dataset_type,
                Dataset.genome_datasets.any(genome_id=parent_dataset.genome_datasets[0].genome_id),
                Dataset.status.in_(['Submitted', 'Processing', 'Processed'])
            ).count()

            if existing_datasets > 0:
                return False  # Skip if a similar dataset already exists

            dependencies = child_dataset_type.depends_on.split(';') if child_dataset_type.depends_on else []
            return all(
                session.query(Dataset).filter(
                    Dataset.dataset_type.has(name=dep),
                    Dataset.status == 'Processed',
                    Dataset.genome_datasets.any(genome_id=parent_dataset.genome_datasets[0].genome_id)
                ).count() > 0 for dep in dependencies
            )

        if self.owns_session:
            with self.metadata_db.session_scope() as session:
                parent_dataset_types, potential_parent_datasets = fetch_parent_datasets(session)
                return process_datasets(session, parent_dataset_types, potential_parent_datasets)
        else:
            session = self.session
            parent_dataset_types, potential_parent_datasets = fetch_parent_datasets(session)
            return process_datasets(session, parent_dataset_types, potential_parent_datasets)


    def get_parent_datasets(self, dataset_uuid):
        """
        Retrieves the parent datasets of a specified dataset. If the dataset does not have a parent,
        it returns the dataset itself and marks it as 'top_level'.

        Parameters:
        dataset_uuid (str): UUID of the dataset for which the parent datasets are to be found.

        Returns:
        tuple: Two lists containing UUIDs and types of the parent datasets.
        """
        parent_uuid = []
        parent_type = []

        def query_parent_datasets(session):
            dataset = self.get_dataset(session, dataset_uuid)
            dataset_type = session.query(DatasetType).filter(
                DatasetType.dataset_type_id == dataset.dataset_type_id).one()
            if dataset_type.parent is None:
                return ['dataset_uuid'], ['top_level']
            parent_dataset_types = dataset_type.parent.split(';')
            genome_id = next((gd.genome_id for gd in dataset.genome_datasets), None)
            if not genome_id:
                raise ValueError("No associated Genome found for the given dataset UUID")

            related_genome_datasets = session.query(GenomeDataset).join(Dataset).join(DatasetType).filter(
                GenomeDataset.genome_id == genome_id,
                DatasetType.name.in_(parent_dataset_types)
            ).all()

            for gd in related_genome_datasets:
                parent_uuid.append(gd.dataset.dataset_uuid)
                parent_type.append(gd.dataset.dataset_type.name)

        if self.owns_session:
            with self.metadata_db.session_scope() as session:
                query_parent_datasets(session)
        else:
            query_parent_datasets(self.session)

        return parent_uuid, parent_type


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

    def _update_status(self, dataset, status=None):

        valid_statuses = ['Submitted', 'Processing', 'Processed', 'Released']
        if status is None:
            old_status = dataset.status
            if old_status == 'Released':
                raise DatasetFactoryException("Unable to change status of Released dataset")
            elif old_status == 'Submitted':
                status = 'Processing'
            elif old_status == 'Processing':
                status = 'Processed'
            elif old_status == 'Processed':
                status = 'Released'
        if status not in valid_statuses:
            raise DatasetFactoryException(f"Unable to change status to {status} as this is not valid. Please use "
                                          f"one of :{valid_statuses}")
        dataset.status = status
        return status

    def update_dataset_status(self, dataset_uuid, status=None):
        """
        Updates the status of a dataset identified by its UUID. The status is updated to the next logical state unless
        a specific state is provided.

        Parameters:
        dataset_uuid (str): UUID of the dataset to update.
        status (str, optional): The new status to set for the dataset. If not provided, status is advanced to the next logical state.

        Returns:
        tuple: Dataset UUID and the updated status.
        """
        if self.owns_session:
            with self.metadata_db.session_scope() as session:
                dataset = self.get_dataset(session, dataset_uuid)
                updated_status = self._update_status(dataset, status)
        else:
            dataset = self.get_dataset(self.session, dataset_uuid)
            updated_status = self._update_status(dataset, status)
        return dataset_uuid, updated_status

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


# This is a direct copy of Marc's code in the core updater in an unmerged branch. I am not sure where we should keep it.

