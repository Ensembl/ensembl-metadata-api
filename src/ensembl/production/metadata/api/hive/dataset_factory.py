from ensembl.database import DBConnection

from ensembl.production.metadata.api.exceptions import *
from ensembl.production.metadata.api.models import Dataset


class DatasetFactory():
    """
    A class used to interact with the ensembl_genome_metadata to modify  dataset and dataset attribute table.

    ...

    Attributes
    ----------
    genome_uuid = uuid

    Methods
    -------
    get_child_datasets()
    """
    def __init__(self,session=None,metadata_uri=None):
        if session:
            self.session = session
            self.owns_session = False
        else:
            if metadata_uri is None:
                raise DatasetFactoryException("session or metadata_uri are required")
            self.owns_session = True
            self.metadata_db = DBConnection(metadata_uri)

    #     #TODO: Determine how to implement genome_uuid when we can have multiples of each dataset type per genome
    def get_child_datasets(self, dataset_uuid=None):
        #Function to get all of the possible children datasets that are not constrained
        #Only returns children of dataset_uuid if specified
        child_datasets = []
        return child_datasets
    def create_child_datasets(self, dataset_uuid=None, dataset_type=None):
        #Recursive function to create all the child datasets that it can. Breaks when no more datasets are created
        #Only returns children of dataset_uuid if specified
        #Should be limited to a single type if dataset_uuid is not specified
        child_datasets = self.get_child_datasets()
        return child_datasets

    def create_dataset(self,genome_uuid, datasource, dataset_type, dataset_attributes):
        dataset_uuid = ''
        return dataset_uuid

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
        if self.owns_session:
            with self.metadata_db.session_scope() as session:
                dataset = self.get_dataset(session, dataset_uuid)
                updated_status = self._update_status(dataset, status)
        else:
            dataset = self.get_dataset(self.session, dataset_uuid)
            updated_status = self._update_status(dataset, status)

        return dataset_uuid, updated_status

    def update_dataset_attributes(self,dataset_uuid, dataset_attributes):
        datset_attribute_indicies = []
        return dataset_uuid,datset_attribute_indicies

    def get_dataset(self, session, dataset_uuid):
        return session.query(Dataset).filter(Dataset.dataset_uuid == dataset_uuid).one()

    def close_session(self):
        if self.owns_session and self.session:
            self.session.close()