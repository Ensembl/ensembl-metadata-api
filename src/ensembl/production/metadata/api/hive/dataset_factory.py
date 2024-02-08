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
        if session is None:
            if metadata_uri is None:
                raise DatasetFactoryException("session or metadata_uri are required")
            self.session = DBConnection(metadata_uri).session_scope()
            self.session_source = "new"
        else:
            self.session = session
            self.session_source = "import"
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
    def update_dataset_status(self,dataset_uuid,status=None):
        dataset=self.get_dataset(dataset_uuid)
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
        dataset.status = status
        #TODO: Check if I have to close the session here.
        return dataset_uuid,status

    def update_dataset_attributes(self,dataset_uuid, dataset_attributes):
        datset_attribute_indicies = []
        return dataset_uuid,datset_attribute_indicies

    def get_dataset(self, dataset_uuid):
        dataset = self.session.query(Dataset).filter(Dataset.dataset_uuid == dataset_uuid).one()
        return dataset