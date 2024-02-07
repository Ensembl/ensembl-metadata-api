from ensembl.production.metadata.api.exceptions import *

class DatasetFactory():
    """
    A class used to interact with the dataset REST endpoint and to add data.

    ...

    Attributes
    ----------
    genome_uuid = uuid

    Methods
    -------
    get_child_datasets()
    """
    def __init__(self, genome_uuid=None,dataset_type=None,dataset_uuid=None):
        if genome_uuid == None and dataset_uuid == None:
            raise DatasetFactoryException("genome_uuid + datset.type or dataset_uuid are required")

    def get_child_datasets(self):
        #Function to get all of the possible children datasets that are not constrained

    def create_child_datasets(self):
        #Recursive function to create all the child datasets that it can. Breaks when no more datasets are created
        print "Not Implemented"

    def create_dataset(self):
        print "Not Implemented"

    def update_dataset_status(self):
        print "Not Implemented"

    def update_dataset_attributes(self):
        print "Not Implemented"

