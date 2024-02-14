from ensembl.database import DBConnection

from ensembl.production.metadata.api.exceptions import *
from ensembl.production.metadata.api.models import Dataset, Attribute, DatasetAttribute, Genome, GenomeDataset, \
    DatasetType
from sqlalchemy.sql import func
import datetime
import uuid


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

    def __init__(self, session=None, metadata_uri=None):
        if session:
            self.session = session
            self.owns_session = False
        else:
            if metadata_uri is None:
                raise DatasetFactoryException("session or metadata_uri are required")
            self.owns_session = True
            self.metadata_db = DBConnection(metadata_uri)

    #     #TODO: Determine how to implement genome_uuid when we can have multiples of each dataset type per genome
    def get_child_datasets(self, dataset_uuid=None,constrained=True):
        # Function to get all of the possible children datasets that are not constrained
        # Only returns children of dataset_uuid if specified
        child_datasets = []
        return child_datasets

    def create_child_datasets(self, dataset_uuid=None, dataset_type=None, constrained=True):
        child_datasets = []

        with self.session as session:
            # Identify parent dataset types
            parent_dataset_types = set()
            if dataset_uuid:
                parent_dataset = session.query(Dataset).filter(Dataset.dataset_uuid == dataset_uuid).first()
                if parent_dataset:
                    parent_dataset_types.add(parent_dataset.dataset_type.name)
            elif dataset_type:
                parent_dataset_types.add(dataset_type)
            else:
                raise ValueError("Either dataset_uuid or dataset_type must be provided")

            potential_child_types = session.query(DatasetType).filter(
                DatasetType.parent.in_(parent_dataset_types)).all()

            for child_type in potential_child_types:
                # Check if dependencies are 'Processed'
                dependencies = child_type.depends_on.split(';') if child_type.depends_on else []
                all_dependencies_processed = all(
                    session.query(Dataset).filter(Dataset.dataset_type.has(name=dep),
                                                  Dataset.status == 'Processed').count() > 0
                    for dep in dependencies
                )
                if all_dependencies_processed:
                    # Create child dataset
                    new_dataset = self.create_dataset(session,genome_uuid, dataset_source, dataset_type, dataset_attributes, name, label,
                       version)
                    session.add(new_dataset)
                    child_datasets.append(new_dataset)

        return child_datasets

        return child_datasets

    def get_parent_datasets(self, dataset_uuid):
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
        if self.owns_session:
            with self.metadata_db.session_scope() as session:
                dataset = self.get_dataset(session, dataset_uuid)
                updated_status = self._update_status(dataset, status)
        else:
            dataset = self.get_dataset(self.session, dataset_uuid)
            updated_status = self._update_status(dataset, status)
        return dataset_uuid, updated_status

    def update_dataset_attributes(self, dataset_uuid, attribut_dict):
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
        return session.query(Dataset).filter(Dataset.dataset_uuid == dataset_uuid).one()


# This is a direct copy of Marc's code in the core updater in an unmerged branch. I am not sure where we should keep it.
def update_attributes(dataset, attributes, session):
    dataset_attributes = []
    for attribute, value in attributes.items():
        meta_attribute = session.query(Attribute).filter(Attribute.name == attribute).one_or_none()
        if meta_attribute is None:
            raise UpdaterException(f"{attribute} does not exist. Add it to the database and reload.")
        dataset_attributes.append(DatasetAttribute(
            value=value,
            dataset=dataset,
            attribute=meta_attribute,
        ))
    return dataset_attributes
