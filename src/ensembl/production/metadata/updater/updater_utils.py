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
from sqlalchemy.orm import Session

from ensembl.ncbi_taxonomy.api import Taxonomy
from ensembl.production.metadata.api import exceptions
from ensembl.production.metadata.api.exceptions import UpdaterException
from ensembl.production.metadata.api.models import Attribute, DatasetAttribute, GenomeGroup
from ensembl.utils.database import DBConnection


def update_attributes(dataset, attributes, session, replace=False):
    """
    Update or create dataset attributes.

    Args:
        dataset: The dataset object to attach attributes to
        attributes: Dictionary of {attribute_name: value} where value can be:
                   - A single value: "GRCh38"
                   - A list of values: ["hg38", "Human"]
        session: Database session
        replace: If True, delete existing attributes before adding new ones

    Returns:
        list: List of created DatasetAttribute objects
    """
    dataset_attributes = []

    if replace:
        for dataset_attribute in dataset.dataset_attributes:
            session.delete(dataset_attribute)
        session.flush()

    for attribute_name, attribute_value in attributes.items():
        meta_attribute = session.query(Attribute).filter(Attribute.name == attribute_name).one_or_none()
        if meta_attribute is None:
            raise UpdaterException(f"{attribute_name} does not exist. Add it to the database and reload.")

        # Normalize to list format
        values = attribute_value if isinstance(attribute_value, list) else [attribute_value]

        # Create a DatasetAttribute for each value
        for value in values:
            new_dataset_attribute = DatasetAttribute(
                value=value,
                dataset=dataset,
                attribute=meta_attribute,
            )
            session.add(new_dataset_attribute)
            dataset_attributes.append(new_dataset_attribute)

    return dataset_attributes


def get_homology_reference_collection(taxonomy_id: int, taxonomy_uri: str, session: Session) -> str:
    """Determine the compara homology reference collection for a given taxonomy ID.

    This method expects the metadata `genome_group` table to contain the data in the following format::

        +-----------------+-------------------+------+----------------+-------------+
        | genome_group_id | type              | name | label          | description |
        +-----------------+-------------------+------+----------------+-------------+
        |               1 | compara_reference | 7898 | Actinopterygii | NULL        |
        |               2 | compara_reference | 6656 | Arthropoda     | NULL        |
        |             ... | ...               | ...  | ...            | ...         |
        +-----------------+-------------------+------+----------------+-------------+

    Args:
        taxonomy_id: NCBI taxonomy ID.
        taxonomy_uri: NCBI taxonomy database URI.
        session: Metadata database session.

    Returns:
        str: The reference collection name for the `compara.homology_reference_set` attribute.

    Raises:
        MetadataUpdateException: If no reference collection can be determined for ``taxonomy_id``.

    """
    tax_db = DBConnection(taxonomy_uri)
    # Get all available homology reference collections, but keep only their name, i.e. the
    # collection's taxon ID
    reference_collections = session.query(GenomeGroup).filter(GenomeGroup.type == "compara_reference").all()
    collection_ids = {str(collection.name) for collection in reference_collections}
    selected_collection: str | None = None
    # Explore the taxonomy tree from the given taxonomy ID to the root, stopping at the first taxon ID
    # that matches a homology reference collection
    with tax_db.session_scope() as tax_session:
        current = Taxonomy.fetch_node_by_id(tax_session, taxonomy_id)
        while not Taxonomy.is_root(tax_session, current.taxon_id):
            parent = Taxonomy.parent(tax_session, current.taxon_id)
            parent_taxon_id = str(parent.taxon_id)
            if parent_taxon_id in collection_ids:
                selected_collection = parent_taxon_id
                break
            current = parent
    if selected_collection is None:
        raise exceptions.MetadataUpdateException(
            f"Taxonomy ID '{taxonomy_id}' did not get assigned any homology reference collection"
        )
    return selected_collection
