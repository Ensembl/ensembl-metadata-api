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
from ensembl.production.metadata.api.exceptions import UpdaterException
from ensembl.production.metadata.api.models import Attribute, DatasetAttribute


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