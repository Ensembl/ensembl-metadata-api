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
    # TODO If attributes already exist, update them. Add option to replace all.
    dataset_attributes = []
    if replace:
        for dataset_attribute in dataset.dataset_attributes:
            session.delete(dataset_attribute)
            session.flush()
    for attribute, value in attributes.items():
        meta_attribute = session.query(Attribute).filter(Attribute.name == attribute).one_or_none()
        if meta_attribute is None:
            raise UpdaterException(f"{attribute} does not exist. Add it to the database and reload.")
        new_dataset_attribute = DatasetAttribute(
            value=value,
            dataset=dataset,
            attribute=meta_attribute,
        )
        session.add(new_dataset_attribute)
        dataset_attributes.append(new_dataset_attribute)
    return dataset_attributes