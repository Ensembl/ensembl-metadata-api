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
import sqlalchemy as db
from sqlalchemy import inspect
from sqlalchemy.engine import make_url

from ensembl.core.models import Meta

from ensembl.production.metadata.api.exceptions import UpdaterException
from ensembl.production.metadata.api.models import DatasetSource, Attribute
from ensembl.database import DBConnection
from ensembl.production.metadata.api.models import EnsemblRelease


class BaseMetaUpdater:
    def __init__(self, db_uri, metadata_uri, taxonomy_uri, release=None, force=None):
        self.db_uri = db_uri
        self.force = force
        self.taxonomy_uri = taxonomy_uri
        self.metadata_uri = metadata_uri
        self.db = DBConnection(self.db_uri)
        self.metadata_db = DBConnection(metadata_uri)
        self.taxonomy_db = DBConnection(taxonomy_uri)
        # We will add a release later. For now, the release must be specified for it to be used.
        if release is None:
            self.listed_release = None
            self.listed_release_is_current = None
        else:
            self.listed_release = release
            self.listed_release_is_current = EnsemblRelease.is_current

    def is_object_new(self, obj):
        """
        Returns True if the object is new in the current session. Mostly here for code readibility
        """
        insp = inspect(obj)
        return insp.transient or insp.pending

    def get_or_new_source(self, meta_session, db_type):
        db_uri = self.db_uri
        name = make_url(db_uri).database
        dataset_source = meta_session.query(DatasetSource).filter(DatasetSource.name == name).one_or_none()
        if dataset_source is None:
            dataset_source = DatasetSource(
                type=db_type,  # core/fungen etc
                name=name  # dbname
            )
            meta_session.add(dataset_source)  # Only add a new DatasetSource to the session if it doesn't exist
        return dataset_source

    def update_attributes(self, dataset, attributes, session):
        genebuild_dataset_attributes = []
        for attribute, value in attributes.items():
            meta_attribute = session.query(Attribute).filter(Attribute.name == attribute).one_or_none()
            if meta_attribute is None:
                raise UpdaterException(f"{attribute} does not exist. Add it to the database and reload.")
            genebuild_dataset_attributes.append(DatasetAttribute(
                value=value,
                dataset=dataset,
                attribute=meta_attribute,
            ))
        return genebuild_dataset_attributes