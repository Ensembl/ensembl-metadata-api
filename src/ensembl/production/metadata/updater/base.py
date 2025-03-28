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
from ensembl.utils.database import DBConnection
from sqlalchemy import inspect
from sqlalchemy.engine import make_url

from ensembl.production.metadata.api.models import DatasetSource
from ensembl.production.metadata.api.models import EnsemblRelease


class BaseMetaUpdater:
    def __init__(self, db_uri, metadata_uri, release=None):
        self.db_uri = db_uri
        self.metadata_uri = metadata_uri
        self.db = DBConnection(self.db_uri)
        self.metadata_db = DBConnection(metadata_uri)
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

    def get_or_new_source(self, meta_session, db_type, name=None):
        db_uri = self.db_uri
        if name is None:
            # For core databases
            name = make_url(db_uri).database
        dataset_source = meta_session.query(DatasetSource).filter(DatasetSource.name == name).one_or_none()
        if dataset_source is None:
            dataset_source = DatasetSource(
                type=db_type,  # core/fungen etc
                name=name  # dbname
            )
            meta_session.add(dataset_source)  # Only add a new DatasetSource to the session if it doesn't exist
        return dataset_source
