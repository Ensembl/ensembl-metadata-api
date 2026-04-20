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
import logging

from ensembl.utils.database import DBConnection
from sqlalchemy import inspect, cast, Integer
from sqlalchemy.engine import make_url

from ensembl.production.metadata.api import exceptions
from ensembl.production.metadata.api.models import DatasetSource, EnsemblRelease, ReleaseStatus

logger = logging.getLogger(__name__)

class BaseMetaUpdater:
    def __init__(self, db_uri, metadata_uri, taxonomy_uri, release_name=None):
        self.db_uri = db_uri
        self.metadata_uri = metadata_uri
        self.taxonomy_uri = taxonomy_uri
        self.db = DBConnection(self.db_uri)
        self.metadata_db = DBConnection(metadata_uri)
        self.taxonomy_db = DBConnection(taxonomy_uri)
        if release_name is None:
            self.release_name = None
        else:
            self.release_name = release_name

    def is_object_new(self, obj):
        """
        Returns True if the object is new in the current session. Mostly here for code readibility
        """
        insp = inspect(obj)
        return insp.transient or insp.pending

    def get_or_new_source(self, meta_session, db_type, name=None):
        db_uri = self.db_uri
        parsed_url = make_url(db_uri)
        location = parsed_url.host
        if name is None:
            name = parsed_url.database

        dataset_source = meta_session.query(DatasetSource).filter(DatasetSource.name == name).one_or_none()
        if dataset_source is None:
            dataset_source = DatasetSource(
                type=db_type,  # core/fungen etc
                name=name,  # dbname
                location=location
            )
            meta_session.add(dataset_source)  # Only add a new DatasetSource to the session if it doesn't exist
        return dataset_source

    def get_release(self, session) -> EnsemblRelease:
        if self.release_name is not None:
            release = session.query(EnsemblRelease).filter(
                EnsemblRelease.name == self.release_name
            ).one_or_none()
            if release is None:
                raise exceptions.MetadataUpdateException(
                    f"No EnsemblRelease with name '{self.release_name}' found"
                )
            if release.status != ReleaseStatus.PLANNED:
                raise exceptions.MetadataUpdateException(
                    f"EnsemblRelease '{self.release_name}' is not of status Planned."
                )
        else:
            release = session.query(EnsemblRelease).filter(
                EnsemblRelease.status == ReleaseStatus.PLANNED
            ).order_by(cast(EnsemblRelease.name, Integer)).first()
            if release is None:
                raise exceptions.MetadataUpdateException(
                    "No EnsemblRelease with status PLANNED found"
                )

        logger.debug(f"Using release {release.name}")
        return release
