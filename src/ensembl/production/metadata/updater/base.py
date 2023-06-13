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

from ensembl.core.models import Meta
from ensembl.production.metadata import config
from ensembl.database import DBConnection
from ensembl.production.metadata.models import EnsemblRelease

class BaseMetaUpdater:
    def __init__(self, db_uri, metadata_uri=None, release=None):
        self.db_uri = db_uri
        self.db = DBConnection(self.db_uri)
        self.species = None
        self.db_type = None
        if metadata_uri is None:
            metadata_uri = config.get_metadata_uri()
            # We will add a release later. For now, the release must be specified for it to be used.
        if release is None:
            self.listed_release = None
            self.listed_release_is_current = None
        else:
            self.listed_release = release
            self.listed_release_is_current = EnsemblRelease.is_current
        self.metadata_db = DBConnection(metadata_uri)

    # Basic API for the meta table in the submission database.
    def get_meta_single_meta_key(self, species_id, parameter):
        with self.db.session_scope() as session:
            result = (session.execute(db.select(Meta.meta_value).filter(
                Meta.meta_key == parameter).filter(Meta.species_id == species_id)).one_or_none())
            if result is None:
                return None
            else:
                return result[0]


