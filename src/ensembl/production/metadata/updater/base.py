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
from sqlalchemy.engine import make_url

from ensembl.core.models import Meta
from ensembl.production.metadata.api.models import DatasetSource
from ensembl.database import DBConnection
from ensembl.production.metadata.api.models import EnsemblRelease


class BaseMetaUpdater:
    def __init__(self, db_uri, metadata_uri, taxonomy_uri, release=None):
        self.db_uri = db_uri
        self.db = DBConnection(self.db_uri)
        self.metadata_db = DBConnection(metadata_uri)
        # We will add a release later. For now, the release must be specified for it to be used.
        if release is None:
            self.listed_release = None
            self.listed_release_is_current = None
        else:
            self.listed_release = release
            self.listed_release_is_current = EnsemblRelease.is_current


    # Basic API for the meta table in the submission database.
    def get_meta_single_meta_key(self, species_id, parameter):
        with self.db.session_scope() as session:
            result = (session.execute(db.select(Meta.meta_value).filter(
                Meta.meta_key == parameter).filter(Meta.species_id == species_id)).one_or_none())
            if result is None:
                return None
            else:
                return result[0]

    def get_meta_list_from_prefix_meta_key(self, species_id, prefix):
        with self.db.session_scope() as session:
            query = session.query(Meta.meta_key, Meta.meta_value).filter(
                Meta.meta_key.like(f'{prefix}%'),
                Meta.species_id == species_id
            )
            result = query.all()
            if not result:
                return None
            else:
                # Build a dictionary out of the results.
                result_dict = {key: value for key, value in result}
                return result_dict


    def get_or_new_source(self, meta_session, db_uri, db_type):
        name = make_url(db_uri).database
        dataset_source = meta_session.query(DatasetSource).filter(DatasetSource.name == name).one_or_none()
        if dataset_source is None:
            dataset_source = DatasetSource(
                type=db_type,  # core/fungen etc
                name=name  # dbname
            )
            return dataset_source, "new"
        else:
            return dataset_source, "existing"
