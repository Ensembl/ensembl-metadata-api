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
#   limitations under the License

from sqlalchemy.engine import make_url

from ensembl.production.metadata.updater.core import CoreMetaUpdater


def meta_factory(db_uri, metadata_uri, taxonomy_uri, release=None):
    db_url = make_url(db_uri)
    if '_core_' in db_url.database:
        return CoreMetaUpdater(db_uri, metadata_uri, taxonomy_uri, release=release)
    else:
        raise ValueError("Can't find data_type for database " + db_url.database)
