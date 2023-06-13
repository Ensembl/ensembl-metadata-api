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
from ensembl.database import DBConnection

from ensembl.production.metadata.config import get_metadata_uri


class BaseAdaptor:
    def __init__(self, metadata_uri=None):
        if metadata_uri is None:
            metadata_uri = get_metadata_uri()
        self.metadata_db = DBConnection(metadata_uri)


def check_parameter(param):
    if param is not None and not isinstance(param, list):
        param = [param]
    return param
