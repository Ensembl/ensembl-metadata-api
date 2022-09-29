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
"""
Unit tests for api module
"""
from ensembl.production.metadata.api import *

def test_load_database():
    DB_TEST = ReleaseAdaptor('sqlite:///TEST.db')
    assert DB_TEST, "DB should not be empty"

def test_Release_adaptor():
    conn = ReleaseAdaptor('sqlite:///TEST.db')
    TEST2 = conn.fetch_releases().one()
    #Test the one to many connection
    assert TEST2[6] == '2020-map'
    #Test the direct access.
    assert TEST2[3] == '2020 MAP 7 species'
