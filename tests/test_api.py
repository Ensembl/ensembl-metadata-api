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
from os.path import dirname
from ensembl.production.metadata.api import *

DB_NAME = 'sqlite:///' + dirname(__file__) + '/TEST.db'

def test_load_database():
    DB_TEST = ReleaseAdaptor(DB_NAME)
    assert DB_TEST, "DB should not be empty"

def test_fetch_releases():
    conn = ReleaseAdaptor(DB_NAME)
    TEST = conn.fetch_releases(release_id=1).one()
    #Test the one to many connection
    assert TEST.EnsemblSite.name == '2020-map'
    #Test the direct access.
    assert TEST.EnsemblRelease.label == '2020 MAP 7 species'

#currently only have one release, so the testing is not comprehensive
def test_fetch_releases_for_genome():
    conn = ReleaseAdaptor(DB_NAME)
    TEST = conn.fetch_releases_for_genome('a733574a-93e7-11ec-a39d-005056b38ce3').one()
    assert TEST.EnsemblSite.name == '2020-map'

def test_fetch_releases_for_dataset():
    conn = ReleaseAdaptor(DB_NAME)
    TEST = conn.fetch_releases_for_dataset('76ffa505-948d-11ec-a39d-005056b38ce3').one()
    assert TEST.EnsemblSite.name == '2020-map'