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
from pathlib import Path

import pytest
from ensembl.utils.database import UnitTestDB

db_directory = Path(__file__).parent / 'databases'
db_directory = db_directory.resolve()


@pytest.mark.parametrize("test_dbs", [[{'src': Path(__file__).parent / "databases/ensembl_genome_metadata"},
                                       {'src': Path(__file__).parent / "databases/ncbi_taxonomy"},
                                       {'src': Path(__file__).parent / "databases/core_1"},
                                       {'src': Path(__file__).parent / "databases/core_2"},
                                       {'src': Path(__file__).parent / "databases/core_3"},
                                       {'src': Path(__file__).parent / "databases/core_4"},
                                       {'src': Path(__file__).parent / "databases/core_5"},
                                       {'src': Path(__file__).parent / "databases/core_6"},
                                       {'src': Path(__file__).parent / "databases/core_7"},
                                       {'src': Path(__file__).parent / "databases/core_8"},
                                       {'src': Path(__file__).parent / "databases/core_9"}
                                       ]],
                         indirect=True)
class TestUpdater:
    dbc = None  # type: UnitTestDB

# Test before loading

# Test full load works nicely with genome
# Test full load works with dbname.
# Test assembly fail/success
# Test genome fail/success
# Test organism fail/success
# Test assembly_dataset fail/success
# Test genebuild_dataset fail/success
