#  See the NOTICE file distributed with this work for additional information
#  regarding copyright ownership.
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#      http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Unit tests for ``ensembl.production.metadata.updater.updater_utils``."""
from contextlib import nullcontext as does_not_raise
import logging
from pathlib import Path
from typing import ContextManager

import pytest

from ensembl.production.metadata.api.exceptions import MetadataUpdateException
from ensembl.production.metadata.updater.updater_utils import get_homology_reference_collection
from ensembl.utils.database import UnitTestDB

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "test_dbs",
    [
        [
            {'src': Path(__file__).parent / "databases" / "ensembl_genome_metadata"},
            {'src': Path(__file__).parent / "databases" / "ncbi_taxonomy"},
        ]
    ],
    indirect=True,
)
class TestUpdaterUtils:

    @pytest.mark.parametrize(
        ("taxonomy_id", "expectation"),
        [
            pytest.param(3702, does_not_raise("Viridiplantae"), id="Collection found for taxon ID"),
            pytest.param(
                4932,
                pytest.raises(
                    MetadataUpdateException,
                    match="Taxonomy ID '4932' did not get assigned any homology reference collection",
                ),
                id="Taxon ID not matching any collection",
            ),
        ],
    )
    def test_get_homology_reference_collection(
        self, test_dbs: dict[str, UnitTestDB], taxonomy_id: int, expectation: ContextManager
    ) -> None:
        """Test :func:`ensembl.production.metadata.updater.updater_utils.get_homology_reference_collection()`.
        
        Args:
            test_dbs:
            taxonomy_id: NCBI taxonomy ID.
            expectation: Context manager describing whether the function should raise.

        """
        metadata_dbc = test_dbs["ensembl_genome_metadata"].dbc
        taxonomy_uri = test_dbs['ncbi_taxonomy'].dbc.url
        with metadata_dbc.session_scope() as metadata_session:
            with expectation as expected:
                output = get_homology_reference_collection(taxonomy_id, taxonomy_uri, metadata_session)
                assert output == expected
