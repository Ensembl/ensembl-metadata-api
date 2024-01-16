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
from unittest import mock
from unittest.mock import Mock, patch

import pytest
import re

import sqlalchemy
from ensembl.database import UnitTestDB, DBConnection

from ensembl.production.metadata.api.exceptions import UpdateBackCoreException
from ensembl.production.metadata.api.factory import meta_factory
from ensembl.production.metadata.api.models import Organism, Assembly, Dataset, AssemblySequence, DatasetAttribute, \
    DatasetSource, DatasetType, Attribute, Genome
from ensembl.core.models import Meta

from ensembl.production.metadata.updater.core import CoreMetaUpdater

db_directory = Path(__file__).parent / 'databases'
db_directory = db_directory.resolve()


@pytest.mark.parametrize("multi_dbs", [[{'src': 'ensembl_metadata'}, {'src': 'ncbi_taxonomy'}]], indirect=True)
class TestApi:
    dbc = None  # type: UnitTestDB

    def test_get_public_path_genebuild(self, multi_dbs):
        metadata_db = DBConnection(multi_dbs['ensembl_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            genome = session.query(Genome).filter(Genome.genome_uuid == 'a733574a-93e7-11ec-a39d-005056b38ce3').first()
            paths = genome.get_public_path(type='all')
            assert len(paths) == 5
            # assert all("/genebuild/" in path for path in paths)
            path = genome.get_public_path(type='genebuild')
            assert path[0] == 'Saccharomyces_cerevisiae_S288c/GCA_000146045.2/test_anno_source/genebuild/test_version'
            path = genome.get_public_path(type='assembly')
            assert path[0] == 'Saccharomyces_cerevisiae_S288c/GCA_000146045.2/test_anno_source/genome'
            path = genome.get_public_path(type='variation')
            assert path[0] == 'Saccharomyces_cerevisiae_S288c/GCA_000146045.2/test_anno_source/variation'
            path = genome.get_public_path(type='homologies')
            assert path[0] == 'Saccharomyces_cerevisiae_S288c/GCA_000146045.2/test_anno_source/homology'
            path = genome.get_public_path(type='regulatory_features')
            assert path[0] == 'Saccharomyces_cerevisiae_S288c/GCA_000146045.2/test_anno_source/regulation'

    def test_organism_ensembl_name_compat(self, multi_dbs):
        """ Validate that we can still yse ensembl_name in queries from SQLAlchemy
        This test will fail when we remove the ORM column for good
        """
        metadata_db = DBConnection(multi_dbs['ensembl_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            ensembl_name = session.query(Organism).filter(Organism.ensembl_name == 'homo_sapiens').first()
            biosample_id = session.query(Organism).filter(Organism.biosample_id == 'homo_sapiens').first()
            assert ensembl_name.organism_uuid == biosample_id.organism_uuid
