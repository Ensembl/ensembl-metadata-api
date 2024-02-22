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

from pathlib import Path

import pytest

from ensembl.database import DBConnection
from ensembl.database import UnitTestDB
from ensembl.production.metadata.api.exceptions import TypeNotFoundException
from ensembl.production.metadata.api.models import Organism, Genome

sample_path = Path(__file__).parent.parent / "ensembl" / "production" / "metadata" / "api" / "sample"


@pytest.mark.parametrize("multi_dbs", [[{"src": sample_path / "ensembl_genome_metadata"},
                                        {"src": sample_path / "ncbi_taxonomy"}]], indirect=True)
class TestApi:
    dbc = None  # type: UnitTestDB

    def test_get_public_path(self, multi_dbs):
        metadata_db = DBConnection(multi_dbs['ensembl_genome_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            genome = session.query(Genome).filter(Genome.genome_uuid == 'a733574a-93e7-11ec-a39d-005056b38ce3').first()
            paths = genome.get_public_path(dataset_type='all')
            assert len(paths) == 4
            # assert all("/genebuild/" in path for path in paths)
            path = genome.get_public_path(dataset_type='genebuild')
            assert path[0]['path'] == 'Saccharomyces_cerevisiae_S288c/GCA_000146045.2/ensembl/geneset/2018_10'
            path = genome.get_public_path(dataset_type='assembly')
            assert path[0]['path'] == 'Saccharomyces_cerevisiae_S288c/GCA_000146045.2/ensembl/genome'
            path = genome.get_public_path(dataset_type='variation')
            assert path[0]['path'] == 'Saccharomyces_cerevisiae_S288c/GCA_000146045.2/ensembl/variation/2018_10'
            path = genome.get_public_path(dataset_type='homologies')
            assert path[0]['path'] == 'Saccharomyces_cerevisiae_S288c/GCA_000146045.2/ensembl/homology/2018_10'
            with pytest.raises(TypeNotFoundException):
                genome.get_public_path(dataset_type='regulatory_features')
                # assert path[0]['path'] == 'Saccharomyces_cerevisiae_S288c/GCA_000146045.2/ensembl/regulation'

    def test_default_public_path(self, multi_dbs):
        metadata_db = DBConnection(multi_dbs['ensembl_genome_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            # Homo sapien GRCH38
            genome = session.query(Genome).filter(Genome.genome_uuid == 'a7335667-93e7-11ec-a39d-005056b38ce3').first()
            paths = genome.get_public_path(dataset_type='all')
            assert len(paths) == 5
            # assert all("/genebuild/" in path for path in paths)
            path = genome.get_public_path(dataset_type='genebuild')
            assert path[0]['path'] == 'Homo_sapiens/GCA_000001405.29/ensembl/geneset/2023_03'
            path = genome.get_public_path(dataset_type='assembly')
            assert path[0]['path'] == 'Homo_sapiens/GCA_000001405.29/ensembl/genome'
            path = genome.get_public_path(dataset_type='variation')
            assert path[0]['path'] == 'Homo_sapiens/GCA_000001405.29/ensembl/variation/2023_03'
            path = genome.get_public_path(dataset_type='homologies')
            assert path[0]['path'] == 'Homo_sapiens/GCA_000001405.29/ensembl/homology/2023_03'
            path = genome.get_public_path(dataset_type='regulatory_features')
            assert path[0]['path'] == 'Homo_sapiens/GCA_000001405.29/ensembl/regulation'

    def test_organism_ensembl_name_compat(self, multi_dbs):
        """ Validate that we can still yse ensembl_name in queries from SQLAlchemy
        This test will fail when we remove the ORM column for good
        """
        metadata_db = DBConnection(multi_dbs['ensembl_genome_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            ensembl_name = session.query(Organism).filter(Organism.ensembl_name == 'SAMN12121739').first()
            biosample_id = session.query(Organism).filter(Organism.biosample_id == 'SAMN12121739').first()
            assert ensembl_name.organism_uuid == biosample_id.organism_uuid

