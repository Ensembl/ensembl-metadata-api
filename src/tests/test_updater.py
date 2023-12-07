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
import re

from ensembl.database import UnitTestDB, DBConnection
from ensembl.production.metadata.api.factory import meta_factory
from ensembl.production.metadata.api.models import Organism, Assembly, Dataset, AssemblySequence

db_directory = Path(__file__).parent / 'databases'
db_directory = db_directory.resolve()


@pytest.mark.parametrize("multi_dbs", [[{'src': 'ensembl_metadata'}, {'src': 'ncbi_taxonomy'},
                                        {'src': db_directory / 'core_1'}, {'src': db_directory / 'core_2'},
                                        {'src': db_directory / 'core_3'}, {'src': db_directory / 'core_4'},
                                        {'src': db_directory / 'core_5'}]],
                         indirect=True)
class TestUpdater:
    dbc = None  # type: UnitTestDB

    def test_new_organism(self, multi_dbs):
        test = meta_factory(multi_dbs['core_1'].dbc.url, multi_dbs['ensembl_metadata'].dbc.url,
                            multi_dbs['ncbi_taxonomy'].dbc.url)
        test.process_core()
        # Look for organism, assembly and geneset
        metadata_db = DBConnection(multi_dbs['ensembl_metadata'].dbc.url)
        # Test the species
        with metadata_db.session_scope() as session:
            organism = session.query(Organism).where(Organism.ensembl_name == 'Jabberwocky').first()
            assembly = session.query(Assembly).where(Assembly.name == 'jaber01').first()
            assert organism.scientific_name == 'carol_jabberwocky'
            # Test the Assembly
            assert assembly.accession == 'weird01'
            # select * from genebuild where version = 999 and name = 'genebuild and label =01
            dataset = session.query(Dataset).where(
                (Dataset.version == 1) & (Dataset.name == 'genebuild')
            ).first()
            assert dataset is not None
            assert re.match(".*_core_1", dataset.dataset_source.name)
            assert dataset.dataset_source.type == "core"
            assert dataset.dataset_type.name == "genebuild"
            #Testing assembly sequence is circular
            sequence = session.query(AssemblySequence).where(
                (AssemblySequence.is_circular == 1) & (AssemblySequence.name == 'TEST1_seq')
            ).first()
            assert sequence is not None
            assert sequence.type == "primary_assembly"
            sequence2 = session.query(AssemblySequence).where(
                (AssemblySequence.is_circular == 0) & (AssemblySequence.name == 'TEST2_seq')
            ).first()
            assert sequence2 is not None
            assert sequence.type == "primary_assembly"
            sequence3 = session.query(AssemblySequence).where(
                (AssemblySequence.is_circular == 0) & (AssemblySequence.name == 'TEST3_seq')
            ).first()
            assert sequence3 is not None


    def test_update_organism(self, multi_dbs):
        test = meta_factory(multi_dbs['core_2'].dbc.url, multi_dbs['ensembl_metadata'].dbc.url,
                            multi_dbs['ncbi_taxonomy'].dbc.url)
        test.process_core()
        metadata_db = DBConnection(multi_dbs['ensembl_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            organism = session.query(Organism).where(Organism.ensembl_name == 'Jabberwocky').first()
            assert organism.scientific_name == 'carol_jabberwocky'
            assert len(organism.genomes) == 1

    def test_update_assembly(self, multi_dbs):
        test = meta_factory(multi_dbs['core_3'].dbc.url, multi_dbs['ensembl_metadata'].dbc.url,
                            multi_dbs['ncbi_taxonomy'].dbc.url)
        test.process_core()
        metadata_db = DBConnection(multi_dbs['ensembl_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            organism = session.query(Organism).where(Organism.ensembl_name == 'Jabberwocky').first()
            assert organism.scientific_name == 'carol_jabberwocky'
            assert organism.genomes[1].assembly.accession == 'weird02'

    #
    def test_update_geneset(self, multi_dbs):
        test = meta_factory(multi_dbs['core_4'].dbc.url, multi_dbs['ensembl_metadata'].dbc.url,
                            multi_dbs['ncbi_taxonomy'].dbc.url)
        test.process_core()
        metadata_db = DBConnection(multi_dbs['ensembl_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            dataset = session.query(Dataset).where(
                (Dataset.version == 2) & (Dataset.name == 'genebuild')
            ).first()
            assert dataset is not None
            assert re.match(".*_core_4", dataset.dataset_source.name)
            assert dataset.dataset_source.type == "core"
            assert dataset.dataset_type.name == "genebuild"

    def test_taxonomy_common_name(self, multi_dbs):
        test = meta_factory(multi_dbs['core_5'].dbc.url, multi_dbs['ensembl_metadata'].dbc.url,
                            multi_dbs['ncbi_taxonomy'].dbc.url)
        test.process_core()
        metadata_db = DBConnection(multi_dbs['ensembl_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            organism = session.query(Organism).where(Organism.ensembl_name == 'Hominoide').first()
            assert organism.common_name == 'apes'
