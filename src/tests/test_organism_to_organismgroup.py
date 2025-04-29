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
import re
from pathlib import Path
import pytest
from collections import namedtuple
from ensembl.production.metadata.api.models import OrganismGroup, Genome, Organism, OrganismGroupMember
from ensembl.production.metadata.scripts.organism_to_organismgroup import process_genomes, \
    create_or_remove_organism_group
from ensembl.utils.database import UnitTestDB, DBConnection

db_directory = Path(__file__).parent / 'databases'
db_directory = db_directory.resolve()

# Define a named tuple for script args
Args = namedtuple('Args', [
    'metadata_db_uri', 'core_server_uri', 'organism_group_type',
    'organism_group_name', 'genome_uuid', 'release_id', 'remove', 'raise_error'
])


@pytest.mark.parametrize("test_dbs", [[{'src': Path(__file__).parent / "databases/ensembl_genome_metadata"},
                                       {'src': Path(__file__).parent / "databases/ncbi_taxonomy"},
                                       {'src': Path(__file__).parent / "databases/core_1"},
                                       ]],
                         indirect=True)
class TestAddOrRemoveOrganismGroup:
    dbc = None

    @pytest.mark.parametrize(
        "genome_uuids, organism_group_type, organism_group_name, release_id, remove",
        [
            ('a7335667-93e7-11ec-a39d-005056b38ce3', 'Test', 'EnsemblTest', '', False),
            ('a7335667-93e7-11ec-a39d-005056b38ce3', 'Test', 'EnsemblTest', '', True),

        ]
    )
    def test_add_organismgroup(self, test_dbs, genome_uuids, organism_group_type, organism_group_name, release_id,
                               remove):
        metadata_db = DBConnection(test_dbs['ensembl_genome_metadata'].dbc.url)
        args = Args(
            metadata_db_uri=test_dbs['ensembl_genome_metadata'].dbc.url,
            core_server_uri=None,
            organism_group_type='Test',
            organism_group_name='EnsemblTest',
            genome_uuid=['a7335667-93e7-11ec-a39d-005056b38ce3'],
            release_id=[],
            remove=remove,
            raise_error=False
        )

        # Mock the database connection
        with metadata_db.session_scope() as session:
            organism_group = session.query(OrganismGroup).filter(
                OrganismGroup.name == args.organism_group_name,
                OrganismGroup.type == args.organism_group_type
            ).one_or_none()

            organism_group_id = organism_group.organism_group_id if organism_group else None
            assert organism_group_id is not None
            process_genomes(session, args, organism_group_id=organism_group_id)
            session.commit()
            # Check if the organism group was added
            query = (
                session.query(Genome, Organism, OrganismGroup).join(Organism, Organism.organism_id == Genome.organism_id
                                                          ).join(OrganismGroupMember,
                                                                 OrganismGroupMember.organism_id == Organism.organism_id
                                                                 ).join(OrganismGroup,
                                                                        OrganismGroup.organism_group_id == OrganismGroupMember.organism_group_id
                                                                        ).filter(
                                                                            Genome.genome_uuid.in_([args.genome_uuid]),
                                                                            OrganismGroup.name == args.organism_group_name,
                                                                        )
            )
            if remove:
                assert query.count() == 0, "Organism group member should be removed"
            else:
                assert query.count() > 0
                for genome, organism, organism_group in query.all():
                    assert organism_group.name == args.organism_group_name, f"Expected {args.organism_group_name}, got {organism_group.name}"
                    assert organism_group.type == args.organism_group_type, f"Expected {args.organism_group_type}, got {organism_group.type}"
