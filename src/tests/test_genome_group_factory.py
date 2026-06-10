# See the NOTICE file distributed with this work for additional information
#   regarding copyright ownership.
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may obtain a copy of the License at
#       http://www.apache.org/licenses/LICENSE-2.0
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import csv
import uuid
from pathlib import Path

import pytest
from ensembl.utils.database import DBConnection

from ensembl.production.metadata.api.factories.genome_groups import GenomeGroupFactory
from ensembl.production.metadata.api.models import Genome, GenomeGroup, GenomeGroupMember


@pytest.mark.parametrize(
    "test_dbs",
    [[
        {'src': Path(__file__).parent / "databases/ensembl_genome_metadata"},
        {'src': Path(__file__).parent / "databases/ncbi_taxonomy"},
    ]],
    indirect=True,
)
class TestGenomeGroupFactory:

    def test_add_remove_and_delete_genome_group(self, test_dbs, tmp_path):
        metadata_uri = test_dbs['ensembl_genome_metadata'].dbc.url
        group_name = f"test_genome_group_{uuid.uuid4().hex[:8]}"

        with DBConnection(metadata_uri).session_scope() as session:
            genome = session.query(Genome).filter(Genome.suppressed == 0).first()
            assert genome is not None

        csv_file = tmp_path / "genome_group.csv"
        with csv_file.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["production_name", "genome_uuid"])
            writer.writeheader()
            writer.writerow({
                "production_name": genome.production_name,
                "genome_uuid": genome.genome_uuid,
            })
            writer.writerow({
                "production_name": "missing_species",
                "genome_uuid": "missing-genome-uuid",
            })

        factory = GenomeGroupFactory(metadata_uri)
        result = factory.add_genomes_from_csv(metadata_uri, group_name, str(csv_file))

        assert result["added"] == 1
        assert result["skipped_missing"] == 1
        assert result["skipped_suppressed"] == 0

        with DBConnection(metadata_uri).session_scope() as session:
            group = session.query(GenomeGroup).filter(GenomeGroup.name == group_name).one()
            member = session.query(GenomeGroupMember).filter(
                GenomeGroupMember.genome_group_id == group.genome_group_id,
                GenomeGroupMember.genome_id == genome.genome_id,
            ).one_or_none()
            assert member is not None
            assert member.is_current == 1
            assert member.genome_id == genome.genome_id

        remove_result = factory.remove_genomes_from_group(metadata_uri, group_name, [genome.genome_uuid])
        assert remove_result["removed"] == 1
        assert remove_result["not_found"] == 0

        with DBConnection(metadata_uri).session_scope() as session:
            group = session.query(GenomeGroup).filter(GenomeGroup.name == group_name).one()
            member = session.query(GenomeGroupMember).filter(
                GenomeGroupMember.genome_group_id == group.genome_group_id,
                GenomeGroupMember.genome_id == genome.genome_id,
            ).one_or_none()
            assert member is None

        delete_result = factory.delete_genome_group(metadata_uri, group_name)
        assert delete_result["deleted"] == 1
        assert delete_result["members_deleted"] >= 0

        with DBConnection(metadata_uri).session_scope() as session:
            assert session.query(GenomeGroup).filter(GenomeGroup.name == group_name).one_or_none() is None
