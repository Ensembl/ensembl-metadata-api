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

import logging
from pathlib import Path

import pytest
from ensembl.database import UnitTestDB

from ensembl.production.metadata.grpc.adaptors.genome import GenomeAdaptor

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("multi_dbs", [[{"src": Path(__file__).parent / "databases/ensembl_genome_metadata"},
                                        {"src": Path(__file__).parent / "databases/ncbi_taxonomy"}]],
                         indirect=True)
class TestGRPCGenomeAdaptor:
    dbc: UnitTestDB = None

    @pytest.mark.parametrize(
        "allow_unreleased, unreleased_only, current_only, output_count",
        [
            (True, False, False, 19),  # Allow Unreleased
            (False, False, False, 10),  # Do not allow unreleased - fetch all even from previous releases
            (False, True, False, 10),  # unreleased_only has no effect when ALLOW_UNRELEASED is False
            (False, False, True, 3)  # Only the ones from current release
        ],
        indirect=['allow_unreleased']
    )
    def test_fetch_genomes_release(self, genome_conn, allow_unreleased, unreleased_only, current_only, output_count):
        """
        Test fetching all released genomes
        - Unreleased_only: should have no effect on results
        - Current_only: Should filter against only release 110.1 (current)
        """
        test = genome_conn.fetch_genomes(unreleased_only=unreleased_only, current_only=current_only)
        assert len(test) == output_count

    @pytest.mark.parametrize(
        "allow_unreleased, release_version, current_only, output_count",
        [
            (True, 108.0, False, 1),  # Released/Unreleased has no effect on released genome TRUE
            (False, 108.0, False, 1),  # Released/Unreleased has no effect on released genome FALSE
            (False, 110.1, False, 1),  # Wrong Release specified, not current release only
            (False, 108.0, False, 1),  # Right Release with current False
            (False, 108.0, True, 0),  # Right Release with only_current True
            (False, 110.1, True, 0),  # Wrong Release with only_current True
            (True, 110.1, True, 1)  # Unreleased
        ],
        indirect=['allow_unreleased']
    )
    def test_fetch_with_celegans_all_args(self, genome_conn, allow_unreleased, release_version,
                                          current_only, output_count):
        """ Version is not right """
        celegans = genome_conn.fetch_genomes(genome_uuid="a733550b-93e7-11ec-a39d-005056b38ce3",
                                             assembly_accession="GCA_000002985.3", assembly_name="WBcel235",
                                             biosample_id="SAMN04256190", taxonomy_id="6239", group="EnsemblMetazoa",
                                             site_name="Ensembl", release_type="partial",
                                             release_version=release_version, current_only=current_only)
        assert len(celegans) == output_count
        if output_count == 1:
            assert celegans[0].Organism.biosample_id == 'SAMN04256190'
            assert celegans[0].EnsemblRelease.version == 108.0

    def test_fetch_taxonomy_names(self, genome_conn):
        taxon_names = genome_conn.fetch_taxonomy_names(taxonomy_ids=[6239, 511145])
        assert taxon_names[511145]['scientific_name'] == 'Escherichia coli str. K-12 substr. MG1655'

    def test_fetch_taxonomy_ids(self, genome_conn):
        taxon_ids = genome_conn.fetch_taxonomy_ids(taxonomy_names='Caenorhabditis elegans')
        assert taxon_ids[0] == 6239

    def test_fetch_genomes(self, genome_conn):
        human = genome_conn.fetch_genomes(genome_uuid='9caa2cae-d1c8-4cfc-9ffd-2e13bc3e95b1')
        assert human[0].Organism.scientific_name == 'Homo sapiens'

    @pytest.mark.parametrize(
        "allow_unreleased, division, output_count",
        [
            (False, 'EnsemblVertebrates', 5),  # Released genomes for Vertebrates
            (True, 'EnsemblVertebrates', 14),  # Unreleased genomes for Vertebrates
        ],
        indirect=['allow_unreleased']
    )
    def test_fetch_genomes_by_group_division(self, genome_conn, allow_unreleased, division, output_count):
        division_members = genome_conn.fetch_genomes(group=division)
        assert len(division_members) == output_count

    @pytest.mark.parametrize(
        "allow_unreleased, output_count",
        [(True, 1), (False, 0)],
        indirect=['allow_unreleased']
    )
    def test_fetch_genomes_by_genome_uuid(self, genome_conn, allow_unreleased, output_count):
        genomes = genome_conn.fetch_genomes_by_genome_uuid('a73357ab-93e7-11ec-a39d-005056b38ce3')
        assert len(genomes) == output_count
        if output_count:
            assert genomes[0].Organism.scientific_name == 'Triticum aestivum'

    @pytest.mark.parametrize(
        "allow_unreleased, output_count",
        [(True, 1), (False, 0)],
        indirect=['allow_unreleased']
    )
    def test_fetch_genome_by_ensembl_and_assembly_name(self, genome_conn, allow_unreleased, output_count):
        genomes = genome_conn.fetch_genomes(assembly_name='R64-1-1', biosample_id='SAMEA3184125')
        if output_count:
            assert genomes[0].Organism.scientific_name == 'Saccharomyces cerevisiae S288c'

    @pytest.mark.parametrize(
        "allow_unreleased, output_count",
        [(True, 1), (False, 0)],
        indirect=['allow_unreleased']
    )
    def test_fetch_genomes_by_assembly_accession(self, genome_conn, allow_unreleased, output_count):
        genomes = genome_conn.fetch_genomes_by_assembly_accession('GCA_000005845.2')
        if output_count:
            assert genomes[0].Organism.scientific_name == 'Escherichia coli str. K-12 substr. MG1655 str. K12'

    def test_fetch_genomes_by_assembly_sequence_accession(self, genome_conn):
        sequences = genome_conn.fetch_sequences(
            genome_uuid='2020e8d5-4d87-47af-be78-0b15e48970a7',
            assembly_accession='GCA_018469415.1',
            assembly_sequence_accession='JAGYYT010000001.1'
        )
        assert sequences[0].AssemblySequence.name == 'JAGYYT010000001.1'
        assert sequences[0].AssemblySequence.sequence_location == 'SO:0000738'

    def test_fetch_genomes_by_assembly_sequence_accession_empty(self, genome_conn):
        sequences = genome_conn.fetch_sequences(
            genome_uuid='s0m3-r4nd0m-g3n3-uu1d-v4lu3',
            assembly_accession='GCA_000001405.14',
            assembly_sequence_accession='11'
        )
        assert len(sequences) == 0

    @pytest.mark.parametrize(
        "allow_unreleased, output_count",
        [(True, 1), (False, 0)],
        indirect=['allow_unreleased']
    )
    def test_fetch_genomes_by_ensembl_name(self, genome_conn, allow_unreleased, output_count):
        genomes = genome_conn.fetch_genomes_by_ensembl_name('SAMN00102897')
        assert len(genomes) == output_count
        if output_count:
            assert genomes[0].Organism.scientific_name == 'Plasmodium falciparum 3D7'

    @pytest.mark.parametrize(
        "allow_unreleased, output_count",
        [(True, 1), (False, 0)],
        indirect=['allow_unreleased']
    )
    def test_fetch_genomes_by_taxonomy_id(self, genome_conn, allow_unreleased, output_count):
        genomes = genome_conn.fetch_genomes_by_taxonomy_id(36329)
        assert len(genomes) == output_count
        if output_count:
            assert genomes[0].Organism.scientific_name == 'Plasmodium falciparum 3D7'

    @pytest.mark.parametrize(
        "allow_unreleased, output_count",
        [(True, 1), (False, 0)],
        indirect=['allow_unreleased']
    )
    def test_fetch_genomes_by_scientific_name(self, genome_conn, allow_unreleased, output_count):
        genomes = genome_conn.fetch_genomes_by_scientific_name(scientific_name='Plasmodium falciparum 3D7')
        assert len(genomes) == output_count
        if output_count:
            assert genomes[0].Organism.common_name == 'Malaria parasite'

    def test_fetch_sequences(self, genome_conn):
        test = genome_conn.fetch_sequences(assembly_uuid='9d6b239c-46dd-4c79-bc29-1089f348d31d')
        # this test is going to drive me nuts
        # Locally and on GitLab CI/CD: AssemblySequence.accession == 'CHR_HG107_PATCH'
        # in Travis, its: AssemblySequence.accession == 'KI270757.1'
        # to please bothI'm using 'sequence_location' for now
        assert test[0].AssemblySequence.sequence_location == 'SO:0000738'

    @pytest.mark.parametrize(
        "genome_uuid, assembly_accession, chromosomal_only, expected_output",
        [
            # Chromosomal and non-chromosomal
            ("3704ceb1-948d-11ec-a39d-005056b38ce3", "GCA_000001405.14", False, 0),
            # Chromosomal only
            ("a7335667-93e7-11ec-a39d-005056b38ce3", "GCA_000001405.29", True, 1),
        ]
    )
    def test_fetch_sequences_chromosomal(self, genome_conn, genome_uuid, assembly_accession, chromosomal_only,
                                         expected_output):
        sequences = genome_conn.fetch_sequences(
            genome_uuid=genome_uuid,
            assembly_accession=assembly_accession,
            chromosomal_only=chromosomal_only
        )
        logger.debug(f"Retrieved {sequences[0]}")
        assert sequences[-1].AssemblySequence.chromosomal == expected_output

    @pytest.mark.parametrize(
        "genome_uuid, assembly_sequence_name, chromosomal_only, expected_output",
        [
            ("a7335667-93e7-11ec-a39d-005056b38ce3", "MT", False, "J01415.2"),
            ("a7335667-93e7-11ec-a39d-005056b38ce3", "LRG_778", False, "LRG_778"),
            ("a7335667-93e7-11ec-a39d-005056b38ce3", "LRG_778", True, None),
            ("some-random-genome-uuid", "LRG_778", False, None),
            ("a7335667-93e7-11ec-a39d-005056b38ce3", "fake_assembly_name", False, None),
            ("some-random-genome-uuid", "fake_assembly_name", False, None),
        ]
    )
    def test_fetch_sequences_by_assembly_seq_name(self, genome_conn, genome_uuid, assembly_sequence_name,
                                                  chromosomal_only, expected_output):
        sequences = genome_conn.fetch_sequences(
            genome_uuid=genome_uuid,
            assembly_sequence_name=assembly_sequence_name,
            chromosomal_only=chromosomal_only
        )
        for result in sequences:
            assert result.AssemblySequence.accession == expected_output

    @pytest.mark.parametrize(
        "genome_uuid, dataset_uuid, allow_unreleased, unreleased_only, expected_dataset_uuid, expected_count",
        [
            # nothing specified + allow_unreleased -> fetches everything
            (None, None, True, False, "786344d1-a71f-4bab-aa37-6ee315ed60a4", 86),
            # specifying genome_uuid
            ("a73357ab-93e7-11ec-a39d-005056b38ce3", None, False, False, "999315f6-6d25-481f-a017-297f7e1490c8", 5),
            # specifying dataset_uuid
            (None, "949defef-c4d2-4ab1-8a73-f41d2b3c7719", False, False, "949defef-c4d2-4ab1-8a73-f41d2b3c7719", 1),
            # fetch unreleased datasets only
            (None, None, False, True, "45aec801-4fe7-4ac2-9afa-19aea2a8409e", 44),
            (None, None, True, True, "6f8bd121-0345-4b77-9dc1-d567ac13447d", 9),
        ],
        indirect=['allow_unreleased']
    )
    def test_fetch_genome_dataset_all(
            self, genome_conn, genome_uuid,
            dataset_uuid, allow_unreleased,
            unreleased_only, expected_dataset_uuid,
            expected_count):
        genome_datasets = genome_conn.fetch_genome_datasets(genome_uuid=genome_uuid, unreleased_only=unreleased_only,
                                                            dataset_uuid=dataset_uuid, dataset_type_name="all")
        assert genome_datasets[0].Dataset.dataset_uuid == expected_dataset_uuid
        assert len(genome_datasets) == expected_count

    @pytest.mark.parametrize(
        "allow_unreleased, organism_uuid, expected_count",
        [
            # homo_sapiens_37
            (False, "1d336185-affe-4a91-85bb-04ebd73cbb56", 11),
            (True, "1d336185-affe-4a91-85bb-04ebd73cbb56", 13),
            # e-coli
            (False, "1e579f8d-3880-424e-9b4f-190eb69280d9", 3),
            (True, "1e579f8d-3880-424e-9b4f-190eb69280d9", 4),
            # non-existing organism
            (False, "organism-yet-to-be-discovered", 0),
        ],
        indirect=['allow_unreleased']
    )
    def test_fetch_genome_dataset_by_organism_uuid(self, genome_conn, allow_unreleased, organism_uuid, expected_count):
        genomes = genome_conn.fetch_genome_datasets(organism_uuid=organism_uuid, dataset_type_name="all")
        assert len(genomes) == expected_count

    @pytest.mark.parametrize(
        "production_name, assembly_name, use_default_assembly, expected_output",
        [
            ("homo_sapiens_37", "GRCh37.p13", False, "3704ceb1-948d-11ec-a39d-005056b38ce3"),
            ("homo_sapiens_37", "GRCh37", True, "3704ceb1-948d-11ec-a39d-005056b38ce3"),
        ]
    )
    def test_fetch_genome_uuid(self, genome_conn, production_name, assembly_name, use_default_assembly,
                               expected_output):
        genomes = genome_conn.fetch_genomes(assembly_name=assembly_name, use_default_assembly=use_default_assembly,
                                            production_name=production_name, current_only=False)
        assert len(genomes) == 1
        assert genomes[0].Genome.genome_uuid == expected_output

    @pytest.mark.parametrize(
        "production_name, assembly_name, use_default_assembly, expected_output",
        [
            ("homo_sapiens", "GRCh38.p14", False, "a7335667-93e7-11ec-a39d-005056b38ce3"),
            ("homo_sapiens", "GRCh38", True, "a7335667-93e7-11ec-a39d-005056b38ce3"),
        ]
    )
    def test_fetch_genome_uuid_is_current(self, genome_conn, production_name, assembly_name, use_default_assembly,
                                          expected_output):
        genomes = genome_conn.fetch_genomes(assembly_name=assembly_name, use_default_assembly=use_default_assembly,
                                            production_name=production_name)
        assert len(genomes) == 1
        assert genomes[0].Genome.genome_uuid == expected_output

    @pytest.mark.parametrize(
        "production_name, assembly_name, use_default_assembly",
        [
            ("homo_sapiens", "GRCh37", False),
            ("homo_sapiens", "GRCh37.p13", True),
        ]
    )
    def test_fetch_genome_uuid_empty(self, genome_conn, production_name, assembly_name, use_default_assembly):
        genomes = genome_conn.fetch_genomes(assembly_name=assembly_name, use_default_assembly=use_default_assembly,
                                            production_name=production_name)
        assert len(genomes) == 0

    @pytest.mark.parametrize(
        "group_code, expected_assemblies_count, allow_unreleased",
        [
            (None, 5, False),  # Default is 'Popular' group
            ('vertebrates', 5, False),  # Returns only vertebrates
            ('EnsemblVertebrates', 14, True),  # Returns only vertebrates with unreleased
        ],
        indirect=['allow_unreleased']
    )
    def test_fetch_organisms_group_counts(self, genome_conn, group_code, expected_assemblies_count, allow_unreleased):
        genomes = genome_conn.fetch_organisms_group_counts()
        # First result should be Human with priority set
        assert genomes[0].common_name == 'Human'
        # We should have three assemblies associated with Human (Two for grch37.38 organism + one t2t)
        assert genomes[0].count == expected_assemblies_count

    @pytest.mark.parametrize(
        "taxon_id, version, expected_assemblies_count, allow_unreleased",
        [
            (9606, None, 5, False),  # Human 5 genomes are released
            (9606, 110.2, 5, False),  # Human specify release doesn't change is not ALLOWED_UNRELEASED
            (9606, 110.2, 14, True),  # Human specify release changes with ALLOWED_UNRELEASED
            (562, None, 1, True),  # E.Coli Only return one since no alternative Assembly
        ],
        indirect=['allow_unreleased']
    )
    def test_fetch_related_assemblies_count(self, genome_conn, taxon_id, version,
                                            expected_assemblies_count, allow_unreleased):
        genomes = genome_conn.fetch_assemblies_count(taxon_id)
        # We should have three assemblies associated with Human (Two for grch37.38 organism + one t2t)
        assert genomes == expected_assemblies_count

    @pytest.mark.parametrize(
        "allow_unreleased, group, version, output_count",
        [
            # fetches everything from every release
            (True, None, None, 19),
            # fetches Metazoa only, no unreleased
            (False, 'EnsemblMetazoa', None, 1),
            # fetches Vertebrates only, no unreleased
            (False, 'vertebrates', None, 5),
            # fetches Vertebrates only, with unreleased
            (True, 'vertebrates', None, 14),
            (True, 'vertebrates', 110.2, 9),  # up to 110.2
        ],
        indirect=['allow_unreleased']
    )
    def test_fetch_genomes_info(self, genome_conn, allow_unreleased, group, version, output_count):
        genomes = genome_conn.fetch_genomes_info(group=group, release_version=version)
        assert len(genomes) == output_count
