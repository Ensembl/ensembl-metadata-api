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
from ensembl.utils.database import UnitTestDB

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("test_dbs", [[{"src": Path(__file__).parent / "databases/ensembl_genome_metadata"},
                                        {"src": Path(__file__).parent / "databases/ncbi_taxonomy"}]],
                         indirect=True)
class TestGRPCGenomeAdaptor:
    dbc: UnitTestDB = None

    @pytest.mark.parametrize(
        "allow_unreleased, unreleased_only, release_version, current_only, output_count",
        [
            (True, False, None, False, 30),  # Allow Unreleased
            (False, False, 110.1, False, 10),  # Do not allow unreleased - fetch all from previous releases
            (False, True, 112.0, False, 10),  # unreleased_only has no effect when ALLOW_UNRELEASED is False
            (False, False, 110.3, True, 10)  # Only the ones from current release
        ],
        indirect=['allow_unreleased']
    )
    def test_fetch_genomes_release(self, genome_conn, allow_unreleased, unreleased_only, release_version,
                                   current_only, output_count):
        """
        Test fetching all released genomes
        - Unreleased_only: should have no effect on results
        - Current_only: Should filter against only release 110.1 (current)
        """
        test = genome_conn.fetch_genomes(unreleased_only=unreleased_only, current_only=current_only,
                                         release_version=release_version)
        assert len(test) == output_count

    @pytest.mark.parametrize(
        "allow_unreleased, release_version, current_only, output_count",
        [
            (True, 108.0, False, 1),  # Released/Unreleased has no effect on released genome TRUE
            (False, 108.0, False, 1),  # Released/Unreleased has no effect on released genome FALSE
            (False, 110.1, False, 1),  # Wrong Release specified, not current release only
            (False, 108.0, False, 1),  # Right Release with current False
            (False, 108.0, True, 1),  # Right Release with only_current True
            # wrong release version with is current true :##########################################
            # checks given release is current or any release less than given release
            #Todo: genome_select = genome_select.filter(EnsemblRelease.version <= release_version)
            (False, 110.1, True, 1),  # Wrong Release with only_current True
            #########################################################################################
            (True, 110.2, False, 2),  # Unreleased should return 2
            (False, 110.2, True, 1)  # Unreleased should return 2
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
            (True, 'EnsemblVertebrates', 20),  # Unreleased genomes for Vertebrates
        ],
        indirect=['allow_unreleased']
    )
    def test_fetch_genomes_by_group_division(self, genome_conn, allow_unreleased, division, output_count):
        division_members = genome_conn.fetch_genomes(group=division)
        assert len(division_members) == output_count

    @pytest.mark.parametrize(
        "allow_unreleased, current_only, output_count",
        [(True, False, 2), (True, True, 1), (False, False, 1)],
        indirect=['allow_unreleased']
    )
    def test_fetch_genomes_by_genome_uuid(self, genome_conn, allow_unreleased, current_only, output_count):
        genomes = genome_conn.fetch_genomes_by_genome_uuid('a73357ab-93e7-11ec-a39d-005056b38ce3',
                                                           current_only=current_only)
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
        [(True, 2), (False, 2)],
        indirect=['allow_unreleased']
    )
    def test_fetch_genomes_by_ensembl_name(self, genome_conn, allow_unreleased, output_count):
        genomes = genome_conn.fetch_genomes_by_ensembl_name('SAMN17861670')
        assert len(genomes) == output_count
        if output_count:
            assert genomes[0].Organism.scientific_name == 'Homo sapiens'

    @pytest.mark.parametrize(
        "allow_unreleased, taxon_id, output_count",
        [
            (True, 559292, 2),
            (False, 559292, 1)
        ], indirect=['allow_unreleased']
    )
    def test_fetch_genomes_by_taxonomy_id(self, genome_conn, allow_unreleased, taxon_id, output_count):
        genomes = genome_conn.fetch_genomes_by_taxonomy_id(taxonomy_id=taxon_id, current_only=False)
        assert len(genomes) == output_count

    @pytest.mark.parametrize(
        "allow_unreleased, output_count",
        # WAS [(True, 2), (False, 0)],
        # TODO check consistency Test DB holds 19 genomes of HUMAN (Released)
        #  and 5 Attached to unreleased
        [(True, 11), (False, 5)],
        indirect=['allow_unreleased']
    )
    def test_fetch_genomes_by_scientific_name(self, genome_conn, allow_unreleased, output_count):
        genomes = genome_conn.fetch_genomes_by_scientific_name(scientific_name='Homo sapiens')
        assert len(genomes) == output_count
        if output_count:
            assert genomes[0].Organism.common_name == 'Human'

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
            (None, None, True, False, "6c1896f9-10dd-423e-a1ff-db8b5815cb66", 30),
            (None, None, False, False, "6c1896f9-10dd-423e-a1ff-db8b5815cb66", 10),
            ("8364a820-5485-42d7-a648-1a5eeb858319", None, True, False, "3c67123a-e9e1-41ef-9014-2aadc8acf12a", 1),
            # specifying genome_uuid -- Triticum aestivum (SAMEA4791365)
            ("a73357ab-93e7-11ec-a39d-005056b38ce3", None, True, False, "999315f6-6d25-481f-a017-297f7e1490c8", 2),
            ("a73357ab-93e7-11ec-a39d-005056b38ce3", None, True, True, "999315f6-6d25-481f-a017-297f7e1490c8", 1),
            # fetch unreleased datasets only
            (None, None, False, True, "6c1896f9-10dd-423e-a1ff-db8b5815cb66", 10),
            (None, 'f93d21ca-9a24-4c31-ae11-b0f8d3deab6d', True, True, "3c67123a-e9e1-41ef-9014-2aadc8acf12a", 1),
        ],
        indirect=['allow_unreleased']
    )
    def test_fetch_genome_dataset_all(
            self, genome_conn, genome_uuid,
            dataset_uuid, allow_unreleased,
            unreleased_only, expected_dataset_uuid,
            expected_count):
        genome_datasets = genome_conn.fetch_genome_datasets(genome_uuid=genome_uuid,
                                                            dataset_uuid=dataset_uuid,
                                                            unreleased_only=unreleased_only,
                                                            dataset_type_name="all")
        logger.debug(f"Genome Datasets retrieved {[gd.genome.genome_uuid for gd in genome_datasets]}")
        assert len(genome_datasets) > 0
        logger.debug(f"First element {genome_datasets[0]}")
        assert len(genome_datasets[0].datasets) >= 2  # At least genebuild + assembly
        assert genome_datasets[0].datasets[0].dataset.dataset_uuid == expected_dataset_uuid
        assert len(genome_datasets) == expected_count

    @pytest.mark.parametrize(
        "allow_unreleased, organism_uuid, expected_count",
        [
            # homo_sapiens_37
            (False, "1d336185-affe-4a91-85bb-04ebd73cbb56", 2),
            (True, "1d336185-affe-4a91-85bb-04ebd73cbb56", 4),
            # Homo sapiens Gambian in Western Division
            (False, "18bd7042-d861-4a10-b5d0-68c8bccfc87e", 2),
            (True, "18bd7042-d861-4a10-b5d0-68c8bccfc87e", 5),
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
            ('EnsemblVertebrates', 11, True),  # Returns only vertebrates with unreleased
            # Update this test once integrated releases are added to tests
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
            (9606, 110.2, 20, True),  # Human specify release changes with ALLOWED_UNRELEASED
            (562, None, 2, True),  # E.Coli return 2 since two genomes released
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
            (True, None, None, 20),
            # fetches Metazoa only, no unreleased
            (False, 'EnsemblMetazoa', None, 1),
            # fetches Vertebrates only, no unreleased
            (False, 'vertebrates', None, 5),
            # fetches Vertebrates only, with unreleased
            (True, 'vertebrates', None, 15),
            # (True, 'vertebrates', 110.2, 9),  # up to 110.2
            # Broke this one. Not sure if it is necessary.
        ],
        indirect=['allow_unreleased']
    )
    def test_fetch_genomes_info(self, genome_conn, allow_unreleased, group, version, output_count):
        genomes = genome_conn.fetch_genomes_info(group=group, release_version=version)
        assert len(genomes) == output_count
