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

import pytest
import pkg_resources
from pathlib import Path

from ensembl.database import UnitTestDB
from ensembl.production.metadata.grpc.adaptors.genome import GenomeAdaptor
from ensembl.production.metadata.grpc.adaptors.release import ReleaseAdaptor
import logging

sample_path = Path(__file__).parent.parent / "ensembl" / "production" / "metadata" / "api" / "sample"

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("multi_dbs", [[{"src": sample_path / "ensembl_genome_metadata"},
                                        {"src": sample_path / "ncbi_taxonomy"}]],
                         indirect=True)
class TestMetadataDB:
    dbc = None  # type: UnitTestDB

    def test_load_database(self, multi_dbs):
        db_test = ReleaseAdaptor(multi_dbs['ensembl_genome_metadata'].dbc.url)
        assert db_test, "DB should not be empty"

    @pytest.mark.parametrize(
        "allow_unreleased, unreleased_only, current_only, output_count",
        [
            # fetches everything (141 released + 100 unreleased)
            (True, False, True, 241),
            # fetches all released genomes (with current_only=0)
            (False, False, False, 100),
            # fetches released genomes with current_only=1 (default)
            (False, False, True, 100),
            # fetches all unreleased genomes
            (False, True, True, 141),
        ]
    )
    def test_fetch_all_genomes(self, multi_dbs, allow_unreleased, unreleased_only, current_only, output_count):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_genome_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genomes(
            allow_unreleased=allow_unreleased,
            unreleased_only=unreleased_only,
            current_only=current_only
        )
        assert len(test) == output_count

    def test_fetch_with_all_args_no_conflict(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_genome_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genomes(
            genome_uuid="a733550b-93e7-11ec-a39d-005056b38ce3",
            assembly_accession="GCA_000002985.3",
            assembly_name="WBcel235",
            ensembl_name="caenorhabditis_elegans",
            taxonomy_id="6239",
            group="EnsemblMetazoa",
            allow_unreleased=False,
            site_name="Ensembl",
            release_type="integrated",
            release_version="108.0",
            current_only=True
        )
        assert len(test) == 0

    def test_fetch_with_all_args_conflict(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_genome_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genomes(
            genome_uuid="a733550b-93e7-11ec-a39d-005056b38ce3",
            assembly_accession="GCA_000002985.3",
            assembly_name="WBcel235",
            ensembl_name="caenorhabditis_elegans",
            taxonomy_id="9606",  # Conflicting taxonomy_id
            group="EnsemblBacteria",  # Conflicting group
            allow_unreleased=False,
            site_name="Ensembl",
            release_type="integrated",
            release_version="108.0",
            current_only=True
        )
        assert len(test) == 0

    def test_fetch_releases(self, multi_dbs):
        conn = ReleaseAdaptor(multi_dbs['ensembl_genome_metadata'].dbc.url)
        test = conn.fetch_releases(release_id=2)
        # test the one to many connection
        assert test[0].EnsemblSite.name == 'Ensembl'
        assert test[0].EnsemblSite.label == 'MVP Ensembl'
        # test the direct access.
        assert test[0].EnsemblRelease.label == 'Scaling Phase 1'

    # currently only have one release, so the testing is not comprehensive
    def test_fetch_releases_for_genome(self, multi_dbs):
        conn = ReleaseAdaptor(multi_dbs['ensembl_genome_metadata'].dbc.url)
        test = conn.fetch_releases_for_genome('ae794660-8751-41cc-8883-b2fcdc7a74e8')
        assert test[0].EnsemblSite.name == 'Ensembl'

    def test_fetch_releases_for_dataset(self, multi_dbs):
        conn = ReleaseAdaptor(multi_dbs['ensembl_genome_metadata'].dbc.url)
        test = conn.fetch_releases_for_dataset('3d653b2d-aa8d-4f7e-8f92-55f57c7cac3a')
        assert test[0].EnsemblSite.name == 'Ensembl'
        assert test[0].EnsemblRelease.label == 'beta-1'

    def test_fetch_taxonomy_names(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_genome_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_taxonomy_names(taxonomy_ids=[6239, 511145])
        assert test[511145]['scientific_name'] == 'Escherichia coli str. K-12 substr. MG1655'

    def test_fetch_taxonomy_ids(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_genome_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_taxonomy_ids(taxonomy_names='Caenorhabditis elegans')
        assert test[0] == 6239

    def test_fetch_genomes(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_genome_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genomes(genome_uuid='a7335667-93e7-11ec-a39d-005056b38ce3')
        assert test[0].Organism.scientific_name == 'Homo sapiens'

    # def test_fetch_genomes_by_group_division(self, multi_dbs):
    #     conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_genome_metadata'].dbc.url,
    #                          taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
    #     division_filter = 'EnsemblVertebrates'
    #     test = conn.fetch_genomes(group=division_filter)
    #     assert len(test) == 1
    #        Other PR will likely change this drastically, so the effort is not really necessary. Their are 7 groups.
    #        assert division_filter in division_results

    def test_fetch_genomes_by_genome_uuid(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_genome_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genomes_by_genome_uuid('b00f5b0a-b434-4949-9c05-140826c96cd4')
        assert test[0].Organism.scientific_name == 'Oryzias latipes'

    def test_fetch_genome_by_ensembl_and_assembly_name(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_genome_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genomes(assembly_name='NOD_ShiLtJ_v1', ensembl_name='SAMN04489827')
        assert test[0].Organism.scientific_name == 'Mus musculus'

    def test_fetch_genomes_by_assembly_accession(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_genome_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genomes_by_assembly_accession('GCA_000005845.2')
        assert test[0].Organism.scientific_name == 'Escherichia coli str. K-12 substr. MG1655 str. K12'

    def test_fetch_genomes_by_assembly_sequence_accession(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_genome_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_sequences(
            genome_uuid='a7335667-93e7-11ec-a39d-005056b38ce3',
            assembly_accession='GCA_000001405.29',
            assembly_sequence_accession='HG2280_PATCH'
        )
        assert test[0].AssemblySequence.name == 'HG2280_PATCH'

    def test_fetch_genomes_by_assembly_sequence_accession_empty(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_genome_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_sequences(
            genome_uuid='s0m3-r4nd0m-g3n3-uu1d-v4lu3',
            assembly_accession='GCA_000001405.14',
            assembly_sequence_accession='11'
        )
        assert len(test) == 0

    def test_fetch_genomes_by_ensembl_name(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_genome_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genomes_by_ensembl_name('SAMN04489826')
        assert test[0].Organism.scientific_name == 'Mus musculus'

    def test_fetch_genomes_by_taxonomy_id(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_genome_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genomes_by_taxonomy_id(10090)
        assert test[0].Organism.scientific_name == 'Mus musculus'

    def test_fetch_genomes_by_scientific_name(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_genome_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genomes_by_scientific_name(
            scientific_name='Oryzias latipes',
            site_name='Ensembl'
        )
        assert test[0].Organism.common_name == 'Japanese medaka'

    def test_fetch_sequences(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_genome_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_sequences(assembly_uuid='9d6b239c-46dd-4c79-bc29-1089f348d31d')
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
    def test_fetch_sequences_chromosomal(self, multi_dbs, genome_uuid, assembly_accession, chromosomal_only,
                                         expected_output):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_genome_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_sequences(
            genome_uuid=genome_uuid,
            assembly_accession=assembly_accession,
            chromosomal_only=chromosomal_only
        )
        logger.debug(f"Retrieved {test[0]}")
        assert test[-1].AssemblySequence.chromosomal == expected_output

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
    def test_fetch_sequences_by_assembly_seq_name(self, multi_dbs, genome_uuid, assembly_sequence_name,
                                                  chromosomal_only, expected_output):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_genome_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_sequences(
            genome_uuid=genome_uuid,
            assembly_sequence_name=assembly_sequence_name,
            chromosomal_only=chromosomal_only
        )
        for result in test:
            assert result.AssemblySequence.accession == expected_output

    @pytest.mark.parametrize(
        "genome_uuid, dataset_uuid, allow_unreleased, unreleased_only, expected_dataset_uuid, expected_count",
        [
            # nothing specified + allow_unreleased -> fetches everything
            (None, None, True, False, "0fdb2bd2-db62-455c-abe9-794fc99b35d2", 888),
            # specifying genome_uuid
            ("a73357ab-93e7-11ec-a39d-005056b38ce3", None, False, False, "287a5483-55a4-46e6-a58b-a84ba0ddacd6", 5),
            # specifying dataset_uuid
            (None, "3674ac83-c8ad-453f-a143-d02304d4aa36", False, False, "3674ac83-c8ad-453f-a143-d02304d4aa36", 1),
            # fetch unreleased datasets only
            (None, None, False, True, "0fdb2bd2-db62-455c-abe9-794fc99b35d2", 521),
        ]
    )
    def test_fetch_genome_dataset_all(
            self, multi_dbs, genome_uuid,
            dataset_uuid, allow_unreleased,
            unreleased_only, expected_dataset_uuid,
            expected_count):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_genome_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genome_datasets(
            genome_uuid=genome_uuid,
            dataset_uuid=dataset_uuid,
            unreleased_only=unreleased_only,
            allow_unreleased=allow_unreleased,
            # fetch all datasets (default: dataset_type_name="assembly")
            dataset_type_name="all"
        )
        assert test[0].Dataset.dataset_uuid == expected_dataset_uuid
        assert len(test) == expected_count

    @pytest.mark.parametrize(
        "organism_uuid, expected_count",
        [
            # homo_sapiens_37
            ("1d336185-affe-4a91-85bb-04ebd73cbb56", 11),
            # e-coli
            ("1e579f8d-3880-424e-9b4f-190eb69280d9", 3),
            # non-existing organism
            ("organism-yet-to-be-discovered", 0),
        ]
    )
    def test_fetch_genome_dataset_by_organism_uuid(self, multi_dbs, organism_uuid, expected_count):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_genome_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genome_datasets(
            organism_uuid=organism_uuid,
            # fetch all datasets (default: dataset_type_name="assembly")
            dataset_type_name="all"
        )
        assert len(test) == expected_count

    @pytest.mark.parametrize(
        "production_name, assembly_name, use_default_assembly, expected_output",
        [
            ("homo_sapiens_37", "GRCh37.p13", False, "3704ceb1-948d-11ec-a39d-005056b38ce3"),
            ("homo_sapiens_37", "GRCh37", True, "3704ceb1-948d-11ec-a39d-005056b38ce3"),
        ]
    )
    def test_fetch_genome_uuid(self, multi_dbs, production_name, assembly_name, use_default_assembly, expected_output):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_genome_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genomes(
            production_name=production_name,
            assembly_name=assembly_name,
            use_default_assembly=use_default_assembly,
            allow_unreleased=True,
            current_only=False
        )
        assert len(test) == 1
        assert test[0].Genome.genome_uuid == expected_output

    @pytest.mark.parametrize(
        "production_name, assembly_name, use_default_assembly, expected_output",
        [
            ("homo_sapiens", "GRCh38.p14", False, "a7335667-93e7-11ec-a39d-005056b38ce3"),
            ("homo_sapiens", "GRCh38", True, "a7335667-93e7-11ec-a39d-005056b38ce3"),
        ]
    )
    def test_fetch_genome_uuid_is_current(self, multi_dbs, production_name, assembly_name, use_default_assembly,
                                          expected_output):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_genome_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genomes(
            production_name=production_name,
            assembly_name=assembly_name,
            use_default_assembly=use_default_assembly,
            allow_unreleased=True
        )
        assert len(test) == 1
        assert test[0].Genome.genome_uuid == expected_output

    @pytest.mark.parametrize(
        "production_name, assembly_name, use_default_assembly",
        [
            ("homo_sapiens", "GRCh37", False),
            ("homo_sapiens", "GRCh37.p13", True),
        ]
    )
    def test_fetch_genome_uuid_empty(self, multi_dbs, production_name, assembly_name, use_default_assembly):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_genome_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genomes(
            production_name=production_name,
            assembly_name=assembly_name,
            use_default_assembly=use_default_assembly
        )
        assert len(test) == 0

    @pytest.mark.parametrize(
        "species_taxonomy_id, expected_organism, expected_assemblies_count",
        [
            # fetch everything
            (None, "human", 99)
        ]
    )
    def test_fetch_organisms_group_counts(self, multi_dbs, species_taxonomy_id, expected_organism,
                                          expected_assemblies_count):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_genome_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_organisms_group_counts()
        # When fetching everything:
        # First result should be Human
        assert test[0][1] == expected_organism
        # We should have three assemblies associated with Human (Two for grch37.38 organism + one t2t)
        assert test[0][4] == expected_assemblies_count

        # for data in test[1:]:
        #     # All others have only one genome in test DB
        #     assert data[4] == 1

    @pytest.mark.parametrize(
        "organism_uuid, expected_assemblies_count",
        [
            # Human
            ('1d336185-affe-4a91-85bb-04ebd73cbb56', 99),
            # Triticum aestivum
            ('8dbb0666-8a06-46a7-80eb-e63055ae93d2', 1),
        ]
    )
    def test_fetch_related_assemblies_count(self, multi_dbs, organism_uuid, expected_assemblies_count):
        conn = GenomeAdaptor(
            metadata_uri=multi_dbs['ensembl_genome_metadata'].dbc.url,
            taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url
        )

        test = conn.fetch_related_assemblies_count(organism_uuid=organism_uuid)
        # We should have three assemblies associated with Human (Two for grch37.38 organism + one t2t)
        assert test == expected_assemblies_count

    @pytest.mark.parametrize(
        "allow_unreleased, output_count, expected_genome_uuid",
        [
            # fetches everything
            (True, 241, "041b8327-222c-4bfe-ae27-1d93c6025428"),
            # fetches released datasets and genomes with current_only=1 (default)
            (False, 100, "08b99cae-d007-4284-b20b-9f222827edb6"),
        ]
    )
    def test_fetch_genomes_info(self, multi_dbs, allow_unreleased, output_count, expected_genome_uuid):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_genome_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genomes_info(
            allow_unreleased_genomes=allow_unreleased,
            allow_unreleased_datasets=allow_unreleased,
            group_type=['division', 'internal']
        )
        output_to_list = list(test)
        assert len(output_to_list) == output_count
        assert output_to_list[0][0]['genome'].genome_uuid == expected_genome_uuid
