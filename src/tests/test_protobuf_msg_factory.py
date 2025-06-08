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
"""
Unit tests for protobuf_msg_factory.py
"""
import json
from pathlib import Path

import pytest
from ensembl.utils.database import UnitTestDB
from google.protobuf import json_format

import ensembl.production.metadata.grpc.protobuf_msg_factory as msg_factory


@pytest.mark.parametrize("test_dbs", [[{"src": Path(__file__).parent / "databases/ensembl_genome_metadata"},
                                        {"src": Path(__file__).parent / "databases/ncbi_taxonomy"}]],
                         indirect=True)
class TestClass:
    dbc = None  # type: UnitTestDB

    @pytest.mark.parametrize(
        "allow_unreleased, genome_uuid, ds_type_name, expected_genome_count, expected_ds_count, expected_assembly_count, ensembl_name",
        [
            (False, 'a7335667-93e7-11ec-a39d-005056b38ce3', 'assembly', 1, 1, 5, 'SAMN12121739'),
            (False, 'a7335667-93e7-11ec-a39d-005056b38ce3', 'all', 1, 6, 5, 'SAMN12121739'),
            (True, 'a7335667-93e7-11ec-a39d-005056b38ce3', 'assembly', 2, 1, 11, 'SAMN12121739'),
            (False, 'a7335667-93e7-11ec-a39d-005056b38ce3', 'homologies', 1, 1, 5, 'SAMN12121739'),
            (True, 'a7335667-93e7-11ec-a39d-005056b38ce3', 'homologies', 2, 2, 11, 'SAMN12121739'),
        ],
        indirect=['allow_unreleased']
    )
    def test_create_genome(self, genome_conn, allow_unreleased, genome_uuid, ds_type_name, expected_genome_count,
                           expected_ds_count, expected_assembly_count, ensembl_name):
        """Test service.create_genome function"""
        # FIXME all returned genome is now unique following method
        genome_input_data = genome_conn.fetch_genomes(genome_uuid=genome_uuid)
        # Make sure we are only getting one
        assert len(genome_input_data) == expected_genome_count

        attrib_input_data = genome_conn.fetch_genome_datasets(genome_uuid=genome_uuid, dataset_type_name=ds_type_name)
        # 11 attributes
        assert len(attrib_input_data[0].datasets) == expected_ds_count
        # assert len(attrib_input_data[0].datasets) == expected_ds_count

        related_assemblies_input_count = genome_conn.fetch_assemblies_count(
            genome_input_data[0].Organism.species_taxonomy_id)
        # There are three related assemblies
        assert related_assemblies_input_count == expected_assembly_count
        attributes = []
        for dataset in attrib_input_data[0].datasets:
            attributes.extend([ds for ds in dataset.attributes])
        output = json_format.MessageToJson(
            msg_factory.create_genome(
                data=genome_input_data[0],
                attributes=attributes,
                count=related_assemblies_input_count
            )

        )
        assert json.loads(output)['organism']['ensemblName'] == ensembl_name

    def test_create_assembly_info(self, genome_conn):
        input_data = genome_conn.fetch_sequences(assembly_uuid="fd7fea38-981a-4d73-a879-6f9daef86f08")
        expected_output = {
            'accession': 'GCA_000001405.29',
            'assemblyUuid': 'fd7fea38-981a-4d73-a879-6f9daef86f08',
            'chromosomal': 1,
            'length': '248956422',
            'level': 'chromosome',
            'md5': '2648ae1bacce4ec4b6cf337dcae37816',
            'name': 'GRCh38.p14',
            'sequenceLocation': 'SO:0000738',
            'sha512t24u': '2YnepKM7OkBoOrKmvHbGqguVfF9amCST'
        }

        output = json_format.MessageToJson(msg_factory.create_assembly_info(input_data[0]))
        assert json.loads(output) == expected_output

    def test_create_species(self, genome_conn):
        species_input_data = genome_conn.fetch_genomes(genome_uuid="a7335667-93e7-11ec-a39d-005056b38ce3")
        tax_id = species_input_data[0].Organism.taxonomy_id
        taxo_results = genome_conn.fetch_taxonomy_names(tax_id)
        expected_output = {
            "genbankCommonName": "human",
            "genomeUuid": "a7335667-93e7-11ec-a39d-005056b38ce3",
            "scientificName": "Homo sapiens",
            "scientificParlanceName": "Human",
            "taxonId": 9606
        }

        output = json_format.MessageToJson(msg_factory.create_species(species_input_data[0], taxo_results[tax_id]))
        assert json.loads(output) == expected_output

    def test_create_stats_by_organism_uuid(self, genome_conn):
        # ecoli
        organism_uuid = "1e579f8d-3880-424e-9b4f-190eb69280d9"
        input_data = genome_conn.fetch_genome_datasets(organism_uuid=organism_uuid, dataset_type_name="all")

        first_expected_stat = {
            'label': 'assembly.accession',
            'name': 'assembly.accession',
            'statisticType': 'string',
            'statisticValue': 'GCA_000005845.2'
        }
        output = json_format.MessageToJson(msg_factory.create_stats_by_genome_uuid(input_data)[0])
        assert json.loads(output)['genomeUuid'] == "a73351f7-93e7-11ec-a39d-005056b38ce3"
        # check the first stat info of the first genome_uuid
        # print(json.loads(output)['statistics'])
        assert json.loads(output)['statistics'][0] == first_expected_stat

    def test_create_top_level_statistics(self, genome_conn):
        # ecoli
        organism_uuid = "1e579f8d-3880-424e-9b4f-190eb69280d9"
        input_data = genome_conn.fetch_genome_datasets(organism_uuid=organism_uuid, dataset_type_name="all")

        first_expected_stat = {
            'label': 'assembly.accession',
            'name': 'assembly.accession',
            'statisticType': 'string',
            'statisticValue': 'GCA_000005845.2'
        }
        stats_by_genome_uuid = msg_factory.create_stats_by_genome_uuid(input_data)

        output = json_format.MessageToJson(
            msg_factory.create_top_level_statistics({
                'organism_uuid': organism_uuid,
                'stats_by_genome_uuid': stats_by_genome_uuid
            })
        )
        output_dict = json.loads(output)
        assert 'organismUuid' in output_dict.keys() and 'statsByGenomeUuid' in output_dict.keys()
        # These tests are pain in the back
        # TODO: find a way to improve this spaghetti
        assert output_dict["organismUuid"] == "1e579f8d-3880-424e-9b4f-190eb69280d9"
        assert output_dict['statsByGenomeUuid'][0]['genomeUuid'] == "a73351f7-93e7-11ec-a39d-005056b38ce3"
        assert output_dict['statsByGenomeUuid'][0]['statistics'][0] == first_expected_stat

    def test_create_genome_sequence(self, genome_conn):
        input_data = genome_conn.fetch_sequences(genome_uuid="a7335667-93e7-11ec-a39d-005056b38ce3")
        expected_output = {
            'accession': '1',
            'chromosomal': True,
            'length': '248956422',
            'name': '1',
            'sequenceLocation': 'SO:0000738'
        }
        output = json_format.MessageToJson(msg_factory.create_genome_sequence(input_data[0]))
        assert json.loads(output) == expected_output

    def test_create_assembly_region(self, genome_conn):
        input_data = genome_conn.fetch_sequences(
            genome_uuid="a7335667-93e7-11ec-a39d-005056b38ce3",
        )
        # TODO: Check why this is failing when name and chromosomal is provided
        expected_output = {
            'chromosomal': True,
            'length': '248956422',
            'md5': '2648ae1bacce4ec4b6cf337dcae37816',
            'name': '1',
            'rank': 1,
            'sha512t24u': '2YnepKM7OkBoOrKmvHbGqguVfF9amCST'
        }
        output = json_format.MessageToJson(msg_factory.create_assembly_region(input_data[0]))
        assert json.loads(output) == expected_output

    def test_create_genome_assembly_sequence_region(self, genome_conn):
        input_data = genome_conn.fetch_sequences(
            genome_uuid="a7335667-93e7-11ec-a39d-005056b38ce3",
            assembly_accession="GCA_000001405.29",
            assembly_sequence_accession="17"
        )
        expected_output = {
            'chromosomal': True,
            'length': '83257441',
            'md5': 'a8499ca51d6fb77332c2d242923994eb',
            'name': '17',
            'rank': 17,
            'sha512t24u': 'upqChCoU-Gtd_61IidCsln-r8cxUTFeP'
        }
        output = json_format.MessageToJson(msg_factory.create_assembly_region(input_data[0]))
        assert json.loads(output) == expected_output

    @pytest.mark.parametrize(
        "allow_unreleased, version, output",
        [
            (False, 108.0, {
                "releaseVersion": 108.0,
                "releaseDate": "2023-06-15",
                "releaseLabel": "First Beta",
                "releaseType": "partial",
                "isCurrent": False,
                "siteName": "Ensembl",
                "siteLabel": "MVP Ensembl",
                "siteUri": "https://beta.ensembl.org"
            }),
            (False, 110.1, {
                "releaseVersion": 110.1,
                "releaseDate": "2023-10-18",
                "releaseLabel": "MVP Beta-1",
                "releaseType": "partial",
                "isCurrent": True,
                "siteName": "Ensembl",
                "siteLabel": "MVP Ensembl",
                "siteUri": "https://beta.ensembl.org"
            }),
            (True, 110.3, {
                "releaseVersion": 110.3,
                "releaseDate": "Unreleased",
                "releaseLabel": "MVP Beta-3",
                "releaseType": "partial",
                "isCurrent": False,
                "siteName": "Ensembl",
                "siteLabel": "MVP Ensembl",
                "siteUri": "https://beta.ensembl.org"
            })
        ],
        indirect=['allow_unreleased']
    )
    def test_create_release(self, release_conn, allow_unreleased, version, output):
        input_data = release_conn.fetch_releases(release_version=version)
        actual = json_format.MessageToJson(msg_factory.create_release(input_data[-1]),
                                           always_print_fields_with_no_presence=True)

        assert json.loads(actual) == output

    @pytest.mark.parametrize(
        "allow_unreleased, expected_count",
        [
            (False, 5),
            (True, 11)  # Update this test once integrated releases are added to tests
        ],
        indirect=['allow_unreleased']
    )
    def test_create_organisms_group_count(self, genome_conn, expected_count, allow_unreleased):
        input_data = genome_conn.fetch_organisms_group_counts()
        expected_result = {
            "organismsGroupCount": [
                {
                    "speciesTaxonomyId": 9606,
                    "commonName": "Human",
                    "scientificName": "Homo sapiens",
                    "order": 1,
                    "count": expected_count
                }
            ]
        }
        # send just the first element
        output = json_format.MessageToJson(
            msg_factory.create_organisms_group_count(
                data=[input_data[0]],
                release_label=None
            )
        )
        assert json.loads(output) == expected_result

    @pytest.mark.parametrize(
        "genome_tag, current_only, expected_output",
        [
            # url_name = GRCh38 => homo_sapien 38
            ("GRCh38", True, {'genomeUuid': 'a7335667-93e7-11ec-a39d-005056b38ce3'}),
            #Todo: Need to review how genomes are fetched from release version (minor revision)
            #genome_select = genome_select.filter(EnsemblRelease.version <= release_version)
            #if a genome is assigned to 110.1 & 108.0 and current release version is 110.3
            #the return should be ordered to its genome last release version 110.1
            ("GRCh38", False, {"genomeUuid": "a7335667-93e7-11ec-a39d-005056b38ce3"}),
            # tol_id = mHomSap1 => homo_sapien 37
            # I randomly picked up this tol_id, probably wrong (biologically speaking)
            ("GRCh37", False, {"genomeUuid": "3704ceb1-948d-11ec-a39d-005056b38ce3"}),
            # Null
            ("iDontExist", False, {}),
        ]
    )
    def test_create_genome_uuid(self, genome_conn, genome_tag, current_only, expected_output):
        input_data = genome_conn.fetch_genomes(genome_tag=genome_tag, current_only=current_only)

        genome_uuid = input_data[0].Genome.genome_uuid if len(input_data) else ""
        output = json_format.MessageToJson(
            msg_factory.create_genome_uuid({"genome_uuid": genome_uuid})
        )
        assert json.loads(output) == expected_output

    @pytest.mark.parametrize(
        "genome_uuid, expected_output",
        [
            (
                # Human
                "65d4f21f-695a-4ed0-be67-5732a551fea4",
                {
                    "faaLocation": "Homo_sapiens/GCA_018473295.1/vep/genome/softmasked.fa.bgz",
                    "gffLocation": "Homo_sapiens/GCA_018473295.1/vep/ensembl/geneset/2022_08/genes.gff3.bgz"
                }
            ),
            (
                # Ecoli
                "a73351f7-93e7-11ec-a39d-005056b38ce3",
                {
                    'faaLocation': 'Escherichia_coli_str_K_12_substr_MG1655_str_K12/GCA_000005845.2/vep/genome/softmasked.fa.bgz',
                    'gffLocation': 'Escherichia_coli_str_K_12_substr_MG1655_str_K12/GCA_000005845.2/vep/community/geneset/2018_09/genes.gff3.bgz'
                }
            )
        ]
    )
    def test_create_vep_file_paths(self, vep_conn, genome_uuid, expected_output):
        input_data = vep_conn.fetch_vep_locations(genome_uuid)
        output = json_format.MessageToJson(
            msg_factory.create_vep_file_paths(input_data)
        )
        assert json.loads(output) == expected_output


    def test_create_vep_file_paths_invalid_uuid(self, vep_conn):
        invalid_uuid = "some-invalid-genome-uuid-000000000000"
        with pytest.raises(ValueError, match=f"No data found for genome UUID: {invalid_uuid}"):
            vep_conn.fetch_vep_locations(invalid_uuid)
