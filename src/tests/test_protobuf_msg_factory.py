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

import pkg_resources
import pytest
from ensembl.database import UnitTestDB
from google.protobuf import json_format

import ensembl.production.metadata.grpc.protobuf_msg_factory as msg_factory

sample_path = Path(__file__).parent.parent / "ensembl" / "production" / "metadata" / "api" / "sample"


@pytest.mark.parametrize("multi_dbs", [[{"src": sample_path / "ensembl_genome_metadata"},
                                        {"src": sample_path / "ncbi_taxonomy"}]],
                         indirect=True)
class TestClass:
    dbc = None  # type: UnitTestDB

    def test_create_genome(self, multi_dbs, genome_conn):
        """Test service.create_genome function"""
        genome_input_data = genome_conn.fetch_genomes(genome_uuid="a7335667-93e7-11ec-a39d-005056b38ce3")
        # Make sure we are only getting one
        assert len(genome_input_data) == 1

        attrib_input_data = genome_conn.fetch_genome_datasets(genome_uuid=genome_input_data[0].Genome.genome_uuid,
                                                              dataset_attributes=True)
        # 11 attributes
        assert len(attrib_input_data) == 25

        related_assemblies_input_count = genome_conn.fetch_assemblies_count(
            genome_input_data[0].Organism.species_taxonomy_id)
        # There are three related assemblies
        assert related_assemblies_input_count == 99

        expected_output = {
            "genomeUuid": "a7335667-93e7-11ec-a39d-005056b38ce3",
            "assembly": {
                "accession": "GCA_000001405.29",
                "assemblyUuid": "fd7fea38-981a-4d73-a879-6f9daef86f08",
                "name": "GRCh38.p14",
                "ucscName": "hg38",
                "level": "chromosome",
                "ensemblName": "GRCh38.p14",
                "isReference": True,
                "urlName": "grch38"
            },
            "taxon": {
                "taxonomyId": 9606,
                "scientificName": "Homo sapiens"
            },
            "created": "2023-09-22 15:04:45",
            "attributesInfo": {
                "assemblyLevel": "chromosome",
                "assemblyDate": "2013-12",
                "assemblyProviderName": "Genome Reference Consortium",
                "assemblyProviderUrl": "https://www.ncbi.nlm.nih.gov/grc"
            },
            "organism": {
                "commonName": "human",
                "ensemblName": "SAMN12121739",
                "organismUuid": "1d336185-affe-4a91-85bb-04ebd73cbb56",
                "scientificName": "Homo sapiens",
                "scientificParlanceName": "Human",
                "speciesTaxonomyId": 9606,
                "taxonomyId": 9606
            },
            "relatedAssembliesCount": 99,
            "release": {
                "releaseVersion": 110.1,
                "releaseDate": "2023-10-18",
                "releaseLabel": "beta-1",
                "isCurrent": True,
                "siteName": "Ensembl",
                "siteLabel": "MVP Ensembl",
                "siteUri": "https://beta.ensembl.org"
            }
        }

        output = json_format.MessageToJson(
            msg_factory.create_genome(
                data=genome_input_data[0],
                attributes=attrib_input_data,
                count=related_assemblies_input_count
            )
        )
        assert json.loads(output) == expected_output

    def test_create_assembly_info(self, multi_dbs, genome_conn):
        input_data = genome_conn.fetch_sequences(assembly_uuid="fd7fea38-981a-4d73-a879-6f9daef86f08")
        expected_output = {
            "accession": "GCA_000001405.29",
            "assemblyUuid": "fd7fea38-981a-4d73-a879-6f9daef86f08",
            "chromosomal": 1,
            "length": "135086622",
            "level": "chromosome",
            "name": "GRCh38.p14",
            "sequenceLocation": "SO:0000738"
        }

        output = json_format.MessageToJson(msg_factory.create_assembly_info(input_data[0]))
        assert json.loads(output) == expected_output

    def test_create_species(self, multi_dbs, genome_conn):
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
        input_data = genome_conn.fetch_genome_datasets(organism_uuid=organism_uuid, dataset_type_name="all",
                                                       dataset_attributes=True)

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

    def test_create_top_level_statistics(self, multi_dbs, genome_conn):
        # ecoli
        organism_uuid = "1e579f8d-3880-424e-9b4f-190eb69280d9"
        input_data = genome_conn.fetch_genome_datasets(organism_uuid=organism_uuid, dataset_type_name="all",
                                                       dataset_attributes=True)

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

    def test_create_genome_sequence(self, multi_dbs, genome_conn):
        input_data = genome_conn.fetch_sequences(genome_uuid="a7335667-93e7-11ec-a39d-005056b38ce3")
        expected_output = {
            'accession': '11',
            'chromosomal': True,
            'length': '135086622',
            'name': '11',
            'sequenceLocation': 'SO:0000738'
        }
        output = json_format.MessageToJson(msg_factory.create_genome_sequence(input_data[0]))
        assert json.loads(output) == expected_output

    def test_create_assembly_region(self, multi_dbs, genome_conn):
        input_data = genome_conn.fetch_sequences(
            genome_uuid="a7335667-93e7-11ec-a39d-005056b38ce3",
        )
        # TODO: Check why this is failing when name and chromosomal is provided
        expected_output = {'chromosomal': True, 'length': '135086622', 'name': '11', 'rank': 11}
        output = json_format.MessageToJson(msg_factory.create_assembly_region(input_data[0]))
        assert json.loads(output) == expected_output

    def test_create_genome_assembly_sequence_region(self, multi_dbs, genome_conn):
        input_data = genome_conn.fetch_sequences(
            genome_uuid="a7335667-93e7-11ec-a39d-005056b38ce3",
            assembly_accession="GCA_000001405.29",
            assembly_sequence_accession="Y"
        )
        expected_output = {
            "name": "Y",
            "length": "57227415",
            "chromosomal": True,
            "rank": 24
        }
        output = json_format.MessageToJson(msg_factory.create_assembly_region(input_data[0]))
        assert json.loads(output) == expected_output

    def test_create_release(self, multi_dbs, release_db_conn):
        input_data = release_db_conn.fetch_releases(release_version=110.1)
        expected_output = {
            "releaseVersion": 110.1,
            "releaseDate": "2023-10-18",
            "releaseLabel": "beta-1",
            "isCurrent": True,
            "siteName": "Ensembl",
            "siteLabel": "MVP Ensembl",
            "siteUri": "https://beta.ensembl.org"
        }
        output = json_format.MessageToJson(msg_factory.create_release(input_data[0]))
        assert json.loads(output) == expected_output

    def test_create_organisms_group_count(self, multi_dbs, genome_conn):
        input_data = genome_conn.fetch_organisms_group_counts()
        expected_result = {
            "organismsGroupCount": [
                {
                    "speciesTaxonomyId": 9606,
                    "commonName": "human",
                    "scientificName": "Homo sapiens",
                    "order": 1,
                    "count": 99
                }
            ]
        }
        # we have 6 organism in the test data
        assert len(input_data) == 41
        # send just the first element
        output = json_format.MessageToJson(
            msg_factory.create_organisms_group_count(
                data=[input_data[0]],
                release_version=None
            )
        )
        assert json.loads(output) == expected_result

    @pytest.mark.parametrize(
        "genome_tag, current_only, expected_output",
        [
            # url_name = GRCh38 => homo_sapien 38
            ("GRCh38", True, {"genomeUuid": "a7335667-93e7-11ec-a39d-005056b38ce3"}),
            # tol_id = mHomSap1 => homo_sapien 37
            # I randomly picked up this tol_id, probably wrong (biologically speaking)
            ("GRCh37", False, {"genomeUuid": "3704ceb1-948d-11ec-a39d-005056b38ce3"}),
            # Null
            ("iDontExist", False, {}),
        ]
    )
    def test_create_genome_uuid(self, genome_conn, genome_tag, current_only, expected_output):
        input_data = genome_conn.fetch_genomes(genome_tag=genome_tag, current_only=current_only)

        genome_uuid = input_data[0].Genome.genome_uuid if len(input_data) == 1 else ""
        output = json_format.MessageToJson(
            msg_factory.create_genome_uuid({"genome_uuid": genome_uuid})
        )
        assert json.loads(output) == expected_output
