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
Unit tests for utils.py
"""
import json
from pathlib import Path

import pkg_resources
import pytest
from ensembl.database import UnitTestDB
from google.protobuf import json_format

from ensembl.production.metadata.grpc import ensembl_metadata_pb2, utils

distribution = pkg_resources.get_distribution("ensembl-metadata-api")
sample_path = Path(distribution.location) / "ensembl" / "production" / "metadata" / "api" / "sample"


@pytest.mark.parametrize("multi_dbs", [[{"src": sample_path / "ensembl_metadata"},
                                        {"src": sample_path / "ncbi_taxonomy"}]],
                         indirect=True)
class TestUtils:
    dbc = None  # type: UnitTestDB

    def test_get_karyotype_information(self, genome_db_conn):
        output = json_format.MessageToJson(
            utils.get_karyotype_information(genome_db_conn, "3c4cec7f-fb69-11eb-8dac-005056b32883"))
        expected_output = {}
        assert json.loads(output) == expected_output

    def test_get_assembly_information(self, genome_db_conn):
        output = json_format.MessageToJson(
            utils.get_assembly_information(genome_db_conn, "eeaaa2bf-151c-4848-8b85-a05a9993101e"))
        expected_output = {"accession": "GCA_000001405.28",
                           "assemblyUuid": "eeaaa2bf-151c-4848-8b85-a05a9993101e",
                           # "chromosomal": 1,
                           "length": "71251",
                           "level": "chromosome",
                           "name": "GRCh38.p13",
                           "sequenceLocation": "SO:0000738"}
        assert json.loads(output) == expected_output

    def test_get_genomes_from_assembly_accession_iterator(self, genome_db_conn):
        output = [
            json.loads(json_format.MessageToJson(response)) for response in
            utils.get_genomes_from_assembly_accession_iterator(
                db_conn=genome_db_conn, assembly_accession="GCA_000005845.2", release_version=None
            )
        ]

        expected_output = [
          {
            "assembly": {
              "accession": "GCA_000005845.2",
              "assemblyUuid": "f78618ef-1075-47ee-a496-be26cad47912",
              "ensemblName": "ASM584v2",
              "level": "chromosome",
              "name": "ASM584v2"
            },
            "attributesInfo": {},
            "created": "2023-05-12 13:32:14",
            "genomeUuid": "a73351f7-93e7-11ec-a39d-005056b38ce3",
            "organism": {
              "commonName": "Escherichia coli str. K-12 substr. MG1655 str. K12 (GCA_000005845)",
              "ensemblName": "Escherichia_coli_str_k_12_substr_mg1655_gca_000005845",
              "organismUuid": "21279e3e-e651-43e1-a6fc-79e390b9e8a8",
              "scientificName": "Escherichia coli str. K-12 substr. MG1655 str. K12 (GCA_000005845)",
              "scientificParlanceName": "escherichia_coli_str_k_12_substr_mg1655_gca_000005845",
              "speciesTaxonomyId": 562,
              "taxonomyId": 511145
            },
            "relatedAssembliesCount": 1,
            "release": {
              "isCurrent": True,
              "releaseDate": "2023-05-15",
              "releaseLabel": "Beta Release 1",
              "releaseVersion": 108.0,
              "siteLabel": "Ensembl Genome Browser",
              "siteName": "Ensembl",
              "siteUri": "https://beta.ensembl.org"
            },
            "taxon": {
              "scientificName": "Escherichia coli str. K-12 substr. MG1655 str. K12 (GCA_000005845)",
              "taxonomyId": 511145
            }
          }
        ]
        assert output == expected_output


    @pytest.mark.parametrize(
        "assembly_accession, release_version",
        [
            # null
            (None, None),
            # no matches
            ("asdfasdfadf", None),
        ]
    )
    def test_get_genomes_from_assembly_accession_iterator_null(self, genome_db_conn, assembly_accession, release_version):
        output = [
            json.loads(json_format.MessageToJson(response)) for response in
            utils.get_genomes_from_assembly_accession_iterator(
                db_conn=genome_db_conn, assembly_accession=assembly_accession, release_version=release_version
            )
        ]
        assert output == []

    # TODO: Ask Daniel / Investigate why organism_group_member test table is not populated
    # def test_get_sub_species_info(self, genome_db_conn):
    #     output = json_format.MessageToJson(
    #         utils.get_sub_species_info(
    #             db_conn=genome_db_conn,
    #             organism_uuid="21279e3e-e651-43e1-a6fc-79e390b9e8a8",
    #             group="EnsemblBacteria"
    #         )
    #     )
    #     print(f"output ===> {output}")
    #     expected_output = {
    #         "organismUuid": "21279e3e-e651-43e1-a6fc-79e390b9e8a8",
    #         "speciesName": ["EnsemblBacteria"],
    #         "speciesType": ["Division"]}
    #     assert json.loads(output) == expected_output
    #
    #     output2 = json_format.MessageToJson(utils.get_sub_species_info(genome_db_conn, "s0m3-r4nd0m-0rg4n1sm-uu1d"))
    #     expected_output2 = {}
    #     assert json.loads(output2) == expected_output2

    def test_get_top_level_statistics(self, genome_db_conn):
        # Triticum aestivum
        output = json_format.MessageToJson(
            utils.get_top_level_statistics(
                db_conn=genome_db_conn,
                group="EnsemblPlants",
                organism_uuid="d64c34ca-b37a-476b-83b5-f21d07a3ae67",
            )
        )
        output = json.loads(output)
        first_genome_stats = output["statsByGenomeUuid"][0]["statistics"]
        assert len(first_genome_stats) == 51
        assert first_genome_stats[0] == {
            "label": "Contig N50",
            "name": "contig_n50",
            "statisticType": "bp",
            "statisticValue": "51842",
        }
        assert first_genome_stats[1] == {
            "label": "Total genome length",
            "name": "total_genome_length",
            "statisticType": "bp",
            "statisticValue": "14547261565",
        }

    def test_get_top_level_statistics_by_uuid(self, genome_db_conn):
        output = json_format.MessageToJson(
            utils.get_top_level_statistics_by_uuid(
                genome_db_conn, "a73357ab-93e7-11ec-a39d-005056b38ce3"
            )
        )
        output = json.loads(output)
        assert len(output["statistics"]) == 51
        assert output["statistics"][0] == {
            "label": "Contig N50",
            "name": "contig_n50",
            "statisticType": "bp",
            "statisticValue": "51842",
        }
        assert output["statistics"][2] == {
            "label": "Total coding sequence length",
            "name": "total_coding_sequence_length",
            "statisticType": "bp",
            "statisticValue": "133312441"
        }

    def test_get_datasets_list_by_uuid(self, genome_db_conn):
        # the expected_output is too long and duplicated
        # because of the returned attributes
        # TODO: Fix this later
        output = json_format.MessageToJson(
            utils.get_datasets_list_by_uuid(genome_db_conn, "a73357ab-93e7-11ec-a39d-005056b38ce3", 108.0))

        expected_output = {
            "genomeUuid": "a73357ab-93e7-11ec-a39d-005056b38ce3",
            "datasets": {
                "evidence": {
                    "datasetInfos": [
                        {
                            "datasetUuid": "64a66f22-07a9-476e-9816-785e2ccb9c30",
                            "datasetName": "evidence",
                            "datasetLabel": "Manual Add",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "64a66f22-07a9-476e-9816-785e2ccb9c30",
                            "datasetName": "evidence",
                            "datasetLabel": "Manual Add",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "64a66f22-07a9-476e-9816-785e2ccb9c30",
                            "datasetName": "evidence",
                            "datasetLabel": "Manual Add",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "64a66f22-07a9-476e-9816-785e2ccb9c30",
                            "datasetName": "evidence",
                            "datasetLabel": "Manual Add",
                            "version": 108.0
                        }
                    ]
                },
                "assembly": {
                    "datasetInfos": [
                        {
                            "datasetUuid": "b4ff55e3-d06a-4772-bb13-81c3207669e3",
                            "datasetName": "assembly",
                            "datasetLabel": "GCA_900519105.1",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "b4ff55e3-d06a-4772-bb13-81c3207669e3",
                            "datasetName": "assembly",
                            "datasetLabel": "GCA_900519105.1",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "b4ff55e3-d06a-4772-bb13-81c3207669e3",
                            "datasetName": "assembly",
                            "datasetLabel": "GCA_900519105.1",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "b4ff55e3-d06a-4772-bb13-81c3207669e3",
                            "datasetName": "assembly",
                            "datasetLabel": "GCA_900519105.1",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "b4ff55e3-d06a-4772-bb13-81c3207669e3",
                            "datasetName": "assembly",
                            "datasetLabel": "GCA_900519105.1",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "b4ff55e3-d06a-4772-bb13-81c3207669e3",
                            "datasetName": "assembly",
                            "datasetLabel": "GCA_900519105.1",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "b4ff55e3-d06a-4772-bb13-81c3207669e3",
                            "datasetName": "assembly",
                            "datasetLabel": "GCA_900519105.1",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "b4ff55e3-d06a-4772-bb13-81c3207669e3",
                            "datasetName": "assembly",
                            "datasetLabel": "GCA_900519105.1",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "b4ff55e3-d06a-4772-bb13-81c3207669e3",
                            "datasetName": "assembly",
                            "datasetLabel": "GCA_900519105.1",
                            "version": 108.0
                        }
                    ]
                },
                "homologies": {
                    "datasetInfos": [
                        {
                            "datasetUuid": "e67ca09d-2e7b-4135-a990-6a2d1bca7285",
                            "datasetName": "homologies",
                            "datasetLabel": "Manual Add",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "e67ca09d-2e7b-4135-a990-6a2d1bca7285",
                            "datasetName": "homologies",
                            "datasetLabel": "Manual Add",
                            "version": 108.0
                        }
                    ]
                },
                "genebuild": {
                    "datasetInfos": [
                        {
                            "datasetUuid": "0dc05c6e-2910-4dbd-879a-719ba97d5824",
                            "datasetName": "genebuild",
                            "datasetLabel": "2018-04-IWGSC",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "0dc05c6e-2910-4dbd-879a-719ba97d5824",
                            "datasetName": "genebuild",
                            "datasetLabel": "2018-04-IWGSC",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "0dc05c6e-2910-4dbd-879a-719ba97d5824",
                            "datasetName": "genebuild",
                            "datasetLabel": "2018-04-IWGSC",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "0dc05c6e-2910-4dbd-879a-719ba97d5824",
                            "datasetName": "genebuild",
                            "datasetLabel": "2018-04-IWGSC",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "0dc05c6e-2910-4dbd-879a-719ba97d5824",
                            "datasetName": "genebuild",
                            "datasetLabel": "2018-04-IWGSC",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "0dc05c6e-2910-4dbd-879a-719ba97d5824",
                            "datasetName": "genebuild",
                            "datasetLabel": "2018-04-IWGSC",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "0dc05c6e-2910-4dbd-879a-719ba97d5824",
                            "datasetName": "genebuild",
                            "datasetLabel": "2018-04-IWGSC",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "0dc05c6e-2910-4dbd-879a-719ba97d5824",
                            "datasetName": "genebuild",
                            "datasetLabel": "2018-04-IWGSC",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "0dc05c6e-2910-4dbd-879a-719ba97d5824",
                            "datasetName": "genebuild",
                            "datasetLabel": "2018-04-IWGSC",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "0dc05c6e-2910-4dbd-879a-719ba97d5824",
                            "datasetName": "genebuild",
                            "datasetLabel": "2018-04-IWGSC",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "0dc05c6e-2910-4dbd-879a-719ba97d5824",
                            "datasetName": "genebuild",
                            "datasetLabel": "2018-04-IWGSC",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "0dc05c6e-2910-4dbd-879a-719ba97d5824",
                            "datasetName": "genebuild",
                            "datasetLabel": "2018-04-IWGSC",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "0dc05c6e-2910-4dbd-879a-719ba97d5824",
                            "datasetName": "genebuild",
                            "datasetLabel": "2018-04-IWGSC",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "0dc05c6e-2910-4dbd-879a-719ba97d5824",
                            "datasetName": "genebuild",
                            "datasetLabel": "2018-04-IWGSC",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "0dc05c6e-2910-4dbd-879a-719ba97d5824",
                            "datasetName": "genebuild",
                            "datasetLabel": "2018-04-IWGSC",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "0dc05c6e-2910-4dbd-879a-719ba97d5824",
                            "datasetName": "genebuild",
                            "datasetLabel": "2018-04-IWGSC",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "0dc05c6e-2910-4dbd-879a-719ba97d5824",
                            "datasetName": "genebuild",
                            "datasetLabel": "2018-04-IWGSC",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "0dc05c6e-2910-4dbd-879a-719ba97d5824",
                            "datasetName": "genebuild",
                            "datasetLabel": "2018-04-IWGSC",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "0dc05c6e-2910-4dbd-879a-719ba97d5824",
                            "datasetName": "genebuild",
                            "datasetLabel": "2018-04-IWGSC",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "0dc05c6e-2910-4dbd-879a-719ba97d5824",
                            "datasetName": "genebuild",
                            "datasetLabel": "2018-04-IWGSC",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "0dc05c6e-2910-4dbd-879a-719ba97d5824",
                            "datasetName": "genebuild",
                            "datasetLabel": "2018-04-IWGSC",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "0dc05c6e-2910-4dbd-879a-719ba97d5824",
                            "datasetName": "genebuild",
                            "datasetLabel": "2018-04-IWGSC",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "0dc05c6e-2910-4dbd-879a-719ba97d5824",
                            "datasetName": "genebuild",
                            "datasetLabel": "2018-04-IWGSC",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "0dc05c6e-2910-4dbd-879a-719ba97d5824",
                            "datasetName": "genebuild",
                            "datasetLabel": "2018-04-IWGSC",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "0dc05c6e-2910-4dbd-879a-719ba97d5824",
                            "datasetName": "genebuild",
                            "datasetLabel": "2018-04-IWGSC",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "0dc05c6e-2910-4dbd-879a-719ba97d5824",
                            "datasetName": "genebuild",
                            "datasetLabel": "2018-04-IWGSC",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "0dc05c6e-2910-4dbd-879a-719ba97d5824",
                            "datasetName": "genebuild",
                            "datasetLabel": "2018-04-IWGSC",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "0dc05c6e-2910-4dbd-879a-719ba97d5824",
                            "datasetName": "genebuild",
                            "datasetLabel": "2018-04-IWGSC",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "0dc05c6e-2910-4dbd-879a-719ba97d5824",
                            "datasetName": "genebuild",
                            "datasetLabel": "2018-04-IWGSC",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "0dc05c6e-2910-4dbd-879a-719ba97d5824",
                            "datasetName": "genebuild",
                            "datasetLabel": "2018-04-IWGSC",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "0dc05c6e-2910-4dbd-879a-719ba97d5824",
                            "datasetName": "genebuild",
                            "datasetLabel": "2018-04-IWGSC",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "0dc05c6e-2910-4dbd-879a-719ba97d5824",
                            "datasetName": "genebuild",
                            "datasetLabel": "2018-04-IWGSC",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "0dc05c6e-2910-4dbd-879a-719ba97d5824",
                            "datasetName": "genebuild",
                            "datasetLabel": "2018-04-IWGSC",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "0dc05c6e-2910-4dbd-879a-719ba97d5824",
                            "datasetName": "genebuild",
                            "datasetLabel": "2018-04-IWGSC",
                            "version": 108.0
                        }
                    ]
                },
                "variation": {
                    "datasetInfos": [
                        {
                            "datasetUuid": "4d411e2d-676e-4fe0-b0d7-65a9e33fd47f",
                            "datasetName": "variation",
                            "datasetLabel": "Manual Add",
                            "version": 108.0
                        },
                        {
                            "datasetUuid": "4d411e2d-676e-4fe0-b0d7-65a9e33fd47f",
                            "datasetName": "variation",
                            "datasetLabel": "Manual Add",
                            "version": 108.0
                        }
                    ]
                }
            }
        }
        assert json.loads(output) == expected_output

    def test_get_datasets_list_by_uuid_no_results(self, genome_db_conn):
        output = json_format.MessageToJson(
            utils.get_datasets_list_by_uuid(genome_db_conn, "some-random-uuid-f00-b4r", 103.0))
        output = json.loads(output)
        expected_output = {}
        assert output == expected_output

    def test_get_dataset_by_genome_and_dataset_type(self, genome_db_conn):
        output = json_format.MessageToJson(
            utils.get_dataset_by_genome_and_dataset_type(genome_db_conn, "a7335667-93e7-11ec-a39d-005056b38ce3",
                                                         "assembly")
        )
        output = json.loads(output)
        assert output == {'genomeUuid': 'a7335667-93e7-11ec-a39d-005056b38ce3', 'datasetType': 'assembly',
                          'datasetInfos': [
                              {'datasetUuid': '559d7660-d92d-47e1-924e-e741151c2cef',
                               'datasetName': 'assembly',
                               'name': 'contig_n50',
                               'type': 'bp',
                               'datasetLabel': 'GCA_000001405.28',
                               'version': 108.0,
                               'value': '56413054'},
                              {'datasetUuid': '559d7660-d92d-47e1-924e-e741151c2cef',
                               'datasetName': 'assembly',
                               'name': 'total_genome_length',
                               'type': 'bp',
                               'datasetLabel': 'GCA_000001405.28',
                               'version': 108.0,
                               'value': '3272116950'},
                              {'datasetUuid': '559d7660-d92d-47e1-924e-e741151c2cef',
                               'datasetName': 'assembly',
                               'name': 'total_coding_sequence_length',
                               'type': 'bp',
                               'datasetLabel': 'GCA_000001405.28',
                               'version': 108.0,
                               'value': '34459298'},
                              {'datasetUuid': '559d7660-d92d-47e1-924e-e741151c2cef',
                               'datasetName': 'assembly',
                               'name': 'total_gap_length',
                               'type': 'bp',
                               'datasetLabel': 'GCA_000001405.28',
                               'version': 108.0,
                               'value': '161368351'},
                              {'datasetUuid': '559d7660-d92d-47e1-924e-e741151c2cef',
                               'datasetName': 'assembly',
                               'name': 'spanned_gaps',
                               'type': 'integer',
                               'datasetLabel': 'GCA_000001405.28',
                               'version': 108.0,
                               'value': '661'},
                              {'datasetUuid': '559d7660-d92d-47e1-924e-e741151c2cef',
                               'datasetName': 'assembly',
                               'name': 'chromosomes',
                               'type': 'integer',
                               'datasetLabel': 'GCA_000001405.28',
                               'version': 108.0,
                               'value': '25'},
                              {'datasetUuid': '559d7660-d92d-47e1-924e-e741151c2cef',
                               'datasetName': 'assembly',
                               'name': 'toplevel_sequences',
                               'type': 'integer',
                               'datasetLabel': 'GCA_000001405.28',
                               'version': 108.0,
                               'value': '640'},
                              {'datasetUuid': '559d7660-d92d-47e1-924e-e741151c2cef',
                               'datasetName': 'assembly',
                               'name': 'component_sequences',
                               'type': 'integer',
                               'datasetLabel': 'GCA_000001405.28',
                               'version': 108.0,
                               'value': '36734'},
                              {'datasetUuid': '559d7660-d92d-47e1-924e-e741151c2cef',
                               'datasetName': 'assembly',
                               'name': 'gc_percentage',
                               'type': 'percent',
                               'datasetLabel': 'GCA_000001405.28',
                               'version': 108.0,
                               'value': '38.87'},
                              {'datasetUuid': '559d7660-d92d-47e1-924e-e741151c2cef',
                               'datasetName': 'assembly',
                               'name': 'assembly.date',
                               'type': 'string',
                               'datasetLabel': 'GCA_000001405.28',
                               'version': 108.0,
                               'value': '2013-12'},
                              {'datasetUuid': '559d7660-d92d-47e1-924e-e741151c2cef',
                               'datasetName': 'assembly',
                               'name': 'assembly.level',
                               'type': 'string',
                               'datasetLabel': 'GCA_000001405.28',
                               'version': 108.0,
                               'value': 'chromosome'}]
                          }

    def test_get_dataset_by_genome_id_no_results(self, genome_db_conn):
        output = json_format.MessageToJson(
            utils.get_dataset_by_genome_and_dataset_type(genome_db_conn, "a7335667-93e7-11ec-a39d-005056b38ce3",
                                                         "blah blah blah"))
        output = json.loads(output)
        assert output == {}

    @pytest.mark.parametrize(
        "ensembl_name, assembly_name, use_default, expected_output",
        [
            ("homo_sapiens", "GRCh38.p13", False, {"genomeUuid": "a7335667-93e7-11ec-a39d-005056b38ce3"}),
            ("homo_sapiens", "GRCh38.p13", True, {}),
            ("homo_sapiens", "GRCh38", True, {"genomeUuid": "a7335667-93e7-11ec-a39d-005056b38ce3"}),
            ("random_ensembl_name", "GRCh38", False, {"genomeUuid": "a7335667-93e7-11ec-a39d-005056b38ce3"}),
            ("random_ensembl_name", "random_assembly_name", True, {}),
            ("random_ensembl_name", "random_assembly_name", False, {}),
        ]
    )
    def test_get_genome_uuid(self, genome_db_conn, ensembl_name, assembly_name, use_default, expected_output):
        output = json_format.MessageToJson(
            utils.get_genome_uuid(
                db_conn=genome_db_conn,
                ensembl_name=ensembl_name,
                assembly_name=assembly_name,
                use_default=use_default
            ))
        assert json.loads(output) == expected_output

    def test_get_genome_by_uuid(self, genome_db_conn):
        output = json_format.MessageToJson(
            utils.get_genome_by_uuid(
                db_conn=genome_db_conn,
                genome_uuid="a73357ab-93e7-11ec-a39d-005056b38ce3",
                release_version=108.0
            ))
        expected_output = {"assembly": {"accession": "GCA_900519105.1",
                                        "ensemblName": "IWGSC",
                                        "assemblyUuid": "ec1c4b53-c2ef-431c-ad0e-b2aef19b44f1",
                                        "level": "chromosome",
                                        "name": "IWGSC"},
                           "attributesInfo": {},
                           "created": "2023-05-12 13:32:36",
                           "genomeUuid": "a73357ab-93e7-11ec-a39d-005056b38ce3",
                           "organism": {"commonName": "Triticum aestivum",
                                        "ensemblName": "Triticum_aestivum",
                                        "organismUuid": "d64c34ca-b37a-476b-83b5-f21d07a3ae67",
                                        "scientificName": "Triticum aestivum",
                                        "scientificParlanceName": "triticum_aestivum",
                                        "speciesTaxonomyId": 4565,
                                        "taxonomyId": 4565,
                                        "strain": "reference (Chinese spring)"},
                           "relatedAssembliesCount": 1,
                           "release": {"isCurrent": True,
                                       "releaseDate": "2023-05-15",
                                       "releaseLabel": "Beta Release 1",
                                       "releaseVersion": 108.0,
                                       "siteLabel": "Ensembl Genome Browser",
                                       "siteName": "Ensembl",
                                       "siteUri": "https://beta.ensembl.org"},
                           "taxon": {"scientificName": "Triticum aestivum",
                                     "strain": "reference (Chinese spring)",
                                     "taxonomyId": 4565}}
        assert json.loads(output) == expected_output

    def test_genome_by_uuid_release_version_unspecified(self, genome_db_conn):
        output = json_format.MessageToJson(
            utils.get_genome_by_uuid(
                db_conn=genome_db_conn,
                genome_uuid="a73357ab-93e7-11ec-a39d-005056b38ce3",
                release_version=None
            ))
        expected_output = {"assembly": {"accession": "GCA_900519105.1",
                                        "ensemblName": "IWGSC",
                                        "assemblyUuid": "ec1c4b53-c2ef-431c-ad0e-b2aef19b44f1",
                                        "level": "chromosome",
                                        "name": "IWGSC"},
                           "attributesInfo": {},
                           "created": "2023-05-12 13:32:36",
                           "genomeUuid": "a73357ab-93e7-11ec-a39d-005056b38ce3",
                           "organism": {"commonName": "Triticum aestivum",
                                        "ensemblName": "Triticum_aestivum",
                                        "organismUuid": "d64c34ca-b37a-476b-83b5-f21d07a3ae67",
                                        "scientificParlanceName": "triticum_aestivum",
                                        "scientificName": "Triticum aestivum",
                                        "speciesTaxonomyId": 4565,
                                        "taxonomyId": 4565,
                                        "strain": "reference (Chinese spring)"},
                           "relatedAssembliesCount": 1,
                           "release": {"isCurrent": True,
                                       "releaseDate": "2023-05-15",
                                       "releaseLabel": "Beta Release 1",
                                       "releaseVersion": 108.0,
                                       "siteLabel": "Ensembl Genome Browser",
                                       "siteName": "Ensembl",
                                       "siteUri": "https://beta.ensembl.org"},
                           "taxon": {"scientificName": "Triticum aestivum",
                                     "strain": "reference (Chinese spring)",
                                     "taxonomyId": 4565}}
        assert json.loads(output) == expected_output

    def test_get_genomes_by_uuid_null(self, genome_db_conn):
        output = utils.get_genome_by_uuid(genome_db_conn, None, 0)
        assert output == ensembl_metadata_pb2.Genome()

    def test_get_genomes_by_keyword(self, genome_db_conn):
        output = [json.loads(json_format.MessageToJson(response)) for response in
                  utils.get_genomes_by_keyword_iterator(genome_db_conn, "Human", 108.0)]
        expected_output = [{"assembly": {"accession": "GCA_000001405.28",
                                         "ensemblName": "GRCh38.p13",
                                         "assemblyUuid": "eeaaa2bf-151c-4848-8b85-a05a9993101e",
                                         "level": "chromosome",
                                         "isReference": True,
                                         "name": "GRCh38.p13",
                                         "ucscName": "hg38"},
                            "attributesInfo": {"assemblyDate": "2013-12",
                                               "assemblyLevel": "chromosome"},
                            "created": "2023-05-12 13:30:58",
                            "genomeUuid": "a7335667-93e7-11ec-a39d-005056b38ce3",
                            "organism": {"commonName": "Human",
                                         "ensemblName": "Homo_sapiens",
                                         "organismUuid": "db2a5f09-2db8-429b-a407-c15a4ca2876d",
                                         "scientificName": "Homo sapiens",
                                         "speciesTaxonomyId": 9606,
                                         "taxonomyId": 9606,
                                         "scientificParlanceName": "homo_sapiens"},
                            "release": {"isCurrent": True,
                                        "releaseDate": "2023-05-15",
                                        "releaseLabel": "Beta Release 1",
                                        "releaseVersion": 108.0,
                                        "siteLabel": "Ensembl Genome Browser",
                                        "siteName": "Ensembl",
                                        "siteUri": "https://beta.ensembl.org"},
                            "taxon": {"scientificName": "Homo sapiens", "taxonomyId": 9606},
                            "relatedAssembliesCount": 3,
                            },
                           {"assembly": {"accession": "GCA_000001405.14",
                                         "ensemblName": "GRCh37.p13",
                                         "assemblyUuid": "633034c3-2268-40a2-866a-9f492cac84bf",
                                         "level": "chromosome",
                                         "name": "GRCh37.p13",
                                         "ucscName": "hg19"},
                            "attributesInfo": {},
                            "created": "2023-05-12 13:32:06",
                            "genomeUuid": "3704ceb1-948d-11ec-a39d-005056b38ce3",
                            "organism": {"commonName": "Human",
                                         "ensemblName": "Homo_sapiens",
                                         "organismUuid": "db2a5f09-2db8-429b-a407-c15a4ca2876d",
                                         "scientificName": "Homo sapiens",
                                         "speciesTaxonomyId": 9606,
                                         "taxonomyId": 9606,
                                         "scientificParlanceName": "homo_sapiens"},
                            "relatedAssembliesCount": 3,
                            "release": {"isCurrent": True,
                                        "releaseDate": "2023-05-15",
                                        "releaseLabel": "Beta Release 1",
                                        "releaseVersion": 108.0,
                                        "siteLabel": "Ensembl Genome Browser",
                                        "siteName": "Ensembl",
                                        "siteUri": "https://beta.ensembl.org"},
                            "taxon": {"scientificName": "Homo sapiens", "taxonomyId": 9606}}
                           ]
        assert output == expected_output

    def test_get_genomes_by_keyword_release_unspecified(self, genome_db_conn):
        output = [json.loads(json_format.MessageToJson(response)) for response in
                  utils.get_genomes_by_keyword_iterator(genome_db_conn, "Homo Sapiens", 0.0)]
        # TODO: DRY the expected_output
        expected_output = [{"assembly": {"accession": "GCA_000001405.28",
                                         "ensemblName": "GRCh38.p13",
                                         "assemblyUuid": "eeaaa2bf-151c-4848-8b85-a05a9993101e",
                                         "level": "chromosome",
                                         "isReference": True,
                                         "name": "GRCh38.p13",
                                         "ucscName": "hg38"},
                            "attributesInfo": {"assemblyDate": "2013-12",
                                               "assemblyLevel": "chromosome"},
                            "created": "2023-05-12 13:30:58",
                            "genomeUuid": "a7335667-93e7-11ec-a39d-005056b38ce3",
                            "organism": {"commonName": "Human",
                                         "ensemblName": "Homo_sapiens",
                                         "organismUuid": "db2a5f09-2db8-429b-a407-c15a4ca2876d",
                                         "scientificName": "Homo sapiens",
                                         "speciesTaxonomyId": 9606,
                                         "taxonomyId": 9606,
                                         "scientificParlanceName": "homo_sapiens"},
                            "release": {"isCurrent": True,
                                        "releaseDate": "2023-05-15",
                                        "releaseLabel": "Beta Release 1",
                                        "releaseVersion": 108.0,
                                        "siteLabel": "Ensembl Genome Browser",
                                        "siteName": "Ensembl",
                                        "siteUri": "https://beta.ensembl.org"},
                            "taxon": {"scientificName": "Homo sapiens", "taxonomyId": 9606},
                            "relatedAssembliesCount": 3,
                            },
                           {"assembly": {"accession": "GCA_000001405.14",
                                         "ensemblName": "GRCh37.p13",
                                         "assemblyUuid": "633034c3-2268-40a2-866a-9f492cac84bf",
                                         "level": "chromosome",
                                         "name": "GRCh37.p13",
                                         "ucscName": "hg19"},
                            "attributesInfo": {},
                            "created": "2023-05-12 13:32:06",
                            "genomeUuid": "3704ceb1-948d-11ec-a39d-005056b38ce3",
                            "organism": {"commonName": "Human",
                                         "ensemblName": "Homo_sapiens",
                                         "organismUuid": "db2a5f09-2db8-429b-a407-c15a4ca2876d",
                                         "scientificName": "Homo sapiens",
                                         "speciesTaxonomyId": 9606,
                                         "taxonomyId": 9606,
                                         "scientificParlanceName": "homo_sapiens"},
                            "relatedAssembliesCount": 3,
                            "release": {"isCurrent": True,
                                        "releaseDate": "2023-05-15",
                                        "releaseLabel": "Beta Release 1",
                                        "releaseVersion": 108.0,
                                        "siteLabel": "Ensembl Genome Browser",
                                        "siteName": "Ensembl",
                                        "siteUri": "https://beta.ensembl.org"},
                            "taxon": {"scientificName": "Homo sapiens", "taxonomyId": 9606}}
                           ]
        assert output == expected_output

    def test_get_genomes_by_keyword_null(self, genome_db_conn):
        output = list(
            utils.get_genomes_by_keyword_iterator(genome_db_conn, None, 0))
        assert output == []

    def test_get_genomes_by_keyword_no_matches(self, genome_db_conn):
        output = list(
            utils.get_genomes_by_keyword_iterator(genome_db_conn, "bigfoot",
                                                  1))
        assert output == []

    def test_get_genomes_by_name(self, genome_db_conn):
        output = json_format.MessageToJson(utils.get_genome_by_name(
            db_conn=genome_db_conn,
            site_name="Ensembl",
            ensembl_name="Triticum_aestivum",
            release_version=108.0
        ))
        expected_output = {"assembly": {"accession": "GCA_900519105.1",
                                        "ensemblName": "IWGSC",
                                        "assemblyUuid": "ec1c4b53-c2ef-431c-ad0e-b2aef19b44f1",
                                        "level": "chromosome",
                                        "name": "IWGSC"},
                           "attributesInfo": {},
                           "created": "2023-05-12 13:32:36",
                           "genomeUuid": "a73357ab-93e7-11ec-a39d-005056b38ce3",
                           "organism": {"commonName": "Triticum aestivum",
                                        "ensemblName": "Triticum_aestivum",
                                        "organismUuid": "d64c34ca-b37a-476b-83b5-f21d07a3ae67",
                                        "scientificName": "Triticum aestivum",
                                        "scientificParlanceName": "triticum_aestivum",
                                        "speciesTaxonomyId": 4565,
                                        "taxonomyId": 4565,
                                        "strain": "reference (Chinese spring)"},
                           "relatedAssembliesCount": 1,
                           "release": {"isCurrent": True,
                                       "releaseDate": "2023-05-15",
                                       "releaseLabel": "Beta Release 1",
                                       "releaseVersion": 108.0,
                                       "siteLabel": "Ensembl Genome Browser",
                                       "siteName": "Ensembl",
                                       "siteUri": "https://beta.ensembl.org"},
                           "taxon": {
                               "scientificName": "Triticum aestivum",
                               "strain": "reference (Chinese spring)",
                               "taxonomyId": 4565
                           }}
        assert json.loads(output) == expected_output

    def test_get_genomes_by_name_release_unspecified(self, genome_db_conn):
        # We are expecting the same result as test_get_genomes_by_name() above
        # because no release is specified get_genome_by_name() -> fetch_genomes
        # checks if the fetched genome is released and picks it up
        output = json_format.MessageToJson(utils.get_genome_by_name(
            db_conn=genome_db_conn,
            site_name="Ensembl",
            ensembl_name="Triticum_aestivum",
            release_version=None
        ))
        expected_output = {"assembly": {"accession": "GCA_900519105.1",
                                        "ensemblName": "IWGSC",
                                        "assemblyUuid": "ec1c4b53-c2ef-431c-ad0e-b2aef19b44f1",
                                        "level": "chromosome",
                                        "name": "IWGSC"},
                           "attributesInfo": {},
                           "created": "2023-05-12 13:32:36",
                           "genomeUuid": "a73357ab-93e7-11ec-a39d-005056b38ce3",
                           "organism": {"commonName": "Triticum aestivum",
                                        "ensemblName": "Triticum_aestivum",
                                        "organismUuid": "d64c34ca-b37a-476b-83b5-f21d07a3ae67",
                                        "scientificName": "Triticum aestivum",
                                        "scientificParlanceName": "triticum_aestivum",
                                        "speciesTaxonomyId": 4565,
                                        "taxonomyId": 4565,
                                        "strain": "reference (Chinese spring)"},
                           "relatedAssembliesCount": 1,
                           "release": {"isCurrent": True,
                                       "releaseDate": "2023-05-15",
                                       "releaseLabel": "Beta Release 1",
                                       "releaseVersion": 108.0,
                                       "siteLabel": "Ensembl Genome Browser",
                                       "siteName": "Ensembl",
                                       "siteUri": "https://beta.ensembl.org"},
                           "taxon": {
                               "scientificName": "Triticum aestivum",
                               "strain": "reference (Chinese spring)",
                               "taxonomyId": 4565
                           }}
        assert json.loads(output) == expected_output

    def test_get_organisms_group_count(self, genome_db_conn):
        output = json_format.MessageToJson(
            utils.get_organisms_group_count(
                db_conn=genome_db_conn,
                release_version=None
            )
        )
        expected_output = {
            "organismsGroupCount": [
                {
                    "speciesTaxonomyId": 9606,
                    "ensemblName": "Homo_sapiens",
                    "commonName": "Human",
                    "scientificName": "Homo sapiens",
                    "order": 1,
                    "count": 3
                }
            ]
        }
        # make sure it returns 6 organisms
        json_output = json.loads(output)
        assert len(json_output['organismsGroupCount']) == 6
        # and pick up the first element to check if it matches the expected output
        # I picked up only the first element for the sake of shortening the code
        assert json_output['organismsGroupCount'][0] == expected_output['organismsGroupCount'][0]
