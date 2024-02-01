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

db_directory = Path(__file__).parent / 'databases'
db_directory = db_directory.resolve()


@pytest.mark.parametrize("multi_dbs", [[{'src': 'ensembl_metadata'}, {'src': 'ncbi_taxonomy'}]], indirect=True)
class TestUtils:
    dbc = None  # type: UnitTestDB

    @pytest.mark.parametrize(
        "taxon_id, expected_output",
        [
            # e-coli
            (
                    562,
                    [
                        "Bacillus coli", "Bacterium coli", "Bacterium coli commune",
                        "E. coli", "Enterococcus coli", "Escherichia/Shigella coli"
                    ]
            ),
            # wheat
            (
                    4565,
                    [
                        'Canadian hard winter wheat', 'Triticum aestivum subsp. aestivum',
                        'Triticum vulgare', 'bread wheat', 'common wheat', 'wheat'
                    ]
            ),
            # human
            (9606, ["human"]),
            # non-existent
            (100, []),
        ]
    )
    def test_get_alternative_names(self, genome_db_conn, taxon_id, expected_output):
        output = utils.get_alternative_names(genome_db_conn, taxon_id)
        assert output == expected_output

    def test_get_assembly_information(self, genome_db_conn):
        output = json_format.MessageToJson(
            utils.get_assembly_information(genome_db_conn, "fd7fea38-981a-4d73-a879-6f9daef86f08"))
        expected_output = {
            "accession": "GCA_000001405.29",
            "assemblyUuid": "fd7fea38-981a-4d73-a879-6f9daef86f08",
            # "chromosomal": 1,
            "length": "71251",
            "level": "chromosome",
            "name": "GRCh38.p13",
            "sequenceLocation": "SO:0000738"
        }
        assert json.loads(output) == expected_output

    def test_get_genomes_from_assembly_accession_iterator(self, genome_db_conn):
        output = [
            json.loads(json_format.MessageToJson(response)) for response in
            utils.get_genomes_from_assembly_accession_iterator(
                db_conn=genome_db_conn, assembly_accession="GCA_000005845.2", release_version=None
            )
        ]

        expected_output = [{
            'assembly': {
                'accession': 'GCA_000005845.2',
                'assemblyUuid': '532aa68f-6500-404e-a470-8afb718a770a',
                'ensemblName': 'ASM584v2',
                'isReference': True,
                'level': 'chromosome',
                'name': 'ASM584v2',
                'urlName': 'asm584v2'
            },
            'attributesInfo': {},
            'created': '2023-09-22 15:01:44',
            'genomeUuid': 'a73351f7-93e7-11ec-a39d-005056b38ce3',
            'organism': {
                'commonName': 'Escherichia coli K-12',
                'ensemblName': 'SAMN02604091',
                'organismUuid': '1e579f8d-3880-424e-9b4f-190eb69280d9',
                'scientificName': 'Escherichia coli str. K-12 substr. MG1655 '
                                  'str. K12',
                'scientificParlanceName': 'E coli K 12',
                'speciesTaxonomyId': 562,
                'strain': 'K-12 substr. MG1655',
                'strainType': 'strain',
                'taxonomyId': 511145
            },
            'release': {
                'isCurrent': True,
                'releaseDate': '2023-10-18',
                'releaseLabel': 'beta-1',
                'releaseVersion': 110.1,
                'siteLabel': 'MVP Ensembl',
                'siteName': 'Ensembl',
                'siteUri': 'https://beta.ensembl.org'
            },
            'taxon': {
                'scientificName': 'Escherichia coli str. K-12 substr. MG1655 str. '
                                  'K12',
                'strain': 'K-12 substr. MG1655',
                'taxonomyId': 511145
            }
        }]
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
    def test_get_genomes_from_assembly_accession_iterator_null(self, genome_db_conn, assembly_accession,
                                                               release_version):
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
        assert len(first_genome_stats) == 80
        assert first_genome_stats[0] == {
            'label': 'Average CDS length',
            'name': 'average_cds_length',
            'statisticType': 'bp',
            'statisticValue': '1332.42'
        }
        assert first_genome_stats[1] == {
            'label': 'Average coding exons per transcript',
            'name': 'average_coding_exons_per_coding_transcript',
            'statisticType': 'float',
            'statisticValue': '5.34'
        }

    # assert first_genome_stats[1] == {
    #	'label': 'Average exon length per coding gene',
    #	'name': 'average_coding_exon_length',
    #	'statisticType': 'bp',
    #	'statisticValue': '249.47'
    # }

    def test_get_top_level_statistics_by_uuid(self, genome_db_conn):
        output = json_format.MessageToJson(
            utils.get_top_level_statistics_by_uuid(
                genome_db_conn, "a73357ab-93e7-11ec-a39d-005056b38ce3"
            )
        )
        output = json.loads(output)
        assert len(output["statistics"]) == 80
        assert output["statistics"][0] == {
            'label': 'Average CDS length',
            'name': 'average_cds_length',
            'statisticType': 'bp',
            'statisticValue': '1332.42'
        }
        assert output["statistics"][2] == {
            'label': 'Average exon length per coding gene',
            'name': 'average_coding_exon_length',
            'statisticType': 'bp',
            'statisticValue': '249.47'
        }

    # assert output["statistics"][2] == {
    #	'label': 'Average coding exons per transcript',
    #	'name': 'average_coding_exons_per_coding_transcript',
    #	'statisticType': 'float',
    #	'statisticValue': '5.34'
    # }

    def test_get_datasets_list_by_uuid(self, genome_db_conn):
        # the expected_output is too long and duplicated
        # because of the returned attributes
        # TODO: Fix this later
        output = json_format.MessageToJson(
            utils.get_datasets_list_by_uuid(genome_db_conn, "a73357ab-93e7-11ec-a39d-005056b38ce3", 110.1))

        expected_output = {
            'datasets': {
                'assembly': {
                    'datasetInfos': [{
                        'datasetLabel': 'GCA_900519105.1',
                        'datasetName': 'assembly',
                        'datasetUuid': '999315f6-6d25-481f-a017-297f7e1490c8',
                        'version': 110.0
                    },
                        {
                            'datasetLabel': 'GCA_900519105.1',
                            'datasetName': 'assembly',
                            'datasetUuid': '999315f6-6d25-481f-a017-297f7e1490c8',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1',
                            'datasetName': 'assembly',
                            'datasetUuid': '999315f6-6d25-481f-a017-297f7e1490c8',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1',
                            'datasetName': 'assembly',
                            'datasetUuid': '999315f6-6d25-481f-a017-297f7e1490c8',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1',
                            'datasetName': 'assembly',
                            'datasetUuid': '999315f6-6d25-481f-a017-297f7e1490c8',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1',
                            'datasetName': 'assembly',
                            'datasetUuid': '999315f6-6d25-481f-a017-297f7e1490c8',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1',
                            'datasetName': 'assembly',
                            'datasetUuid': '999315f6-6d25-481f-a017-297f7e1490c8',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1',
                            'datasetName': 'assembly',
                            'datasetUuid': '999315f6-6d25-481f-a017-297f7e1490c8',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1',
                            'datasetName': 'assembly',
                            'datasetUuid': '999315f6-6d25-481f-a017-297f7e1490c8',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1',
                            'datasetName': 'assembly',
                            'datasetUuid': '999315f6-6d25-481f-a017-297f7e1490c8',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1',
                            'datasetName': 'assembly',
                            'datasetUuid': '999315f6-6d25-481f-a017-297f7e1490c8',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1',
                            'datasetName': 'assembly',
                            'datasetUuid': '999315f6-6d25-481f-a017-297f7e1490c8',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1',
                            'datasetName': 'assembly',
                            'datasetUuid': '999315f6-6d25-481f-a017-297f7e1490c8',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1',
                            'datasetName': 'assembly',
                            'datasetUuid': '999315f6-6d25-481f-a017-297f7e1490c8',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1',
                            'datasetName': 'assembly',
                            'datasetUuid': '999315f6-6d25-481f-a017-297f7e1490c8',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1',
                            'datasetName': 'assembly',
                            'datasetUuid': '999315f6-6d25-481f-a017-297f7e1490c8',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1',
                            'datasetName': 'assembly',
                            'datasetUuid': '999315f6-6d25-481f-a017-297f7e1490c8',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1',
                            'datasetName': 'assembly',
                            'datasetUuid': '999315f6-6d25-481f-a017-297f7e1490c8',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1',
                            'datasetName': 'assembly',
                            'datasetUuid': '999315f6-6d25-481f-a017-297f7e1490c8',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1',
                            'datasetName': 'assembly',
                            'datasetUuid': '999315f6-6d25-481f-a017-297f7e1490c8',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1',
                            'datasetName': 'assembly',
                            'datasetUuid': '999315f6-6d25-481f-a017-297f7e1490c8',
                            'version': 110.0
                        }]
                },
                'genebuild': {
                    'datasetInfos': [{
                        'datasetLabel': 'GCA_900519105.1_EXT01',
                        'datasetName': 'genebuild',
                        'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                        'datasetVersion': 'EXT01',
                        'version': 110.0
                    },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        },
                        {
                            'datasetLabel': 'GCA_900519105.1_EXT01',
                            'datasetName': 'genebuild',
                            'datasetUuid': '287a5483-55a4-46e6-a58b-a84ba0ddacd6',
                            'datasetVersion': 'EXT01',
                            'version': 110.0
                        }]
                },
                'homologies': {
                    'datasetInfos': [{
                        'datasetLabel': 'Compara '
                                        'homologies',
                        'datasetName': 'compara_homologies',
                        'datasetUuid': '9f45f1a6-d4d0-4c02-9509-dec5a0d523fb',
                        'datasetVersion': '1.0',
                        'version': 110.0
                    },
                        {
                            'datasetLabel': 'Compara '
                                            'homologies',
                            'datasetName': 'compara_homologies',
                            'datasetUuid': '9f45f1a6-d4d0-4c02-9509-dec5a0d523fb',
                            'datasetVersion': '1.0',
                            'version': 110.0
                        }]
                },
                'variation': {
                    'datasetInfos': [{
                        'datasetLabel': 'IWGSC',
                        'datasetName': 'variation',
                        'datasetUuid': 'e659bef9-22f7-4ad2-8215-4a48ecd228df',
                        'datasetVersion': '1.0',
                        'version': 110.0
                    },
                        {
                            'datasetLabel': 'IWGSC',
                            'datasetName': 'variation',
                            'datasetUuid': 'e659bef9-22f7-4ad2-8215-4a48ecd228df',
                            'datasetVersion': '1.0',
                            'version': 110.0
                        }]
                }
            },
            'genomeUuid': 'a73357ab-93e7-11ec-a39d-005056b38ce3'

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
        assert output == {
            'datasetInfos': [{
                'datasetLabel': 'GCA_000001405.29',
                'datasetName': 'assembly',
                'datasetUuid': 'c813f7b7-645c-45ac-8536-08190fd7daa0',
                'name': 'assembly.accession',
                'type': 'string',
                'value': 'GCA_000001405.29',
                'version': 110.0
            },
                {
                    'datasetLabel': 'GCA_000001405.29',
                    'datasetName': 'assembly',
                    'datasetUuid': 'c813f7b7-645c-45ac-8536-08190fd7daa0',
                    'name': 'assembly.chromosomes',
                    'type': 'string',
                    'value': '25',
                    'version': 110.0
                },
                {
                    'datasetLabel': 'GCA_000001405.29',
                    'datasetName': 'assembly',
                    'datasetUuid': 'c813f7b7-645c-45ac-8536-08190fd7daa0',
                    'name': 'assembly.component_sequences',
                    'type': 'string',
                    'value': '36829',
                    'version': 110.0
                },
                {
                    'datasetLabel': 'GCA_000001405.29',
                    'datasetName': 'assembly',
                    'datasetUuid': 'c813f7b7-645c-45ac-8536-08190fd7daa0',
                    'name': 'assembly.contig_n50',
                    'type': 'string',
                    'value': '54806562',
                    'version': 110.0
                },
                {
                    'datasetLabel': 'GCA_000001405.29',
                    'datasetName': 'assembly',
                    'datasetUuid': 'c813f7b7-645c-45ac-8536-08190fd7daa0',
                    'name': 'assembly.coverage_depth',
                    'type': 'string',
                    'value': 'high',
                    'version': 110.0
                },
                {
                    'datasetLabel': 'GCA_000001405.29',
                    'datasetName': 'assembly',
                    'datasetUuid': 'c813f7b7-645c-45ac-8536-08190fd7daa0',
                    'name': 'assembly.date',
                    'type': 'string',
                    'value': '2013-12',
                    'version': 110.0
                },
                {
                    'datasetLabel': 'GCA_000001405.29',
                    'datasetName': 'assembly',
                    'datasetUuid': 'c813f7b7-645c-45ac-8536-08190fd7daa0',
                    'name': 'assembly.default',
                    'type': 'string',
                    'value': 'GRCh38',
                    'version': 110.0
                },
                {
                    'datasetLabel': 'GCA_000001405.29',
                    'datasetName': 'assembly',
                    'datasetUuid': 'c813f7b7-645c-45ac-8536-08190fd7daa0',
                    'name': 'assembly.gc_percentage',
                    'type': 'string',
                    'value': '38.88',
                    'version': 110.0
                },
                {
                    'datasetLabel': 'GCA_000001405.29',
                    'datasetName': 'assembly',
                    'datasetUuid': 'c813f7b7-645c-45ac-8536-08190fd7daa0',
                    'name': 'assembly.is_reference',
                    'type': 'string',
                    'value': '1',
                    'version': 110.0
                },
                {
                    'datasetLabel': 'GCA_000001405.29',
                    'datasetName': 'assembly',
                    'datasetUuid': 'c813f7b7-645c-45ac-8536-08190fd7daa0',
                    'name': 'assembly.level',
                    'type': 'string',
                    'value': 'chromosome',
                    'version': 110.0
                },
                {
                    'datasetLabel': 'GCA_000001405.29',
                    'datasetName': 'assembly',
                    'datasetUuid': 'c813f7b7-645c-45ac-8536-08190fd7daa0',
                    'name': 'assembly.long_name',
                    'type': 'string',
                    'value': 'Genome Reference Consortium Human Build 38',
                    'version': 110.0
                },
                {
                    'datasetLabel': 'GCA_000001405.29',
                    'datasetName': 'assembly',
                    'datasetUuid': 'c813f7b7-645c-45ac-8536-08190fd7daa0',
                    'name': 'assembly.mapping',
                    'type': 'string',
                    'value': 'scaffold:GRCh38#contig|clone',
                    'version': 110.0
                },
                {
                    'datasetLabel': 'GCA_000001405.29',
                    'datasetName': 'assembly',
                    'datasetUuid': 'c813f7b7-645c-45ac-8536-08190fd7daa0',
                    'name': 'assembly.name',
                    'type': 'string',
                    'value': 'GRCh38.p14',
                    'version': 110.0
                },
                {
                    'datasetLabel': 'GCA_000001405.29',
                    'datasetName': 'assembly',
                    'datasetUuid': 'c813f7b7-645c-45ac-8536-08190fd7daa0',
                    'name': 'assembly.provider_name',
                    'type': 'string',
                    'value': 'Genome Reference Consortium',
                    'version': 110.0
                },
                {
                    'datasetLabel': 'GCA_000001405.29',
                    'datasetName': 'assembly',
                    'datasetUuid': 'c813f7b7-645c-45ac-8536-08190fd7daa0',
                    'name': 'assembly.provider_url',
                    'type': 'string',
                    'value': 'https://www.ncbi.nlm.nih.gov/grc',
                    'version': 110.0
                },
                {
                    'datasetLabel': 'GCA_000001405.29',
                    'datasetName': 'assembly',
                    'datasetUuid': 'c813f7b7-645c-45ac-8536-08190fd7daa0',
                    'name': 'assembly.spanned_gaps',
                    'type': 'string',
                    'value': '663',
                    'version': 110.0
                },
                {
                    'datasetLabel': 'GCA_000001405.29',
                    'datasetName': 'assembly',
                    'datasetUuid': 'c813f7b7-645c-45ac-8536-08190fd7daa0',
                    'name': 'assembly.tolid',
                    'type': 'string',
                    'version': 110.0
                },
                {
                    'datasetLabel': 'GCA_000001405.29',
                    'datasetName': 'assembly',
                    'datasetUuid': 'c813f7b7-645c-45ac-8536-08190fd7daa0',
                    'name': 'assembly.toplevel_sequences',
                    'type': 'string',
                    'value': '709',
                    'version': 110.0
                },
                {
                    'datasetLabel': 'GCA_000001405.29',
                    'datasetName': 'assembly',
                    'datasetUuid': 'c813f7b7-645c-45ac-8536-08190fd7daa0',
                    'name': 'assembly.total_coding_sequence_length',
                    'type': 'string',
                    'value': '34493611',
                    'version': 110.0
                },
                {
                    'datasetLabel': 'GCA_000001405.29',
                    'datasetName': 'assembly',
                    'datasetUuid': 'c813f7b7-645c-45ac-8536-08190fd7daa0',
                    'name': 'assembly.total_gap_length',
                    'type': 'string',
                    'value': '161611139',
                    'version': 110.0
                },
                {
                    'datasetLabel': 'GCA_000001405.29',
                    'datasetName': 'assembly',
                    'datasetUuid': 'c813f7b7-645c-45ac-8536-08190fd7daa0',
                    'name': 'assembly.total_genome_length',
                    'type': 'string',
                    'value': '3298912062',
                    'version': 110.0
                },
                {
                    'datasetLabel': 'GCA_000001405.29',
                    'datasetName': 'assembly',
                    'datasetUuid': 'c813f7b7-645c-45ac-8536-08190fd7daa0',
                    'name': 'assembly.ucsc_alias',
                    'type': 'string',
                    'value': 'hg38',
                    'version': 110.0
                },
                {
                    'datasetLabel': 'GCA_000001405.29',
                    'datasetName': 'assembly',
                    'datasetUuid': 'c813f7b7-645c-45ac-8536-08190fd7daa0',
                    'name': 'assembly.url_name',
                    'type': 'string',
                    'value': 'GRCh38',
                    'version': 110.0
                },
                {
                    'datasetLabel': 'GCA_000001405.29',
                    'datasetName': 'assembly',
                    'datasetUuid': 'c813f7b7-645c-45ac-8536-08190fd7daa0',
                    'name': 'assembly.web_accession_source',
                    'type': 'string',
                    'value': 'NCBI',
                    'version': 110.0
                },
                {
                    'datasetLabel': 'GCA_000001405.29',
                    'datasetName': 'assembly',
                    'datasetUuid': 'c813f7b7-645c-45ac-8536-08190fd7daa0',
                    'name': 'assembly.web_accession_type',
                    'type': 'string',
                    'value': 'GenBank Assembly ID',
                    'version': 110.0
                }],
            'datasetType': 'assembly',
            'genomeUuid': 'a7335667-93e7-11ec-a39d-005056b38ce3'
        }

    def test_get_dataset_by_genome_id_no_results(self, genome_db_conn):
        output = json_format.MessageToJson(
            utils.get_dataset_by_genome_and_dataset_type(genome_db_conn, "a7335667-93e7-11ec-a39d-005056b38ce3",
                                                         "blah blah blah"))
        output = json.loads(output)
        assert output == {}

    @pytest.mark.parametrize(
        "production_name, assembly_name, use_default, expected_output",
        [
            ("homo_sapiens", "GRCh38.p13", False, {"genomeUuid": "a7335667-93e7-11ec-a39d-005056b38ce3"}),
            ("homo_sapiens", "GRCh38.p13", True, {}),
            ("homo_sapiens", "GRCh38", True, {"genomeUuid": "a7335667-93e7-11ec-a39d-005056b38ce3"}),
            ("random_production_name", "random_assembly_name", True, {}),
            ("random_production_name", "random_assembly_name", False, {}),
        ]
    )
    def test_get_genome_uuid(self, genome_db_conn, production_name, assembly_name, use_default, expected_output):
        output = json_format.MessageToJson(
            utils.get_genome_uuid(
                db_conn=genome_db_conn,
                production_name=production_name,
                assembly_name=assembly_name,
                use_default=use_default
            ))
        assert json.loads(output) == expected_output

    def test_get_genome_by_uuid(self, genome_db_conn):
        output = json_format.MessageToJson(
            utils.get_genome_by_uuid(
                db_conn=genome_db_conn,
                genome_uuid="a73357ab-93e7-11ec-a39d-005056b38ce3",
                release_version=110.1
            ))
        expected_output = {
            "assembly": {
                "accession": "GCA_900519105.1",
                "ensemblName": "IWGSC",
                "assemblyUuid": "ec1c4b53-c2ef-431c-ad0e-b2aef19b44f1",
                "level": "chromosome",
                "name": "IWGSC"
            },
            "attributesInfo": {},
            "created": "2023-05-12 13:32:36",
            "genomeUuid": "a73357ab-93e7-11ec-a39d-005056b38ce3",
            "organism": {
                "commonName": "Triticum aestivum",
                "ensemblName": "Triticum_aestivum",
                "organismUuid": "d64c34ca-b37a-476b-83b5-f21d07a3ae67",
                "scientificName": "Triticum aestivum",
                "scientificParlanceName": "triticum_aestivum",
                "speciesTaxonomyId": 4565,
                "taxonomyId": 4565,
                "strain": "reference (Chinese spring)"
            },
            "relatedAssembliesCount": 1,
            "release": {
                "isCurrent": True,
                "releaseDate": "2023-05-15",
                "releaseLabel": "Beta Release 1",
                "releaseVersion": 110.1,
                "siteLabel": "Ensembl Genome Browser",
                "siteName": "Ensembl",
                "siteUri": "https://beta.ensembl.org"
            },
            "taxon": {
                "alternativeNames": [
                    "Canadian hard winter wheat",
                    "Triticum aestivum subsp. aestivum",
                    "Triticum vulgare",
                    "bread wheat",
                    "common wheat",
                    "wheat"
                ],
                "scientificName": "Triticum aestivum",
                "strain": "reference (Chinese spring)",
                "taxonomyId": 4565
            }
        }
        assert json.loads(output) == expected_output

    def test_genome_by_uuid_release_version_unspecified(self, genome_db_conn):
        output = json_format.MessageToJson(
            utils.get_genome_by_uuid(
                db_conn=genome_db_conn,
                genome_uuid="a73357ab-93e7-11ec-a39d-005056b38ce3",
                release_version=None
            ))
        expected_output = {
            "assembly": {
                "accession": "GCA_900519105.1",
                "ensemblName": "IWGSC",
                "assemblyUuid": "ec1c4b53-c2ef-431c-ad0e-b2aef19b44f1",
                "level": "chromosome",
                "name": "IWGSC"
            },
            "attributesInfo": {},
            "created": "2023-05-12 13:32:36",
            "genomeUuid": "a73357ab-93e7-11ec-a39d-005056b38ce3",
            "organism": {
                "commonName": "Triticum aestivum",
                "ensemblName": "Triticum_aestivum",
                "organismUuid": "d64c34ca-b37a-476b-83b5-f21d07a3ae67",
                "scientificName": "Triticum aestivum",
                "scientificParlanceName": "triticum_aestivum",
                "speciesTaxonomyId": 4565,
                "taxonomyId": 4565,
                "strain": "reference (Chinese spring)"
            },
            "relatedAssembliesCount": 1,
            "release": {
                "isCurrent": True,
                "releaseDate": "2023-05-15",
                "releaseLabel": "Beta Release 1",
                "releaseVersion": 110.1,
                "siteLabel": "Ensembl Genome Browser",
                "siteName": "Ensembl",
                "siteUri": "https://beta.ensembl.org"
            },
            "taxon": {
                "alternativeNames": [
                    "Canadian hard winter wheat",
                    "Triticum aestivum subsp. aestivum",
                    "Triticum vulgare",
                    "bread wheat",
                    "common wheat",
                    "wheat"
                ],
                "scientificName": "Triticum aestivum",
                "strain": "reference (Chinese spring)",
                "taxonomyId": 4565
            }
        }
        assert json.loads(output) == expected_output

    def test_get_genomes_by_uuid_null(self, genome_db_conn):
        output = utils.get_genome_by_uuid(genome_db_conn, None, 0)
        assert output == ensembl_metadata_pb2.Genome()

    def test_get_genomes_by_keyword(self, genome_db_conn):
        output = [json.loads(json_format.MessageToJson(response)) for response in
                  utils.get_genomes_by_keyword_iterator(genome_db_conn, "Human", 110.1)]
        expected_output = [
            {
                "assembly": {
                    "accession": "GCA_000001405.28",
                    "ensemblName": "GRCh38.p13",
                    "assemblyUuid": "eeaaa2bf-151c-4848-8b85-a05a9993101e",
                    "level": "chromosome",
                    "isReference": True,
                    "name": "GRCh38.p13",
                    "ucscName": "hg38",
                    "urlName": "GRCh38"
                },
                "attributesInfo": {},
                "created": "2023-05-12 13:30:58",
                "genomeUuid": "a7335667-93e7-11ec-a39d-005056b38ce3",
                "organism": {
                    "commonName": "Human",
                    "ensemblName": "Homo_sapiens",
                    "organismUuid": "db2a5f09-2db8-429b-a407-c15a4ca2876d",
                    "scientificName": "Homo sapiens",
                    "speciesTaxonomyId": 9606,
                    "taxonomyId": 9606,
                    "scientificParlanceName": "homo_sapiens"
                },
                "release": {
                    "isCurrent": True,
                    "releaseDate": "2023-05-15",
                    "releaseLabel": "Beta Release 1",
                    "releaseVersion": 110.1,
                    "siteLabel": "Ensembl Genome Browser",
                    "siteName": "Ensembl",
                    "siteUri": "https://beta.ensembl.org"
                },
                "taxon": {
                    "scientificName": "Homo sapiens",
                    "taxonomyId": 9606
                },
            },
            {
                "assembly": {
                    "accession": "GCA_000001405.14",
                    "ensemblName": "GRCh37.p13",
                    "assemblyUuid": "633034c3-2268-40a2-866a-9f492cac84bf",
                    "level": "chromosome",
                    "name": "GRCh37.p13",
                    "ucscName": "hg19",
                    "urlName": "GRCh37"
                },
                "attributesInfo": {},
                "created": "2023-05-12 13:32:06",
                "genomeUuid": "3704ceb1-948d-11ec-a39d-005056b38ce3",
                "organism": {
                    "commonName": "Human",
                    "ensemblName": "Homo_sapiens",
                    "organismUuid": "db2a5f09-2db8-429b-a407-c15a4ca2876d",
                    "scientificName": "Homo sapiens",
                    "speciesTaxonomyId": 9606,
                    "taxonomyId": 9606,
                    "scientificParlanceName": "homo_sapiens"
                },
                "release": {
                    "isCurrent": True,
                    "releaseDate": "2023-05-15",
                    "releaseLabel": "Beta Release 1",
                    "releaseVersion": 110.1,
                    "siteLabel": "Ensembl Genome Browser",
                    "siteName": "Ensembl",
                    "siteUri": "https://beta.ensembl.org"
                },
                "taxon": {
                    "scientificName": "Homo sapiens",
                    "taxonomyId": 9606
                }
            }
        ]
        assert output == expected_output

    def test_get_genomes_by_keyword_release_unspecified(self, genome_db_conn):
        output = [json.loads(json_format.MessageToJson(response)) for response in
                  utils.get_genomes_by_keyword_iterator(genome_db_conn, "Homo Sapiens", 0.0)]
        # TODO: DRY the expected_output
        expected_output = [
            {
                "assembly": {
                    "accession": "GCA_000001405.28",
                    "ensemblName": "GRCh38.p13",
                    "assemblyUuid": "eeaaa2bf-151c-4848-8b85-a05a9993101e",
                    "level": "chromosome",
                    "isReference": True,
                    "name": "GRCh38.p13",
                    "ucscName": "hg38",
                    "urlName": "GRCh38"
                },
                "attributesInfo": {},
                "created": "2023-05-12 13:30:58",
                "genomeUuid": "a7335667-93e7-11ec-a39d-005056b38ce3",
                "organism": {
                    "commonName": "Human",
                    "ensemblName": "Homo_sapiens",
                    "organismUuid": "db2a5f09-2db8-429b-a407-c15a4ca2876d",
                    "scientificName": "Homo sapiens",
                    "speciesTaxonomyId": 9606,
                    "taxonomyId": 9606,
                    "scientificParlanceName": "homo_sapiens"
                },
                "release": {
                    "isCurrent": True,
                    "releaseDate": "2023-05-15",
                    "releaseLabel": "Beta Release 1",
                    "releaseVersion": 110.1,
                    "siteLabel": "Ensembl Genome Browser",
                    "siteName": "Ensembl",
                    "siteUri": "https://beta.ensembl.org"
                },
                "taxon": {
                    "scientificName": "Homo sapiens",
                    "taxonomyId": 9606
                },
            },
            {
                "assembly": {
                    "accession": "GCA_000001405.14",
                    "ensemblName": "GRCh37.p13",
                    "assemblyUuid": "633034c3-2268-40a2-866a-9f492cac84bf",
                    "level": "chromosome",
                    "name": "GRCh37.p13",
                    "ucscName": "hg19",
                    "urlName": "GRCh37"
                },
                "attributesInfo": {},
                "created": "2023-05-12 13:32:06",
                "genomeUuid": "3704ceb1-948d-11ec-a39d-005056b38ce3",
                "organism": {
                    "commonName": "Human",
                    "ensemblName": "Homo_sapiens",
                    "organismUuid": "db2a5f09-2db8-429b-a407-c15a4ca2876d",
                    "scientificName": "Homo sapiens",
                    "speciesTaxonomyId": 9606,
                    "taxonomyId": 9606,
                    "scientificParlanceName": "homo_sapiens"
                },
                "release": {
                    "isCurrent": True,
                    "releaseDate": "2023-05-15",
                    "releaseLabel": "Beta Release 1",
                    "releaseVersion": 110.1,
                    "siteLabel": "Ensembl Genome Browser",
                    "siteName": "Ensembl",
                    "siteUri": "https://beta.ensembl.org"
                },
                "taxon": {
                    "scientificName": "Homo sapiens",
                    "taxonomyId": 9606
                }
            }
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
            release_version=110.1
        ))
        expected_output = {
            "assembly": {
                "accession": "GCA_900519105.1",
                "ensemblName": "IWGSC",
                "assemblyUuid": "ec1c4b53-c2ef-431c-ad0e-b2aef19b44f1",
                "level": "chromosome",
                "name": "IWGSC"
            },
            "attributesInfo": {},
            "created": "2023-05-12 13:32:36",
            "genomeUuid": "a73357ab-93e7-11ec-a39d-005056b38ce3",
            "organism": {
                "commonName": "Triticum aestivum",
                "ensemblName": "Triticum_aestivum",
                "organismUuid": "d64c34ca-b37a-476b-83b5-f21d07a3ae67",
                "scientificName": "Triticum aestivum",
                "scientificParlanceName": "triticum_aestivum",
                "speciesTaxonomyId": 4565,
                "taxonomyId": 4565,
                "strain": "reference (Chinese spring)"
            },
            "relatedAssembliesCount": 1,
            "release": {
                "isCurrent": True,
                "releaseDate": "2023-05-15",
                "releaseLabel": "Beta Release 1",
                "releaseVersion": 110.1,
                "siteLabel": "Ensembl Genome Browser",
                "siteName": "Ensembl",
                "siteUri": "https://beta.ensembl.org"
            },
            "taxon": {
                "alternativeNames": [
                    "Canadian hard winter wheat",
                    "Triticum aestivum subsp. aestivum",
                    "Triticum vulgare",
                    "bread wheat",
                    "common wheat",
                    "wheat"
                ],
                "scientificName": "Triticum aestivum",
                "strain": "reference (Chinese spring)",
                "taxonomyId": 4565
            }
        }
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
        expected_output = {
            "assembly": {
                "accession": "GCA_900519105.1",
                "ensemblName": "IWGSC",
                "assemblyUuid": "ec1c4b53-c2ef-431c-ad0e-b2aef19b44f1",
                "level": "chromosome",
                "name": "IWGSC"
            },
            "attributesInfo": {},
            "created": "2023-05-12 13:32:36",
            "genomeUuid": "a73357ab-93e7-11ec-a39d-005056b38ce3",
            "organism": {
                "commonName": "Triticum aestivum",
                "ensemblName": "Triticum_aestivum",
                "organismUuid": "d64c34ca-b37a-476b-83b5-f21d07a3ae67",
                "scientificName": "Triticum aestivum",
                "scientificParlanceName": "triticum_aestivum",
                "speciesTaxonomyId": 4565,
                "taxonomyId": 4565,
                "strain": "reference (Chinese spring)"
            },
            "relatedAssembliesCount": 1,
            "release": {
                "isCurrent": True,
                "releaseDate": "2023-05-15",
                "releaseLabel": "Beta Release 1",
                "releaseVersion": 110.1,
                "siteLabel": "Ensembl Genome Browser",
                "siteName": "Ensembl",
                "siteUri": "https://beta.ensembl.org"
            },
            "taxon": {
                "alternativeNames": [
                    "Canadian hard winter wheat",
                    "Triticum aestivum subsp. aestivum",
                    "Triticum vulgare",
                    "bread wheat",
                    "common wheat",
                    "wheat"
                ],
                "scientificName": "Triticum aestivum",
                "strain": "reference (Chinese spring)",
                "taxonomyId": 4565
            }
        }
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

    @pytest.mark.parametrize(
        "genome_tag, expected_output",
        [
            # url_name = GRCh38 => homo_sapien 38
            ("GRCh38", {"genomeUuid": "a7335667-93e7-11ec-a39d-005056b38ce3"}),
            # Null
            ("iDontExist", {}),
        ]
    )
    def test_get_genome_uuid_by_tag(self, genome_db_conn, genome_tag, expected_output):
        output = json_format.MessageToJson(
            utils.get_genome_uuid_by_tag(
                db_conn=genome_db_conn,
                genome_tag=genome_tag,
            ))
        assert json.loads(output) == expected_output

    @pytest.mark.parametrize(
        "genome_uuid, dataset_type, release_version, expected_output",
        [
            # valid genome uuid and no dataset should return all the datasets links of that genome uuid
            ("a733574a-93e7-11ec-a39d-005056b38ce3", 'all', None, {
                "Links": [
                    "Saccharomyces_cerevisiae_S288c/GCA_000146045.2/test_anno_source/genome",
                    "Saccharomyces_cerevisiae_S288c/GCA_000146045.2/test_anno_source/regulation",
                    "Saccharomyces_cerevisiae_S288c/GCA_000146045.2/test_anno_source/variation/test_version",
                    "Saccharomyces_cerevisiae_S288c/GCA_000146045.2/test_anno_source/homology/test_version",
                    "Saccharomyces_cerevisiae_S288c/GCA_000146045.2/test_anno_source/genebuild/test_version"
                ]
            }),

            # valid genome uuid and a valid dataset should return corresponding dataset link
            ("a733574a-93e7-11ec-a39d-005056b38ce3", 'assembly', None, {
                "Links": [
                    "Saccharomyces_cerevisiae_S288c/GCA_000146045.2/test_anno_source/genome",
                    "Saccharomyces_cerevisiae_S288c/GCA_000146045.2/test_anno_source/genebuild/test_version"
                ]
            }),

            # invalid genome uuid should return no dataset links
            ("a73351f7-93e7-11ec-a39d-", "assembly", None, {}),

            # valid genome uuid and invalid dataset should return no dataset links
            ("a73351f7-93e7-11ec-a39d-005056b38ce3", "test_dataset", None, {}),

            # no genome uuid should return no dataset links
            (None, "test_dataset", None, {})
        ]
    )
    def test_ftp_links(self, genome_db_conn, genome_uuid, dataset_type, release_version, expected_output):
        output = json_format.MessageToJson(
            utils.get_ftp_links(
                db_conn=genome_db_conn,
                genome_uuid=genome_uuid,
                dataset_type=dataset_type,
                release_version=release_version
            )
        )
        assert sorted(json.loads(output)) == sorted(expected_output)

    @pytest.mark.parametrize(
        "genome_uuid, dataset_type, release_version, expected_output",
        [
            # genome_uuid only
            ("a73351f7-93e7-11ec-a39d-005056b38ce3", None, None, {"releaseVersion": 110.1}),
            # wrong genome_uuid
            ("some-random-genome-uuid-000000000000", None, None, {}),
            # genome_uuid and data_type_name
            ("a73351f7-93e7-11ec-a39d-005056b38ce3", "genebuild", None, {"releaseVersion": 110.1}),
            # genome_uuid and release_version
            ("a73351f7-93e7-11ec-a39d-005056b38ce3", "genebuild", 111.1, {"releaseVersion": 110.1}),
            # genome_uuid, data_type_name and release_version
            ("a73351f7-93e7-11ec-a39d-005056b38ce3", "genebuild", 111.1, {"releaseVersion": 110.1}),
            # no genome_uuid
            (None, "genebuild", 111.1, {}),
            # empty params
            (None, None, None, {}),
        ]
    )
    def test_get_release_version_by_uuid(self, genome_db_conn, genome_uuid, dataset_type, release_version,
                                         expected_output):
        output = json_format.MessageToJson(
            utils.get_release_version_by_uuid(
                db_conn=genome_db_conn,
                genome_uuid=genome_uuid,
                dataset_type=dataset_type,
                release_version=release_version
            ))
        assert json.loads(output) == expected_output
