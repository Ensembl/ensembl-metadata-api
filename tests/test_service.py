#  See the NOTICE file distributed with this work for additional information
#  regarding copyright ownership.
#
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""
Unit tests for service module
"""
import sqlalchemy as db
import json
from google.protobuf import json_format

import sqlite3
from tempfile import TemporaryDirectory

from ensembl.production.metadata import service


class TestClass:
    dirpath = TemporaryDirectory()
    connection = sqlite3.connect(f'{dirpath.name}/test.db')
    cursor = connection.cursor()

    sql_file = open("sampledb.sql")

    sql_as_string = sql_file.read()
    cursor.executescript(sql_as_string)

    connection.commit()
    connection.close()

    try:
        engine = db.create_engine(f'sqlite:////{dirpath.name}/test.db')
    except AttributeError:
        raise ValueError(f'Could not connect to database. Check METADATA_URI env variable.')

    try:
        connection = engine.connect()
    except db.exc.OperationalError as err:
        raise ValueError(f'Could not connect to database: {err}.') from err

    def test_create_genome(self):
        """Test service.create_genome function"""
        input_dict = {
            'genome_uuid': 'f9d8c1dc-45dd-11ec-81d3-0242ac130003',
            'ensembl_name':  'some name',
            'url_name': 'http://url_name.com',
            'display_name': 'Display Name',
            'is_current': True,
            'assembly_accession': 'X.AE500',
            'assembly_name': 'assembly name',
            'assembly_ucsc_name': 'ucsc name',
            'assembly_level': 'level',
            'taxonomy_id': 1234,
            'scientific_name': 'scientific name',
            'scientific_parlance_name': 'scientific_parlance_name',
            'strain': 'test strain'
        }
        expected_output = {
            'genomeUuid': 'f9d8c1dc-45dd-11ec-81d3-0242ac130003',
            'ensemblName': 'some name',
            'urlName': 'http://url_name.com',
            'displayName': 'Display Name',
            'isCurrent': True,
            'assembly' : {
                'accession': 'X.AE500',
                'name': 'assembly name',
                'ucscName': 'ucsc name',
                'level': 'level'
            },
            'taxon': {
                'taxonomyId': 1234,
                'scientificName': 'scientific name',
                'strain': 'test strain'
            }
        }
        output = json_format.MessageToJson(service.create_genome(input_dict))
        assert json.loads(output) == expected_output


    def test_create_assembly(self):
        input_dict = {
            'assembly_id': '1234',
            'accession': 'XE.1234',
            'level': '5',
            'name': 'test name',
            'chromosomal':1223,
            'length': 5,
            'sequence_location': 'location',
            'sequence_checksum':'checksum',
            'ga4gh_identifier': 'test identifier'
        }

        expected_output = {
            'assemblyId': '1234',
            'accession': 'XE.1234',
            'level': '5',
            'name': 'test name',
            'chromosomal': 1223,
            'length': 5,
            'sequenceLocation': 'location',
            'sequenceChecksum': 'checksum',
            'ga4ghIdentifier': 'test identifier'
        }

        output = json_format.MessageToJson(service.create_assembly(input_dict))
        assert json.loads(output) == expected_output


    def test_create_karyotype(self):
        input_dict = {
            'genome_uuid': 'f9d8c1dc-45dd-11ec-81d3-0242ac130003',
            'code': '5',
            'chromosomal': '25',
            'location': '129729'
        }

        expected_output = {
            'genomeUuid': 'f9d8c1dc-45dd-11ec-81d3-0242ac130003',
            'code': '5',
            'chromosomal': '25',
            'location': '129729'
        }

        output = json_format.MessageToJson(service.create_karyotype(input_dict))
        assert json.loads(output) == expected_output


    def test_create_species(self):
        input_dict = {
            'genome_uuid': 'f9d8c1dc-45dd-11ec-81d3-0242ac130003',
            'common_name': 'cow',
            'ncbi_common_name': 'cattle',
            'scientific_name': 'Bos taurus',
            'alternative_names': ["bovine", "cow", "dairy cow", "domestic cattle", "domestic cow"],
            'scientific_parlance_name': 'Bos taurus',
            'taxonomy_id': 9913
        }

        expected_output = {
            'genomeUuid': 'f9d8c1dc-45dd-11ec-81d3-0242ac130003',
            'commonName': 'cow',
            'ncbiCommonName': 'cattle',
            'scientificName': 'Bos taurus',
            'scientificParlanceName': 'Bos taurus',
            'alternativeNames': ["bovine", "cow", "dairy cow", "domestic cattle", "domestic cow"],
            'taxonId': 9913
        }

        output = json_format.MessageToJson(service.create_species(input_dict))
        assert json.loads(output) == expected_output


    def test_create_top_level_statistics(self):
        input_dict = {
            'organism_id': '41',
            'statistics': [
                {
                    'name': "transcript_genomic_mnoncoding",
                    'label': "Non-coding transcript",
                    'statistic_type': "length_bp",
                    'statistic_value': "5873"
                },
                {
                    'name': "transcript_genomic_pseudogene",
                    'label': "Pseudogenic transcript",
                    'statistic_type': "length_bp",
                    'statistic_value': "3305648"
                }
            ]
        }

        expected_output = {
            'organismId': '41',
            'statistics': [{
                'label': 'Non-coding transcript',
                'name': 'transcript_genomic_mnoncoding',
                'statisticType': 'length_bp',
                'statisticValue': '5873'
            },
                {
                'label': 'Pseudogenic transcript',
                'name': 'transcript_genomic_pseudogene',
                'statisticType': 'length_bp',
                'statisticValue': '3305648'}
            ]
        }
        output = json_format.MessageToJson(service.create_top_level_statistics(input_dict))
        assert json.loads(output) == expected_output


    def test_create_genome_sequence(self):
        input_dict = {
            'accession': 'XQ1234',
            'name': 'test_seq',
            'sequence_location': 'some location',
            'length': 1234,
            'chromosomal': True
        }
        expected_output = {
            'accession': 'XQ1234',
            'chromosomal': True,
            'length': 1234,
            'name': 'test_seq',
            'sequenceLocation': 'some location',
        }
        output = json_format.MessageToJson(service.create_genome_sequence(input_dict))
        assert json.loads(output) == expected_output


    def test_create_release(self):
        input_dict = {
            'release_version': 5,
            'release_date': '12-10-2020',
            'release_label': 'prod',
            'is_current': False,
            'site_name': 'EBI',
            'site_label': 'EBI',
            'site_uri': 'test uri',
        }
        expected_output = {
            'releaseDate': '12-10-2020',
            'releaseLabel': 'prod',
            'releaseVersion': 5,
            'siteLabel': 'EBI',
            'siteName': 'EBI',
            'siteUri': 'test uri',
        }
        output = json_format.MessageToJson(service.create_release(input_dict))
        assert json.loads(output) == expected_output


    def test_karyotype_information(self):
        output = json_format.MessageToJson(
            service.get_karyotype_information(self.engine, '3c4cec7f-fb69-11eb-8dac-005056b32883'))
        expected_output = {}
        assert json.loads(output) == expected_output


    def test_assembly_information(self):
        output = json_format.MessageToJson(service.get_assembly_information(self.engine, '2'))
        expected_output = {
            'assemblyId': "2",
            'accession': "GCA_009873245.2",
            'level': "chromosome",
            'name': "mBalMus1.v2",
            'chromosomal': 1,
            'length': 185157308,
            'sequenceLocation': "SO:0000738",
            'sequenceChecksum': "bb967773a69d45e191a5e0fcfe277f7c"
        }
        assert json.loads(output) == expected_output


    def test_sub_species_info(self):
        output = json_format.MessageToJson(service.get_sub_species_info(self.engine, '41'))
        expected_output = {
            'organismId': "41",
            'speciesType': ["breeds"],
            'speciesName': ["Dog breeds"]
        }
        assert json.loads(output) == expected_output

        output2 = json_format.MessageToJson(service.get_grouping_info(self.engine, '51'))
        expected_output2 = {}
        assert json.loads(output2) == expected_output2


    def test_get_grouping_info(self):
        output = json_format.MessageToJson(service.get_grouping_info(self.engine, '41'))
        expected_output = {
            'organismId': "41",
            'speciesType': ["breeds"],
            'speciesName': ["Dog breeds"]
        }
        assert json.loads(output) == expected_output

        output2 = json_format.MessageToJson(service.get_grouping_info(self.engine, '51'))
        expected_output2 = {}
        assert json.loads(output2) == expected_output2


    def test_get_top_level_statistics(self):
        output = json_format.MessageToJson(service.get_top_level_statistics(self.engine, '41'))
        output = json.loads(output)
        print(output)
        assert len(output['statistics']) == 138
        assert output['statistics'][0] == {
            'name': "ungapped_genome",
            'label': "Base pairs",
            'statisticType': "length_bp",
            'statisticValue': "2410429933"
        }
        assert output['statistics'][1] == {
            'name': "gene_lnoncoding",
            'label': "Long ncRNA gene",
            'statisticType': "count",
            'statisticValue': "1817"
        }
        assert output['statistics'][2] == {
            'name': "gene_coding",
            'label': "Protein-coding gene",
            'statisticType': "count",
            'statisticValue': "20804"
        }
        assert output['statistics'][3] == {
            'name': "exon_mnoncoding",
            'label': "Non-coding exon",
            'statisticType': "count",
            'statisticValue': "22"
        }


    def test_get_datasets_list_by_uuid(self):
        output = json_format.MessageToJson(service.get_datasets_list_by_uuid(self.engine, 'a73351f7-93e7-11ec-a39d-005056b38ce3', 2020))

        expected_output = {
            "datasets": {
                "assembly": {
                    "datasets_list": [
                        {
                            "dataset_uuid": "a734138a-93e7-11ec-a39d-005056b38ce3",
                            "dataset_name": "assembly",
                            "dataset_version": "",
                            "dataset_label": "GCA_000002765.2",
                            "version": 2020
                        }
                    ]
                },
                "alignment_xrefs": {
                    "datasets_list": [
                        {
                            "dataset_uuid": "a7429ca8-93e7-11ec-a39d-005056b38ce3",
                            "dataset_name": "xref_alignment",
                            "dataset_version": "2019-06-25",
                            "dataset_label": "Alignment-based cross-references",
                            "version": 2020
                        }
                    ]
                },
                "checksum_xrefs": {
                    "datasets_list": [
                        {
                            "dataset_uuid": "a7429fe3-93e7-11ec-a39d-005056b38ce3",
                            "dataset_name": "uniparc_checksum",
                            "dataset_version": "EG Xref pipeline; 2019-06-20T15:27:05",
                            "dataset_label": "UniParc",
                            "version": 2020
                        }
                    ]
                },
                "geneset": {
                    "datasets_list": [
                        {
                            "dataset_uuid": "a734938a-93e7-11ec-a39d-005056b38ce3",
                            "dataset_name": "gene_core",
                            "dataset_version": "",
                            "dataset_label": "2017-10-ENA",
                            "version": 2020
                        }
                    ]
                },
                "dependent_xrefs": {
                    "datasets_list": [
                        {
                            "dataset_uuid": "a742a364-93e7-11ec-a39d-005056b38ce3",
                            "dataset_name": "xref_dependent",
                            "dataset_version": "2019-06-25",
                            "dataset_label": "Dependent cross-references",
                            "version": 2020
                        },
                        {
                            "dataset_uuid": "a742a3d5-93e7-11ec-a39d-005056b38ce3",
                            "dataset_name": "xref_dependent",
                            "dataset_version": "2021-01-06",
                            "dataset_label": "Dependent cross-references",
                            "version": 2020
                        }
                    ]
                },
                "go_terms": {
                    "datasets_list": [
                        {
                            "dataset_uuid": "a7434489-93e7-11ec-a39d-005056b38ce3",
                            "dataset_name": "goa_import",
                            "dataset_version": "2021-02-16",
                            "dataset_label": "GOA annotation",
                            "version": 2020
                        },
                        {
                            "dataset_uuid": "a74344fa-93e7-11ec-a39d-005056b38ce3",
                            "dataset_name": "interpro2go",
                            "dataset_version": "2021-01-06",
                            "dataset_label": "InterPro2GO mapping",
                            "version": 2020
                        }
                    ]
                },
                "protein_features": {
                    "datasets_list": [
                        {
                            "dataset_uuid": "a7434811-93e7-11ec-a39d-005056b38ce3",
                            "dataset_name": "interproscan",
                            "dataset_version": "5.48-83.0",
                            "dataset_label": "InterProScan",
                            "version": 2020
                        }
                    ]
                }
            },
            "genome_uuid": "a73356e1-93e7-11ec-a39d-005056b38ce3"
        }
        assert json.loads(output) == expected_output


    def test_get_datasets_list_by_uuid_no_results(self):
        output = json_format.MessageToJson(
            service.get_datasets_list_by_uuid(self.engine, 'some-random-uuid-f00-b4r', 2020)
        )
        output = json.loads(output)
        expected_output = {}
        assert output == expected_output


    def test_get_dataset_by_genome_id(self):
        output = json_format.MessageToJson(service.get_dataset_by_genome_id(self.engine, '3c4cec7f-fb69-11eb-8dac-005056b32883', 'assembly'))
        output = json.loads(output)
        assert output == {'genomeUuid': '3c4cec7f-fb69-11eb-8dac-005056b32883', 'datasetType': 'assembly',
                          'datasetInfos': [
                              {
                                  'datasetUuid': '40a98446-fb69-11eb-8dac-005056b32883',
                                  'datasetName': 'assembly',
                                  'name': 'ungapped_genome',
                                  'type': 'length_bp',
                                  'datasetLabel': 'GCA_009873245.2',
                                  'version': 22,
                                  'value': '2379995981'
                              }]
                          }


    def test_get_dataset_by_genome_id_no_results(self):
        output = json_format.MessageToJson(
            service.get_dataset_by_genome_id(self.engine, '3c4cec7f-fb69-11eb-8dac-005056b32883', 'blah blah blah'))
        output = json.loads(output)
        assert output == {}
