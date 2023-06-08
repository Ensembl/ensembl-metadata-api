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
Unit tests for service module
"""
import datetime

import sqlalchemy as db
import json
from google.protobuf import json_format

import sqlite3
from tempfile import TemporaryDirectory

from ensembl.production.metadata import service, ensembl_metadata_pb2


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
            'created': datetime.date(2022, 8, 15),
            'ensembl_name':  'some name',
            'url_name': 'http://url_name.com',
            'display_name': 'Display Name',
            'is_current': True,
            'assembly_accession': 'X.AE500',
            'assembly_name': 'assembly name',
            'assembly_ucsc_name': 'ucsc name',
            'assembly_level': 'level',
            'assembly_ensembl_name': 'some assembly ensembl name',
            'taxonomy_id': 1234,
            'scientific_name': 'scientific name',
            'scientific_parlance_name': 'scientific_parlance_name',
            'strain': 'test strain',
            'release_version': 1,
            'release_date': datetime.date(2022, 8, 15),
            'release_label': 'release_label'
        }
        expected_output = {
            'assembly': {
                'accession': 'X.AE500',
                'level': 'level',
                'name': 'assembly name',
                'ucscName': 'ucsc name',
                'ensemblName': 'some assembly ensembl name',
            },
            'created': '2022-08-15',
            'genomeUuid': 'f9d8c1dc-45dd-11ec-81d3-0242ac130003',
            'organism': {
                'displayName': 'Display Name',
                'ensemblName': 'some name',
                'scientificName': 'scientific name',
                'scientificParlanceName': 'scientific_parlance_name',
                'strain': 'test strain',
                'urlName': 'http://url_name.com'
            },
            'release': {
                'isCurrent': True,
                'releaseDate': '2022-08-15',
                'releaseLabel': 'release_label',
                'releaseVersion': 1
            },
            'taxon': {
                'scientificName': 'scientific name',
                'strain': 'test strain',
                'taxonomyId': 1234
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


    def test_get_genomes_from_assembly_accession_iterator(self):
        output = [json.loads(json_format.MessageToJson(response)) for response in
            service.get_genomes_from_assembly_accession_iterator(self.engine, "test accession")]
        expected_output = [
            {
                'assembly': {
                    'accession': 'test accession',
                    'level': 'test level',
                    'name': 'test name',
                    'ensemblName': 'test assembly ensembl name'
                },
                'created': '2021-07-19 13:22:26',
                'genomeUuid': '3c52097a-fb69-11eb-8dac-005056b32883',
                'organism': {
                    'displayName': 'Sus scrofa (Pig) - GCA_000003025.6',
                    'ensemblName': 'sus_scrofa_gca000003025v6',
                    'scientificName': 'Sus scrofa',
                    'scientificParlanceName': 'Sus scrofa',
                    'urlName': 'Sus_scrofa_GCA_000003025.6'
                },
                'release': {
                    'isCurrent': True,
                    'releaseDate': '2021-06-30',
                    'releaseVersion': 24.0
                },
                'taxon': {
                    'scientificName': 'Sus scrofa',
                    'taxonomyId': 9823
                }
            },
            {
                'assembly': {
                    'accession': 'test accession',
                    'level': 'test level',
                    'name': 'test name'
                },
                'created': '2021-07-19 13:22:26',
                'genomeUuid': '244fdac6-729f-4c05-a2e9-38021f9593dd',
                'organism': {
                    'displayName': 'test organism',
                    'ensemblName': 'test_organism_gca000003025v6',
                    'scientificName': 'test organism',
                    'scientificParlanceName': 'test organism',
                    'urlName': 'test_organism_GCA_000003025.6'
                },
                'release': {
                    'isCurrent': True,
                    'releaseDate': '2021-03-25',
                    'releaseVersion': 104.0
                },
                'taxon': {
                    'scientificName': 'test organism',
                    'taxonomyId': 9823
                }
            }
        ]
        assert output == expected_output


    def test_get_genomes_from_assembly_accession_iterator_null(self):
        output = [json.loads(json_format.MessageToJson(response)) for response in
            service.get_genomes_from_assembly_accession_iterator(self.engine, None)]
        assert output == []


    def test_get_genomes_from_assembly_accession_iterator_no_matches(self):
        output = [json.loads(json_format.MessageToJson(response)) for response in
            service.get_genomes_from_assembly_accession_iterator(self.engine, "asdfasdfadf")]
        assert output == []


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
        output = json_format.MessageToJson(service.get_datasets_list_by_uuid(self.engine, '3c51ff24-fb69-11eb-8dac-005056b32883', 103.0))

        expected_output = {
            "genomeUuid": "3c51ff24-fb69-11eb-8dac-005056b32883",
            "datasets": {
                "assembly": {
                "datasetInfos": [
                    {
                    "datasetUuid": "40aa7070-fb69-11eb-8dac-005056b32883",
                    "datasetName": "assembly",
                    "datasetLabel": "GCA_902859565.1",
                    "version": 103
                    }
                ]
                },
                "checksum_xrefs": {
                "datasetInfos": [
                    {
                    "datasetUuid": "56cc017f-fb69-11eb-8dac-005056b32883",
                    "datasetName": "uniparc_checksum",
                    "datasetVersion": "2021-05-01",
                    "datasetLabel": "UniParc",
                    "version": 103
                    }
                ]
                },
                "go_terms": {
                "datasetInfos": [
                    {
                    "datasetUuid": "56ce6e0b-fb69-11eb-8dac-005056b32883",
                    "datasetName": "interpro2go",
                    "datasetVersion": "2021-04-10",
                    "datasetLabel": "InterPro2GO mapping",
                    "version": 103
                    }
                ]
                },
                "repeat_features": {
                "datasetInfos": [
                    {
                    "datasetUuid": "56d08e6e-fb69-11eb-8dac-005056b32883",
                    "datasetName": "dust",
                    "datasetVersion": "2021-02-16",
                    "datasetLabel": "Low complexity (Dust)",
                    "version": 103
                    },
                    {
                    "datasetUuid": "56d08ed9-fb69-11eb-8dac-005056b32883",
                    "datasetName": "repeatdetector_annotated",
                    "datasetVersion": "2.17-r974-dirty",
                    "datasetLabel": "Repeats: Red (annotated)",
                    "version": 103
                    },
                    {
                    "datasetUuid": "56d08f3c-fb69-11eb-8dac-005056b32883",
                    "datasetName": "repeatdetector",
                    "datasetVersion": "2.0",
                    "datasetLabel": "Repeats: Red",
                    "version": 103
                    },
                    {
                    "datasetUuid": "56d08fa1-fb69-11eb-8dac-005056b32883",
                    "datasetName": "repeatmask_nrplants",
                    "datasetVersion": "4.0.5",
                    "datasetLabel": "Repeats: nrplants",
                    "version": 103
                    },
                    {
                    "datasetUuid": "56d09002-fb69-11eb-8dac-005056b32883",
                    "datasetName": "repeatmask_redat",
                    "datasetVersion": "4.0.5",
                    "datasetLabel": "Repeats: REdat",
                    "version": 103
                    },
                    {
                    "datasetUuid": "56d09060-fb69-11eb-8dac-005056b32883",
                    "datasetName": "repeatmask_repbase",
                    "datasetVersion": "4.0.5",
                    "datasetLabel": "Repeats: Repbase",
                    "version": 103
                    },
                    {
                    "datasetUuid": "56d090c2-fb69-11eb-8dac-005056b32883",
                    "datasetName": "trf",
                    "datasetVersion": "4.0",
                    "datasetLabel": "Tandem repeats (TRF)",
                    "version": 103
                    }
                ]
                },
                "protein_features": {
                "datasetInfos": [
                    {
                    "datasetUuid": "56cf650f-fb69-11eb-8dac-005056b32883",
                    "datasetName": "interproscan",
                    "datasetVersion": "5.51-85.0",
                    "datasetLabel": "InterProScan",
                    "version": 103
                    },
                    {
                    "datasetUuid": "56cf656f-fb69-11eb-8dac-005056b32883",
                    "datasetName": "seg",
                    "datasetLabel": "seg",
                    "version": 103
                    }
                ]
                },
                "geneset": {
                "datasetInfos": [
                    {
                    "datasetUuid": "43ed89eb-fb69-11eb-8dac-005056b32883",
                    "datasetName": "gene_core",
                    "datasetLabel": "2021-01-KAUST",
                    "version": 103
                    }
                ]
                }
            }
        }
        assert json.loads(output) == expected_output


    def test_get_datasets_list_by_uuid_no_results(self):
        output = json_format.MessageToJson(
            service.get_datasets_list_by_uuid(self.engine, 'some-random-uuid-f00-b4r', 103.0)
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

    def test_get_genome_by_uuid(self):
        output = json_format.MessageToJson(service.get_genome_by_uuid(self.engine, '3c4cec7f-fb69-11eb-8dac-005056b32883', 1.0))
        expected_output = {
            "genomeUuid": "3c4cec7f-fb69-11eb-8dac-005056b32883",
            "assembly": {
                "accession": "GCA_009873245.2",
                "name": "mBalMus1.v2",
                "level": "chromosome"
            },
            "taxon": {
                "taxonomyId": 9771,
                "scientificName": "Balaenoptera musculus"
            },
            "created": "2020-05-01 13:28:33",
            "organism": {
                "displayName": "Balaenoptera musculus (Blue whale) - GCA_009873245.2",
                "scientificName": "Balaenoptera musculus",
                "urlName": "Balaenoptera_musculus_GCA_009873245.2",
                "ensemblName": "balaenoptera_musculus",
                "scientificParlanceName": "Balaenoptera musculus"
            },
            "release": {
                "releaseVersion": 1,
                "releaseDate": "2020-06-04"
            }
        }
        assert json.loads(output) == expected_output


    def test_genome_by_uuid_release_version_unspecified(self):
        output = json_format.MessageToJson(service.get_genome_by_uuid(self.engine, '3c52097a-fb69-11eb-8dac-005056b32883', 0.0))
        expected_output = {
            "genomeUuid": "3c52097a-fb69-11eb-8dac-005056b32883",
            "assembly": {
                "accession": "test accession",
                "name": "test name",
                "level": "test level",
                "ensemblName": "test assembly ensembl name"
            },
            "taxon": {
                "taxonomyId": 9823,
                "scientificName": "Sus scrofa"
            },
            "created": "2021-07-19 13:22:26",
            "organism": {
                "displayName": "Sus scrofa (Pig) - GCA_000003025.6",
                "scientificName": "Sus scrofa",
                "scientificParlanceName": "Sus scrofa",
                "urlName": "Sus_scrofa_GCA_000003025.6",
                "ensemblName": "sus_scrofa_gca000003025v6"
            },
            "release": {
                "releaseVersion": 24,
                "releaseDate": "2021-06-30",
                "isCurrent": True
            }
        }
        assert json.loads(output) == expected_output



    def test_get_genomes_by_uuid_null(self):
        output = service.get_genome_by_uuid(self.engine, None, 0)
        assert output == ensembl_metadata_pb2.Genome()


    def test_get_genomes_by_keyword(self):
        output = [json.loads(json_format.MessageToJson(response)) for response in service.get_genomes_by_keyword_iterator(self.engine, 'Melitaea cinxia', 23.0)]
        expected_output = [
            {
                'assembly': {},
                'created': '2021-06-08 19:37:40',
                'genomeUuid': '3c52036e-fb69-11eb-8dac-005056b32883',
                'organism': {
                    'displayName': 'Melitaea cinxia (Glanville fritillary) - '
                                   'GCA_905220565.1',
                    'ensemblName': 'melitaea_cinxia_gca905220565v1',
                    'scientificName': 'Melitaea cinxia',
                    'scientificParlanceName': 'Melitaea cinxia',
                    'urlName': 'Melitaea_cinxia_GCA_905220565.1'
                },
                'release': {
                    'releaseDate': '2021-06-17',
                    'releaseVersion': 23.0
                },
                'taxon': {
                    'scientificName': 'Melitaea cinxia',
                    'taxonomyId': 113334
                }
            }
        ]
        assert output == expected_output


    def test_get_genomes_by_keyword_release_unspecified(self):
        output = [json.loads(json_format.MessageToJson(response)) for response in service.get_genomes_by_keyword_iterator(self.engine, 'Sus scrofa', 0.0)]
        expected_output = [
            {
                'genomeUuid': '3c52097a-fb69-11eb-8dac-005056b32883',
                'assembly': {
                    'accession': 'test accession',
                    'name': 'test name',
                    'level': 'test level',
                    'ensemblName': 'test assembly ensembl name'
                },
                'taxon': {
                    'taxonomyId': 9823,
                    'scientificName': 'Sus scrofa'
                },
                'created': '2021-07-19 13:22:26',
                'organism': {
                    'displayName': 'Sus scrofa (Pig) - GCA_000003025.6',
                    'scientificName': 'Sus scrofa',
                    'scientificParlanceName': 'Sus scrofa',
                    'urlName': 'Sus_scrofa_GCA_000003025.6',
                    'ensemblName': 'sus_scrofa_gca000003025v6'
                },
                'release': {
                    'releaseVersion': 24,
                    'releaseDate': '2021-06-30',
                    'isCurrent': True
                }
            }
        ]
        assert output == expected_output


    def test_get_genomes_by_keyword_null(self):
        output = list(service.get_genomes_by_keyword_iterator(self.engine, None, 0))
        assert output == []


    def test_get_genomes_by_keyword_no_matches(self):
        output = list(service.get_genomes_by_keyword_iterator(self.engine, "bigfoot", 1))
        assert output == []


    def test_get_genomes_by_name(self):
        output = json_format.MessageToJson(service.get_genome_by_name(self.engine, 'balaenoptera_musculus', 'vertebrates', 1.0))
        expected_output = {
            "genomeUuid": "3c4cec7f-fb69-11eb-8dac-005056b32883",
            "assembly": {
                "accession": "GCA_009873245.2",
                "name": "mBalMus1.v2",
                "level": "chromosome"
            },
            "taxon": {
                "taxonomyId": 9771,
                "scientificName": "Balaenoptera musculus"
            },
            "created": "2020-05-01 13:28:33",
            "organism": {
                "displayName": "Balaenoptera musculus (Blue whale) - GCA_009873245.2",
                "scientificName": "Balaenoptera musculus",
                "scientificParlanceName": "Balaenoptera musculus",
                "urlName": "Balaenoptera_musculus_GCA_009873245.2",
                "ensemblName": "balaenoptera_musculus"
            },
            "release": {
                "releaseVersion": 1,
                "releaseDate": "2020-06-04"
            }
        }
        assert json.loads(output) == expected_output

    def test_get_genomes_by_name_release_unspecified(self):
        output = json_format.MessageToJson(service.get_genome_by_name(self.engine, 'sus_scrofa_gca000003025v6', 'vertebrates', 0.0))
        expected_output = {
            'genomeUuid': '3c52097a-fb69-11eb-8dac-005056b32883',
            'assembly': {
                'accession': 'test accession',
                'name': 'test name',
                'level': 'test level',
                'ensemblName': 'test assembly ensembl name'
            },
            'taxon': {
                'taxonomyId': 9823,
                'scientificName': 'Sus scrofa'
            },
            'created': '2021-07-19 13:22:26',
            'organism': {
                'displayName': 'Sus scrofa (Pig) - GCA_000003025.6',
                'scientificName': 'Sus scrofa',
                'scientificParlanceName': 'Sus scrofa',
                'urlName': 'Sus_scrofa_GCA_000003025.6',
                'ensemblName': 'sus_scrofa_gca000003025v6'
            },
            'release': {
                'releaseVersion': 24,
                'releaseDate': '2021-06-30',
                'isCurrent': True
            }
        }
        assert json.loads(output) == expected_output
