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
import os.path

import sqlalchemy as db
import json
from google.protobuf import json_format

import sqlite3
from tempfile import TemporaryDirectory

from ensembl.production.metadata import service, ensembl_metadata_pb2
import glob


class TestClass:
    dir_path = TemporaryDirectory()
    connection = sqlite3.connect(f'{dir_path.name}/test.db')
    cursor = connection.cursor()

    sql_file = open(f"{os.path.dirname(__file__)}/databases/tables.sql")
    sql_as_string = sql_file.read()

    cursor.executescript(sql_as_string)

    file_pattern = 'insert.*.sql'
    file_list = glob.glob(f'{os.path.dirname(__file__)}/databases/{file_pattern}')
    connection.execute("PRAGMA foreign_keys = OFF;")
    for file_path in file_list:
        with open(file_path, 'r') as sql_file:
            sql_as_string = sql_file.read()
            cursor.executescript(sql_as_string)
    connection.execute("PRAGMA foreign_keys = ON;")
    connection.commit()
    connection.close()

    try:
        engine = db.create_engine(f'sqlite:////{dir_path.name}/test.db')
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
            'ensembl_name': 'some name',
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
            'chromosomal': 1223,
            'length': 5,
            'sequence_location': 'location',
            'sequence_checksum': 'checksum',
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
        output = json_format.MessageToJson(service.get_assembly_information(self.engine, '1'))
        expected_output = {'accession': 'GCA_000001405.28',
                           'assemblyId': '1',
                           'length': 71251,
                           'level': 'chromosome',
                           'name': 'GRCh38.p13',
                           'sequenceLocation': 'SO:0000738'}
        assert json.loads(output) == expected_output

    def test_get_genomes_from_assembly_accession_iterator(self):
        output = [json.loads(json_format.MessageToJson(response)) for response in
                  service.get_genomes_from_assembly_accession_iterator(self.engine, "GCA_000005845.2")]
        expected_output = [{'assembly': {'accession': 'GCA_000005845.2',
                                         'ensemblName': 'ASM584v2',
                                         'level': 'chromosome',
                                         'name': 'ASM584v2'},
                            'created': '2023-05-12 13:32:14',
                            'genomeUuid': 'a73351f7-93e7-11ec-a39d-005056b38ce3',
                            'organism': {'displayName': 'Escherichia coli str. K-12 substr. MG1655 str. '
                                                        'K12 (GCA_000005845)',
                                         'ensemblName': 'escherichia_coli_str_k_12_substr_mg1655_gca_000005845',
                                         'scientificName': 'Escherichia coli str. K-12 substr. MG1655 '
                                                           'str. K12 (GCA_000005845)',
                                         'urlName': 'Escherichia_coli_str_k_12_substr_mg1655_gca_000005845'},
                            'release': {'isCurrent': True,
                                        'releaseDate': '2023-06-05',
                                        'releaseLabel': 'Release 110',
                                        'releaseVersion': 110.0},
                            'taxon': {'scientificName': 'Escherichia coli str. K-12 substr. MG1655 str. '
                                                        'K12 (GCA_000005845)',
                                      'taxonomyId': 511145}}]
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
        output = json_format.MessageToJson(service.get_sub_species_info(self.engine, '1'))
        expected_output = {
            'organismId': '1',
            'speciesName': ['EnsemblVertebrates', 'Western Europe'],
            'speciesType': ['Division', 'Population']
        }
        assert json.loads(output) == expected_output

        output2 = json_format.MessageToJson(service.get_grouping_info(self.engine, '51'))
        expected_output2 = {}
        assert json.loads(output2) == expected_output2

    def test_get_grouping_info(self):
        output = json_format.MessageToJson(service.get_grouping_info(self.engine, '1'))
        expected_output = {
            'organismId': "1",
            'speciesType': ['Division', 'Population'],
            'speciesName': ["EnsemblVertebrates", "Western Europe"]
        }
        assert json.loads(output) == expected_output

        output2 = json_format.MessageToJson(service.get_grouping_info(self.engine, '51'))
        expected_output2 = {}
        assert json.loads(output2) == expected_output2

    def test_get_top_level_statistics(self):
        output = json_format.MessageToJson(service.get_top_level_statistics(self.engine, '4'))
        output = json.loads(output)
        print(output)
        assert len(output['statistics']) == 94
        assert output['statistics'][0] == {
            'label': 'Contig N50',
            'name': 'contig_n50',
            'statisticType': 'bp',
            'statisticValue': '51842'
        }
        assert output['statistics'][1] == {
            'label': 'Total genome length',
            'name': 'total_genome_length',
            'statisticType': 'bp',
            'statisticValue': '14547261565'
        }

    def test_get_datasets_list_by_uuid(self):

        output = json_format.MessageToJson(
            service.get_datasets_list_by_uuid(self.engine, 'a73356e1-93e7-11ec-a39d-005056b38ce3'))

        expected_output = {'datasets': {'assembly': {'datasetInfos': [{'datasetLabel': 'GCA_000002765.2',
                                                                       'datasetName': 'assembly',
                                                                       'datasetUuid': '29dbda41-5188-4323-9318-ce546a87eee7',
                                                                       'version': 110.0}]},
                                        'genebuild': {'datasetInfos': [{'datasetLabel': '2017-10',
                                                                        'datasetName': 'genebuild',
                                                                        'datasetUuid': 'e33e0506-dc12-47c7-b291-a1a8ee6c17b6',
                                                                        'version': 110.0}]},
                                        'homologies': {'datasetInfos': [{'datasetLabel': 'Manual Add',
                                                                         'datasetName': 'homologies',
                                                                         'datasetUuid': '24d31c67-412e-44cc-8790-f196a16629ec',
                                                                         'version': 110.0}]}},
                           'genomeUuid': 'a73356e1-93e7-11ec-a39d-005056b38ce3'}

        assert json.loads(output) == expected_output

    def test_get_datasets_list_by_uuid_no_results(self):

        output = json_format.MessageToJson(
            service.get_datasets_list_by_uuid(self.engine, 'some-random-uuid-f00-b4r', 103.0)
        )
        output = json.loads(output)
        expected_output = {}
        assert output == expected_output

    def test_get_dataset_by_genome_id(self):

        output = json_format.MessageToJson(
            service.get_dataset_by_genome_id(self.engine, 'a73356e1-93e7-11ec-a39d-005056b38ce3', 'assembly'))
        output = json.loads(output)
        assert output == {'datasetInfos': [{'datasetLabel': 'GCA_000002765.2',
                                            'datasetName': 'assembly',
                                            'datasetUuid': '29dbda41-5188-4323-9318-ce546a87eee7',
                                            'name': 'total_genome_length',
                                            'type': 'bp',
                                            'value': '23292622',
                                            'version': 110.0},
                                           {'datasetLabel': 'GCA_000002765.2',
                                            'datasetName': 'assembly',
                                            'datasetUuid': '29dbda41-5188-4323-9318-ce546a87eee7',
                                            'name': 'total_coding_sequence_length',
                                            'type': 'bp',
                                            'value': '12309897',
                                            'version': 110.0},
                                           {'datasetLabel': 'GCA_000002765.2',
                                            'datasetName': 'assembly',
                                            'datasetUuid': '29dbda41-5188-4323-9318-ce546a87eee7',
                                            'name': 'total_gap_length',
                                            'type': 'bp',
                                            'value': '0',
                                            'version': 110.0},
                                           {'datasetLabel': 'GCA_000002765.2',
                                            'datasetName': 'assembly',
                                            'datasetUuid': '29dbda41-5188-4323-9318-ce546a87eee7',
                                            'name': 'spanned_gaps',
                                            'type': 'integer',
                                            'value': '0',
                                            'version': 110.0},
                                           {'datasetLabel': 'GCA_000002765.2',
                                            'datasetName': 'assembly',
                                            'datasetUuid': '29dbda41-5188-4323-9318-ce546a87eee7',
                                            'name': 'chromosomes',
                                            'type': 'integer',
                                            'value': '14',
                                            'version': 110.0},
                                           {'datasetLabel': 'GCA_000002765.2',
                                            'datasetName': 'assembly',
                                            'datasetUuid': '29dbda41-5188-4323-9318-ce546a87eee7',
                                            'name': 'toplevel_sequences',
                                            'type': 'integer',
                                            'value': '14',
                                            'version': 110.0},
                                           {'datasetLabel': 'GCA_000002765.2',
                                            'datasetName': 'assembly',
                                            'datasetUuid': '29dbda41-5188-4323-9318-ce546a87eee7',
                                            'name': 'component_sequences',
                                            'type': 'integer',
                                            'value': '14',
                                            'version': 110.0},
                                           {'datasetLabel': 'GCA_000002765.2',
                                            'datasetName': 'assembly',
                                            'datasetUuid': '29dbda41-5188-4323-9318-ce546a87eee7',
                                            'name': 'gc_percentage',
                                            'type': 'percent',
                                            'value': '19.34',
                                            'version': 110.0}],
                          'datasetType': 'assembly',
                          'genomeUuid': 'a73356e1-93e7-11ec-a39d-005056b38ce3'}

    def test_get_dataset_by_genome_id_no_results(self):

        output = json_format.MessageToJson(
            service.get_dataset_by_genome_id(self.engine, 'a7335667-93e7-11ec-a39d-005056b38ce3', 'blah blah blah'))
        output = json.loads(output)
        assert output == {}

    def test_get_genome_by_uuid(self):

        output = json_format.MessageToJson(
            service.get_genome_by_uuid(self.engine, 'a7335667-93e7-11ec-a39d-005056b38ce3', 110.0))
        expected_output = {'assembly': {'accession': 'GCA_000001405.28',
                                        'ensemblName': 'GRCh38.p13',
                                        'level': 'chromosome',
                                        'name': 'GRCh38.p13',
                                        'ucscName': 'hg38'},
                           'created': '2023-05-12 13:30:58',
                           'genomeUuid': 'a7335667-93e7-11ec-a39d-005056b38ce3',
                           'organism': {'displayName': 'Human',
                                        'ensemblName': 'homo_sapiens',
                                        'scientificName': 'Homo sapiens',
                                        'urlName': 'Homo_sapiens'},
                           'release': {'isCurrent': True,
                                       'releaseDate': '2023-06-05',
                                       'releaseLabel': 'Release 110',
                                       'releaseVersion': 110.0},
                           'taxon': {'scientificName': 'Homo sapiens', 'taxonomyId': 9606}
                           }
        assert json.loads(output) == expected_output

    def test_genome_by_uuid_release_version_unspecified(self):

        output = json_format.MessageToJson(
            service.get_genome_by_uuid(self.engine, 'a733574a-93e7-11ec-a39d-005056b38ce3', 0.0))
        expected_output = {'assembly': {'accession': 'GCA_000146045.2',
                                        'ensemblName': 'R64-1-1',
                                        'level': 'chromosome',
                                        'name': 'R64-1-1'},
                           'created': '2023-05-12 13:32:46',
                           'genomeUuid': 'a733574a-93e7-11ec-a39d-005056b38ce3',
                           'organism': {'displayName': 'Saccharomyces cerevisiae',
                                        'ensemblName': 'saccharomyces_cerevisiae',
                                        'scientificName': 'Saccharomyces cerevisiae S288c',
                                        'strain': 'S288C',
                                        'urlName': 'Saccharomyces_cerevisiae'},
                           'release': {'isCurrent': True,
                                       'releaseDate': '2023-06-05',
                                       'releaseLabel': 'Release 110',
                                       'releaseVersion': 110.0},
                           'taxon': {'scientificName': 'Saccharomyces cerevisiae S288c',
                                     'strain': 'S288C',
                                     'taxonomyId': 559292}}
        assert json.loads(output) == expected_output

    def test_get_genomes_by_uuid_null(self):

        output = service.get_genome_by_uuid(self.engine, None, 0)
        assert output == ensembl_metadata_pb2.Genome()

    def test_get_genomes_by_keyword(self):

        output = [json.loads(json_format.MessageToJson(response)) for response in
                  service.get_genomes_by_keyword_iterator(self.engine, 'Human', 110.0)]
        expected_output = [{'assembly': {'accession': 'GCA_000001405.28',
                                         'ensemblName': 'GRCh38.p13',
                                         'level': 'chromosome',
                                         'name': 'GRCh38.p13',
                                         'ucscName': 'hg38'},
                            'created': '2023-05-12 13:30:58',
                            'genomeUuid': 'a7335667-93e7-11ec-a39d-005056b38ce3',
                            'organism': {'displayName': 'Human',
                                         'ensemblName': 'homo_sapiens',
                                         'scientificName': 'Homo sapiens',
                                         'urlName': 'Homo_sapiens'},
                            'release': {'isCurrent': True,
                                        'releaseDate': '2023-06-05',
                                        'releaseLabel': 'Release 110',
                                        'releaseVersion': 110.0},
                            'taxon': {'scientificName': 'Homo sapiens', 'taxonomyId': 9606}}]
        assert output == expected_output

    def test_get_genomes_by_keyword_release_unspecified(self):

        output = [json.loads(json_format.MessageToJson(response)) for response in
                  service.get_genomes_by_keyword_iterator(self.engine, 'Homo Sapiens', 0.0)]
        expected_output = [{'assembly': {'accession': 'GCA_000001405.28',
                                         'ensemblName': 'GRCh38.p13',
                                         'level': 'chromosome',
                                         'name': 'GRCh38.p13',
                                         'ucscName': 'hg38'},
                            'created': '2023-05-12 13:30:58',
                            'genomeUuid': 'a7335667-93e7-11ec-a39d-005056b38ce3',
                            'organism': {'displayName': 'Human',
                                         'ensemblName': 'homo_sapiens',
                                         'scientificName': 'Homo sapiens',
                                         'urlName': 'Homo_sapiens'},
                            'release': {'isCurrent': True,
                                        'releaseDate': '2023-06-05',
                                        'releaseLabel': 'Release 110',
                                        'releaseVersion': 110.0},
                            'taxon': {'scientificName': 'Homo sapiens', 'taxonomyId': 9606}}]
        assert output == expected_output

    def test_get_genomes_by_keyword_null(self):

        output = list(service.get_genomes_by_keyword_iterator(self.engine, None, 0))
        assert output == []

    def test_get_genomes_by_keyword_no_matches(self):

        output = list(service.get_genomes_by_keyword_iterator(self.engine, "bigfoot", 1))
        assert output == []

    def test_get_genomes_by_name(self):

        output = json_format.MessageToJson(
            service.get_genome_by_name(self.engine, 'homo_sapiens', 'beta', 110.0))
        expected_output = {'assembly': {'accession': 'GCA_000001405.28',
                                        'ensemblName': 'GRCh38.p13',
                                        'level': 'chromosome',
                                        'name': 'GRCh38.p13',
                                        'ucscName': 'hg38'},
                           'created': '2023-05-12 13:30:58',
                           'genomeUuid': 'a7335667-93e7-11ec-a39d-005056b38ce3',
                           'organism': {'displayName': 'Human',
                                        'ensemblName': 'homo_sapiens',
                                        'scientificName': 'Homo sapiens',
                                        'urlName': 'Homo_sapiens'},
                           'release': {'isCurrent': True,
                                       'releaseDate': '2023-06-05',
                                       'releaseLabel': 'Release 110',
                                       'releaseVersion': 110.0},
                           'taxon': {'scientificName': 'Homo sapiens', 'taxonomyId': 9606}}
        assert json.loads(output) == expected_output

    def test_get_genomes_by_name_release_unspecified(self):

        output = json_format.MessageToJson(
            service.get_genome_by_name(self.engine, 'homo_sapiens', 'beta', 0.0))
        expected_output = {'assembly': {'accession': 'GCA_000001405.28',
                                        'ensemblName': 'GRCh38.p13',
                                        'level': 'chromosome',
                                        'name': 'GRCh38.p13',
                                        'ucscName': 'hg38'},
                           'created': '2023-05-12 13:30:58',
                           'genomeUuid': 'a7335667-93e7-11ec-a39d-005056b38ce3',
                           'organism': {'displayName': 'Human',
                                        'ensemblName': 'homo_sapiens',
                                        'scientificName': 'Homo sapiens',
                                        'urlName': 'Homo_sapiens'},
                           'release': {'isCurrent': True,
                                       'releaseDate': '2023-06-05',
                                       'releaseLabel': 'Release 110',
                                       'releaseVersion': 110.0},
                           'taxon': {'scientificName': 'Homo sapiens', 'taxonomyId': 9606}}
        assert json.loads(output) == expected_output
