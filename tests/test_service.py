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
from concurrent import futures
import grpc
import logging
import sqlalchemy as db
from sqlalchemy.orm import Session
import pymysql
import pytest
from google.protobuf import json_format
from config_test import MetadataRegistryConfig as config
from ensembl.production.metadata import service


def load_database():
    uri = config.METADATA_URI
    taxonomy_uri = config.TAXONOMY_URI

    try:
        engine = db.create_engine(uri)
        taxonomy_engine = db.create_engine(taxonomy_uri)
    except AttributeError:
        raise ValueError(f'Could not connect to database. Check METADATA_URI env variable.')

    try:
        connection = engine.connect()
        taxonomy_connection = taxonomy_engine.connect()
    except db.exc.OperationalError as err:
        raise ValueError(f'Could not connect to database {uri}: {err}.') from err

    connection.close()
    taxonomy_connection.close()
    return engine, taxonomy_engine
    pass


def test_create_genome():
    """Test service.create_genome function"""
    input_dict = {
        'genome_uuid': 'f9d8c1dc-45dd-11ec-81d3-0242ac130003',
        'ensembl_name':  'some name',
        'url_name': 'http://url_name.com',
        'display_name': 'Display Name',
        'is_current': True,
        'accession': 'X.AE500',
        'name': 'assembly name',
        'ucsc_name': 'ucsc name',
        'level': 'level',
        'taxonomy_id': '1234',
        'scientific_name': 'scientific name',
        'strain': 'test strain'
    }
    expected_output = {
        'genome_uuid': 'f9d8c1dc-45dd-11ec-81d3-0242ac130003',
        'ensembl_name': 'some name',
        'url_name': 'http://url_name.com',
        'display_name': 'Display Name',
        'is_current': True,
        'assembly' : {
            'accession': 'X.AE500',
            'name': 'assembly name',
            'ucsc_name': 'ucsc name',
            'level': 'level'
        },
        'taxon': {
            'taxonomy_id': '1234',
            'scientific_name': 'scientific name',
            'strain': 'test strain'
        }
    }
    output = json_format.MessageToJson(service.create_genome(input_dict))
    assert output == expected_output


def test_create_assembly():
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

    output = json_format.MessageToJson(service.create_assembly(input_dict))
    assert output == expected_output

def test_create_karyotype():
    input_dict = {
        'genome_uuid': 'f9d8c1dc-45dd-11ec-81d3-0242ac130003',
        'code': '5',
        'chromosomal': '25',
        'location': '129729'
    }

    expected_output = {
        'genome_uuid': 'f9d8c1dc-45dd-11ec-81d3-0242ac130003',
        'code': '5',
        'chromosomal': '25',
        'location': '129729'
    }

    output = json_format.MessageToJson(service.create_karyotype(input_dict))
    assert output == expected_output


def test_create_species():
    input_dict = {
        'genome_uuid': 'f9d8c1dc-45dd-11ec-81d3-0242ac130003',
        'common_name': 'cow',
        'ncbi_common_name': 'cattle',
        'scientific_name': 'Bos taurus',
        'alternative_names': ["bovine", "cow", "dairy cow", "domestic cattle", "domestic cow"],
        'taxonomy_id': 9913
    }

    expected_output = {
        'genome_uuid': 'f9d8c1dc-45dd-11ec-81d3-0242ac130003',
        'common_name': 'cow',
        'ncbi_common_name': 'cattle',
        'scientific_name': 'Bos taurus',
        'alternative_names': ["bovine", "cow", "dairy cow", "domestic cattle", "domestic cow"],
        'taxonomy_id': 9913
    }

    output = json_format.MessageToJson(service.create_species(input_dict))
    assert output == expected_output


def test_karyotype_information():
    output = json_format.MessageToJson(service.get_karyotype_information(db, '3c4cec7f-fb69-11eb-8dac-005056b32883'))
    expected_output = {}
    assert output == expected_output


def test_assembly_information():
    output = json_format.MessageToJson(service.get_assembly_information(db, '2'))
    expected_output = {
        'assembly_id': "2",
        'accession': "GCA_009873245.2",
        'level': "chromosome",
        'name': "mBalMus1.v2",
        'chromosomal': 1,
        'length': 185157308,
        'sequence_location': "SO:0000738",
        'sequence_checksum': "bb967773a69d45e191a5e0fcfe277f7c"
    }
    assert output == expected_output


def test_species_information():
    output = json_format.MessageToJson(service.get_species_information(db, '3c4cec7f-fb69-11eb-8dac-005056b32883'))
    expected_output = {
        'genome_uuid': "3c4cec7f-fb69-11eb-8dac-005056b32883",
        'ncbi_common_name': "Blue whale",
        'taxon_id': 9771,
        'scientific_name': "Balaenoptera musculus"
    }
    assert output == expected_output


def test_sub_species_info():
    output = json_format.MessageToJson(service.get_sub_species_info(db, '41'))
    expected_output = {
        'organism_id': "41",
        'species_type': "breeds",
        'species_name': "Dog breeds"
    }
    assert output == expected_output


def test_get_grouping_info():
    output = json_format.MessageToJson(service.get_grouping_info(db, '41'))
    expected_output = {
        'organism_id': "41",
        'species_type': "breeds",
        'species_name': "Dog breeds"
    }
    assert output == expected_output


def test_get_top_level_statistics():
    output = json_format.MessageToJson(service.get_top_level_statistics(db, '41'))
    assert len(output['statistics']) == 15
    assert output['statistics'][0] == {
        'name': "ungapped_genome",
        'label': "Base pairs",
        'statistic_type': "length_bp",
        'statistic_value': "2410429933"
    }
