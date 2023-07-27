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
import os.path
from pathlib import Path

import pkg_resources
import pytest
import sqlalchemy as db
from ensembl.database import UnitTestDB
from ensembl.production.metadata.api.genome import GenomeAdaptor
from ensembl.production.metadata.api.release import ReleaseAdaptor
from google.protobuf import json_format

from ensembl.production.metadata.grpc import ensembl_metadata_pb2, utils

distribution = pkg_resources.get_distribution("ensembl-metadata-api")
sample_path = Path(distribution.location) / "ensembl" / "production" / "metadata" / "api" / "sample"


@pytest.mark.parametrize("multi_dbs", [[{"src": sample_path / "ensembl_metadata"},
                                        {"src": sample_path / "ncbi_taxonomy"}]],
                         indirect=True)
class TestClass:
    dbc = None  # type: UnitTestDB

    @pytest.fixture(scope="class")
    def engine(self, multi_dbs):
        os.environ["METADATA_URI"] = multi_dbs["ensembl_metadata"].dbc.url
        os.environ["TAXONOMY_URI"] = multi_dbs["ncbi_taxonomy"].dbc.url
        yield db.create_engine(multi_dbs["ensembl_metadata"].dbc.url)

    @pytest.fixture(scope="class")
    def genome_db_conn(self, multi_dbs):
        genome_conn = GenomeAdaptor(
            metadata_uri=multi_dbs["ensembl_metadata"].dbc.url,
            taxonomy_uri=multi_dbs["ncbi_taxonomy"].dbc.url
        )
        yield genome_conn

    @pytest.fixture(scope="class")
    def release_db_conn(self, multi_dbs):
        release_conn = ReleaseAdaptor(
            metadata_uri=multi_dbs["ensembl_metadata"].dbc.url
        )
        yield release_conn

    def test_create_genome(self, multi_dbs, genome_db_conn):
        """Test service.create_genome function"""
        input_data = genome_db_conn.fetch_genomes(genome_uuid="a7335667-93e7-11ec-a39d-005056b38ce3")
        expected_output = {
          "genomeUuid": "a7335667-93e7-11ec-a39d-005056b38ce3",
          "assembly": {
            "accession": "GCA_000001405.28",
            "name": "GRCh38.p13",
            "ucscName": "hg38",
            "level": "chromosome",
            "ensemblName": "GRCh38.p13"
          },
          "taxon": {
            "taxonomyId": 9606,
            "scientificName": "Homo sapiens"
          },
          "created": "2023-05-12 13:30:58",
          "organism": {
            "displayName": "Human",
            "scientificName": "Homo sapiens",
            "urlName": "Homo_sapiens",
            "ensemblName": "homo_sapiens",
            "organismUuid": "db2a5f09-2db8-429b-a407-c15a4ca2876d"
          },
          "release": {
            "releaseVersion": 108.0,
            "releaseDate": "2023-05-15",
            "releaseLabel": "Beta Release 1",
            "isCurrent": True,
            "siteName": "Ensembl",
            "siteLabel": "Ensembl Genome Browser",
            "siteUri": "https://beta.ensembl.org"
          }
        }

        output = json_format.MessageToJson(utils.create_genome(input_data[0]))
        assert json.loads(output) == expected_output

    def test_create_assembly(self, multi_dbs, genome_db_conn):
        input_data = genome_db_conn.fetch_sequences(assembly_uuid="eeaaa2bf-151c-4848-8b85-a05a9993101e")
        expected_output = {
            "assemblyUuid": "eeaaa2bf-151c-4848-8b85-a05a9993101e",
            "accession": "GCA_000001405.28",
            "level": "chromosome",
            "name": "GRCh38.p13",
            "chromosomal": 1,
            "length": 107043717,
            "sequenceLocation": "SO:0000738"
        }

        output = json_format.MessageToJson(utils.create_assembly(input_data[0]))
        assert json.loads(output) == expected_output

    def test_create_karyotype(self, multi_dbs, genome_db_conn):
        input_data = genome_db_conn.fetch_sequences(genome_uuid="a7335667-93e7-11ec-a39d-005056b38ce3")
        expected_output = {
            "code": "chromosome",
            "chromosomal": "1",
            "location": "SO:0000738",
            "genomeUuid": "a7335667-93e7-11ec-a39d-005056b38ce3"
        }

        output = json_format.MessageToJson(utils.create_karyotype(input_data[0]))
        assert json.loads(output) == expected_output

    def test_create_species(self, multi_dbs, genome_db_conn):
        species_input_data = genome_db_conn.fetch_genomes(genome_uuid="a7335667-93e7-11ec-a39d-005056b38ce3")
        tax_id = species_input_data[0].Organism.taxonomy_id
        taxo_results = genome_db_conn.fetch_taxonomy_names(tax_id)
        expected_output = {
            "genomeUuid": "a7335667-93e7-11ec-a39d-005056b38ce3",
            "ncbiCommonName": "human",
            "taxonId": 9606,
            "scientificName": "Homo sapiens"
        }

        output = json_format.MessageToJson(utils.create_species(species_input_data[0], taxo_results[tax_id]))
        assert json.loads(output) == expected_output

    def test_create_top_level_statistics(self, multi_dbs, genome_db_conn):
        organism_uuid = "21279e3e-e651-43e1-a6fc-79e390b9e8a8"
        input_data = genome_db_conn.fetch_genome_datasets(organism_uuid=organism_uuid, dataset_name="all")

        statistics = []
        # getting just the first element
        statistics.append({
            'name': input_data[0].Attribute.name,
            'label': input_data[0].Attribute.label,
            'statistic_type': input_data[0].Attribute.type,
            'statistic_value': input_data[0].DatasetAttribute.value
        })

        expected_output = {
            "organismUuid": "21279e3e-e651-43e1-a6fc-79e390b9e8a8",
            "statistics": [
                {
                    "name": "total_genome_length",
                    "label": "Total genome length",
                    "statisticType": "bp",
                    "statisticValue": "4641652"
                }
            ]
        }

        output = json_format.MessageToJson(
            utils.create_top_level_statistics({
                'organism_uuid': organism_uuid,
                'statistics': statistics
            })
        )
        assert json.loads(output) == expected_output

    def test_create_genome_sequence(self, multi_dbs, genome_db_conn):
        input_data = genome_db_conn.fetch_sequences(genome_uuid="a7335667-93e7-11ec-a39d-005056b38ce3")
        expected_output = {
            "accession": "CHR_HG1_PATCH",
            "name": "CHR_HG1_PATCH",
            "sequenceLocation": "SO:0000738",
            "length": 107043717,
            "chromosomal": True
        }
        output = json_format.MessageToJson(utils.create_genome_sequence(input_data[0]))
        assert json.loads(output) == expected_output

    def test_create_release(self, multi_dbs, release_db_conn):
        input_data = release_db_conn.fetch_releases(release_version=108)
        expected_output = {
            "releaseVersion": 108.0,
            "releaseDate": "2023-05-15",
            "releaseLabel": "Beta Release 1",
            "isCurrent": True,
            "siteName": "Ensembl",
            "siteLabel": "Ensembl Genome Browser",
            "siteUri": "https://beta.ensembl.org"
        }
        output = json_format.MessageToJson(utils.create_release(input_data[0]))
        assert json.loads(output) == expected_output
