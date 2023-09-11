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

from ensembl.production.metadata.grpc import utils

distribution = pkg_resources.get_distribution("ensembl-metadata-api")
sample_path = Path(distribution.location) / "ensembl" / "production" / "metadata" / "api" / "sample"


@pytest.mark.parametrize("multi_dbs", [[{"src": sample_path / "ensembl_metadata"},
                                        {"src": sample_path / "ncbi_taxonomy"}]],
                         indirect=True)
class TestClass:
    dbc = None  # type: UnitTestDB

    def test_create_genome(self, multi_dbs, genome_db_conn):
        """Test service.create_genome function"""
        input_data = genome_db_conn.fetch_genomes(
            genome_uuid="a7335667-93e7-11ec-a39d-005056b38ce3"
        )
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
            "commonName": "Human",
            "ensemblName": "Homo_sapiens",
            "organismUuid": "db2a5f09-2db8-429b-a407-c15a4ca2876d",
            "scientificName": "Homo sapiens",
            "scientificParlanceName": "homo_sapiens"
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
            "accession": "GCA_000001405.28",
            "assemblyUuid": "eeaaa2bf-151c-4848-8b85-a05a9993101e",
            # "chromosomal": 1,
            "length": 71251,
            "level": "chromosome",
            "name": "GRCh38.p13",
            "sequenceLocation": "SO:0000738"
        }

        output = json_format.MessageToJson(utils.create_assembly(input_data[0]))
        assert json.loads(output) == expected_output

    def test_create_karyotype(self, multi_dbs, genome_db_conn):
        input_data = genome_db_conn.fetch_sequences(genome_uuid="a7335667-93e7-11ec-a39d-005056b38ce3")
        expected_output = {
            "code": "chromosome",
            "chromosomal": "0",
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
            "genbankCommonName": "human",
            "genomeUuid": "a7335667-93e7-11ec-a39d-005056b38ce3",
            "scientificName": "Homo sapiens",
            "scientificParlanceName": "homo_sapiens",
            "taxonId": 9606
        }

        output = json_format.MessageToJson(utils.create_species(species_input_data[0], taxo_results[tax_id]))
        assert json.loads(output) == expected_output

    def test_create_top_level_statistics(self, multi_dbs, genome_db_conn):
        organism_uuid = "21279e3e-e651-43e1-a6fc-79e390b9e8a8"
        input_data = genome_db_conn.fetch_genome_datasets(
            organism_uuid=organism_uuid,
            dataset_name="all"
        )

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
                    "label": "Contig N50",
                    "name": "contig_n50",
                    "statisticType": "bp",
                    "statisticValue": "56413054"
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
            "accession": "KI270757.1",
            # "chromosomal": True,
            "length": 71251,
            # "name": "CHR_HG1_PATCH",
            "sequenceLocation": "SO:0000738"
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
