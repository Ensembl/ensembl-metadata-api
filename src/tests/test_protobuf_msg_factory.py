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
            "assemblyUuid": "eeaaa2bf-151c-4848-8b85-a05a9993101e",
            "name": "GRCh38.p13",
            "ucscName": "hg38",
            "level": "chromosome",
            "ensemblName": "GRCh38.p13",
            "isReference": True
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
            "scientificParlanceName": "homo_sapiens",
            "speciesTaxonomyId": 9606,
            "taxonomyId": 9606
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

    def test_create_assembly_info(self, multi_dbs, genome_db_conn):
        input_data = genome_db_conn.fetch_sequences(assembly_uuid="eeaaa2bf-151c-4848-8b85-a05a9993101e")
        expected_output = {
            "accession": "GCA_000001405.28",
            "assemblyUuid": "eeaaa2bf-151c-4848-8b85-a05a9993101e",
            # "chromosomal": 1,
            "length": "71251",
            "level": "chromosome",
            "name": "GRCh38.p13",
            "sequenceLocation": "SO:0000738"
        }

        output = json_format.MessageToJson(utils.create_assembly_info(input_data[0]))
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
            dataset_attributes=True,
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
            "length": "71251",
            # "name": "CHR_HG1_PATCH",
            "sequenceLocation": "SO:0000738"
        }
        output = json_format.MessageToJson(utils.create_genome_sequence(input_data[0]))
        assert json.loads(output) == expected_output

    def test_create_genome_assembly_sequence(self, multi_dbs, genome_db_conn):
        input_data = genome_db_conn.fetch_sequences(
            genome_uuid="a7335667-93e7-11ec-a39d-005056b38ce3",
        )
        # TODO: Check why this is failing when name and chromosomal is provided
        expected_output = {
            # "name": "CHR_HG1_PATCH",
            "length": "71251",
            # "chromosomal": True
        }
        output = json_format.MessageToJson(utils.create_genome_assembly_sequence(input_data[0]))
        assert json.loads(output) == expected_output

    def test_create_genome_assembly_sequence_region(self, multi_dbs, genome_db_conn):
        input_data = genome_db_conn.fetch_sequences(
            genome_uuid="a7335667-93e7-11ec-a39d-005056b38ce3",
            assembly_accession="GCA_000001405.28",
            assembly_sequence_accession="CM000686.2"
        )
        expected_output = {
            "name": "Y",
            "length": "57227415",
            "chromosomal": True
        }
        output = json_format.MessageToJson(utils.create_genome_assembly_sequence(input_data[0]))
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

    def test_create_organisms_group_count(self, multi_dbs, genome_db_conn):
        input_data = genome_db_conn.fetch_organisms_group_counts()
        expected_result = {
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
        # we have 6 organism in the test data
        assert len(input_data) == 6
        # send just the first element
        output = json_format.MessageToJson(
            utils.create_organisms_group_count(
                data=[input_data[0]],
                release_version=None
            )
        )
        assert json.loads(output) == expected_result

    def test_create_genome_info(self, genome_db_conn):
        genome_input_data = genome_db_conn.fetch_genomes(
            assembly_uuid="eeaaa2bf-151c-4848-8b85-a05a9993101e"
        )
        # Make sure we are only getting one
        assert len(genome_input_data) == 1

        attrib_input_data = genome_db_conn.fetch_genome_datasets(
            genome_uuid=genome_input_data[0].Genome.genome_uuid,
            dataset_attributes=True
        )
        # 11 attributes
        assert len(attrib_input_data) == 11

        related_assemblies_input_count = genome_db_conn.fetch_organisms_group_counts(
            species_taxonomy_id=genome_input_data[0].Organism.species_taxonomy_id
        )[0].count
        # There are three related assemblies
        assert related_assemblies_input_count == 3

        expected_result = {
          "genomeUuid": "a7335667-93e7-11ec-a39d-005056b38ce3",
          "assembly": {
            "accession": "GCA_000001405.28",
            "name": "GRCh38.p13",
            "ucscName": "hg38",
            "level": "chromosome",
            "ensemblName": "GRCh38.p13",
            "assemblyUuid": "eeaaa2bf-151c-4848-8b85-a05a9993101e",
            "isReference": True
          },
          "organism": {
            "commonName": "Human",
            "scientificName": "Homo sapiens",
            "ensemblName": "Homo_sapiens",
            "scientificParlanceName": "homo_sapiens",
            "organismUuid": "db2a5f09-2db8-429b-a407-c15a4ca2876d",
            "taxonomyId": 9606,
            "speciesTaxonomyId": 9606
          },
          "created": "2023-05-12 13:30:58",
          "attributesInfo": {
            "assemblyLevel": "chromosome",
            "assemblyDate": "2013-12"
          },
          "relatedAssembliesCount": 3,
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

        output = json_format.MessageToJson(
            utils.create_genome_info(
                data=genome_input_data[0],
                attributes=attrib_input_data,
                count=related_assemblies_input_count
            )
        )

        assert json.loads(output) == expected_result
