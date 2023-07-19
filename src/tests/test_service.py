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
import json
import os.path
from pathlib import Path

import pkg_resources
import pytest
import sqlalchemy as db
from ensembl.database import UnitTestDB
from google.protobuf import json_format

from ensembl.production.metadata import service, ensembl_metadata_pb2

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

    def test_create_genome(self, multi_dbs):
        """Test service.create_genome function"""
        input_dict = {
            "genome_uuid": "f9d8c1dc-45dd-11ec-81d3-0242ac130003",
            "created": datetime.date(2022, 8, 15),
            "ensembl_name": "some name",
            "url_name": "http://url_name.com",
            "display_name": "Display Name",
            "is_current": True,
            "assembly_accession": "X.AE500",
            "assembly_name": "assembly name",
            "assembly_ucsc_name": "ucsc name",
            "assembly_level": "level",
            "assembly_ensembl_name": "some assembly ensembl name",
            "taxonomy_id": 1234,
            "scientific_name": "scientific name",
            "scientific_parlance_name": "scientific_parlance_name",
            "strain": "test strain",
            "release_version": 1,
            "release_date": datetime.date(2022, 8, 15),
            "release_label": "release_label",
        }
        expected_output = {
            "assembly": {
                "accession": "X.AE500",
                "level": "level",
                "name": "assembly name",
                "ucscName": "ucsc name",
                "ensemblName": "some assembly ensembl name",
            },
            "created": "2022-08-15",
            "genomeUuid": "f9d8c1dc-45dd-11ec-81d3-0242ac130003",
            "organism": {
                "displayName": "Display Name",
                "ensemblName": "some name",
                "scientificName": "scientific name",
                "scientificParlanceName": "scientific_parlance_name",
                "strain": "test strain",
                "urlName": "http://url_name.com",
            },
            "release": {
                "isCurrent": True,
                "releaseDate": "2022-08-15",
                "releaseLabel": "release_label",
                "releaseVersion": 1,
            },
            "taxon": {
                "scientificName": "scientific name",
                "strain": "test strain",
                "taxonomyId": 1234,
            },
        }
        output = json_format.MessageToJson(service.create_genome(input_dict))
        assert json.loads(output) == expected_output

    def test_create_assembly(self, multi_dbs):
        input_dict = {
            "assembly_uuid": "1234",
            "accession": "XE.1234",
            "level": "5",
            "name": "test name",
            "chromosomal": 1223,
            "length": 5,
            "sequence_location": "location",
            "sequence_checksum": "checksum",
            "ga4gh_identifier": "test identifier",
        }

        expected_output = {
            "assemblyUuid": "1234",
            "accession": "XE.1234",
            "level": "5",
            "name": "test name",
            "chromosomal": 1223,
            "length": 5,
            "sequenceLocation": "location",
            "sequenceChecksum": "checksum",
            "ga4ghIdentifier": "test identifier",
        }

        output = json_format.MessageToJson(service.create_assembly(input_dict))
        assert json.loads(output) == expected_output

    def test_create_karyotype(self, multi_dbs):
        input_dict = {
            "genome_uuid": "f9d8c1dc-45dd-11ec-81d3-0242ac130003",
            "code": "5",
            "chromosomal": "25",
            "location": "129729",
        }

        expected_output = {
            "genomeUuid": "f9d8c1dc-45dd-11ec-81d3-0242ac130003",
            "code": "5",
            "chromosomal": "25",
            "location": "129729",
        }

        output = json_format.MessageToJson(service.create_karyotype(input_dict))
        assert json.loads(output) == expected_output

    def test_create_species(self, multi_dbs):
        input_dict = {
            "genome_uuid": "f9d8c1dc-45dd-11ec-81d3-0242ac130003",
            "common_name": "cow",
            "ncbi_common_name": "cattle",
            "scientific_name": "Bos taurus",
            "alternative_names": [
                "bovine",
                "cow",
                "dairy cow",
                "domestic cattle",
                "domestic cow",
            ],
            "scientific_parlance_name": "Bos taurus",
            "taxonomy_id": 9913,
        }

        expected_output = {
            "genomeUuid": "f9d8c1dc-45dd-11ec-81d3-0242ac130003",
            "commonName": "cow",
            "ncbiCommonName": "cattle",
            "scientificName": "Bos taurus",
            "scientificParlanceName": "Bos taurus",
            "alternativeNames": [
                "bovine",
                "cow",
                "dairy cow",
                "domestic cattle",
                "domestic cow",
            ],
            "taxonId": 9913,
        }

        output = json_format.MessageToJson(service.create_species(input_dict))
        assert json.loads(output) == expected_output

    def test_create_top_level_statistics(self, multi_dbs):
        input_dict = {
            "organism_uuid": "48357d41-0029-4ba6-8f66-66f526f71603",
            "statistics": [
                {
                    "name": "transcript_genomic_mnoncoding",
                    "label": "Non-coding transcript",
                    "statistic_type": "length_bp",
                    "statistic_value": "5873",
                },
                {
                    "name": "transcript_genomic_pseudogene",
                    "label": "Pseudogenic transcript",
                    "statistic_type": "length_bp",
                    "statistic_value": "3305648",
                },
            ]
        }

        expected_output = {
            "organismUuid": "48357d41-0029-4ba6-8f66-66f526f71603",
            "statistics": [
                {"label": "Non-coding transcript",
                 "name": "transcript_genomic_mnoncoding",
                 "statisticType": "length_bp",
                 "statisticValue": "5873"},
                {"label": "Pseudogenic transcript",
                 "name": "transcript_genomic_pseudogene",
                 "statisticType": "length_bp",
                 "statisticValue": "3305648"}]
        }

        output = json_format.MessageToJson(
            service.create_top_level_statistics(input_dict)
        )
        assert json.loads(output) == expected_output

    def test_create_genome_sequence(self, multi_dbs):
        input_dict = {
            "accession": "XQ1234",
            "name": "test_seq",
            "sequence_location": "some location",
            "length": 1234,
            "chromosomal": True,
        }
        expected_output = {
            "accession": "XQ1234",
            "chromosomal": True,
            "length": 1234,
            "name": "test_seq",
            "sequenceLocation": "some location",
        }
        output = json_format.MessageToJson(service.create_genome_sequence(input_dict))
        assert json.loads(output) == expected_output

    def test_create_release(self, multi_dbs):
        input_dict = {
            "release_version": 5,
            "release_date": "12-10-2020",
            "release_label": "prod",
            "is_current": False,
            "site_name": "EBI",
            "site_label": "EBI",
            "site_uri": "test uri",
        }
        expected_output = {
            "releaseDate": "12-10-2020",
            "releaseLabel": "prod",
            "releaseVersion": 5,
            "siteLabel": "EBI",
            "siteName": "EBI",
            "siteUri": "test uri",
        }
        output = json_format.MessageToJson(service.create_release(input_dict))
        assert json.loads(output) == expected_output

    def test_karyotype_information(self, engine):
        output = json_format.MessageToJson(
            service.get_karyotype_information(engine, "3c4cec7f-fb69-11eb-8dac-005056b32883"))
        expected_output = {}
        assert json.loads(output) == expected_output

    def test_assembly_information(self, engine):
        output = json_format.MessageToJson(
            service.get_assembly_information(engine, "eeaaa2bf-151c-4848-8b85-a05a9993101e"))
        expected_output = {"accession": "GCA_000001405.28",
                           "assemblyUuid": "eeaaa2bf-151c-4848-8b85-a05a9993101e",
                           "length": 71251,
                           "level": "chromosome",
                           "name": "GRCh38.p13",
                           "sequenceLocation": "SO:0000738"}
        assert json.loads(output) == expected_output

    def test_get_genomes_from_assembly_accession_iterator(self, engine):
        output = [json.loads(json_format.MessageToJson(response)) for response in
                  service.get_genomes_from_assembly_accession_iterator(
                      engine, "GCA_000005845.2")]
        expected_output = [{"assembly": {"accession": "GCA_000005845.2",
                                         "ensemblName": "ASM584v2",
                                         "level": "chromosome",
                                         "name": "ASM584v2"},
                            "created": "2023-05-12 13:32:14",
                            "genomeUuid": "a73351f7-93e7-11ec-a39d-005056b38ce3",
                            "organism": {"displayName": "Escherichia coli str. K-12 substr. MG1655 str. "
                                                        "K12 (GCA_000005845)",
                                         "ensemblName": "escherichia_coli_str_k_12_substr_mg1655_gca_000005845",
                                         "scientificName": "Escherichia coli str. K-12 substr. MG1655 "
                                                           "str. K12 (GCA_000005845)",
                                         "urlName": "Escherichia_coli_str_k_12_substr_mg1655_gca_000005845"},
                            "release": {"isCurrent": True,
                                        "releaseDate": "2023-05-15",
                                        "releaseLabel": "Beta Release 1",
                                        "releaseVersion": 108.0},
                            "taxon": {"scientificName": "Escherichia coli str. K-12 substr. MG1655 str. "
                                                        "K12 (GCA_000005845)",
                                      "taxonomyId": 511145}}]
        assert output == expected_output

    def test_get_genomes_from_assembly_accession_iterator_null(self, engine):
        output = [json.loads(json_format.MessageToJson(response)) for response in
                  service.get_genomes_from_assembly_accession_iterator(engine, None)]
        assert output == []

    def test_get_genomes_from_assembly_accession_iterator_no_matches(self, engine):
        output = [json.loads(json_format.MessageToJson(response)) for response in
                  service.get_genomes_from_assembly_accession_iterator(engine, "asdfasdfadf")]
        assert output == []

    def test_sub_species_info(self, engine):
        output = json_format.MessageToJson(
            service.get_sub_species_info(engine, "1"))
        expected_output = {
            "organismId": "1",
            "speciesName": ["EnsemblVertebrates"],
            "speciesType": ["Division"]}
        assert json.loads(output) == expected_output

        output2 = json_format.MessageToJson(service.get_grouping_info(engine, "51"))
        expected_output2 = {}
        assert json.loads(output2) == expected_output2

    def test_get_grouping_info(self, engine):
        output = json_format.MessageToJson(service.get_grouping_info(engine, "1"))
        expected_output = {
            "organismId": "1",
            "speciesName": ["EnsemblVertebrates"],
            "speciesType": ["Division"]}
        assert json.loads(output) == expected_output

        output2 = json_format.MessageToJson(
            service.get_grouping_info(engine, "51"))
        expected_output2 = {}
        assert json.loads(output2) == expected_output2

    def test_get_top_level_statistics(self, engine):
        # Triticum aestivum
        output = json_format.MessageToJson(
            service.get_top_level_statistics(engine, "d64c34ca-b37a-476b-83b5-f21d07a3ae67")
        )
        output = json.loads(output)
        assert len(output["statistics"]) == 51
        assert output["statistics"][0] == {
            "label": "Contig N50",
            "name": "contig_n50",
            "statisticType": "bp",
            "statisticValue": "51842",
        }
        assert output["statistics"][1] == {
            "label": "Total genome length",
            "name": "total_genome_length",
            "statisticType": "bp",
            "statisticValue": "14547261565",
        }

    def test_get_top_level_statistics_by_uuid(self, engine):
        output = json_format.MessageToJson(
            service.get_top_level_statistics_by_uuid(
                engine, "a73357ab-93e7-11ec-a39d-005056b38ce3"
            )
        )
        output = json.loads(output)
        print(output)
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

    def test_get_datasets_list_by_uuid(self, engine):
        output = json_format.MessageToJson(
            service.get_datasets_list_by_uuid(engine, "a73357ab-93e7-11ec-a39d-005056b38ce3"))

        expected_output = {"datasets": {"assembly": {"datasetInfos": [{"datasetLabel": "GCA_900519105.1",
                                                                       "datasetName": "assembly",
                                                                       "datasetUuid": "b4ff55e3-d06a-4772-bb13-81c3207669e3",
                                                                       "version": 108.0}]},
                                        "evidence": {"datasetInfos": [{"datasetLabel": "Manual Add",
                                                                       "datasetName": "evidence",
                                                                       "datasetUuid": "64a66f22-07a9-476e-9816-785e2ccb9c30",
                                                                       "version": 108.0}]},
                                        "genebuild": {"datasetInfos": [{"datasetLabel": "2018-04-IWGSC",
                                                                        "datasetName": "genebuild",
                                                                        "datasetUuid": "0dc05c6e-2910-4dbd-879a-719ba97d5824",
                                                                        "version": 108.0}]},
                                        "homologies": {"datasetInfos": [{"datasetLabel": "Manual Add",
                                                                         "datasetName": "homologies",
                                                                         "datasetUuid": "e67ca09d-2e7b-4135-a990-6a2d1bca7285",
                                                                         "version": 108.0}]},
                                        "variation": {"datasetInfos": [{"datasetLabel": "Manual Add",
                                                                        "datasetName": "variation",
                                                                        "datasetUuid": "4d411e2d-676e-4fe0-b0d7-65a9e33fd47f",
                                                                        "version": 108.0}]}},
                           "genomeUuid": "a73357ab-93e7-11ec-a39d-005056b38ce3"}

        assert json.loads(output) == expected_output

    def test_get_datasets_list_by_uuid_no_results(self, engine):
        output = json_format.MessageToJson(service.get_datasets_list_by_uuid(engine, "some-random-uuid-f00-b4r", 103.0))
        output = json.loads(output)
        expected_output = {}
        assert output == expected_output

    def test_get_dataset_by_genome_id(self, engine):
        output = json_format.MessageToJson(
            service.get_dataset_by_genome_id(engine,
                                             "a73356e1-93e7-11ec-a39d-005056b38ce3", "assembly"))
        output = json.loads(output)
        assert output == {"datasetInfos": [{"datasetLabel": "GCA_000002765.2",
                                            "datasetName": "assembly",
                                            "datasetUuid": "29dbda41-5188-4323-9318-ce546a87eee7",
                                            "name": "chromosomes",
                                            "type": "integer",
                                            "value": "14",
                                            "version": 108.0},
                                           {"datasetLabel": "GCA_000002765.2",
                                            "datasetName": "assembly",
                                            "datasetUuid": "29dbda41-5188-4323-9318-ce546a87eee7",
                                            "name": "component_sequences",
                                            "type": "integer",
                                            "value": "14",
                                            "version": 108.0},
                                           {"datasetLabel": "GCA_000002765.2",
                                            "datasetName": "assembly",
                                            "datasetUuid": "29dbda41-5188-4323-9318-ce546a87eee7",
                                            "name": "gc_percentage",
                                            "type": "percent",
                                            "value": "19.34",
                                            "version": 108.0},
                                           {"datasetLabel": "GCA_000002765.2",
                                            "datasetName": "assembly",
                                            "datasetUuid": "29dbda41-5188-4323-9318-ce546a87eee7",
                                            "name": "spanned_gaps",
                                            "type": "integer",
                                            "value": "0",
                                            "version": 108.0},
                                           {"datasetLabel": "GCA_000002765.2",
                                            "datasetName": "assembly",
                                            "datasetUuid": "29dbda41-5188-4323-9318-ce546a87eee7",
                                            "name": "toplevel_sequences",
                                            "type": "integer",
                                            "value": "14",
                                            "version": 108.0},
                                           {"datasetLabel": "GCA_000002765.2",
                                            "datasetName": "assembly",
                                            "datasetUuid": "29dbda41-5188-4323-9318-ce546a87eee7",
                                            "name": "total_coding_sequence_length",
                                            "type": "bp",
                                            "value": "12309897",
                                            "version": 108.0},
                                           {"datasetLabel": "GCA_000002765.2",
                                            "datasetName": "assembly",
                                            "datasetUuid": "29dbda41-5188-4323-9318-ce546a87eee7",
                                            "name": "total_gap_length",
                                            "type": "bp",
                                            "value": "0",
                                            "version": 108.0},
                                           {"datasetLabel": "GCA_000002765.2",
                                            "datasetName": "assembly",
                                            "datasetUuid": "29dbda41-5188-4323-9318-ce546a87eee7",
                                            "name": "total_genome_length",
                                            "type": "bp",
                                            "value": "23292622",
                                            "version": 108.0}],
                          "datasetType": "assembly",
                          "genomeUuid": "a73356e1-93e7-11ec-a39d-005056b38ce3"}

    def test_get_dataset_by_genome_id_no_results(self, engine):
        output = json_format.MessageToJson(
            service.get_dataset_by_genome_id(engine,
                                             "a7335667-93e7-11ec-a39d-005056b38ce3", "blah blah blah"))
        output = json.loads(output)
        assert output == {}

    def test_get_genome_uuid(self, engine):
        output = json_format.MessageToJson(
            service.get_genome_uuid(
                engine,
                "homo_sapiens", "GRCh37.p13"))
        expected_output = {
            "genomeUuid": "3704ceb1-948d-11ec-a39d-005056b38ce3"
        }
        assert json.loads(output) == expected_output

    def test_get_genome_by_uuid(self, engine):
        output = json_format.MessageToJson(
            service.get_genome_by_uuid(engine,
                                       "a73357ab-93e7-11ec-a39d-005056b38ce3", 108.0))
        expected_output = {"assembly": {"accession": "GCA_900519105.1",
                                        "ensemblName": "IWGSC",
                                        "level": "chromosome",
                                        "name": "IWGSC"},
                           "created": "2023-05-12 13:32:36",
                           "genomeUuid": "a73357ab-93e7-11ec-a39d-005056b38ce3",
                           "organism": {"displayName": "Triticum aestivum",
                                        "ensemblName": "triticum_aestivum",
                                        "scientificName": "Triticum aestivum",
                                        "strain": "reference (Chinese spring)",
                                        "urlName": "Triticum_aestivum"},
                           "release": {"isCurrent": True,
                                       "releaseDate": "2023-05-15",
                                       "releaseLabel": "Beta Release 1",
                                       "releaseVersion": 108.0},
                           "taxon": {"scientificName": "Triticum aestivum",
                                     "strain": "reference (Chinese spring)",
                                     "taxonomyId": 4565}}
        assert json.loads(output) == expected_output

    def test_genome_by_uuid_release_version_unspecified(self, engine):
        output = json_format.MessageToJson(
            service.get_genome_by_uuid(engine, "a73357ab-93e7-11ec-a39d-005056b38ce3", 0.0))
        expected_output = {"assembly": {"accession": "GCA_900519105.1",
                                        "ensemblName": "IWGSC",
                                        "level": "chromosome",
                                        "name": "IWGSC"},
                           "created": "2023-05-12 13:32:36",
                           "genomeUuid": "a73357ab-93e7-11ec-a39d-005056b38ce3",
                           "organism": {"displayName": "Triticum aestivum",
                                        "ensemblName": "triticum_aestivum",
                                        "scientificName": "Triticum aestivum",
                                        "strain": "reference (Chinese spring)",
                                        "urlName": "Triticum_aestivum"},
                           "release": {"isCurrent": True,
                                       "releaseDate": "2023-05-15",
                                       "releaseLabel": "Beta Release 1",
                                       "releaseVersion": 108.0},
                           "taxon": {"scientificName": "Triticum aestivum",
                                     "strain": "reference (Chinese spring)",
                                     "taxonomyId": 4565}}
        assert json.loads(output) == expected_output

    def test_get_genomes_by_uuid_null(self, engine):
        output = service.get_genome_by_uuid(engine, None, 0)
        assert output == ensembl_metadata_pb2.Genome()

    def test_get_genomes_by_keyword(self, engine):
        output = [json.loads(json_format.MessageToJson(response)) for response in
                  service.get_genomes_by_keyword_iterator(engine, "Human", 108.0)]
        expected_output = [{"assembly": {"accession": "GCA_000001405.28",
                                         "ensemblName": "GRCh38.p13",
                                         "level": "chromosome",
                                         "name": "GRCh38.p13",
                                         "ucscName": "hg38"},
                            "created": "2023-05-12 13:30:58",
                            "genomeUuid": "a7335667-93e7-11ec-a39d-005056b38ce3",
                            "organism": {"displayName": "Human",
                                         "ensemblName": "homo_sapiens",
                                         "scientificName": "Homo sapiens",
                                         "urlName": "Homo_sapiens"},
                            "release": {"isCurrent": True,
                                        "releaseDate": "2023-05-15",
                                        "releaseLabel": "Beta Release 1",
                                        "releaseVersion": 108.0},
                            "taxon": {"scientificName": "Homo sapiens", "taxonomyId": 9606}}]
        assert output == expected_output

    def test_get_genomes_by_keyword_release_unspecified(self, engine):
        output = [json.loads(json_format.MessageToJson(response)) for response in
                  service.get_genomes_by_keyword_iterator(engine, "Homo Sapiens", 0.0)]
        expected_output = [{"assembly": {"accession": "GCA_000001405.28",
                                         "ensemblName": "GRCh38.p13",
                                         "level": "chromosome",
                                         "name": "GRCh38.p13",
                                         "ucscName": "hg38"},
                            "created": "2023-05-12 13:30:58",
                            "genomeUuid": "a7335667-93e7-11ec-a39d-005056b38ce3",
                            "organism": {"displayName": "Human",
                                         "ensemblName": "homo_sapiens",
                                         "scientificName": "Homo sapiens",
                                         "urlName": "Homo_sapiens"},
                            "release": {"isCurrent": True,
                                        "releaseDate": "2023-05-15",
                                        "releaseLabel": "Beta Release 1",
                                        "releaseVersion": 108.0},
                            "taxon": {"scientificName": "Homo sapiens", "taxonomyId": 9606}}]
        assert output == expected_output

    def test_get_genomes_by_keyword_null(self, engine):
        output = list(
            service.get_genomes_by_keyword_iterator(engine, None, 0))
        assert output == []

    def test_get_genomes_by_keyword_no_matches(self, engine):
        output = list(
            service.get_genomes_by_keyword_iterator(engine, "bigfoot",
                                                    1))
        assert output == []

    def test_get_genomes_by_name(self, engine):
        output = json_format.MessageToJson(
            service.get_genome_by_name(engine, "homo_sapiens", "Ensembl", 108.0))
        expected_output = {"assembly": {"accession": "GCA_000001405.28",
                                        "ensemblName": "GRCh38.p13",
                                        "level": "chromosome",
                                        "name": "GRCh38.p13",
                                        "ucscName": "hg38"},
                           "created": "2023-05-12 13:30:58",
                           "genomeUuid": "a7335667-93e7-11ec-a39d-005056b38ce3",
                           "organism": {"displayName": "Human",
                                        "ensemblName": "homo_sapiens",
                                        "scientificName": "Homo sapiens",
                                        "urlName": "Homo_sapiens"},
                           "release": {"isCurrent": True,
                                       "releaseDate": "2023-05-15",
                                       "releaseLabel": "Beta Release 1",
                                       "releaseVersion": 108.0},
                           "taxon": {"scientificName": "Homo sapiens", "taxonomyId": 9606}}
        assert json.loads(output) == expected_output

    def test_get_genomes_by_name_release_unspecified(self, engine):
        output = json_format.MessageToJson(service.get_genome_by_name(engine, "homo_sapiens", "Ensembl", 0.0))
        expected_output = {"assembly": {"accession": "GCA_000001405.28",
                                        "ensemblName": "GRCh38.p13",
                                        "level": "chromosome",
                                        "name": "GRCh38.p13",
                                        "ucscName": "hg38"},
                           "created": "2023-05-12 13:30:58",
                           "genomeUuid": "a7335667-93e7-11ec-a39d-005056b38ce3",
                           "organism": {"displayName": "Human",
                                        "ensemblName": "homo_sapiens",
                                        "scientificName": "Homo sapiens",
                                        "urlName": "Homo_sapiens"},
                           "release": {"isCurrent": True,
                                       "releaseDate": "2023-05-15",
                                       "releaseLabel": "Beta Release 1",
                                       "releaseVersion": 108.0},
                           "taxon": {"scientificName": "Homo sapiens", "taxonomyId": 9606}}
        assert json.loads(output) == expected_output
