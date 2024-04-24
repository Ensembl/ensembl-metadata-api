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
import logging
from pathlib import Path
from typing import List

import pytest
from ensembl.database import UnitTestDB, DBConnection
from google.protobuf import json_format

from ensembl.production.metadata.api.models import Genome, Dataset
from ensembl.production.metadata.grpc import ensembl_metadata_pb2, utils

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("multi_dbs", [[{'src': Path(__file__).parent / "databases/ensembl_genome_metadata"},
                                        {'src': Path(__file__).parent / "databases/ncbi_taxonomy"}]],
                         indirect=True)
class TestUtils:
    dbc: UnitTestDB = None

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
    def test_get_alternative_names(self, genome_conn, taxon_id, expected_output):
        output = utils.get_alternative_names(genome_conn, taxon_id)
        assert output == expected_output

    def test_get_assembly_information(self, genome_conn):
        output = json_format.MessageToJson(
            utils.get_assembly_information(genome_conn, "fd7fea38-981a-4d73-a879-6f9daef86f08"))
        expected_output = {
            'accession': 'GCA_000001405.29',
            'assemblyUuid': 'fd7fea38-981a-4d73-a879-6f9daef86f08',
            'chromosomal': 1,
            'length': '248956422',
            'level': 'chromosome',
            'md5': '2648ae1bacce4ec4b6cf337dcae37816',
            'name': 'GRCh38.p14',
            'sequenceLocation': 'SO:0000738',
            'sha512t24u': '2YnepKM7OkBoOrKmvHbGqguVfF9amCST'
        }
        assert json.loads(output) == expected_output

    @pytest.mark.parametrize(
        "allow_unreleased, expected_count",
        [
            (False, 1),
            (True, 2)
        ],
        indirect=['allow_unreleased']
    )
    def test_get_genomes_from_assembly_accession_iterator(self, genome_conn, allow_unreleased, expected_count):
        output = [
            json.loads(json_format.MessageToJson(response)) for response in
            utils.get_genomes_from_assembly_accession_iterator(
                db_conn=genome_conn, assembly_accession="GCA_000005845.2", release_version=None
            )
        ]

        assert len(output) == expected_count

    @pytest.mark.parametrize(
        "assembly_accession, release_version",
        [
            # null
            (None, None),
            # no matches
            ("asdfasdfadf", None),
        ]
    )
    def test_get_genomes_from_assembly_accession_iterator_null(self, genome_conn, assembly_accession,
                                                               release_version):
        output = [
            json.loads(json_format.MessageToJson(response)) for response in
            utils.get_genomes_from_assembly_accession_iterator(
                db_conn=genome_conn, assembly_accession=assembly_accession, release_version=release_version
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

    def test_get_top_level_statistics(self, genome_conn):
        # Triticum aestivum
        output = json_format.MessageToJson(
            utils.get_top_level_statistics(
                db_conn=genome_conn,
                group="EnsemblPlants",
                organism_uuid="86dd50f1-421e-4829-aca5-13ccc9a459f6",
            )
        )
        output = json.loads(output)
        first_genome_stats = output["statsByGenomeUuid"][0]["statistics"]
        assert first_genome_stats[0] == {
            'label': 'assembly.accession',
            'name': 'assembly.accession',
            'statisticType': 'string',
            'statisticValue': 'GCA_900519105.1'
        }
        assert first_genome_stats[1] == {
            'label': 'Chromosomes or plasmids',
            'name': 'assembly.chromosomes',
            'statisticType': 'integer',
            'statisticValue': '22'
        }

    def test_get_top_level_statistics_by_uuid(self, genome_conn):
        output = json_format.MessageToJson(
            utils.get_top_level_statistics_by_uuid(
                genome_conn, "a73357ab-93e7-11ec-a39d-005056b38ce3"
            )
        )
        output = json.loads(output)
        assert output["statistics"][0] == {
            'label': 'assembly.accession',
            'name': 'assembly.accession',
            'statisticType': 'string',
            'statisticValue': 'GCA_900519105.1'
        }
        assert output["statistics"][2] == {
            'label': 'Component sequences',
            'name': 'assembly.component_sequences',
            'statisticType': 'integer',
            'statisticValue': '22'
        }

    def test_get_datasets_list_by_uuid(self, genome_conn):
        # the expected_output is too long and duplicated
        # because of the returned attributes
        # TODO: Fix this later
        output = json_format.MessageToJson(
            utils.get_datasets_list_by_uuid(genome_conn, "a73357ab-93e7-11ec-a39d-005056b38ce3", 110.1))
        assert json.loads(output) is not None

    def test_get_datasets_list_by_uuid_no_results(self, genome_conn):
        output = json_format.MessageToJson(
            utils.get_datasets_list_by_uuid(genome_conn, "some-random-uuid-f00-b4r", 103.0))
        output = json.loads(output)
        expected_output = {}
        assert output == expected_output

    @pytest.mark.parametrize(
        "allow_unreleased, genome_uuid, dataset_type, count",
        [
            (False, '9caa2cae-d1c8-4cfc-9ffd-2e13bc3e95b1', 'assembly', 1),
            (False, '9caa2cae-d1c8-4cfc-9ffd-2e13bc3e95b1', 'genebuild', 1),
            (False, '9caa2cae-d1c8-4cfc-9ffd-2e13bc3e95b1', 'homologies', 1),
            (True, '9caa2cae-d1c8-4cfc-9ffd-2e13bc3e95b1', 'homologies', 2)
        ],
        indirect=['allow_unreleased']
    )
    def test_get_dataset_by_genome_and_dataset_type(self, multi_dbs, genome_conn, allow_unreleased, genome_uuid,
                                                    dataset_type, count):
        genome_datasets = utils.get_dataset_by_genome_and_dataset_type(genome_conn, genome_uuid, dataset_type)
        logger.debug(genome_datasets)
        output = json_format.MessageToJson(genome_datasets, including_default_value_fields=True)
        output = json.loads(output)
        metadata_db = DBConnection(multi_dbs['ensembl_genome_metadata'].dbc.url)
        with metadata_db.session_scope() as session:
            genome: Genome = session.query(Genome).filter(Genome.genome_uuid == genome_uuid).one()
            datasets: List[Dataset] = [ds.dataset for ds in genome.genome_datasets if
                                       ds.dataset.dataset_type.name == dataset_type]
            datasets_uuids = set([dataset['datasetUuid'] for dataset in output['datasetInfos']])
            logger.debug(datasets_uuids)
            logger.debug(output['datasetInfos'])
            assert len(datasets_uuids) == count
            logger.debug(datasets[0].dataset_attributes)
            if dataset_type == 'genebuild':
                assert datasets[0].version is not None
                assert datasets[0].label == 'GCA_018473315.1_ENS01'
            elif dataset_type == 'assembly':
                assert datasets[0].version is None
                assert datasets[0].label == 'GCA_018473315.1'
            elif dataset_type == 'homologies':
                assert datasets[0].version is not None

            assert output['genomeUuid'] == genome.genome_uuid
            assert [dataset['datasetUuid'] == datasets[0].dataset_uuid for dataset in output['datasetInfos']]

    def test_get_dataset_by_genome_id_no_results(self, genome_conn):
        output = json_format.MessageToJson(
            utils.get_dataset_by_genome_and_dataset_type(genome_conn, "a7335667-93e7-11ec-a39d-005056b38ce3",
                                                         "blah blah blah"))
        output = json.loads(output)
        assert output == {}

    @pytest.mark.parametrize(
        "production_name, assembly_name, use_default, expected_output",
        [
            ("homo_sapiens", "GRCh38.p14", False, {"genomeUuid": "a7335667-93e7-11ec-a39d-005056b38ce3"}),
            ("homo_sapiens", "GRCh38.p14", True, {}),
            ("homo_sapiens", "GRCh38", True, {"genomeUuid": "a7335667-93e7-11ec-a39d-005056b38ce3"}),
            ("random_production_name", "random_assembly_name", True, {}),
            ("random_production_name", "random_assembly_name", False, {}),
        ]
    )
    def test_get_genome_uuid(self, genome_conn, production_name, assembly_name, use_default, expected_output):
        output = json_format.MessageToJson(
            utils.get_genome_uuid(
                db_conn=genome_conn,
                production_name=production_name,
                assembly_name=assembly_name,
                use_default=use_default
            ))
        assert json.loads(output) == expected_output

    def test_get_genome_by_uuid(self, genome_conn):
        output = json_format.MessageToJson(
            utils.get_genome_by_uuid(
                db_conn=genome_conn,
                genome_uuid="a73357ab-93e7-11ec-a39d-005056b38ce3",
                release_version=110.1
            ))
        expected_output = {
            'assembly': {
                'accession': 'GCA_900519105.1',
                'assemblyUuid': '36d6c4f3-8072-4ae3-a485-84a070e725e3',
                'ensemblName': 'IWGSC',
                'isReference': True,
                'level': 'chromosome',
                'name': 'IWGSC',
                'urlName': 'iwgsc'
            },
            'attributesInfo': {
                'assemblyDate': '2018-07',
                'assemblyLevel': 'chromosome',
                'assemblyProviderName': 'International Wheat Genome '
                                        'Sequencing Consortium',
                'assemblyProviderUrl': 'https://www.ebi.ac.uk/ena/data/view/GCA_900519105.1',
                'genebuildMethod': 'import',
                'genebuildMethodDisplay': 'Import',
                'genebuildProviderName': 'PGSB',
                'genebuildProviderUrl': 'https://www.helmholtz-munich.de/en/pgsb',
                'genebuildSampleGene': 'TraesCS3D02G273600',
                'genebuildSampleLocation': '3D:2585940-2634711',
                'genebuildVersion': 'EXT01',
                'variationSampleVariant': '1A:58609:1A_58609'
            },
            'created': '2023-09-22 15:04:29',
            'genomeUuid': 'a73357ab-93e7-11ec-a39d-005056b38ce3',
            'organism': {
                'commonName': 'Bread wheat',
                'ensemblName': 'SAMEA4791365',
                'organismUuid': '86dd50f1-421e-4829-aca5-13ccc9a459f6',
                'scientificName': 'Triticum aestivum',
                'scientificParlanceName': 'Wheat',
                'speciesTaxonomyId': 4565,
                'strain': 'Chinese Spring',
                'strainType': 'cultivar',
                'taxonomyId': 4565
            },
            'release': {
                'releaseDate': '2023-06-15',
                'releaseLabel': 'First Beta',
                'releaseVersion': 108.0,
                'siteLabel': 'MVP Ensembl',
                'siteName': 'Ensembl',
                'siteUri': 'https://beta.ensembl.org'
            },
            'taxon': {
                'alternativeNames': ['Canadian hard winter wheat',
                                     'Triticum aestivum subsp. aestivum',
                                     'Triticum vulgare',
                                     'bread wheat',
                                     'common wheat',
                                     'wheat'],
                'scientificName': 'Triticum aestivum',
                'strain': 'Chinese Spring',
                'taxonomyId': 4565
            }
        }
        assert json.loads(output) == expected_output

    def test_genome_by_uuid_release_version_unspecified(self, genome_conn):
        output = json_format.MessageToJson(
            utils.get_genome_by_uuid(
                db_conn=genome_conn,
                genome_uuid="a73357ab-93e7-11ec-a39d-005056b38ce3",
                release_version=None
            ))
        expected_output = {
            'assembly': {
                'accession': 'GCA_900519105.1',
                'assemblyUuid': '36d6c4f3-8072-4ae3-a485-84a070e725e3',
                'ensemblName': 'IWGSC',
                'isReference': True,
                'level': 'chromosome',
                'name': 'IWGSC',
                'urlName': 'iwgsc'
            },
            'attributesInfo': {
                'assemblyDate': '2018-07',
                'assemblyLevel': 'chromosome',
                'assemblyProviderName': 'International Wheat Genome '
                                        'Sequencing Consortium',
                'assemblyProviderUrl': 'https://www.ebi.ac.uk/ena/data/view/GCA_900519105.1',
                'genebuildMethod': 'import',
                'genebuildMethodDisplay': 'Import',
                'genebuildProviderName': 'PGSB',
                'genebuildProviderUrl': 'https://www.helmholtz-munich.de/en/pgsb',
                'genebuildSampleGene': 'TraesCS3D02G273600',
                'genebuildSampleLocation': '3D:2585940-2634711',
                'genebuildVersion': 'EXT01',
                'variationSampleVariant': '1A:58609:1A_58609'
            },
            'created': '2023-09-22 15:04:29',
            'genomeUuid': 'a73357ab-93e7-11ec-a39d-005056b38ce3',
            'organism': {
                'commonName': 'Bread wheat',
                'ensemblName': 'SAMEA4791365',
                'organismUuid': '86dd50f1-421e-4829-aca5-13ccc9a459f6',
                'scientificName': 'Triticum aestivum',
                'scientificParlanceName': 'Wheat',
                'speciesTaxonomyId': 4565,
                'strain': 'Chinese Spring',
                'strainType': 'cultivar',
                'taxonomyId': 4565
            },
            'release': {
                'releaseDate': '2023-06-15',
                'releaseLabel': 'First Beta',
                'releaseVersion': 108.0,
                'siteLabel': 'MVP Ensembl',
                'siteName': 'Ensembl',
                'siteUri': 'https://beta.ensembl.org'
            },
            'taxon': {
                'alternativeNames': ['Canadian hard winter wheat',
                                     'Triticum aestivum subsp. aestivum',
                                     'Triticum vulgare',
                                     'bread wheat',
                                     'common wheat',
                                     'wheat'],
                'scientificName': 'Triticum aestivum',
                'strain': 'Chinese Spring',
                'taxonomyId': 4565
            }
        }
        assert json.loads(output) == expected_output

    def test_get_genomes_by_uuid_null(self, genome_conn):
        output = utils.get_genome_by_uuid(genome_conn, None, 0)
        assert output == ensembl_metadata_pb2.Genome()

    def test_get_genomes_by_keyword(self, genome_conn):
        output = [json.loads(json_format.MessageToJson(response)) for response in
                  utils.get_genomes_by_keyword_iterator(genome_conn, "Human", 110.1)]

        assert len(output) == 5
        assert all(genome['organism']['commonName'].lower() == 'human' for genome in output)

    @pytest.mark.parametrize(
        "allow_unreleased, output_count",
        [(True, 14), (False, 5)],
        indirect=['allow_unreleased']
    )
    def test_get_genomes_by_keyword_unreleased(self, genome_conn, allow_unreleased, output_count):
        unreleased = [json.loads(json_format.MessageToJson(response)) for response in
                      utils.get_genomes_by_keyword_iterator(genome_conn, "Human")]
        assert len(unreleased) == output_count

    @pytest.mark.parametrize(
        "allow_unreleased, output_count",
        [(True, 14), (False, 5)],
        indirect=['allow_unreleased']
    )
    def test_get_genomes_by_keyword_release_unspecified(self, genome_conn, allow_unreleased, output_count):
        output = [json.loads(json_format.MessageToJson(response)) for response in
                  utils.get_genomes_by_keyword_iterator(genome_conn, "Homo Sapiens")]
        assert len(output) == output_count
        assert all(genome['taxon']['scientificName'] == 'Homo sapiens' for genome in output)

    def test_get_genomes_by_keyword_null(self, genome_conn):
        output = list(
            utils.get_genomes_by_keyword_iterator(genome_conn, None, 0))
        assert output == []

    def test_get_genomes_by_keyword_no_matches(self, genome_conn):
        output = list(
            utils.get_genomes_by_keyword_iterator(genome_conn, "bigfoot",
                                                  1))
        assert output == []

    def test_get_genomes_by_name(self, genome_conn):
        output = json_format.MessageToJson(
            utils.get_genome_by_name(db_conn=genome_conn, biosample_id="SAMN17861241", site_name="Ensembl",
                                     release_version=110.1))
        expected_output = {
            'assembly': {
                'accession': 'GCA_018469415.1',
                'assemblyUuid': '1551e511-bde7-40cf-95cd-de4059678c6f',
                'ensemblName': 'HG03516.alt.pat.f1_v2',
                'level': 'primary_assembly',
                'name': 'HG03516.alt.pat.f1_v2'
            },
            'attributesInfo': {
                'assemblyDate': '2021-05',
                'assemblyLevel': 'scaffold',
                'genebuildLastGenesetUpdate': '2022-07',
                'genebuildMethod': 'projection_build',
                'genebuildMethodDisplay': 'Mapping from reference',
                'genebuildProviderName': 'Ensembl',
                'genebuildProviderUrl': 'https://rapid.ensembl.org/info/genome/genebuild/full_genebuild.html',
                'genebuildVersion': 'ENS01',
                'variationSampleVariant': 'JAGYYT010000001.1:2643538:rs1423484253'
            },
            'created': '2023-09-22 15:02:01',
            'genomeUuid': '2020e8d5-4d87-47af-be78-0b15e48970a7',
            'organism': {
                'commonName': 'human',
                'ensemblName': 'SAMN17861241',
                'organismUuid': 'a3352834-cea1-40aa-9dad-98581620c36b',
                'scientificName': 'Homo sapiens',
                'scientificParlanceName': 'Human',
                'speciesTaxonomyId': 9606,
                'strain': 'Esan in Nigeria',
                'strainType': 'population',
                'taxonomyId': 9606
            },
            'release': {
                'isCurrent': True,
                'releaseDate': '2023-10-18',
                'releaseLabel': 'MVP Beta-1',
                'releaseVersion': 110.1,
                'siteLabel': 'MVP Ensembl',
                'siteName': 'Ensembl',
                'siteUri': 'https://beta.ensembl.org'
            },
            'taxon': {
                'alternativeNames': ['human'],
                'scientificName': 'Homo sapiens',
                'strain': 'Esan in Nigeria',
                'taxonomyId': 9606
            }
        }
        assert json.loads(output) == expected_output

    def test_get_genomes_by_name_release_unspecified(self, genome_conn):
        # We are expecting the same result as test_get_genomes_by_name() above
        # because no release is specified get_genome_by_name() -> fetch_genomes
        # checks if the fetched genome is released and picks it up
        output = json_format.MessageToJson(
            utils.get_genome_by_name(db_conn=genome_conn, biosample_id="SAMN04256190", site_name="Ensembl",
                                     release_version=None))
        expected_output = {
            'assembly': {
                'accession': 'GCA_000002985.3',
                'assemblyUuid': '2598e56f-a579-4fec-9525-0939563056bd',
                'ensemblName': 'WBcel235',
                'isReference': True,
                'level': 'chromosome',
                'name': 'WBcel235',
                'urlName': 'wbcel235'
            },
            'attributesInfo': {
                'assemblyDate': '2012-12',
                'assemblyLevel': 'complete genome',
                'assemblyProviderName': 'WormBase',
                'assemblyProviderUrl': 'http://www.wormbase.org',
                'genebuildLastGenesetUpdate': '2014-10',
                'genebuildMethod': 'import',
                'genebuildMethodDisplay': 'Import',
                'genebuildProviderName': 'Wormbase',
                'genebuildProviderUrl': 'https://wormbase.org/',
                'genebuildSampleGene': 'WBGene00004893',
                'genebuildSampleLocation': 'X:937766-957832',
                'genebuildVersion': 'EXT01'
            },
            'created': '2023-09-22 15:06:58',
            'genomeUuid': 'a733550b-93e7-11ec-a39d-005056b38ce3',
            'organism': {
                'commonName': 'Roundworm',
                'ensemblName': 'SAMN04256190',
                'organismUuid': 'b181947a-a725-4866-ada4-5433e5dfdcac',
                'scientificName': 'Caenorhabditis elegans',
                'scientificParlanceName': 'Roundworm',
                'speciesTaxonomyId': 6239,
                'strain': 'N2',
                'strainType': 'strain',
                'taxonomyId': 6239
            },
            'release': {
                'releaseDate': '2023-06-15',
                'releaseLabel': 'First Beta',
                'releaseVersion': 108.0,
                'siteLabel': 'MVP Ensembl',
                'siteName': 'Ensembl',
                'siteUri': 'https://beta.ensembl.org'
            },
            'taxon': {
                'alternativeNames': ['Rhabditis elegans'],
                'scientificName': 'Caenorhabditis elegans',
                'strain': 'N2',
                'taxonomyId': 6239
            }
        }
        assert json.loads(output) == expected_output

    def test_get_organisms_group_count(self, genome_conn):
        output = json_format.MessageToJson(
            utils.get_organisms_group_count(
                db_conn=genome_conn,
                release_version=None
            )
        )
        expected_output = {
            "organismsGroupCount": [
                {
                    'commonName': 'Human',
                    'count': 5,
                    'order': 1,
                    'scientificName': 'Homo sapiens',
                    'speciesTaxonomyId': 9606
                }
            ]
        }
        # make sure it returns 41 organisms
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
    def test_get_genome_uuid_by_tag(self, genome_conn, genome_tag, expected_output):
        output = json_format.MessageToJson(
            utils.get_genome_uuid_by_tag(
                db_conn=genome_conn,
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
    def test_ftp_links(self, genome_conn, genome_uuid, dataset_type, release_version, expected_output):
        output = json_format.MessageToJson(
            utils.get_ftp_links(
                db_conn=genome_conn,
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
            ("a73351f7-93e7-11ec-a39d-005056b38ce3", None, None, {"releaseVersion": 108.0}),
            # wrong genome_uuid
            ("some-random-genome-uuid-000000000000", None, None, {}),
            # genome_uuid and data_type_name
            ("a73351f7-93e7-11ec-a39d-005056b38ce3", "genebuild", None, {"releaseVersion": 108.0}),
            # genome_uuid and release_version
            ("a73351f7-93e7-11ec-a39d-005056b38ce3", "genebuild", 111.1, {"releaseVersion": 108.0}),
            # genome_uuid, data_type_name and release_version
            ("a73351f7-93e7-11ec-a39d-005056b38ce3", "genebuild", 111.1, {"releaseVersion": 108.0}),
            # genome_uuid, data_type_name is all and release_version
            ("a73351f7-93e7-11ec-a39d-005056b38ce3", "homologies", 111.1, {"releaseVersion": 108.0}),
            # genome_uuid, data_type_name is all and release_version
            # FIXME the message must change, since for all datasets, release Version might be different per dataset type
            # Service would return a list of Version per datasetType
            # ("a73351f7-93e7-11ec-a39d-005056b38ce3", "all", 111.1, {"releaseVersion": 110.1}),
            # no genome_uuid
            (None, "genebuild", 111.1, {}),
            # empty params
            (None, None, None, {}),
        ]
    )
    def test_get_release_version_by_uuid(self, genome_conn, genome_uuid, dataset_type, release_version,
                                         expected_output):
        output = json_format.MessageToJson(
            utils.get_release_version_by_uuid(
                db_conn=genome_conn,
                genome_uuid=genome_uuid,
                dataset_type=dataset_type,
                release_version=release_version
            ))
        assert json.loads(output) == expected_output
