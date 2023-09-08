# See the NOTICE file distributed with this work for additional information
#   regarding copyright ownership.
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#       http://www.apache.org/licenses/LICENSE-2.0
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
"""
Unit tests for api module
"""

import pytest

from ensembl.database import UnitTestDB
from ensembl.production.metadata.api.genome import GenomeAdaptor
from ensembl.production.metadata.api.release import ReleaseAdaptor


@pytest.mark.parametrize("multi_dbs", [[{'src': 'ensembl_metadata'}, {'src': 'ncbi_taxonomy'}]],
                         indirect=True)
class TestMetadataDB:
    dbc = None  # type: UnitTestDB

    def test_load_database(self, multi_dbs):
        db_test = ReleaseAdaptor(multi_dbs['ensembl_metadata'].dbc.url)
        assert db_test, "DB should not be empty"

    def fetch_all_genomes(self, multi_dbs):
        conn = ReleaseAdaptor(multi_dbs['ensembl_metadata'].dbc.url)
        test = conn.fetch_genomes()
        assert len(test) == 7

    def fetch_with_all_args_no_conflict(self, multi_dbs):
        conn = ReleaseAdaptor(multi_dbs['ensembl_metadata'].dbc.url)
        test = conn.fetch_genomes(
            genome_uuid="a733550b-93e7-11ec-a39d-005056b38ce3",
            assembly_accession="GCA_000002985.3",
            assembly_name="WBcel235",
            ensembl_name="caenorhabditis_elegans",
            taxonomy_id="6239",
            group="EnsemblMetazoa",
            unreleased_only=False,
            site_name="Ensembl",
            release_type="integrated",
            release_version="108.0",
            current_only=True
        )
        assert len(test) == 0

    def fetch_with_all_args_conflict(self, multi_dbs):
        conn = ReleaseAdaptor(multi_dbs['ensembl_metadata'].dbc.url)
        test = conn.fetch_genomes(
            genome_uuid="a733550b-93e7-11ec-a39d-005056b38ce3",
            assembly_accession="GCA_000002985.3",
            assembly_name="WBcel235",
            ensembl_name="caenorhabditis_elegans",
            taxonomy_id="9606",  # Conflicting taxonomy_id
            group="EnsemblBacteria",  # Conflicting group
            unreleased_only=False,
            site_name="Ensembl",
            release_type="integrated",
            release_version="108.0",
            current_only=True
        )
        assert test[0].Organism.scientific_name == 'Caenorhabditis elegans'

    def test_fetch_releases(self, multi_dbs):
        conn = ReleaseAdaptor(multi_dbs['ensembl_metadata'].dbc.url)
        test = conn.fetch_releases(release_id=2)
        # test the one to many connection
        assert test[0].EnsemblSite.name == 'Ensembl'
        assert test[0].EnsemblSite.label == 'Ensembl Genome Browser'
        # test the direct access.
        assert test[0].EnsemblRelease.label == 'Scaling Phase 1'

    # currently only have one release, so the testing is not comprehensive
    def test_fetch_releases_for_genome(self, multi_dbs):
        conn = ReleaseAdaptor(multi_dbs['ensembl_metadata'].dbc.url)
        test = conn.fetch_releases_for_genome('a73351f7-93e7-11ec-a39d-005056b38ce3')
        assert test[0].EnsemblSite.name == 'Ensembl'

    def test_fetch_releases_for_dataset(self, multi_dbs):
        conn = ReleaseAdaptor(multi_dbs['ensembl_metadata'].dbc.url)
        test = conn.fetch_releases_for_dataset('3316fe1a-83e7-46da-8a56-cf2b693d8060')
        assert test[0].EnsemblSite.name == 'Ensembl'

    def test_fetch_taxonomy_names(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_taxonomy_names(taxonomy_ids=[6239, 511145])
        assert test[511145]['scientific_name'] == 'Escherichia coli str. K-12 substr. MG1655'

    def test_fetch_taxonomy_ids(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_taxonomy_ids(taxonomy_names='Caenorhabditis elegans')
        assert test[0] == 6239

    def test_fetch_genomes(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genomes(genome_uuid='a7335667-93e7-11ec-a39d-005056b38ce3')
        assert test[0].Organism.scientific_name == 'Homo sapiens'


    # def test_fetch_genomes_by_group_division(self, multi_dbs):
    #     conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url,
    #                          taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
    #     division_filter = 'EnsemblVertebrates'
    #     test = conn.fetch_genomes(group=division_filter)
    #     assert len(test) == 1
#        Other PR will likely change this drastically, so the effort is not really necessary. Their are 7 groups.
#        assert division_filter in division_results


    def test_fetch_genomes_by_genome_uuid(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genomes_by_genome_uuid('a733550b-93e7-11ec-a39d-005056b38ce3')
        assert test[0].Organism.scientific_name == 'Caenorhabditis elegans'

    def test_fetch_genome_by_ensembl_and_assembly_name(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genomes(assembly_name='WBcel235', ensembl_name='caenorhabditis_elegans')
        assert test[0].Organism.scientific_name == 'Caenorhabditis elegans'

    def test_fetch_genomes_by_assembly_accession(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genomes_by_assembly_accession('GCA_000005845.2')
        assert test[0].Organism.scientific_name == 'Escherichia coli str. K-12 substr. MG1655 str. K12 (GCA_000005845)'

    def test_fetch_genomes_by_assembly_sequence_accession(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_sequences(
            genome_uuid='a7335667-93e7-11ec-a39d-005056b38ce3',
            assembly_accession='GCA_000001405.28',
            assembly_sequence_accession='CM000686.2'
        )
        assert test[0].AssemblySequence.name == 'Y'

    def test_fetch_genomes_by_assembly_sequence_accession_empty(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_sequences(
            genome_uuid='s0m3-r4nd0m-g3n3-uu1d-v4lu3',
            assembly_accession='GCA_000001405.28',
            assembly_sequence_accession='CM000686.2'
        )
        assert len(test) == 0

    def test_fetch_genomes_by_ensembl_name(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genomes_by_ensembl_name('caenorhabditis_elegans')
        assert test[0].Organism.scientific_name == 'Caenorhabditis elegans'

    def test_fetch_genomes_by_taxonomy_id(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genomes_by_taxonomy_id(6239)
        assert test[0].Organism.scientific_name == 'Caenorhabditis elegans'

    def test_fetch_genomes_by_scientific_name(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genomes_by_scientific_name(
            scientific_name='Caenorhabditis elegans',
            site_name='Ensembl'
        )
        assert test[0].Organism.scientific_name == 'Caenorhabditis elegans'

    def test_fetch_sequences(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_sequences(assembly_uuid='eeaaa2bf-151c-4848-8b85-a05a9993101e')
        assert test[0].AssemblySequence.accession == 'CHR_HG1_PATCH'

    def test_fetch_sequences_by_gneome_assembly(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_sequences(
            genome_uuid='a7335667-93e7-11ec-a39d-005056b38ce3',
            assembly_accession='GCA_000001405.28',
            chromosomal_only=False
        )
        assert test[-1].AssemblySequence.chromosomal == 0

    def test_fetch_sequences_chromosomal_only(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_sequences(
            genome_uuid='a7335667-93e7-11ec-a39d-005056b38ce3',
            assembly_accession='GCA_000001405.28',
            chromosomal_only=True
        )
        assert test[-1].AssemblySequence.chromosomal == 1

    def test_fetch_genome_dataset_default_topic_assembly(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genome_datasets(genome_uuid='a73357ab-93e7-11ec-a39d-005056b38ce3')
        assert test[0].DatasetType.topic == 'Core Annotation'

    def test_fetch_genome_dataset_uuid(self, multi_dbs):
        uuid = '0dc05c6e-2910-4dbd-879a-719ba97d5824'
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genome_datasets(dataset_uuid=uuid, dataset_name='genebuild')
        assert test[0].Dataset.dataset_uuid == uuid

    def test_fetch_genome_dataset_genome_uuid(self, multi_dbs):
        uuid = 'a73357ab-93e7-11ec-a39d-005056b38ce3'
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genome_datasets(genome_uuid=uuid)
        assert test[0].Genome.genome_uuid == uuid

    def test_fetch_genome_datasets(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genome_datasets()
        assert test[0].Dataset.dataset_uuid == '559d7660-d92d-47e1-924e-e741151c2cef'
        assert test[0].DatasetType.name == 'assembly'

    # TODO: fix it, there are no unreleased datasets (add one?)
    # def test_fetch_genome_datasets_unreleased(self, multi_dbs):
    #     conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url,
    #                          taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
    #     test = conn.fetch_genome_datasets(
    #         dataset_name="all",
    #         unreleased_datasets=True
    #     )
    #     print(f"test ===> {test}")
    #     assert test[0].GenomeDataset.release_id is None
    #     assert test[0].GenomeDataset.is_current == 0


    #Duplicate
    # def test_fetch_genome_info(self, multi_dbs):
    #     conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url,
    #                          taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
    #     test = conn.fetch_genomes_info()
    #     result = next(test)[0]
    #     assert 'genome' in result
    #     assert 'datasets' in result


    # def test_fetch_genome_info_genome_uuid(self, multi_dbs):
    #     uuid = 'a7335667-93e7-11ec-a39d-005056b38ce3'
    #     conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url,
    #                          taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
    #     test = conn.fetch_genomes_info(genome_uuid=uuid)
    #     assert test['genome'][0].genome_uuid == uuid
    #     assert test['datasets'][0][0].genome_uuid == uuid
