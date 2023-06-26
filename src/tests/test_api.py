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
        test = conn.fetch_taxonomy_names(taxonomy_ids=(6239, 511145))
        assert test[511145]['scientific_name'] == 'Escherichia coli str. K-12 substr. MG1655'

    def test_fetch_taxonomy_ids(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_taxonomy_ids(taxonomy_names='Caenorhabditis elegans')
        assert test[0] == 6239

    def test_fetch_genomes(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genomes()
        assert test[0].Organism.scientific_name == 'Caenorhabditis elegans'

    def test_fetch_genomes_by_group_division(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        division_filter = 'EnsemblVertebrates'
        test = conn.fetch_genomes(group=division_filter)
        division_res = set([row[-1].name for row in test])
        assert len(division_res) == 1
        assert division_filter in division_res

    def test_fetch_genomes_by_genome_uuid(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genomes_by_genome_uuid('a733550b-93e7-11ec-a39d-005056b38ce3')
        assert test[0].Organism.scientific_name == 'Caenorhabditis elegans'

    def test_fetch_genome_by_ensembl_and_assembly_name(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genome_by_ensembl_and_assembly_name('caenorhabditis_elegans', 'WBcel235')
        assert test[0].Organism.scientific_name == 'Caenorhabditis elegans'

    def test_fetch_genomes_by_assembly_accession(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genomes_by_assembly_accession('GCA_000005845.2')
        assert test[0].Organism.scientific_name == 'Escherichia coli str. K-12 substr. MG1655 str. K12 (GCA_000005845)'

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
        test = conn.fetch_genomes_by_scientific_name('Caenorhabditis elegans')
        assert test[0].Organism.scientific_name == 'Caenorhabditis elegans'

    def test_fetch_sequences(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_sequences()
        assert test[0].AssemblySequence.accession == 'KI270757.1'

    def test_fetch_genome_dataset_default_topic_assembly(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genome_datasets(genome_uuid='a73357ab-93e7-11ec-a39d-005056b38ce3')
        assert test[0][3].topic == 'Core Annotation'

    def test_fetch_genome_dataset_uuid(self, multi_dbs):
        uuid = '0dc05c6e-2910-4dbd-879a-719ba97d5824'
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genome_datasets(dataset_uuid=uuid, dataset_name='genebuild')
        assert test[0][2].dataset_uuid == uuid

    def test_fetch_genome_dataset_genome_uuid(self, multi_dbs):
        uuid = 'a73357ab-93e7-11ec-a39d-005056b38ce3'
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genome_datasets(genome_uuid=uuid)
        assert test[0][0].genome_uuid == uuid

    def test_fetch_genome_dataset_unreleased(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genome_datasets(unreleased_datasets=True)
        assert test[0][1].release_id is None
        assert test[0][1].is_current == False

    def test_fetch_genome_info(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genome_datasets(unreleased_datasets=True)
        print(test)
        assert test[0][1].release_id is None
        assert test[0][1].is_current is False

    def test_fetch_genome_info_unreleased(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genome_datasets(unreleased_datasets=True)
        assert test[0][1].release_id is None
        assert test[0][1].is_current == False

    def test_fetch_genome_info(self, multi_dbs):
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genomes_info()
        result = next(test)[0]
        assert 'genome' in result
        assert 'datasets' in result

    def test_fetch_genome_info_genome_uuid(self, multi_dbs):
        uuid = 'a7335667-93e7-11ec-a39d-005056b38ce3'
        conn = GenomeAdaptor(metadata_uri=multi_dbs['ensembl_metadata'].dbc.url,
                             taxonomy_uri=multi_dbs['ncbi_taxonomy'].dbc.url)
        test = conn.fetch_genomes_info(genome_uuid=uuid)
        result = next(test)[0]
        assert result['genome'][0].genome_uuid == uuid
        assert result['datasets'][0][0].genome_uuid == uuid
