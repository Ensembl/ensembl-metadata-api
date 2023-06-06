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
import os
from ensembl.production.metadata.api import *

DB_HOST = os.getenv('DB_HOST', 'ensembl@127.0.0.1:3306')
DB_NAME = f'mysql://{DB_HOST}/test_ensembl_genome_metadata'
TX_NAME = f'mysql://{DB_HOST}/test_ncbi_taxonomy'

os.environ["METADATA_URI"] = DB_NAME
os.environ["TAXONOMY_URI"] = TX_NAME


def test_load_database():
    DB_TEST = ReleaseAdaptor(DB_NAME)
    assert DB_TEST, "DB should not be empty"


def test_fetch_releases():
    conn = ReleaseAdaptor(DB_NAME)
    TEST = conn.fetch_releases(release_id=2)
    # Test the one to many connection
    assert TEST[0].EnsemblSite.name == 'Test'
    # Test the direct access.
    assert TEST[0].EnsemblRelease.label == 'New'


# currently only have one release, so the testing is not comprehensive
def test_fetch_releases_for_genome():
    conn = ReleaseAdaptor(DB_NAME)
    TEST = conn.fetch_releases_for_genome('a73351f7-93e7-11ec-a39d-005056b38ce3')
    assert TEST[0].EnsemblSite.name == 'Test'


def test_fetch_releases_for_dataset():
    conn = ReleaseAdaptor(DB_NAME)
    TEST = conn.fetch_releases_for_dataset('3316fe1a-83e7-46da-8a56-cf2b693d8060')
    assert TEST[0].EnsemblSite.name == 'Test'


def test_fetch_taxonomy_names():
    conn = GenomeAdaptor(metadata_uri=DB_NAME, taxonomy_uri=TX_NAME)
    TEST = conn.fetch_taxonomy_names(taxonomy_ids=(6239, 511145))
    assert TEST[511145]['scientific_name'] == 'Escherichia coli str. K-12 substr. MG1655'


def test_fetch_taxonomy_ids():
    conn = GenomeAdaptor(metadata_uri=DB_NAME, taxonomy_uri=TX_NAME)
    TEST = conn.fetch_taxonomy_ids(taxonomy_names='Caenorhabditis elegans')
    assert TEST[0] == 6239


def test_fetch_genomes():
    conn = GenomeAdaptor(metadata_uri=DB_NAME, taxonomy_uri=TX_NAME)
    TEST = conn.fetch_genomes()
    assert TEST[0].Organism.scientific_name == 'Caenorhabditis elegans'


def test_fetch_genomes_by_group_division():
    conn = GenomeAdaptor(metadata_uri=DB_NAME, taxonomy_uri=TX_NAME)
    division_filter = 'EnsemblVertebrates'
    TEST = conn.fetch_genomes(group=division_filter)
    DIVISION_RES = set([row[-1].name for row in TEST])
    assert len(DIVISION_RES) == 1
    assert division_filter in DIVISION_RES


def test_fetch_genomes_by_genome_uuid():
    conn = GenomeAdaptor(metadata_uri=DB_NAME, taxonomy_uri=TX_NAME)
    TEST = conn.fetch_genomes_by_genome_uuid('a733550b-93e7-11ec-a39d-005056b38ce3')
    assert TEST[0].Organism.scientific_name == 'Caenorhabditis elegans'


def test_fetch_genomes_by_assembly_accession():
    conn = GenomeAdaptor(metadata_uri=DB_NAME, taxonomy_uri=TX_NAME)
    TEST = conn.fetch_genomes_by_assembly_accession('GCA_000005845.2')
    assert TEST[0].Organism.scientific_name == 'Escherichia coli str. K-12 substr. MG1655 str. K12 (GCA_000005845)'


def test_fetch_genomes_by_ensembl_name():
    conn = GenomeAdaptor(metadata_uri=DB_NAME, taxonomy_uri=TX_NAME)
    TEST = conn.fetch_genomes_by_ensembl_name('caenorhabditis_elegans')
    assert TEST[0].Organism.scientific_name == 'Caenorhabditis elegans'


def test_fetch_genomes_by_taxonomy_id():
    conn = GenomeAdaptor(metadata_uri=DB_NAME, taxonomy_uri=TX_NAME)
    TEST = conn.fetch_genomes_by_taxonomy_id(6239)
    assert TEST[0].Organism.scientific_name == 'Caenorhabditis elegans'


def test_fetch_genomes_by_scientific_name():
    conn = GenomeAdaptor(metadata_uri=DB_NAME, taxonomy_uri=TX_NAME)
    TEST = conn.fetch_genomes_by_scientific_name('Caenorhabditis elegans')
    assert TEST[0].Organism.scientific_name == 'Caenorhabditis elegans'


def test_fetch_sequences():
    conn = GenomeAdaptor(metadata_uri=DB_NAME, taxonomy_uri=TX_NAME)
    TEST = conn.fetch_sequences()
    assert TEST[0].AssemblySequence.accession == 'KI270757.1'


def test_fetch_genome_dataset_default_topic_assembly():
    conn = GenomeAdaptor(metadata_uri=DB_NAME, taxonomy_uri=TX_NAME)
    TEST = conn.fetch_genome_datasets()
    assert TEST[0][3].topic == 'assembly'


def test_fetch_genome_dataset_uuid():
    uuid = '559d7660-d92d-47e1-924e-e741151c2cef'
    conn = GenomeAdaptor(metadata_uri=DB_NAME, taxonomy_uri=TX_NAME)
    TEST = conn.fetch_genome_datasets(dataset_uuid=uuid)
    assert TEST[0][2].dataset_uuid == uuid


def test_fetch_genome_dataset_genome_uuid():
    uuid = 'a7335667-93e7-11ec-a39d-005056b38ce3'
    conn = GenomeAdaptor(metadata_uri=DB_NAME, taxonomy_uri=TX_NAME)
    TEST = conn.fetch_genome_datasets(genome_uuid=uuid)
    assert TEST[0][0].genome_uuid == uuid


def test_fetch_genome_dataset_unreleased():
    conn = GenomeAdaptor(metadata_uri=DB_NAME, taxonomy_uri=TX_NAME)
    TEST = conn.fetch_genome_datasets(unreleased_datasets=True)
    assert TEST[0][1].release_id is None
    assert TEST[0][1].is_current == False


def test_fetch_genome_info():
    conn = GenomeAdaptor(metadata_uri=DB_NAME, taxonomy_uri=TX_NAME)
    TEST = conn.fetch_genome_datasets(unreleased_datasets=True)
    assert TEST[0][1].release_id is None
    assert TEST[0][1].is_current == False


def test_fetch_genome_info_unreleased():
    conn = GenomeAdaptor(metadata_uri=DB_NAME, taxonomy_uri=TX_NAME)
    TEST = conn.fetch_genome_datasets(unreleased_datasets=True)
    assert TEST[0][1].release_id is None
    assert TEST[0][1].is_current == False


def test_fetch_genome_info():
    conn = GenomeAdaptor(metadata_uri=DB_NAME, taxonomy_uri=TX_NAME)
    TEST = conn.fetch_genomes_info()
    result = next(TEST)[0]
    assert 'genome' in result
    assert 'datasets' in result


def test_fetch_genome_info_genome_uuid():
    uuid = 'a7335667-93e7-11ec-a39d-005056b38ce3'
    conn = GenomeAdaptor(metadata_uri=DB_NAME, taxonomy_uri=TX_NAME)
    TEST = conn.fetch_genomes_info(genome_uuid=uuid)
    result = next(TEST)[0]
    assert result['genome'][0].genome_uuid == uuid
    assert result['datasets'][0][0].genome_uuid == uuid
