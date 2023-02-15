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
from ensembl.production.metadata.api import *

DB_NAME = 'mysql://root:@127.0.0.1:3306/ensembl_genome_metadata'
TX_NAME = 'mysql://root:@127.0.0.1:3306/ncbi_taxonomy'


def test_load_database():
    DB_TEST = ReleaseAdaptor(DB_NAME)
    assert DB_TEST, "DB should not be empty"


def test_fetch_releases():
    conn = ReleaseAdaptor(DB_NAME)
    TEST = conn.fetch_releases(release_id=1)
    # Test the one to many connection
    assert TEST[0].EnsemblSite.name == '2020-map'
    # Test the direct access.
    assert TEST[0].EnsemblRelease.label == '2020 MAP 7 species'


# currently only have one release, so the testing is not comprehensive
def test_fetch_releases_for_genome():
    conn = ReleaseAdaptor(DB_NAME)
    TEST = conn.fetch_releases_for_genome('a733574a-93e7-11ec-a39d-005056b38ce3')
    assert TEST[0].EnsemblSite.name == '2020-map'


def test_fetch_releases_for_dataset():
    conn = ReleaseAdaptor(DB_NAME)
    TEST = conn.fetch_releases_for_dataset('76ffa505-948d-11ec-a39d-005056b38ce3')
    assert TEST[0].EnsemblSite.name == '2020-map'


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


def test_fetch_genomes_by_genome_uuid():
    conn = GenomeAdaptor(metadata_uri=DB_NAME, taxonomy_uri=TX_NAME)
    TEST = conn.fetch_genomes_by_genome_uuid('a733550b-93e7-11ec-a39d-005056b38ce3')
    assert TEST[0].Organism.scientific_name == 'Caenorhabditis elegans'


def test_fetch_genomes_by_assembly_accession():
    conn = GenomeAdaptor(metadata_uri=DB_NAME, taxonomy_uri=TX_NAME)
    TEST = conn.fetch_genomes_by_assembly_accession('GCA_000005845.2')
    assert TEST[0].Organism.scientific_name == 'Escherichia coli str. K-12 substr. MG1655 str. K12'


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
    assert TEST[0].AssemblySequence.accession == 'CM000663.2'
