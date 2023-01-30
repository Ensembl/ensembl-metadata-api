from ensembl.production.metadata.api import *
from ensembl.production.metadata.updater import *
from ensembl.production.metadata.api import *

from tests.test_api import TX_NAME

#MD_NAME = 'mysql://root:@127.0.0.1:3306/ensembl_metadata_2020'
#TX_NAME = 'mysql://root:@127.0.0.1:3306/ncbi_taxonomy'

MD_NAME = 'mysql://root:@127.0.0.1:3306/ensembl_metadata_2020'
DB_NAME = 'mysql://root:@127.0.0.1:3306/test_core'
DB_NAME2 = 'mysql://root:@127.0.0.1:3306/test_core2'
TX_NAME = 'mysql://root:@127.0.0.1:3306/ncbi_taxonomy'

#!DP! Currently local. Add the core_testdb_creator.sql to the travis file before push!!
#Entirely from the Meta table.
def test_new_organism():
    TEST = meta_factory(DB_NAME,MD_NAME)
    TEST.process_core()
    #Return Jaberwokie from the Organism table.
    conn = GenomeAdaptor(metadata_uri=MD_NAME, taxonomy_uri=TX_NAME)
    TEST_Collect = conn.fetch_genomes_by_ensembl_name('Jabberwocky')
    assert TEST_Collect[0].Organism.scientific_name == 'carol_jabberwocky'
    TEST_Collect2 = conn.fetch_genomes_by_ensembl_name('weird01')
    assert TEST_Collect2[0].Organism.scientific_name == 'carol_jabberwocky'

def test_update_assembly():
    TEST = meta_factory(DB_NAME2,MD_NAME)
    TEST.process_core()
    #Return Jaberwokie from the Organism table.
    conn = GenomeAdaptor(metadata_uri=MD_NAME, taxonomy_uri=TX_NAME)
    TEST_Collect = conn.fetch_genomes_by_ensembl_name('Jabberwocky')
    assert TEST_Collect[0].Organism.scientific_name == 'carol_jabberwocky'
    TEST_Collect2 = conn.fetch_genomes_by_ensembl_name('weird02')
    assert TEST_Collect2[0].Organism.scientific_name == 'carol_jabberwocky'
