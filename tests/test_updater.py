
from ensembl.production.metadata.updater import *

MD_NAME = 'mysql://root:@127.0.0.1:3306/ensembl_genome_metadata'
TX_NAME = 'mysql://root:@127.0.0.1:3306/ncbi_taxonomy'
DB_NAME1 = 'mysql://root:@127.0.0.1:3306/test_core_1'
DB_NAME2 = 'mysql://root:@127.0.0.1:3306/test_core_2'
DB_NAME3 = 'mysql://root:@127.0.0.1:3306/test_core_3'
DB_NAME4 = 'mysql://root:@127.0.0.1:3306/test_core_4'


def test_new_organism():
    TEST = meta_factory(DB_NAME1, MD_NAME)
    TEST.process_core()
    # Look for organism, assembly and geneset
    conn = GenomeAdaptor(metadata_uri=MD_NAME, taxonomy_uri=TX_NAME)
    # Test the species
    TEST_Collect = conn.fetch_genomes_by_ensembl_name('Jabberwocky')
    assert TEST_Collect[0].Organism.scientific_name == 'carol_jabberwocky'
    # Test the Assembly
    assert TEST_Collect[0].Assembly.accession == 'weird01'
    # select * from genebuild where version = 999 and name = 'genebuild and label =01
    engine = create_engine(MD_NAME)
    metadata = MetaData()
    dataset = Table('dataset', metadata, autoload=True, autoload_with=engine)
    query = select([dataset]).where(
        (dataset.c.version == 999) & (dataset.c.name == 'genebuild') & (dataset.c.label == '01')
    )
    row = engine.execute(query).fetchone()
    assert row[-1] == '01'


#
def test_update_organism():
    TEST = meta_factory(DB_NAME2, MD_NAME)
    TEST.process_core()
    conn = GenomeAdaptor(metadata_uri=MD_NAME, taxonomy_uri=TX_NAME)
    TEST_Collect = conn.fetch_genomes_by_ensembl_name('Jabberwocky')
    assert TEST_Collect[0].Organism.scientific_name == 'lewis_carol'


def test_update_assembly():
    TEST = meta_factory(DB_NAME3, MD_NAME)
    TEST.process_core()
    conn = GenomeAdaptor(metadata_uri=MD_NAME, taxonomy_uri=TX_NAME)
    TEST_Collect = conn.fetch_genomes_by_ensembl_name('Jabberwocky')
    assert TEST_Collect[1].Organism.scientific_name == 'lewis_carol'
    assert TEST_Collect[1].Assembly.accession == 'weird02'


#
def test_update_geneset():
    TEST = meta_factory(DB_NAME4, MD_NAME)
    TEST.process_core()
    engine = create_engine(MD_NAME)
    metadata = MetaData()
    dataset = Table('dataset', metadata, autoload=True, autoload_with=engine)
    query = select([dataset]).where(
        (dataset.c.version == 999) & (dataset.c.name == 'genebuild') & (dataset.c.label == '02')
    )
    row = engine.execute(query).fetchone()
    assert row[-1] == '02'
