from ensembl.production.metadata.api import *
from ensembl.production.metadata.updater import *
from tests.test_api import TX_NAME

#MD_NAME = 'mysql://root:@127.0.0.1:3306/ensembl_metadata_2020'
#TX_NAME = 'mysql://root:@127.0.0.1:3306/ncbi_taxonomy'

DB_NAME = 'mysql://danielp:Killadam69!@localhost:3306/acanthochromis_polyacanthus_core_109_1'
MD_NAME = 'mysql://danielp:Killadam69!@localhost:3306/ensembl_metadata_2020'

#!DP! Currently local. Add the core_testdb_creator.sql to the travis file before push!!
#Entirely from the Meta table.
TEST = meta_factory(DB_NAME,MD_NAME)
TEST.process_core()
#Return Jaberwokie from the Organism table.


#Testing the integration of a new organism


#Testing the update of said organism

#Testing the insertion of a new assembly


#Testing the update of an assembly


#Testing the addition of a new dataset

#Testing the update of a dataset