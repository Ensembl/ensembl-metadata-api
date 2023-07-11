
from ensembl.production.metadata.api.factory import meta_factory


test = meta_factory( 'mysql://danielp:Killadam69@localhost:3306/acanthochromis_polyacanthus_core_109_1',"mysql://danielp:Killadam69@localhost:3306/ensembl_genome_metadata",'mysql://danielp:Killadam69@localhost:3306/ncbi_taxonomy')
test.process_core()
