
class MetadataRegistryConfig:

    metadata_db_user = "ensro"
    metatata_db_name = "ensembl_metadata_2020_test"
    metadata_db_host = "mysql-ens-test-1"
    METADATA_URI = f"mysql+pymysql://{metadata_db_user}@{metadata_db_host}:4508/{metatata_db_name}"

    taxonomy_db_user = "ensro"
    taxonomy_db_name = "ncbi_taxonomy"
    taxonomy_db_host = "mysql-ens-meta-prod-1"
    TAXONOMY_URI = f"mysql+pymysql://{taxonomy_db_user}@{taxonomy_db_host}:4483/{taxonomy_db_name}"

    taxonomy_db_user = "ensro"
    taxonomy_db_name = "ncbi_taxonomy"
    taxonomy_db_host = "mysql-ens-meta-prod-1"
    TAXONOMY_URI = f"mysql+pymysql://{taxonomy_db_user}@{taxonomy_db_host}:4483/{taxonomy_db_name}"