import os


class MetadataConfig:
    metadata_uri  = os.environ.get("METADATA_URI", f"mysql+pymysql://ensro@localhost:3306/ensembl_metadata_2020" )
    taxon_uri     = os.environ.get("TAXONOMY_URI", f"mysql+pymysql://ensro@localhost:3306/ncbi_taxonomy")
    pool_size     = os.environ.get("POOL_SIZE", 20)
    max_overflow  = os.environ.get("MAX_OVERFLOW", 0)
