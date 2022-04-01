import os


class MetadataConfig:
    metadata_host = os.environ.get("METADATA_DB_HOST")
    metadata_port = os.environ.get("METADATA_DB_PORT")
    metadata_user = os.environ.get("METADATA_DB_USER")
    metadata_pass = os.environ.get("METADATA_DB_PASSWORD")

    taxon_host = os.environ.get("TAXON_DB_HOST")
    taxon_port = os.environ.get("TAXON_DB_PORT")
    taxon_user = os.environ.get("TAXON_DB_USER")
    taxon_pass = os.environ.get("TAXON_DB_PASSWORD")

    pool_size = os.environ.get("POOL_SIZE", 20)
    max_overflow = os.environ.get("MAX_OVERFLOW", 0)
