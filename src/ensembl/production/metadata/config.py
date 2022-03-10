import os


class MetadataConfig:
    metadata_uri = os.environ.get("METADATA_URI")
    taxonomy_uri = os.environ.get("TAXONOMY_URI")
    pool_size = os.environ.get("POOL_SIZE", 20)
    max_overflow = os.environ.get("MAX_OVERFLOW", 0)
