import os


class MetadataRegistryConfig:
    METADATA_URI = os.environ.get("METADATA_URI", None)
