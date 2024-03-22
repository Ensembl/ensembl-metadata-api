#  See the NOTICE file distributed with this work for additional information
#  regarding copyright ownership.
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#      http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
import logging
import os
import warnings

logger = logging.getLogger(__name__)


def parse_boolean_var(var):
    """
    Parse an input variable into a boolean value.

    This function interprets the input `var` and attempts to convert it into a boolean value (`True` or `False`).
    It handles booleans and strings specifically, and defaults to `False` for other types with a warning.

    Args:
        var (bool|str|any): The variable to parse. This can be a boolean, a string, or any other type.
            - If it's a boolean, it's returned as-is.
            - If it's a string, it's considered `True` unless it's 'f', 'false', 'no', 'none', '0'
                or 'n' (case-insensitive), or it's an empty string.
            - For other types, a warning is issued, and `False` is returned.

    Returns:
        bool: The parsed boolean value. Returns `True` or `False` based on the input:
            - `True` if `var` is `True`, a non-falsy string not equal to 'f', 'false', 'no', 'none', '0', or 'n'.
            - `False` if `var` is `False`, a string equal to 'f', 'false', 'no', 'none', '0', or 'n', any non-string and
            non-boolean input, or an empty string.

    Raises:
        Warning: If `var` is not a boolean or a string, a warning is raised indicating the input
        couldn't be parsed to a boolean.
    """
    if isinstance(var, bool):
        return var
    elif isinstance(var, str):
        return not ((var.lower() in ("f", "false", "no", "none", "0", "n")) or (not var))
    else:
        # default to false, something is wrong.
        warnings.warn(f"Var {var} couldn't be parsed to boolean")
        return False


class MetadataConfig:

    def __init__(self):
        super().__init__()
        self.metadata_uri = os.environ.get("METADATA_URI",
                                           f"mysql://ensembl@localhost:3306/ensembl_genome_metadata")
        self.taxon_uri = os.environ.get("TAXONOMY_URI", f"mysql://ensembl@localhost:3306/marco_ncbi_taxonomy")
        self.pool_size = os.environ.get("POOL_SIZE", 20)
        self.max_overflow = os.environ.get("MAX_OVERFLOW", 0)
        self.pool_recycle = os.environ.get("POOL_RECYCLE", 50)
        self.allow_unreleased = parse_boolean_var(os.environ.get("ALLOW_UNRELEASED", False))
        self.ensembl_site_id = os.environ.get("ENSEMBL_SITE", 1)
        self.debug_mode = parse_boolean_var(os.environ.get("DEBUG", False))
        self.service_port = int(os.environ.get("SERVICE_PORT", 50051))

cfg = MetadataConfig()
