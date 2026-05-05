# See the NOTICE file distributed with this work for additional information
#   regarding copyright ownership.
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#       http://www.apache.org/licenses/LICENSE-2.0
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
import os
import re

from ensembl.production.metadata.api.models import Genome, Assembly


def get_genome_sets_by_assembly_and_provider(session):
    """
    Retrieves only those sets where multiple genome_uuids share the same assembly_uuid and genebuild.provider.
    Also includes each genome's genebuild.last_geneset_update value for reference.

    :param session: SQLAlchemy session object
    :return: Dictionary where keys are (assembly_uuid, provider) tuples and values are lists of (genome_uuid, last_geneset_update)

    DOES NOT HAVE A TEST. NOR DO WE HAVE UPDATES IN OUR TEST DB. BIG WORK TO UPDATE THIS.
    """

    # Query to retrieve genome_uuid, assembly_uuid, provider_name, and genebuild_date
    query = (
        session.query(
            Genome.genome_uuid,
            Assembly.assembly_uuid,
            Genome.provider_name,
            Genome.genebuild_date
        )
        .join(Assembly, Genome.assembly_id == Assembly.assembly_id)
    )

    # Organize results into a dictionary grouping genome_uuids by (assembly_uuid, provider)
    genome_sets = {}
    for genome_uuid, assembly_uuid, provider_name, genebuild_date in query.all():
        key = (assembly_uuid, provider_name)
        if key not in genome_sets:
            genome_sets[key] = []
        genome_sets[key].append((genome_uuid, genebuild_date))

    # Create a filtered dictionary where only groups with more than one genome are kept
    genome_sets_with_multiple = {key: genomes for key, genomes in genome_sets.items() if len(genomes) > 1}

    return genome_sets_with_multiple


def format_accession_path(accession: str) -> str:
    """
    Converts  assembly accession (e.g., 'GCF_043381705.1') to a structured path format
    (e.g., 'GCF/043/381/705/1').

    Parameters:
        accession (str): The accession string to format.

    Returns:
        str: Formatted path-like string.

    Raises:
        ValueError: If the accession format is invalid.
    """
    pattern = r'^(GCF|GCA)_(\d+)\.(\d+)$'
    match = re.match(pattern, accession)

    if not match:
        raise ValueError(f"Invalid accession format: '{accession}'. Expected format like '(GCF|GCA)_#########.#'.")

    prefix, number, version = match.groups()

    if len(number) > 9:
        raise ValueError(f"Unexpected number length in accession: '{number}'. Expected up to 9 digits.")

    chunks = [number[i:i + 3] for i in range(0, 9, 3)]

    # Ensure we have exactly 3 chunks, filling with '000' if necessary
    return os.path.join(prefix, *chunks, version)
