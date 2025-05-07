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

from sqlalchemy.orm import aliased

from ensembl.production.metadata.api.models import Dataset, Genome, GenomeDataset, DatasetAttribute, Attribute, Assembly


def get_genome_sets_by_assembly_and_provider(session):
    """
    Retrieves only those sets where multiple genome_uuids share the same assembly_uuid and genebuild.provider.
    Also includes each genome's genebuild.last_geneset_update value for reference.

    :param session: SQLAlchemy session object
    :return: Dictionary where keys are (assembly_uuid, provider) tuples and values are lists of (genome_uuid, last_geneset_update)

    DOES NOT HAVE A TEST. NOR DO WE HAVE UPDATES IN OUR TEST DB. BIG WORK TO UPDATE THIS.
    """

    # Aliases for clarity
    genome_alias = aliased(Genome)
    dataset_alias = aliased(Dataset)
    dataset_attr_provider = aliased(DatasetAttribute)  # Attribute for genebuild.provider
    dataset_attr_geneset = aliased(DatasetAttribute)  # Attribute for genebuild.last_geneset_update
    attribute_provider = aliased(Attribute)
    attribute_geneset = aliased(Attribute)
    assembly_alias = aliased(Assembly)

    # Query to retrieve genome_uuid, assembly_uuid, provider, and last_geneset_update
    query = (
        session.query(
            genome_alias.genome_uuid,
            assembly_alias.assembly_uuid,
            dataset_attr_provider.value.label("provider_name"),
            dataset_attr_geneset.value.label("last_geneset_update")
        )
        .join(assembly_alias, genome_alias.assembly_id == assembly_alias.assembly_id)
        .join(GenomeDataset, GenomeDataset.genome_id == genome_alias.genome_id)
        .join(dataset_alias, GenomeDataset.dataset_id == dataset_alias.dataset_id)
        # Join for provider attribute
        .join(dataset_attr_provider, dataset_attr_provider.dataset_id == dataset_alias.dataset_id)
        .join(attribute_provider, dataset_attr_provider.attribute_id == attribute_provider.attribute_id)
        # Join for last_geneset_update attribute
        .join(dataset_attr_geneset, dataset_attr_geneset.dataset_id == dataset_alias.dataset_id)
        .join(attribute_geneset, dataset_attr_geneset.attribute_id == attribute_geneset.attribute_id)
        .filter(
            dataset_alias.dataset_type.has(name="genebuild"),  # Ensure dataset is of type genebuild
            attribute_provider.name == "genebuild.provider_name",  # Ensure attribute is genebuild.provider_name
            attribute_geneset.name == "genebuild.last_geneset_update"
            # Ensure attribute is genebuild.last_geneset_update
        )
    )

    # Organize results into a dictionary grouping genome_uuids by (assembly_uuid, provider)
    genome_sets = {}
    for genome_uuid, assembly_uuid, provider, last_geneset_update in query.all():
        key = (assembly_uuid, provider)
        if key not in genome_sets:
            genome_sets[key] = []
        genome_sets[key].append((genome_uuid, last_geneset_update))  # Keep last_geneset_update with each genome

    # Create a filtered dictionary where only groups with more than one genome are kept
    genome_sets_with_multiple = {key: genomes for key, genomes in genome_sets.items() if len(genomes) > 1}

    return genome_sets_with_multiple
