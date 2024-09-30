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
from datetime import datetime

from ensembl.production.metadata.grpc import ensembl_metadata_pb2
import logging

logger = logging.getLogger(__name__)


def create_species(species_data=None, taxo_info=None):
    if species_data is None:
        return ensembl_metadata_pb2.Species()

    species = ensembl_metadata_pb2.Species(
        genome_uuid=species_data.Genome.genome_uuid,
        taxon_id=species_data.Organism.taxonomy_id,
        scientific_name=species_data.Organism.scientific_name,
        scientific_parlance_name=species_data.Organism.scientific_parlance_name,
        genbank_common_name=taxo_info["genbank_common_name"],
        synonym=taxo_info["synonym"]
    )
    return species


def create_stats_by_genome_uuid(data=None):
    if data is None:
        return ensembl_metadata_pb2.TopLevelStatisticsByUUID()

    # list of TopLevelStatisticsByUUID (see the proto file)
    genome_uuid_stats = []
    # this dict will help us group stats by genome_uuid, protobuf is pain in the back...
    # it won't let us do that while constructing the object
    statistics = {}
    # data is [GenomeDatasetsListItem]
    # FIXME deduplicate and make sure we keep the right one Sure it can be simplified now we have the NamedTuple
    for result in data:
        # start creating a dictionary with genome_uuid as key and stats as value list
        if result.genome.genome_uuid not in list(statistics.keys()):
            statistics[result.genome.genome_uuid] = []
        for dataset in result.datasets:
            # item is GenomeDatasetItem
            dataset_stats = [attribute for attribute in dataset.attributes]
            statistics[result.genome.genome_uuid].extend(
                [ensembl_metadata_pb2.AttributeStatistics(
                    name=attribute.name,
                    label=attribute.label,
                    statistic_type=attribute.type,
                    statistic_value=attribute.value
                ) for attribute in dataset_stats])  # list of DatasetAttributeItem

    # now we can construct the object after having everything in statistics grouped by genome_uuid
    for genome_uuid in list(statistics.keys()):
        genome_uuid_stat = ensembl_metadata_pb2.TopLevelStatisticsByUUID()
        genome_uuid_stat.genome_uuid = genome_uuid
        for stat in statistics[genome_uuid]:
            genome_uuid_stat.statistics.append(stat)

        genome_uuid_stats.append(genome_uuid_stat)

    return genome_uuid_stats


def create_top_level_statistics(data=None):
    if data is None:
        return ensembl_metadata_pb2.TopLevelStatistics()

    species = ensembl_metadata_pb2.TopLevelStatistics(
        organism_uuid=data["organism_uuid"],
        stats_by_genome_uuid=data["stats_by_genome_uuid"],
    )
    return species


def create_top_level_statistics_by_uuid(data=None):
    if data is None:
        return ensembl_metadata_pb2.TopLevelStatisticsByUUID()

    species = ensembl_metadata_pb2.TopLevelStatisticsByUUID(
        genome_uuid=data["genome_uuid"],
        statistics=data["statistics"],
    )
    return species


def create_sub_species(data=None):
    if data is None:
        return ensembl_metadata_pb2.SubSpecies()

    sub_species = ensembl_metadata_pb2.SubSpecies(
        organism_uuid=data["organism_uuid"],
        species_name=data["species_name"],
        species_type=data["species_type"],
    )
    return sub_species


def create_assembly(data=None):
    if data is None:
        return ensembl_metadata_pb2.AssemblyInfo()

    assembly = ensembl_metadata_pb2.Assembly(
        assembly_uuid=data.Assembly.assembly_uuid,
        accession=data.Assembly.accession,
        level=data.Assembly.level,
        name=data.Assembly.name,
        ucsc_name=data.Assembly.ucsc_name,
        ensembl_name=data.Assembly.ensembl_name,
        is_reference=data.Assembly.is_reference,
        url_name=data.Assembly.url_name,
        tol_id=data.Assembly.tol_id,
    )
    return assembly


def create_taxon(data=None, alternative_names=[]):
    if data is None:
        return ensembl_metadata_pb2.Taxon()

    taxon = ensembl_metadata_pb2.Taxon(
        alternative_names=alternative_names,
        taxonomy_id=data.Organism.taxonomy_id,
        scientific_name=data.Organism.scientific_name,
        strain=data.Organism.strain,
    )
    return taxon


def create_organism(data=None):
    if data is None:
        return ensembl_metadata_pb2.Organism()

    organism = ensembl_metadata_pb2.Organism(
        common_name=data.Organism.common_name,
        strain=data.Organism.strain,
        strain_type=data.Organism.strain_type,
        scientific_name=data.Organism.scientific_name,
        ensembl_name=data.Organism.ensembl_name,
        scientific_parlance_name=data.Organism.scientific_parlance_name,
        organism_uuid=data.Organism.organism_uuid,
        taxonomy_id=data.Organism.taxonomy_id,
        species_taxonomy_id=data.Organism.species_taxonomy_id,
    )
    return organism


def create_attribute(data=None):
    if data is None:
        return ensembl_metadata_pb2.Attribute()

    attribute = ensembl_metadata_pb2.Attribute(
        name=data.Attribute.name,
        label=data.Attribute.label,
        description=data.Attribute.description,
        type=data.Attribute.type,
    )
    return attribute


def create_attributes_info(data=None):
    if data is None:
        return ensembl_metadata_pb2.AttributesInfo()

    # from EA-1105
    required_attributes = {
        "genebuild.method": "",
        "genebuild.method_display": "",
        "genebuild.last_geneset_update": "",
        "genebuild.version": "",
        "genebuild.provider_name": "",
        "genebuild.provider_url": "",
        "genebuild.sample_gene": "",
        "genebuild.sample_location": "",
        "assembly.level": "",
        "assembly.date": "",
        "assembly.provider_name": "",
        "assembly.provider_url": "",
        "variation.sample_variant": ""
    }
    # set required_attributes values
    if type(data) is list and len(data) > 0:
        pass
    else:
        return ensembl_metadata_pb2.AttributesInfo()

    for attrib_data in data:
        attrib_name = attrib_data.name
        if attrib_name in list(required_attributes.keys()):
            required_attributes[attrib_name] = attrib_data.value

    return ensembl_metadata_pb2.AttributesInfo(
        genebuild_method=required_attributes["genebuild.method"],
        genebuild_method_display=required_attributes["genebuild.method_display"],
        genebuild_last_geneset_update=required_attributes["genebuild.last_geneset_update"],
        genebuild_version=required_attributes["genebuild.version"],
        genebuild_provider_name=required_attributes["genebuild.provider_name"],
        genebuild_provider_url=required_attributes["genebuild.provider_url"],
        genebuild_sample_gene=required_attributes["genebuild.sample_gene"],
        genebuild_sample_location=required_attributes["genebuild.sample_location"],
        assembly_level=required_attributes["assembly.level"],
        assembly_date=required_attributes["assembly.date"],
        assembly_provider_name=required_attributes["assembly.provider_name"],
        assembly_provider_url=required_attributes["assembly.provider_url"],
        variation_sample_variant=required_attributes["variation.sample_variant"],
    )


def create_assembly_info(data=None):
    if data is None:
        return ensembl_metadata_pb2.AssemblyInfo()

    assembly_info = ensembl_metadata_pb2.AssemblyInfo(
        assembly_uuid=data.Assembly.assembly_uuid,
        accession=data.Assembly.accession,
        level=data.Assembly.level,
        name=data.Assembly.name,
        chromosomal=data.AssemblySequence.chromosomal,
        length=data.AssemblySequence.length,
        sequence_location=data.AssemblySequence.sequence_location,
        md5=data.AssemblySequence.md5,
        sha512t24u=data.AssemblySequence.sha512t24u,
    )
    return assembly_info


def create_genome_uuid(data=None):
    if data is None:
        return ensembl_metadata_pb2.GenomeUUID()

    genome_uuid = ensembl_metadata_pb2.GenomeUUID(
        genome_uuid=data["genome_uuid"]
    )
    return genome_uuid


def create_genome(data=None, attributes=None, count=0, alternative_names=[]):
    if data is None:
        return ensembl_metadata_pb2.Genome()

    assembly = create_assembly(data)
    taxon = create_taxon(data, alternative_names)
    organism = create_organism(data)
    attributes_info = create_attributes_info(attributes)
    release = create_release(data)

    genome = ensembl_metadata_pb2.Genome(
        genome_uuid=data.Genome.genome_uuid,
        created=str(data.Genome.created),
        assembly=assembly,
        taxon=taxon,
        organism=organism,
        attributes_info=attributes_info,
        release=release,
        related_assemblies_count=count
    )
    return genome


def create_brief_genome_details(data=None):
    if data is None:
        return ensembl_metadata_pb2.BriefGenomeDetails()

    assembly = create_assembly(data)
    organism = create_organism(data)
    release = create_release(data)

    brief_genome_details = ensembl_metadata_pb2.BriefGenomeDetails(
        genome_uuid=data.Genome.genome_uuid,
        created=str(data.Genome.created),
        assembly=assembly,
        organism=organism,
        release=release,
    )
    return brief_genome_details


def create_genome_sequence(data=None):
    if data is None:
        return ensembl_metadata_pb2.GenomeSequence()

    genome_sequence = ensembl_metadata_pb2.GenomeSequence(
        accession=data.AssemblySequence.accession,
        name=data.AssemblySequence.name,
        sequence_location=data.AssemblySequence.sequence_location,
        length=data.AssemblySequence.length,
        chromosomal=data.AssemblySequence.chromosomal
    )
    return genome_sequence


def create_assembly_region(data=None):
    if data is None:
        return ensembl_metadata_pb2.AssemblyRegion()

    assembly_region = ensembl_metadata_pb2.AssemblyRegion(
        name=data.AssemblySequence.name,
        rank=data.AssemblySequence.chromosome_rank,
        md5=data.AssemblySequence.md5,
        length=data.AssemblySequence.length,
        sha512t24u=data.AssemblySequence.sha512t24u,
        chromosomal=data.AssemblySequence.chromosomal
    )

    return assembly_region


def create_genome_assembly_sequence_region(data=None):
    if data is None:
        return ensembl_metadata_pb2.GenomeAssemblySequenceRegion()

    genome_assembly_sequence_region = ensembl_metadata_pb2.GenomeAssemblySequenceRegion(
        name=data.AssemblySequence.name,
        md5=data.AssemblySequence.md5,
        length=data.AssemblySequence.length,
        sha512t24u=data.AssemblySequence.sha512t24u,
        chromosomal=data.AssemblySequence.chromosomal
    )

    return genome_assembly_sequence_region


def create_release(data=None):
    if data is None or data.EnsemblRelease is None:
        return ensembl_metadata_pb2.Release()
    release = ensembl_metadata_pb2.Release(
        release_version=data.EnsemblRelease.version,
        release_date=str(data.EnsemblRelease.release_date) if data.EnsemblRelease.release_date else "Unreleased",
        release_label=data.EnsemblRelease.label,
        is_current=data.EnsemblRelease.is_current,
        site_name=data.EnsemblSite.name,
        site_label=data.EnsemblSite.label,
        site_uri=data.EnsemblSite.uri
    )
    return release


def create_release_version(data=None):
    """
    This function is used by Thoas to determine the MongoDB instance containing
    the data for a specified genome_uuid. It either constructs a ReleaseVersion
    instance with the release version obtained from the provided data or returns
    a default ReleaseVersion instance when data is None or lacks the necessary attributes.

    Args:
        data (Optional[sqlalchemy.engine.row.Row]): The input data from which the release
            version is extracted. It's expected to have an attribute 'EnsemblRelease'
            with a nested attribute 'version'. If None or the 'EnsemblRelease' attribute
            is absent, a default ReleaseVersion instance is returned.

    Returns:
        ensembl_metadata_pb2.ReleaseVersion: An instance of the ReleaseVersion message.
            It contains the release version extracted from the input data if the relevant
            attributes are present; otherwise, it's a default instance of ReleaseVersion.
    """
    if data is None:
        return ensembl_metadata_pb2.ReleaseVersion()
    logger.debug(f"Release data {data}")
    release = ensembl_metadata_pb2.ReleaseVersion(
        release_version=data.release.version if hasattr(data, 'release') else None,
    )
    return release


def create_datasets(data=None):
    if data is None:
        return ensembl_metadata_pb2.Datasets()
    return ensembl_metadata_pb2.Datasets(
        genome_uuid=data["genome_uuid"], datasets=data['datasets']
    )


def populate_dataset_info(data=None):
    if data is None:
        return ensembl_metadata_pb2.DatasetInfo()

    ds_obj_list = []
    for ds_item in data.datasets:
        ds_info = ensembl_metadata_pb2.DatasetInfo(
            dataset_uuid=ds_item.dataset.dataset_uuid,
            dataset_name=ds_item.dataset.name,
            dataset_version=ds_item.dataset.version,
            dataset_label=ds_item.dataset.label,
            dataset_type_topic=ds_item.dataset.dataset_type.topic,
            dataset_source_type=ds_item.dataset.dataset_source.type if ds_item.dataset.dataset_source else "",
            dataset_type_name=ds_item.dataset.dataset_type.name,
            release_version=float(data.release.version) if data.release.version else None,
            release_date=datetime.strftime(data.release.release_date, "%m/%d/%Y"),
            release_type=data.release.release_type,
        )
        ds_obj_list.append(ds_info)
    return ds_obj_list


def create_organisms_group_count(data, release_version):
    if data is None:
        return ensembl_metadata_pb2.OrganismsGroupCount()

    organisms_list = []
    for organism in data:
        created_organism_group = ensembl_metadata_pb2.OrganismsGroup(
            species_taxonomy_id=organism[0],
            common_name=organism[1],
            scientific_name=organism[2],
            order=organism[3],
            count=organism[4],
        )
        organisms_list.append(created_organism_group)

    return ensembl_metadata_pb2.OrganismsGroupCount(
        organisms_group_count=organisms_list,
        release_version=release_version
    )


def create_paths(data=None):
    if data is None:
        return ensembl_metadata_pb2.FTPLinks(
            Links=[]
        )

    ftp_links_list = []
    for ftp_link in data:
        created_ftp_link = ensembl_metadata_pb2.FTPLink(
            dataset_type=ftp_link["dataset_type"],
            path=ftp_link["path"]
        )
        ftp_links_list.append(created_ftp_link)

    return ensembl_metadata_pb2.FTPLinks(
        Links=ftp_links_list
    )


def create_attribute_value(data=None, attribute_names=None, latest_only=False):
    """
    Creates a DatasetAttributesValues message from the provided data.

    If no data is provided, returns an empty DatasetAttributeValue message with an empty attributes list.

    Args:
        data (optional): A list of objects containing dataset attributes.
            The expected structure is that `data` is a list containing an object with a `datasets` attribute,
            which is a list containing an object with an `attributes` attribute. The `attributes` attribute
            is a list of objects each having `name` and `value` attributes.
            it's many nested objects, and we need to go deep in the rabit hole to fetch the data we need
            The nested objects look something like this:
            [GenomeDatasetsListItem]
                [GenomeDatasetItem]
                    [DatasetAttributeItem] <- this is the attributes list we want to extract
                    Dataset
                    GenomeDataset
                Genome
                EnsemblRelease

        attribute_names (optional): A List of attributes names to filter by
        latest_only (optional): Whether to fetch the latest dataset or not (default is `False`)

    Returns:
        ensembl_metadata_pb2.DatasetAttributesValues: A message containing a list of DatasetAttributeValue
        messages, each corresponding to the attributes from the input data.
    """
    def add_attributes(ds_item, attributes_list, dataset_version, dataset_uuid, attribute_names, dataset_type):
        """
        Adds attributes from a dataset item to the attributes list.
        """
        for attrib in ds_item.attributes:
            # (1) if attribute_names is not provided,
            # (2) Or attribute_name from the DB is in the provided attribute_names
            # append it to the list of the returned result
            # if (1) is true, we will be fetching all the attributes
            # if (2) is true, we will be fetching the requested attributes only
            if not attribute_names or attrib.name in attribute_names:
                created_attribute = ensembl_metadata_pb2.DatasetAttributeValue(
                    attribute_name=attrib.name,
                    attribute_value=attrib.value,
                    dataset_version=dataset_version,
                    dataset_uuid=dataset_uuid,
                    dataset_type=dataset_type
                )
                attributes_list.append(created_attribute)

    if data is None:
        return ensembl_metadata_pb2.DatasetAttributesValues(
            attributes=[]
        )

    attributes_list = []
    # we can have more than one dataset
    for ds_item in data[0].datasets:
        # that we can distinguish by version or dataset_uuid
        dataset_version = ds_item.dataset.version
        dataset_uuid = ds_item.dataset.dataset_uuid
        dataset_type = ds_item.dataset.dataset_type.name

        # get the latest if latest_only is True
        if latest_only:
            if ds_item.release.is_current:
                add_attributes(ds_item, attributes_list, dataset_version, dataset_uuid, attribute_names, dataset_type)
        else:
            add_attributes(ds_item, attributes_list, dataset_version, dataset_uuid, attribute_names, dataset_type)

    return ensembl_metadata_pb2.DatasetAttributesValues(
        attributes=attributes_list,
        release_version=data[0].release.version
    )
