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
    # FIXME data['datasets'] doesn't hold the right datatype.
    # dataset_infos =
    return ensembl_metadata_pb2.Datasets(
        genome_uuid=data["genome_uuid"], datasets=data['datasets']
    )


def create_dataset_info(dataset, attribute, release=None):
    if dataset is None:
        return ensembl_metadata_pb2.DatasetInfos.DatasetInfo()
    # FIXME = what is the expected output here? Data.Attribute ONe entry per datasets / attribute combination?
    #   is it used anywhere in web?
    return ensembl_metadata_pb2.DatasetInfos.DatasetInfo(
        dataset_uuid=dataset.dataset_uuid,
        dataset_name=dataset.name,
        name=attribute.name,
        type=attribute.type,
        dataset_version=dataset.version,
        dataset_label=dataset.label,
        version=release.version if release else None,
        value=attribute.value,
    )


def create_dataset_infos(genome_uuid=None, requested_dataset_type=None, data=None):
    if data is None or data == []:
        return ensembl_metadata_pb2.DatasetInfos()
    # NB: data is GenomeDatasetsListItem
    dataset_infos = []
    for dataset in data.datasets:
        dataset_infos.extend(
            [create_dataset_info(dataset.dataset, attribute, dataset.release) for attribute in dataset.attributes])
    return ensembl_metadata_pb2.DatasetInfos(
        genome_uuid=genome_uuid,
        dataset_type=requested_dataset_type,
        dataset_infos=dataset_infos,
    )


def populate_dataset_info(data):
    return ensembl_metadata_pb2.DatasetInfos.DatasetInfo(
        dataset_uuid=data.dataset.dataset_uuid,
        dataset_name=data.dataset.name,
        dataset_version=data.dataset.version,
        dataset_label=data.dataset.label,
        version=int(data.ensembl_release.version) if hasattr(data, 'ensembl_release') else None,
    )


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
