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


def create_species(species_data=None, taxo_info=None):
    if species_data is None:
        return ensembl_metadata_pb2.Species()

    species = ensembl_metadata_pb2.Species(
        genome_uuid=species_data.Genome.genome_uuid,
        taxon_id=species_data.Organism.taxonomy_id,
        scientific_name=species_data.Organism.scientific_name,
        scientific_parlance_name=species_data.Organism.scientific_parlance_name,
        genbank_common_name=taxo_info["genbank_common_name"],
        synonym=taxo_info["synonym"],
    )
    return species


def create_top_level_statistics(data=None):
    if data is None:
        return ensembl_metadata_pb2.TopLevelStatistics()

    species = ensembl_metadata_pb2.TopLevelStatistics(
        organism_uuid=data["organism_uuid"],
        statistics=data["statistics"],
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


def create_karyotype(data=None):
    if data is None:
        return ensembl_metadata_pb2.Karyotype()

    karyotype = ensembl_metadata_pb2.Karyotype(
        genome_uuid=data.Genome.genome_uuid,
        code=data.Assembly.level,
        chromosomal=str(data.AssemblySequence.chromosomal),
        location=data.AssemblySequence.sequence_location,
    )
    return karyotype


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
    )
    return assembly


def create_taxon(data=None):
    if data is None:
        return ensembl_metadata_pb2.Taxon()

    taxon = ensembl_metadata_pb2.Taxon(
        taxonomy_id=data.Organism.taxonomy_id,
        scientific_name=data.Organism.scientific_name,
        strain=data.Organism.strain,
    )
    # TODO: fetch common_name(s) from ncbi_taxonomy database
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
        "genebuild.last_geneset_update": "",
        "genebuild.version": "",
        "annotation.provider_name": "",
        "annotation.provider_url": "",
        "assembly.level": "",
        "assembly.date": "",
        "sample.gene_param": "",
        "sample.location_param": ""
    }

    # set required_attributes values
    for attrib_data in data:
        attrib_name = attrib_data.Attribute.name
        if attrib_name in list(required_attributes.keys()):
            required_attributes[attrib_name] = attrib_data.DatasetAttribute.value

    return ensembl_metadata_pb2.AttributesInfo(
        genebuild_method=required_attributes["genebuild.method"],
        genebuild_last_geneset_update=required_attributes["genebuild.last_geneset_update"],
        genebuild_version=required_attributes["genebuild.version"],
        annotation_provider_name=required_attributes["annotation.provider_name"],
        annotation_provider_url=required_attributes["annotation.provider_url"],
        assembly_level=required_attributes["assembly.level"],
        assembly_date=required_attributes["assembly.date"],
        sample_gene_param=required_attributes["sample.gene_param"],
        sample_location_param=required_attributes["sample.location_param"]
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
        sha512t4u=data.AssemblySequence.sha512t4u,
    )
    return assembly_info


def create_genome_uuid(data=None):
    if data is None:
        return ensembl_metadata_pb2.GenomeUUID()

    genome_uuid = ensembl_metadata_pb2.GenomeUUID(
        genome_uuid=data["genome_uuid"]
    )
    return genome_uuid


def create_genome(data=None):
    if data is None:
        return ensembl_metadata_pb2.Genome()

    assembly = create_assembly(data)
    taxon = create_taxon(data)
    organism = create_organism(data)
    release = create_release(data)

    genome = ensembl_metadata_pb2.Genome(
        genome_uuid=data.Genome.genome_uuid,
        created=str(data.Genome.created),
        assembly=assembly,
        taxon=taxon,
        organism=organism,
        release=release,
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


def create_genome_assembly_sequence(data=None):
    if data is None:
        return ensembl_metadata_pb2.GenomeAssemblySequence()

    genome_assembly_sequence = ensembl_metadata_pb2.GenomeAssemblySequence(
        name=data.AssemblySequence.name,
        rank=data.AssemblySequence.chromosome_rank,
        md5=data.AssemblySequence.md5,
        length=data.AssemblySequence.length,
        sha512t4u=data.AssemblySequence.sha512t4u,
        chromosomal=data.AssemblySequence.chromosomal
    )

    return genome_assembly_sequence


def create_genome_assembly_sequence_region(data=None):
    if data is None:
        return ensembl_metadata_pb2.GenomeAssemblySequenceRegion()

    genome_assembly_sequence_region = ensembl_metadata_pb2.GenomeAssemblySequenceRegion(
        name=data.AssemblySequence.name,
        md5=data.AssemblySequence.md5,
        length=data.AssemblySequence.length,
        sha512t4u=data.AssemblySequence.sha512t4u,
        chromosomal=data.AssemblySequence.chromosomal
    )

    return genome_assembly_sequence_region


def create_release(data=None):
    if data is None:
        return ensembl_metadata_pb2.Release()

    release = ensembl_metadata_pb2.Release(
        release_version=data.EnsemblRelease.version if hasattr(data, 'EnsemblRelease') else None,
        release_date=str(data.EnsemblRelease.release_date) if hasattr(data, 'EnsemblRelease') else "Unreleased",
        release_label=data.EnsemblRelease.label if hasattr(data, 'EnsemblRelease') else "Unreleased",
        is_current=data.EnsemblRelease.is_current if hasattr(data, 'EnsemblRelease') else False,
        site_name=data.EnsemblSite.name if hasattr(data, 'EnsemblSite') else "Unknown (not released yet)",
        site_label=data.EnsemblSite.label if hasattr(data, 'EnsemblSite') else "Unknown (not released yet)",
        site_uri=data.EnsemblSite.uri if hasattr(data, 'EnsemblSite') else "Unknown (not released yet)",
    )
    return release


def create_datasets(data=None):
    if data is None:
        return ensembl_metadata_pb2.Datasets()

    return ensembl_metadata_pb2.Datasets(
        genome_uuid=data["genome_uuid"], datasets=data["datasets"]
    )


def create_dataset_info(data=None):
    if data is None:
        return ensembl_metadata_pb2.DatasetInfos.DatasetInfo()

    return ensembl_metadata_pb2.DatasetInfos.DatasetInfo(
        dataset_uuid=data.Dataset.dataset_uuid,
        dataset_name=data.Dataset.name,
        name=data.Attribute.name,
        type=data.Attribute.type,
        dataset_version=data.Dataset.version,
        dataset_label=data.Dataset.label,
        version=int(data.EnsemblRelease.version) if hasattr(data, 'EnsemblRelease') else None,
        value=data.DatasetAttribute.value,
    )


def create_dataset_infos(genome_uuid=None, requested_dataset_type=None, data=None):
    if data is None or data == []:
        return ensembl_metadata_pb2.DatasetInfos()

    dataset_infos = [create_dataset_info(result) for result in data]
    return ensembl_metadata_pb2.DatasetInfos(
        genome_uuid=genome_uuid,
        dataset_type=requested_dataset_type,
        dataset_infos=dataset_infos,
    )


def populate_dataset_info(data):
    return ensembl_metadata_pb2.DatasetInfos.DatasetInfo(
        dataset_uuid=data.Dataset.dataset_uuid,
        dataset_name=data.Dataset.name,
        dataset_version=data.Dataset.version,
        dataset_label=data.Dataset.label,
        version=int(data.EnsemblRelease.version) if hasattr(data, 'EnsemblRelease') else None,
    )


def create_organisms_group_count(data, release_version):
    if data is None:
        return ensembl_metadata_pb2.OrganismsGroupCount()

    organisms_list = []
    for organism in data:
        created_organism_group = ensembl_metadata_pb2.OrganismsGroup(
            species_taxonomy_id=organism[0],
            ensembl_name=organism[1],
            common_name=organism[2],
            scientific_name=organism[3],
            order=organism[4],
            count=organism[5],
        )
        organisms_list.append(created_organism_group)

    return ensembl_metadata_pb2.OrganismsGroupCount(
        organisms_group_count=organisms_list,
        release_version=release_version
    )


def create_genome_info(data=None, attributes=None, count=0):
    if data is None:
        return ensembl_metadata_pb2.GenomeInfo()

    assembly = create_assembly(data)
    organism = create_organism(data)
    attributes_info = create_attributes_info(attributes)
    release = create_release(data)

    genome_info = ensembl_metadata_pb2.GenomeInfo(
        genome_uuid=data.Genome.genome_uuid,
        created=str(data.Genome.created),
        assembly=assembly,
        organism=organism,
        attributes_info=attributes_info,
        release=release,
        related_assemblies_count=count
    )

    return genome_info
