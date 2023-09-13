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

    assembly = ensembl_metadata_pb2.AssemblyInfo(
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
    return assembly


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

    assembly = ensembl_metadata_pb2.Assembly(
        accession=data.Assembly.accession,
        name=data.Assembly.name,
        ucsc_name=data.Assembly.ucsc_name,
        level=data.Assembly.level,
        ensembl_name=data.Assembly.ensembl_name,
    )

    taxon = ensembl_metadata_pb2.Taxon(
        taxonomy_id=data.Organism.taxonomy_id,
        scientific_name=data.Organism.scientific_name,
        strain=data.Organism.strain,
    )
    # TODO: fetch common_name(s) from ncbi_taxonomy database

    organism = ensembl_metadata_pb2.Organism(
        common_name=data.Organism.common_name,
        strain=data.Organism.strain,
        scientific_name=data.Organism.scientific_name,
        ensembl_name=data.Organism.ensembl_name,
        scientific_parlance_name=data.Organism.scientific_parlance_name,
        organism_uuid=data.Organism.organism_uuid,
    )

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


def create_organisms_group_count(data):
    if data is None:
        return ensembl_metadata_pb2.OrganismsGroupCount()

    organisms_list = []
    for organism in data:
        created_organism_group = ensembl_metadata_pb2.OrganismsGroup(
            ensembl_name=organism[1],
            count=organism[5],
        )
        organisms_list.append(created_organism_group)

    return ensembl_metadata_pb2.OrganismsGroupCount(
        organism_group=organisms_list
    )
