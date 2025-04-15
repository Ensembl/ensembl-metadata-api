from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Genome(_message.Message):
    __slots__ = ("genome_uuid", "assembly", "taxon", "created", "organism", "attributes_info", "related_assemblies_count", "release")
    GENOME_UUID_FIELD_NUMBER: _ClassVar[int]
    ASSEMBLY_FIELD_NUMBER: _ClassVar[int]
    TAXON_FIELD_NUMBER: _ClassVar[int]
    CREATED_FIELD_NUMBER: _ClassVar[int]
    ORGANISM_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_INFO_FIELD_NUMBER: _ClassVar[int]
    RELATED_ASSEMBLIES_COUNT_FIELD_NUMBER: _ClassVar[int]
    RELEASE_FIELD_NUMBER: _ClassVar[int]
    genome_uuid: str
    assembly: Assembly
    taxon: Taxon
    created: str
    organism: Organism
    attributes_info: AttributesInfo
    related_assemblies_count: int
    release: Release
    def __init__(self, genome_uuid: _Optional[str] = ..., assembly: _Optional[_Union[Assembly, _Mapping]] = ..., taxon: _Optional[_Union[Taxon, _Mapping]] = ..., created: _Optional[str] = ..., organism: _Optional[_Union[Organism, _Mapping]] = ..., attributes_info: _Optional[_Union[AttributesInfo, _Mapping]] = ..., related_assemblies_count: _Optional[int] = ..., release: _Optional[_Union[Release, _Mapping]] = ...) -> None: ...

class NewestGenomeInfo(_message.Message):
    __slots__ = ("genome_uuid", "release_date", "release_label", "release_type", "is_current")
    GENOME_UUID_FIELD_NUMBER: _ClassVar[int]
    RELEASE_DATE_FIELD_NUMBER: _ClassVar[int]
    RELEASE_LABEL_FIELD_NUMBER: _ClassVar[int]
    RELEASE_TYPE_FIELD_NUMBER: _ClassVar[int]
    IS_CURRENT_FIELD_NUMBER: _ClassVar[int]
    genome_uuid: str
    release_date: str
    release_label: str
    release_type: str
    is_current: bool
    def __init__(self, genome_uuid: _Optional[str] = ..., release_date: _Optional[str] = ..., release_label: _Optional[str] = ..., release_type: _Optional[str] = ..., is_current: bool = ...) -> None: ...

class BriefGenomeDetails(_message.Message):
    __slots__ = ("genome_uuid", "assembly", "taxon", "created", "organism", "release", "latest_genome")
    GENOME_UUID_FIELD_NUMBER: _ClassVar[int]
    ASSEMBLY_FIELD_NUMBER: _ClassVar[int]
    TAXON_FIELD_NUMBER: _ClassVar[int]
    CREATED_FIELD_NUMBER: _ClassVar[int]
    ORGANISM_FIELD_NUMBER: _ClassVar[int]
    RELEASE_FIELD_NUMBER: _ClassVar[int]
    LATEST_GENOME_FIELD_NUMBER: _ClassVar[int]
    genome_uuid: str
    assembly: Assembly
    taxon: Taxon
    created: str
    organism: Organism
    release: Release
    latest_genome: BriefGenomeDetails
    def __init__(self, genome_uuid: _Optional[str] = ..., assembly: _Optional[_Union[Assembly, _Mapping]] = ..., taxon: _Optional[_Union[Taxon, _Mapping]] = ..., created: _Optional[str] = ..., organism: _Optional[_Union[Organism, _Mapping]] = ..., release: _Optional[_Union[Release, _Mapping]] = ..., latest_genome: _Optional[_Union[BriefGenomeDetails, _Mapping]] = ...) -> None: ...

class AttributesInfoByGenome(_message.Message):
    __slots__ = ("genome_uuid", "attributes_info")
    GENOME_UUID_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_INFO_FIELD_NUMBER: _ClassVar[int]
    genome_uuid: str
    attributes_info: AttributesInfo
    def __init__(self, genome_uuid: _Optional[str] = ..., attributes_info: _Optional[_Union[AttributesInfo, _Mapping]] = ...) -> None: ...

class Species(_message.Message):
    __slots__ = ("genome_uuid", "taxon_id", "scientific_name", "scientific_parlance_name", "genbank_common_name", "synonym")
    GENOME_UUID_FIELD_NUMBER: _ClassVar[int]
    TAXON_ID_FIELD_NUMBER: _ClassVar[int]
    SCIENTIFIC_NAME_FIELD_NUMBER: _ClassVar[int]
    SCIENTIFIC_PARLANCE_NAME_FIELD_NUMBER: _ClassVar[int]
    GENBANK_COMMON_NAME_FIELD_NUMBER: _ClassVar[int]
    SYNONYM_FIELD_NUMBER: _ClassVar[int]
    genome_uuid: str
    taxon_id: int
    scientific_name: str
    scientific_parlance_name: str
    genbank_common_name: str
    synonym: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, genome_uuid: _Optional[str] = ..., taxon_id: _Optional[int] = ..., scientific_name: _Optional[str] = ..., scientific_parlance_name: _Optional[str] = ..., genbank_common_name: _Optional[str] = ..., synonym: _Optional[_Iterable[str]] = ...) -> None: ...

class AssemblyInfo(_message.Message):
    __slots__ = ("assembly_uuid", "accession", "level", "name", "chromosomal", "length", "sequence_location", "md5", "sha512t24u")
    ASSEMBLY_UUID_FIELD_NUMBER: _ClassVar[int]
    ACCESSION_FIELD_NUMBER: _ClassVar[int]
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    CHROMOSOMAL_FIELD_NUMBER: _ClassVar[int]
    LENGTH_FIELD_NUMBER: _ClassVar[int]
    SEQUENCE_LOCATION_FIELD_NUMBER: _ClassVar[int]
    MD5_FIELD_NUMBER: _ClassVar[int]
    SHA512T24U_FIELD_NUMBER: _ClassVar[int]
    assembly_uuid: str
    accession: str
    level: str
    name: str
    chromosomal: int
    length: int
    sequence_location: str
    md5: str
    sha512t24u: str
    def __init__(self, assembly_uuid: _Optional[str] = ..., accession: _Optional[str] = ..., level: _Optional[str] = ..., name: _Optional[str] = ..., chromosomal: _Optional[int] = ..., length: _Optional[int] = ..., sequence_location: _Optional[str] = ..., md5: _Optional[str] = ..., sha512t24u: _Optional[str] = ...) -> None: ...

class SubSpecies(_message.Message):
    __slots__ = ("organism_uuid", "species_type", "species_name")
    ORGANISM_UUID_FIELD_NUMBER: _ClassVar[int]
    SPECIES_TYPE_FIELD_NUMBER: _ClassVar[int]
    SPECIES_NAME_FIELD_NUMBER: _ClassVar[int]
    organism_uuid: str
    species_type: _containers.RepeatedScalarFieldContainer[str]
    species_name: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, organism_uuid: _Optional[str] = ..., species_type: _Optional[_Iterable[str]] = ..., species_name: _Optional[_Iterable[str]] = ...) -> None: ...

class AttributeStatistics(_message.Message):
    __slots__ = ("name", "label", "statistic_type", "statistic_value")
    NAME_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    STATISTIC_TYPE_FIELD_NUMBER: _ClassVar[int]
    STATISTIC_VALUE_FIELD_NUMBER: _ClassVar[int]
    name: str
    label: str
    statistic_type: str
    statistic_value: str
    def __init__(self, name: _Optional[str] = ..., label: _Optional[str] = ..., statistic_type: _Optional[str] = ..., statistic_value: _Optional[str] = ...) -> None: ...

class TopLevelStatisticsByUUID(_message.Message):
    __slots__ = ("genome_uuid", "statistics")
    GENOME_UUID_FIELD_NUMBER: _ClassVar[int]
    STATISTICS_FIELD_NUMBER: _ClassVar[int]
    genome_uuid: str
    statistics: _containers.RepeatedCompositeFieldContainer[AttributeStatistics]
    def __init__(self, genome_uuid: _Optional[str] = ..., statistics: _Optional[_Iterable[_Union[AttributeStatistics, _Mapping]]] = ...) -> None: ...

class TopLevelStatistics(_message.Message):
    __slots__ = ("organism_uuid", "stats_by_genome_uuid")
    ORGANISM_UUID_FIELD_NUMBER: _ClassVar[int]
    STATS_BY_GENOME_UUID_FIELD_NUMBER: _ClassVar[int]
    organism_uuid: str
    stats_by_genome_uuid: _containers.RepeatedCompositeFieldContainer[TopLevelStatisticsByUUID]
    def __init__(self, organism_uuid: _Optional[str] = ..., stats_by_genome_uuid: _Optional[_Iterable[_Union[TopLevelStatisticsByUUID, _Mapping]]] = ...) -> None: ...

class Assembly(_message.Message):
    __slots__ = ("accession", "name", "ucsc_name", "level", "ensembl_name", "assembly_uuid", "is_reference", "url_name", "tol_id")
    ACCESSION_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    UCSC_NAME_FIELD_NUMBER: _ClassVar[int]
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    ENSEMBL_NAME_FIELD_NUMBER: _ClassVar[int]
    ASSEMBLY_UUID_FIELD_NUMBER: _ClassVar[int]
    IS_REFERENCE_FIELD_NUMBER: _ClassVar[int]
    URL_NAME_FIELD_NUMBER: _ClassVar[int]
    TOL_ID_FIELD_NUMBER: _ClassVar[int]
    accession: str
    name: str
    ucsc_name: str
    level: str
    ensembl_name: str
    assembly_uuid: str
    is_reference: bool
    url_name: str
    tol_id: str
    def __init__(self, accession: _Optional[str] = ..., name: _Optional[str] = ..., ucsc_name: _Optional[str] = ..., level: _Optional[str] = ..., ensembl_name: _Optional[str] = ..., assembly_uuid: _Optional[str] = ..., is_reference: bool = ..., url_name: _Optional[str] = ..., tol_id: _Optional[str] = ...) -> None: ...

class Taxon(_message.Message):
    __slots__ = ("taxonomy_id", "scientific_name", "strain", "alternative_names")
    TAXONOMY_ID_FIELD_NUMBER: _ClassVar[int]
    SCIENTIFIC_NAME_FIELD_NUMBER: _ClassVar[int]
    STRAIN_FIELD_NUMBER: _ClassVar[int]
    ALTERNATIVE_NAMES_FIELD_NUMBER: _ClassVar[int]
    taxonomy_id: int
    scientific_name: str
    strain: str
    alternative_names: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, taxonomy_id: _Optional[int] = ..., scientific_name: _Optional[str] = ..., strain: _Optional[str] = ..., alternative_names: _Optional[_Iterable[str]] = ...) -> None: ...

class Release(_message.Message):
    __slots__ = ("release_version", "release_date", "release_label", "release_type", "is_current", "site_name", "site_label", "site_uri")
    RELEASE_VERSION_FIELD_NUMBER: _ClassVar[int]
    RELEASE_DATE_FIELD_NUMBER: _ClassVar[int]
    RELEASE_LABEL_FIELD_NUMBER: _ClassVar[int]
    RELEASE_TYPE_FIELD_NUMBER: _ClassVar[int]
    IS_CURRENT_FIELD_NUMBER: _ClassVar[int]
    SITE_NAME_FIELD_NUMBER: _ClassVar[int]
    SITE_LABEL_FIELD_NUMBER: _ClassVar[int]
    SITE_URI_FIELD_NUMBER: _ClassVar[int]
    release_version: float
    release_date: str
    release_label: str
    release_type: str
    is_current: bool
    site_name: str
    site_label: str
    site_uri: str
    def __init__(self, release_version: _Optional[float] = ..., release_date: _Optional[str] = ..., release_label: _Optional[str] = ..., release_type: _Optional[str] = ..., is_current: bool = ..., site_name: _Optional[str] = ..., site_label: _Optional[str] = ..., site_uri: _Optional[str] = ...) -> None: ...

class Organism(_message.Message):
    __slots__ = ("common_name", "strain", "scientific_name", "ensembl_name", "scientific_parlance_name", "organism_uuid", "strain_type", "taxonomy_id", "species_taxonomy_id")
    COMMON_NAME_FIELD_NUMBER: _ClassVar[int]
    STRAIN_FIELD_NUMBER: _ClassVar[int]
    SCIENTIFIC_NAME_FIELD_NUMBER: _ClassVar[int]
    ENSEMBL_NAME_FIELD_NUMBER: _ClassVar[int]
    SCIENTIFIC_PARLANCE_NAME_FIELD_NUMBER: _ClassVar[int]
    ORGANISM_UUID_FIELD_NUMBER: _ClassVar[int]
    STRAIN_TYPE_FIELD_NUMBER: _ClassVar[int]
    TAXONOMY_ID_FIELD_NUMBER: _ClassVar[int]
    SPECIES_TAXONOMY_ID_FIELD_NUMBER: _ClassVar[int]
    common_name: str
    strain: str
    scientific_name: str
    ensembl_name: str
    scientific_parlance_name: str
    organism_uuid: str
    strain_type: str
    taxonomy_id: int
    species_taxonomy_id: int
    def __init__(self, common_name: _Optional[str] = ..., strain: _Optional[str] = ..., scientific_name: _Optional[str] = ..., ensembl_name: _Optional[str] = ..., scientific_parlance_name: _Optional[str] = ..., organism_uuid: _Optional[str] = ..., strain_type: _Optional[str] = ..., taxonomy_id: _Optional[int] = ..., species_taxonomy_id: _Optional[int] = ...) -> None: ...

class Attribute(_message.Message):
    __slots__ = ("name", "label", "description", "type")
    NAME_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    name: str
    label: str
    description: str
    type: str
    def __init__(self, name: _Optional[str] = ..., label: _Optional[str] = ..., description: _Optional[str] = ..., type: _Optional[str] = ...) -> None: ...

class AttributesInfo(_message.Message):
    __slots__ = ("genebuild_method", "genebuild_method_display", "genebuild_last_geneset_update", "genebuild_provider_version", "genebuild_provider_name", "genebuild_provider_url", "genebuild_sample_gene", "genebuild_sample_location", "assembly_level", "assembly_date", "assembly_provider_name", "assembly_provider_url", "variation_sample_variant")
    GENEBUILD_METHOD_FIELD_NUMBER: _ClassVar[int]
    GENEBUILD_METHOD_DISPLAY_FIELD_NUMBER: _ClassVar[int]
    GENEBUILD_LAST_GENESET_UPDATE_FIELD_NUMBER: _ClassVar[int]
    GENEBUILD_PROVIDER_VERSION_FIELD_NUMBER: _ClassVar[int]
    GENEBUILD_PROVIDER_NAME_FIELD_NUMBER: _ClassVar[int]
    GENEBUILD_PROVIDER_URL_FIELD_NUMBER: _ClassVar[int]
    GENEBUILD_SAMPLE_GENE_FIELD_NUMBER: _ClassVar[int]
    GENEBUILD_SAMPLE_LOCATION_FIELD_NUMBER: _ClassVar[int]
    ASSEMBLY_LEVEL_FIELD_NUMBER: _ClassVar[int]
    ASSEMBLY_DATE_FIELD_NUMBER: _ClassVar[int]
    ASSEMBLY_PROVIDER_NAME_FIELD_NUMBER: _ClassVar[int]
    ASSEMBLY_PROVIDER_URL_FIELD_NUMBER: _ClassVar[int]
    VARIATION_SAMPLE_VARIANT_FIELD_NUMBER: _ClassVar[int]
    genebuild_method: str
    genebuild_method_display: str
    genebuild_last_geneset_update: str
    genebuild_provider_version: str
    genebuild_provider_name: str
    genebuild_provider_url: str
    genebuild_sample_gene: str
    genebuild_sample_location: str
    assembly_level: str
    assembly_date: str
    assembly_provider_name: str
    assembly_provider_url: str
    variation_sample_variant: str
    def __init__(self, genebuild_method: _Optional[str] = ..., genebuild_method_display: _Optional[str] = ..., genebuild_last_geneset_update: _Optional[str] = ..., genebuild_provider_version: _Optional[str] = ..., genebuild_provider_name: _Optional[str] = ..., genebuild_provider_url: _Optional[str] = ..., genebuild_sample_gene: _Optional[str] = ..., genebuild_sample_location: _Optional[str] = ..., assembly_level: _Optional[str] = ..., assembly_date: _Optional[str] = ..., assembly_provider_name: _Optional[str] = ..., assembly_provider_url: _Optional[str] = ..., variation_sample_variant: _Optional[str] = ...) -> None: ...

class GenomeSequence(_message.Message):
    __slots__ = ("accession", "name", "sequence_location", "length", "chromosomal")
    ACCESSION_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    SEQUENCE_LOCATION_FIELD_NUMBER: _ClassVar[int]
    LENGTH_FIELD_NUMBER: _ClassVar[int]
    CHROMOSOMAL_FIELD_NUMBER: _ClassVar[int]
    accession: str
    name: str
    sequence_location: str
    length: int
    chromosomal: bool
    def __init__(self, accession: _Optional[str] = ..., name: _Optional[str] = ..., sequence_location: _Optional[str] = ..., length: _Optional[int] = ..., chromosomal: bool = ...) -> None: ...

class AssemblyRegion(_message.Message):
    __slots__ = ("name", "rank", "md5", "length", "sha512t24u", "chromosomal")
    NAME_FIELD_NUMBER: _ClassVar[int]
    RANK_FIELD_NUMBER: _ClassVar[int]
    MD5_FIELD_NUMBER: _ClassVar[int]
    LENGTH_FIELD_NUMBER: _ClassVar[int]
    SHA512T24U_FIELD_NUMBER: _ClassVar[int]
    CHROMOSOMAL_FIELD_NUMBER: _ClassVar[int]
    name: str
    rank: int
    md5: str
    length: int
    sha512t24u: str
    chromosomal: bool
    def __init__(self, name: _Optional[str] = ..., rank: _Optional[int] = ..., md5: _Optional[str] = ..., length: _Optional[int] = ..., sha512t24u: _Optional[str] = ..., chromosomal: bool = ...) -> None: ...

class GenomeAssemblySequenceRegion(_message.Message):
    __slots__ = ("name", "md5", "length", "sha512t24u", "chromosomal")
    NAME_FIELD_NUMBER: _ClassVar[int]
    MD5_FIELD_NUMBER: _ClassVar[int]
    LENGTH_FIELD_NUMBER: _ClassVar[int]
    SHA512T24U_FIELD_NUMBER: _ClassVar[int]
    CHROMOSOMAL_FIELD_NUMBER: _ClassVar[int]
    name: str
    md5: str
    length: int
    sha512t24u: str
    chromosomal: bool
    def __init__(self, name: _Optional[str] = ..., md5: _Optional[str] = ..., length: _Optional[int] = ..., sha512t24u: _Optional[str] = ..., chromosomal: bool = ...) -> None: ...

class DatasetInfo(_message.Message):
    __slots__ = ("dataset_uuid", "dataset_name", "attribute_name", "attribute_type", "dataset_version", "dataset_label", "release_version", "attribute_value", "dataset_type_topic", "dataset_source_type", "dataset_type_name", "release_date", "release_type")
    DATASET_UUID_FIELD_NUMBER: _ClassVar[int]
    DATASET_NAME_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTE_NAME_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTE_TYPE_FIELD_NUMBER: _ClassVar[int]
    DATASET_VERSION_FIELD_NUMBER: _ClassVar[int]
    DATASET_LABEL_FIELD_NUMBER: _ClassVar[int]
    RELEASE_VERSION_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTE_VALUE_FIELD_NUMBER: _ClassVar[int]
    DATASET_TYPE_TOPIC_FIELD_NUMBER: _ClassVar[int]
    DATASET_SOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    DATASET_TYPE_NAME_FIELD_NUMBER: _ClassVar[int]
    RELEASE_DATE_FIELD_NUMBER: _ClassVar[int]
    RELEASE_TYPE_FIELD_NUMBER: _ClassVar[int]
    dataset_uuid: str
    dataset_name: str
    attribute_name: str
    attribute_type: str
    dataset_version: str
    dataset_label: str
    release_version: float
    attribute_value: str
    dataset_type_topic: str
    dataset_source_type: str
    dataset_type_name: str
    release_date: str
    release_type: str
    def __init__(self, dataset_uuid: _Optional[str] = ..., dataset_name: _Optional[str] = ..., attribute_name: _Optional[str] = ..., attribute_type: _Optional[str] = ..., dataset_version: _Optional[str] = ..., dataset_label: _Optional[str] = ..., release_version: _Optional[float] = ..., attribute_value: _Optional[str] = ..., dataset_type_topic: _Optional[str] = ..., dataset_source_type: _Optional[str] = ..., dataset_type_name: _Optional[str] = ..., release_date: _Optional[str] = ..., release_type: _Optional[str] = ...) -> None: ...

class Datasets(_message.Message):
    __slots__ = ("genome_uuid", "datasets")
    GENOME_UUID_FIELD_NUMBER: _ClassVar[int]
    DATASETS_FIELD_NUMBER: _ClassVar[int]
    genome_uuid: str
    datasets: _containers.RepeatedCompositeFieldContainer[DatasetInfo]
    def __init__(self, genome_uuid: _Optional[str] = ..., datasets: _Optional[_Iterable[_Union[DatasetInfo, _Mapping]]] = ...) -> None: ...

class VepFilePaths(_message.Message):
    __slots__ = ("faa_location", "gff_location")
    FAA_LOCATION_FIELD_NUMBER: _ClassVar[int]
    GFF_LOCATION_FIELD_NUMBER: _ClassVar[int]
    faa_location: str
    gff_location: str
    def __init__(self, faa_location: _Optional[str] = ..., gff_location: _Optional[str] = ...) -> None: ...

class GenomeUUID(_message.Message):
    __slots__ = ("genome_uuid",)
    GENOME_UUID_FIELD_NUMBER: _ClassVar[int]
    genome_uuid: str
    def __init__(self, genome_uuid: _Optional[str] = ...) -> None: ...

class OrganismsGroup(_message.Message):
    __slots__ = ("species_taxonomy_id", "common_name", "scientific_name", "order", "count")
    SPECIES_TAXONOMY_ID_FIELD_NUMBER: _ClassVar[int]
    COMMON_NAME_FIELD_NUMBER: _ClassVar[int]
    SCIENTIFIC_NAME_FIELD_NUMBER: _ClassVar[int]
    ORDER_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    species_taxonomy_id: int
    common_name: str
    scientific_name: str
    order: int
    count: int
    def __init__(self, species_taxonomy_id: _Optional[int] = ..., common_name: _Optional[str] = ..., scientific_name: _Optional[str] = ..., order: _Optional[int] = ..., count: _Optional[int] = ...) -> None: ...

class OrganismsGroupCount(_message.Message):
    __slots__ = ("organisms_group_count", "release_version")
    ORGANISMS_GROUP_COUNT_FIELD_NUMBER: _ClassVar[int]
    RELEASE_VERSION_FIELD_NUMBER: _ClassVar[int]
    organisms_group_count: _containers.RepeatedCompositeFieldContainer[OrganismsGroup]
    release_version: float
    def __init__(self, organisms_group_count: _Optional[_Iterable[_Union[OrganismsGroup, _Mapping]]] = ..., release_version: _Optional[float] = ...) -> None: ...

class FTPLink(_message.Message):
    __slots__ = ("dataset_type", "path")
    DATASET_TYPE_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    dataset_type: str
    path: str
    def __init__(self, dataset_type: _Optional[str] = ..., path: _Optional[str] = ...) -> None: ...

class FTPLinks(_message.Message):
    __slots__ = ("Links",)
    LINKS_FIELD_NUMBER: _ClassVar[int]
    Links: _containers.RepeatedCompositeFieldContainer[FTPLink]
    def __init__(self, Links: _Optional[_Iterable[_Union[FTPLink, _Mapping]]] = ...) -> None: ...

class ReleaseVersion(_message.Message):
    __slots__ = ("release_version",)
    RELEASE_VERSION_FIELD_NUMBER: _ClassVar[int]
    release_version: float
    def __init__(self, release_version: _Optional[float] = ...) -> None: ...

class DatasetAttributeValue(_message.Message):
    __slots__ = ("attribute_name", "attribute_value", "dataset_version", "dataset_uuid", "dataset_type")
    ATTRIBUTE_NAME_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTE_VALUE_FIELD_NUMBER: _ClassVar[int]
    DATASET_VERSION_FIELD_NUMBER: _ClassVar[int]
    DATASET_UUID_FIELD_NUMBER: _ClassVar[int]
    DATASET_TYPE_FIELD_NUMBER: _ClassVar[int]
    attribute_name: str
    attribute_value: str
    dataset_version: str
    dataset_uuid: str
    dataset_type: str
    def __init__(self, attribute_name: _Optional[str] = ..., attribute_value: _Optional[str] = ..., dataset_version: _Optional[str] = ..., dataset_uuid: _Optional[str] = ..., dataset_type: _Optional[str] = ...) -> None: ...

class DatasetAttributesValues(_message.Message):
    __slots__ = ("attributes", "release_version")
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    RELEASE_VERSION_FIELD_NUMBER: _ClassVar[int]
    attributes: _containers.RepeatedCompositeFieldContainer[DatasetAttributeValue]
    release_version: float
    def __init__(self, attributes: _Optional[_Iterable[_Union[DatasetAttributeValue, _Mapping]]] = ..., release_version: _Optional[float] = ...) -> None: ...

class GenomeUUIDRequest(_message.Message):
    __slots__ = ("genome_uuid", "release_version")
    GENOME_UUID_FIELD_NUMBER: _ClassVar[int]
    RELEASE_VERSION_FIELD_NUMBER: _ClassVar[int]
    genome_uuid: str
    release_version: float
    def __init__(self, genome_uuid: _Optional[str] = ..., release_version: _Optional[float] = ...) -> None: ...

class GenomeUUIDOnlyRequest(_message.Message):
    __slots__ = ("genome_uuid",)
    GENOME_UUID_FIELD_NUMBER: _ClassVar[int]
    genome_uuid: str
    def __init__(self, genome_uuid: _Optional[str] = ...) -> None: ...

class GenomeBySpecificKeywordRequest(_message.Message):
    __slots__ = ("tolid", "assembly_accession_id", "assembly_name", "ensembl_name", "common_name", "scientific_name", "scientific_parlance_name", "species_taxonomy_id", "release_version")
    TOLID_FIELD_NUMBER: _ClassVar[int]
    ASSEMBLY_ACCESSION_ID_FIELD_NUMBER: _ClassVar[int]
    ASSEMBLY_NAME_FIELD_NUMBER: _ClassVar[int]
    ENSEMBL_NAME_FIELD_NUMBER: _ClassVar[int]
    COMMON_NAME_FIELD_NUMBER: _ClassVar[int]
    SCIENTIFIC_NAME_FIELD_NUMBER: _ClassVar[int]
    SCIENTIFIC_PARLANCE_NAME_FIELD_NUMBER: _ClassVar[int]
    SPECIES_TAXONOMY_ID_FIELD_NUMBER: _ClassVar[int]
    RELEASE_VERSION_FIELD_NUMBER: _ClassVar[int]
    tolid: str
    assembly_accession_id: str
    assembly_name: str
    ensembl_name: str
    common_name: str
    scientific_name: str
    scientific_parlance_name: str
    species_taxonomy_id: str
    release_version: float
    def __init__(self, tolid: _Optional[str] = ..., assembly_accession_id: _Optional[str] = ..., assembly_name: _Optional[str] = ..., ensembl_name: _Optional[str] = ..., common_name: _Optional[str] = ..., scientific_name: _Optional[str] = ..., scientific_parlance_name: _Optional[str] = ..., species_taxonomy_id: _Optional[str] = ..., release_version: _Optional[float] = ...) -> None: ...

class GenomeByReleaseVersionRequest(_message.Message):
    __slots__ = ("release_version",)
    RELEASE_VERSION_FIELD_NUMBER: _ClassVar[int]
    release_version: float
    def __init__(self, release_version: _Optional[float] = ...) -> None: ...

class GenomeNameRequest(_message.Message):
    __slots__ = ("ensembl_name", "site_name", "release_version")
    ENSEMBL_NAME_FIELD_NUMBER: _ClassVar[int]
    SITE_NAME_FIELD_NUMBER: _ClassVar[int]
    RELEASE_VERSION_FIELD_NUMBER: _ClassVar[int]
    ensembl_name: str
    site_name: str
    release_version: float
    def __init__(self, ensembl_name: _Optional[str] = ..., site_name: _Optional[str] = ..., release_version: _Optional[float] = ...) -> None: ...

class AssemblyIDRequest(_message.Message):
    __slots__ = ("assembly_uuid", "release_version")
    ASSEMBLY_UUID_FIELD_NUMBER: _ClassVar[int]
    RELEASE_VERSION_FIELD_NUMBER: _ClassVar[int]
    assembly_uuid: str
    release_version: float
    def __init__(self, assembly_uuid: _Optional[str] = ..., release_version: _Optional[float] = ...) -> None: ...

class AssemblyAccessionIDRequest(_message.Message):
    __slots__ = ("assembly_accession", "release_version")
    ASSEMBLY_ACCESSION_FIELD_NUMBER: _ClassVar[int]
    RELEASE_VERSION_FIELD_NUMBER: _ClassVar[int]
    assembly_accession: str
    release_version: float
    def __init__(self, assembly_accession: _Optional[str] = ..., release_version: _Optional[float] = ...) -> None: ...

class OrganismIDRequest(_message.Message):
    __slots__ = ("organism_uuid", "group")
    ORGANISM_UUID_FIELD_NUMBER: _ClassVar[int]
    GROUP_FIELD_NUMBER: _ClassVar[int]
    organism_uuid: str
    group: str
    def __init__(self, organism_uuid: _Optional[str] = ..., group: _Optional[str] = ...) -> None: ...

class ReleaseRequest(_message.Message):
    __slots__ = ("site_name", "release_label", "current_only")
    SITE_NAME_FIELD_NUMBER: _ClassVar[int]
    RELEASE_LABEL_FIELD_NUMBER: _ClassVar[int]
    CURRENT_ONLY_FIELD_NUMBER: _ClassVar[int]
    site_name: _containers.RepeatedScalarFieldContainer[str]
    release_label: _containers.RepeatedScalarFieldContainer[str]
    current_only: bool
    def __init__(self, site_name: _Optional[_Iterable[str]] = ..., release_label: _Optional[_Iterable[str]] = ..., current_only: bool = ...) -> None: ...

class GenomeSequenceRequest(_message.Message):
    __slots__ = ("genome_uuid", "chromosomal_only")
    GENOME_UUID_FIELD_NUMBER: _ClassVar[int]
    CHROMOSOMAL_ONLY_FIELD_NUMBER: _ClassVar[int]
    genome_uuid: str
    chromosomal_only: bool
    def __init__(self, genome_uuid: _Optional[str] = ..., chromosomal_only: bool = ...) -> None: ...

class AssemblyRegionRequest(_message.Message):
    __slots__ = ("genome_uuid", "chromosomal_only")
    GENOME_UUID_FIELD_NUMBER: _ClassVar[int]
    CHROMOSOMAL_ONLY_FIELD_NUMBER: _ClassVar[int]
    genome_uuid: str
    chromosomal_only: bool
    def __init__(self, genome_uuid: _Optional[str] = ..., chromosomal_only: bool = ...) -> None: ...

class GenomeAssemblySequenceRegionRequest(_message.Message):
    __slots__ = ("genome_uuid", "sequence_region_name")
    GENOME_UUID_FIELD_NUMBER: _ClassVar[int]
    SEQUENCE_REGION_NAME_FIELD_NUMBER: _ClassVar[int]
    genome_uuid: str
    sequence_region_name: str
    def __init__(self, genome_uuid: _Optional[str] = ..., sequence_region_name: _Optional[str] = ...) -> None: ...

class DatasetsRequest(_message.Message):
    __slots__ = ("genome_uuid", "release_version")
    GENOME_UUID_FIELD_NUMBER: _ClassVar[int]
    RELEASE_VERSION_FIELD_NUMBER: _ClassVar[int]
    genome_uuid: str
    release_version: float
    def __init__(self, genome_uuid: _Optional[str] = ..., release_version: _Optional[float] = ...) -> None: ...

class GenomeDatatypeRequest(_message.Message):
    __slots__ = ("genome_uuid", "dataset_type")
    GENOME_UUID_FIELD_NUMBER: _ClassVar[int]
    DATASET_TYPE_FIELD_NUMBER: _ClassVar[int]
    genome_uuid: str
    dataset_type: str
    def __init__(self, genome_uuid: _Optional[str] = ..., dataset_type: _Optional[str] = ...) -> None: ...

class GenomeInfoRequest(_message.Message):
    __slots__ = ("production_name", "assembly_name", "genebuild_date", "release_version", "use_default")
    PRODUCTION_NAME_FIELD_NUMBER: _ClassVar[int]
    ASSEMBLY_NAME_FIELD_NUMBER: _ClassVar[int]
    GENEBUILD_DATE_FIELD_NUMBER: _ClassVar[int]
    RELEASE_VERSION_FIELD_NUMBER: _ClassVar[int]
    USE_DEFAULT_FIELD_NUMBER: _ClassVar[int]
    production_name: str
    assembly_name: str
    genebuild_date: str
    release_version: float
    use_default: bool
    def __init__(self, production_name: _Optional[str] = ..., assembly_name: _Optional[str] = ..., genebuild_date: _Optional[str] = ..., release_version: _Optional[float] = ..., use_default: bool = ...) -> None: ...

class OrganismsGroupRequest(_message.Message):
    __slots__ = ("release_version",)
    RELEASE_VERSION_FIELD_NUMBER: _ClassVar[int]
    release_version: float
    def __init__(self, release_version: _Optional[float] = ...) -> None: ...

class GenomeTagRequest(_message.Message):
    __slots__ = ("genome_tag",)
    GENOME_TAG_FIELD_NUMBER: _ClassVar[int]
    genome_tag: str
    def __init__(self, genome_tag: _Optional[str] = ...) -> None: ...

class FTPLinksRequest(_message.Message):
    __slots__ = ("genome_uuid", "dataset_type", "release_version")
    GENOME_UUID_FIELD_NUMBER: _ClassVar[int]
    DATASET_TYPE_FIELD_NUMBER: _ClassVar[int]
    RELEASE_VERSION_FIELD_NUMBER: _ClassVar[int]
    genome_uuid: str
    dataset_type: str
    release_version: str
    def __init__(self, genome_uuid: _Optional[str] = ..., dataset_type: _Optional[str] = ..., release_version: _Optional[str] = ...) -> None: ...

class ReleaseVersionRequest(_message.Message):
    __slots__ = ("genome_uuid", "dataset_type", "release_version")
    GENOME_UUID_FIELD_NUMBER: _ClassVar[int]
    DATASET_TYPE_FIELD_NUMBER: _ClassVar[int]
    RELEASE_VERSION_FIELD_NUMBER: _ClassVar[int]
    genome_uuid: str
    dataset_type: str
    release_version: float
    def __init__(self, genome_uuid: _Optional[str] = ..., dataset_type: _Optional[str] = ..., release_version: _Optional[float] = ...) -> None: ...

class DatasetAttributesValuesRequest(_message.Message):
    __slots__ = ("genome_uuid", "dataset_type", "attribute_name", "release_version", "latest_only")
    GENOME_UUID_FIELD_NUMBER: _ClassVar[int]
    DATASET_TYPE_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTE_NAME_FIELD_NUMBER: _ClassVar[int]
    RELEASE_VERSION_FIELD_NUMBER: _ClassVar[int]
    LATEST_ONLY_FIELD_NUMBER: _ClassVar[int]
    genome_uuid: str
    dataset_type: str
    attribute_name: _containers.RepeatedScalarFieldContainer[str]
    release_version: float
    latest_only: bool
    def __init__(self, genome_uuid: _Optional[str] = ..., dataset_type: _Optional[str] = ..., attribute_name: _Optional[_Iterable[str]] = ..., release_version: _Optional[float] = ..., latest_only: bool = ...) -> None: ...
