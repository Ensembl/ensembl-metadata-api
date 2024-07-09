# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: ensembl/production/metadata/grpc/ensembl_metadata.proto
# Protobuf Python Version: 4.25.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n7ensembl/production/metadata/grpc/ensembl_metadata.proto\x12\x10\x65nsembl_metadata\"\xbb\x02\n\x06Genome\x12\x13\n\x0bgenome_uuid\x18\x01 \x01(\t\x12,\n\x08\x61ssembly\x18\x02 \x01(\x0b\x32\x1a.ensembl_metadata.Assembly\x12&\n\x05taxon\x18\x03 \x01(\x0b\x32\x17.ensembl_metadata.Taxon\x12\x0f\n\x07\x63reated\x18\x04 \x01(\t\x12,\n\x08organism\x18\x05 \x01(\x0b\x32\x1a.ensembl_metadata.Organism\x12\x39\n\x0f\x61ttributes_info\x18\x06 \x01(\x0b\x32 .ensembl_metadata.AttributesInfo\x12 \n\x18related_assemblies_count\x18\x07 \x01(\x05\x12*\n\x07release\x18\x08 \x01(\x0b\x32\x19.ensembl_metadata.Release\"\x99\x01\n\x07Species\x12\x13\n\x0bgenome_uuid\x18\x01 \x01(\t\x12\x10\n\x08taxon_id\x18\x02 \x01(\r\x12\x17\n\x0fscientific_name\x18\x03 \x01(\t\x12 \n\x18scientific_parlance_name\x18\x04 \x01(\t\x12\x1b\n\x13genbank_common_name\x18\x05 \x01(\t\x12\x0f\n\x07synonym\x18\x06 \x03(\t\"\xb6\x01\n\x0c\x41ssemblyInfo\x12\x15\n\rassembly_uuid\x18\x01 \x01(\t\x12\x11\n\taccession\x18\x02 \x01(\t\x12\r\n\x05level\x18\x03 \x01(\t\x12\x0c\n\x04name\x18\x04 \x01(\t\x12\x13\n\x0b\x63hromosomal\x18\x05 \x01(\r\x12\x0e\n\x06length\x18\x06 \x01(\x04\x12\x19\n\x11sequence_location\x18\x07 \x01(\t\x12\x0b\n\x03md5\x18\x08 \x01(\t\x12\x12\n\nsha512t24u\x18\t \x01(\t\"O\n\nSubSpecies\x12\x15\n\rorganism_uuid\x18\x01 \x01(\t\x12\x14\n\x0cspecies_type\x18\x02 \x03(\t\x12\x14\n\x0cspecies_name\x18\x03 \x03(\t\"c\n\x13\x41ttributeStatistics\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\r\n\x05label\x18\x02 \x01(\t\x12\x16\n\x0estatistic_type\x18\x03 \x01(\t\x12\x17\n\x0fstatistic_value\x18\x04 \x01(\t\"j\n\x18TopLevelStatisticsByUUID\x12\x13\n\x0bgenome_uuid\x18\x01 \x01(\t\x12\x39\n\nstatistics\x18\x02 \x03(\x0b\x32%.ensembl_metadata.AttributeStatistics\"u\n\x12TopLevelStatistics\x12\x15\n\rorganism_uuid\x18\x01 \x01(\t\x12H\n\x14stats_by_genome_uuid\x18\x02 \x03(\x0b\x32*.ensembl_metadata.TopLevelStatisticsByUUID\"\xb2\x01\n\x08\x41ssembly\x12\x11\n\taccession\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\x12\x11\n\tucsc_name\x18\x03 \x01(\t\x12\r\n\x05level\x18\x04 \x01(\t\x12\x14\n\x0c\x65nsembl_name\x18\x05 \x01(\t\x12\x15\n\rassembly_uuid\x18\x06 \x01(\t\x12\x14\n\x0cis_reference\x18\x07 \x01(\x08\x12\x10\n\x08url_name\x18\x08 \x01(\t\x12\x0e\n\x06tol_id\x18\t \x01(\t\"`\n\x05Taxon\x12\x13\n\x0btaxonomy_id\x18\x01 \x01(\r\x12\x17\n\x0fscientific_name\x18\x02 \x01(\t\x12\x0e\n\x06strain\x18\x03 \x01(\t\x12\x19\n\x11\x61lternative_names\x18\x04 \x03(\t\"\x9c\x01\n\x07Release\x12\x17\n\x0frelease_version\x18\x01 \x01(\x01\x12\x14\n\x0crelease_date\x18\x02 \x01(\t\x12\x15\n\rrelease_label\x18\x03 \x01(\t\x12\x12\n\nis_current\x18\x04 \x01(\x08\x12\x11\n\tsite_name\x18\x05 \x01(\t\x12\x12\n\nsite_label\x18\x06 \x01(\t\x12\x10\n\x08site_uri\x18\x07 \x01(\t\"\xde\x01\n\x08Organism\x12\x13\n\x0b\x63ommon_name\x18\x01 \x01(\t\x12\x0e\n\x06strain\x18\x02 \x01(\t\x12\x17\n\x0fscientific_name\x18\x03 \x01(\t\x12\x14\n\x0c\x65nsembl_name\x18\x04 \x01(\t\x12 \n\x18scientific_parlance_name\x18\x05 \x01(\t\x12\x15\n\rorganism_uuid\x18\x06 \x01(\t\x12\x13\n\x0bstrain_type\x18\x07 \x01(\t\x12\x13\n\x0btaxonomy_id\x18\x08 \x01(\x05\x12\x1b\n\x13species_taxonomy_id\x18\t \x01(\x05\"K\n\tAttribute\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\r\n\x05label\x18\x02 \x01(\t\x12\x13\n\x0b\x64\x65scription\x18\x03 \x01(\t\x12\x0c\n\x04type\x18\x04 \x01(\t\"\xa1\x03\n\x0e\x41ttributesInfo\x12\x18\n\x10genebuild_method\x18\x01 \x01(\t\x12 \n\x18genebuild_method_display\x18\x02 \x01(\t\x12%\n\x1dgenebuild_last_geneset_update\x18\x03 \x01(\t\x12\x19\n\x11genebuild_version\x18\x04 \x01(\t\x12\x1f\n\x17genebuild_provider_name\x18\x05 \x01(\t\x12\x1e\n\x16genebuild_provider_url\x18\x06 \x01(\t\x12\x1d\n\x15genebuild_sample_gene\x18\x07 \x01(\t\x12!\n\x19genebuild_sample_location\x18\x08 \x01(\t\x12\x16\n\x0e\x61ssembly_level\x18\t \x01(\t\x12\x15\n\rassembly_date\x18\n \x01(\t\x12\x1e\n\x16\x61ssembly_provider_name\x18\x0b \x01(\t\x12\x1d\n\x15\x61ssembly_provider_url\x18\x0c \x01(\t\x12 \n\x18variation_sample_variant\x18\r \x01(\t\"\xa4\x02\n\x0c\x44\x61tasetInfos\x12\x13\n\x0bgenome_uuid\x18\x01 \x01(\t\x12\x14\n\x0c\x64\x61taset_type\x18\x02 \x01(\t\x12\x41\n\rdataset_infos\x18\x03 \x03(\x0b\x32*.ensembl_metadata.DatasetInfos.DatasetInfo\x1a\xa5\x01\n\x0b\x44\x61tasetInfo\x12\x14\n\x0c\x64\x61taset_uuid\x18\x01 \x01(\t\x12\x14\n\x0c\x64\x61taset_name\x18\x02 \x01(\t\x12\x0c\n\x04name\x18\x03 \x01(\t\x12\x0c\n\x04type\x18\x04 \x01(\t\x12\x17\n\x0f\x64\x61taset_version\x18\x05 \x01(\t\x12\x15\n\rdataset_label\x18\x06 \x01(\t\x12\x0f\n\x07version\x18\x07 \x01(\x01\x12\r\n\x05value\x18\x08 \x01(\t\"q\n\x0eGenomeSequence\x12\x11\n\taccession\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\x12\x19\n\x11sequence_location\x18\x03 \x01(\t\x12\x0e\n\x06length\x18\x04 \x01(\x04\x12\x13\n\x0b\x63hromosomal\x18\x05 \x01(\x08\"r\n\x0e\x41ssemblyRegion\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x0c\n\x04rank\x18\x02 \x01(\x05\x12\x0b\n\x03md5\x18\x03 \x01(\t\x12\x0e\n\x06length\x18\x04 \x01(\x04\x12\x12\n\nsha512t24u\x18\x05 \x01(\t\x12\x13\n\x0b\x63hromosomal\x18\x06 \x01(\x08\"r\n\x1cGenomeAssemblySequenceRegion\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x0b\n\x03md5\x18\x02 \x01(\t\x12\x0e\n\x06length\x18\x03 \x01(\x04\x12\x12\n\nsha512t24u\x18\x04 \x01(\t\x12\x13\n\x0b\x63hromosomal\x18\x05 \x01(\x08\"\xac\x01\n\x08\x44\x61tasets\x12\x13\n\x0bgenome_uuid\x18\x01 \x01(\t\x12:\n\x08\x64\x61tasets\x18\x02 \x03(\x0b\x32(.ensembl_metadata.Datasets.DatasetsEntry\x1aO\n\rDatasetsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12-\n\x05value\x18\x02 \x01(\x0b\x32\x1e.ensembl_metadata.DatasetInfos:\x02\x38\x01\"!\n\nGenomeUUID\x12\x13\n\x0bgenome_uuid\x18\x01 \x01(\t\"y\n\x0eOrganismsGroup\x12\x1b\n\x13species_taxonomy_id\x18\x01 \x01(\r\x12\x13\n\x0b\x63ommon_name\x18\x02 \x01(\t\x12\x17\n\x0fscientific_name\x18\x03 \x01(\t\x12\r\n\x05order\x18\x04 \x01(\r\x12\r\n\x05\x63ount\x18\x05 \x01(\r\"o\n\x13OrganismsGroupCount\x12?\n\x15organisms_group_count\x18\x01 \x03(\x0b\x32 .ensembl_metadata.OrganismsGroup\x12\x17\n\x0frelease_version\x18\x02 \x01(\x01\"-\n\x07\x46TPLink\x12\x14\n\x0c\x64\x61taset_type\x18\x01 \x01(\t\x12\x0c\n\x04path\x18\x02 \x01(\t\"4\n\x08\x46TPLinks\x12(\n\x05Links\x18\x01 \x03(\x0b\x32\x19.ensembl_metadata.FTPLink\")\n\x0eReleaseVersion\x12\x17\n\x0frelease_version\x18\x01 \x01(\x01\"H\n\x15\x44\x61tasetAttributeValue\x12\x16\n\x0e\x61ttribute_name\x18\x01 \x01(\t\x12\x17\n\x0f\x61ttribute_value\x18\x02 \x01(\t\"o\n\x17\x44\x61tasetAttributesValues\x12;\n\nattributes\x18\x01 \x03(\x0b\x32\'.ensembl_metadata.DatasetAttributeValue\x12\x17\n\x0frelease_version\x18\x02 \x01(\x01\"A\n\x11GenomeUUIDRequest\x12\x13\n\x0bgenome_uuid\x18\x01 \x01(\t\x12\x17\n\x0frelease_version\x18\x02 \x01(\x01\"B\n\x16GenomeByKeywordRequest\x12\x0f\n\x07keyword\x18\x01 \x01(\t\x12\x17\n\x0frelease_version\x18\x02 \x01(\x01\"\x81\x02\n\x1eGenomeBySpecificKeywordRequest\x12\r\n\x05tolid\x18\x01 \x01(\t\x12\x1d\n\x15\x61ssembly_accession_id\x18\x02 \x01(\t\x12\x15\n\rassembly_name\x18\x03 \x01(\t\x12\x14\n\x0c\x65nsembl_name\x18\x04 \x01(\t\x12\x13\n\x0b\x63ommon_name\x18\x05 \x01(\t\x12\x17\n\x0fscientific_name\x18\x06 \x01(\t\x12 \n\x18scientific_parlance_name\x18\x07 \x01(\t\x12\x1b\n\x13species_taxonomy_id\x18\x08 \x01(\t\x12\x17\n\x0frelease_version\x18\t \x01(\x01\"U\n\x11GenomeNameRequest\x12\x14\n\x0c\x65nsembl_name\x18\x01 \x01(\t\x12\x11\n\tsite_name\x18\x02 \x01(\t\x12\x17\n\x0frelease_version\x18\x03 \x01(\x01\"C\n\x11\x41ssemblyIDRequest\x12\x15\n\rassembly_uuid\x18\x01 \x01(\t\x12\x17\n\x0frelease_version\x18\x02 \x01(\x01\"Q\n\x1a\x41ssemblyAccessionIDRequest\x12\x1a\n\x12\x61ssembly_accession\x18\x01 \x01(\t\x12\x17\n\x0frelease_version\x18\x02 \x01(\x01\"9\n\x11OrganismIDRequest\x12\x15\n\rorganism_uuid\x18\x01 \x01(\t\x12\r\n\x05group\x18\x02 \x01(\t\"R\n\x0eReleaseRequest\x12\x11\n\tsite_name\x18\x01 \x03(\t\x12\x17\n\x0frelease_version\x18\x02 \x03(\x01\x12\x14\n\x0c\x63urrent_only\x18\x03 \x01(\x08\"F\n\x15GenomeSequenceRequest\x12\x13\n\x0bgenome_uuid\x18\x01 \x01(\t\x12\x18\n\x10\x63hromosomal_only\x18\x02 \x01(\x08\"F\n\x15\x41ssemblyRegionRequest\x12\x13\n\x0bgenome_uuid\x18\x01 \x01(\t\x12\x18\n\x10\x63hromosomal_only\x18\x02 \x01(\x08\"X\n#GenomeAssemblySequenceRegionRequest\x12\x13\n\x0bgenome_uuid\x18\x01 \x01(\t\x12\x1c\n\x14sequence_region_name\x18\x02 \x01(\t\"?\n\x0f\x44\x61tasetsRequest\x12\x13\n\x0bgenome_uuid\x18\x01 \x01(\t\x12\x17\n\x0frelease_version\x18\x02 \x01(\x01\"B\n\x15GenomeDatatypeRequest\x12\x13\n\x0bgenome_uuid\x18\x01 \x01(\t\x12\x14\n\x0c\x64\x61taset_type\x18\x02 \x01(\t\"\x89\x01\n\x11GenomeInfoRequest\x12\x17\n\x0fproduction_name\x18\x01 \x01(\t\x12\x15\n\rassembly_name\x18\x02 \x01(\t\x12\x16\n\x0egenebuild_date\x18\x03 \x01(\t\x12\x17\n\x0frelease_version\x18\x04 \x01(\x01\x12\x13\n\x0buse_default\x18\x05 \x01(\x08\"0\n\x15OrganismsGroupRequest\x12\x17\n\x0frelease_version\x18\x01 \x01(\x01\"&\n\x10GenomeTagRequest\x12\x12\n\ngenome_tag\x18\x01 \x01(\t\"U\n\x0f\x46TPLinksRequest\x12\x13\n\x0bgenome_uuid\x18\x01 \x01(\t\x12\x14\n\x0c\x64\x61taset_type\x18\x02 \x01(\t\x12\x17\n\x0frelease_version\x18\x03 \x01(\t\"[\n\x15ReleaseVersionRequest\x12\x13\n\x0bgenome_uuid\x18\x01 \x01(\t\x12\x14\n\x0c\x64\x61taset_type\x18\x02 \x01(\t\x12\x17\n\x0frelease_version\x18\x03 \x01(\x01\"|\n\x1e\x44\x61tasetAttributesValuesRequest\x12\x13\n\x0bgenome_uuid\x18\x01 \x01(\t\x12\x14\n\x0c\x64\x61taset_type\x18\x02 \x01(\t\x12\x16\n\x0e\x61ttribute_name\x18\x03 \x03(\t\x12\x17\n\x0frelease_version\x18\x04 \x01(\x01\x32\xf5\x11\n\x0f\x45nsemblMetadata\x12R\n\x0fGetGenomeByUUID\x12#.ensembl_metadata.GenomeUUIDRequest\x1a\x18.ensembl_metadata.Genome\"\x00\x12T\n\rGetGenomeUUID\x12#.ensembl_metadata.GenomeInfoRequest\x1a\x1c.ensembl_metadata.GenomeUUID\"\x00\x12]\n\x13GetGenomesByKeyword\x12(.ensembl_metadata.GenomeByKeywordRequest\x1a\x18.ensembl_metadata.Genome\"\x00\x30\x01\x12m\n\x1bGetGenomesBySpecificKeyword\x12\x30.ensembl_metadata.GenomeBySpecificKeywordRequest\x1a\x18.ensembl_metadata.Genome\"\x00\x30\x01\x12m\n\x1fGetGenomesByAssemblyAccessionID\x12,.ensembl_metadata.AssemblyAccessionIDRequest\x1a\x18.ensembl_metadata.Genome\"\x00\x30\x01\x12Y\n\x15GetSpeciesInformation\x12#.ensembl_metadata.GenomeUUIDRequest\x1a\x19.ensembl_metadata.Species\"\x00\x12_\n\x16GetAssemblyInformation\x12#.ensembl_metadata.AssemblyIDRequest\x1a\x1e.ensembl_metadata.AssemblyInfo\"\x00\x12_\n\x18GetSubSpeciesInformation\x12#.ensembl_metadata.OrganismIDRequest\x1a\x1c.ensembl_metadata.SubSpecies\"\x00\x12\x64\n\x15GetTopLevelStatistics\x12#.ensembl_metadata.OrganismIDRequest\x1a$.ensembl_metadata.TopLevelStatistics\"\x00\x12p\n\x1bGetTopLevelStatisticsByUUID\x12#.ensembl_metadata.GenomeUUIDRequest\x1a*.ensembl_metadata.TopLevelStatisticsByUUID\"\x00\x12R\n\x0fGetGenomeByName\x12#.ensembl_metadata.GenomeNameRequest\x1a\x18.ensembl_metadata.Genome\"\x00\x12M\n\nGetRelease\x12 .ensembl_metadata.ReleaseRequest\x1a\x19.ensembl_metadata.Release\"\x00\x30\x01\x12V\n\x10GetReleaseByUUID\x12#.ensembl_metadata.GenomeUUIDRequest\x1a\x19.ensembl_metadata.Release\"\x00\x30\x01\x12\x62\n\x11GetGenomeSequence\x12\'.ensembl_metadata.GenomeSequenceRequest\x1a .ensembl_metadata.GenomeSequence\"\x00\x30\x01\x12\x62\n\x11GetAssemblyRegion\x12\'.ensembl_metadata.AssemblyRegionRequest\x1a .ensembl_metadata.AssemblyRegion\"\x00\x30\x01\x12\x8a\x01\n\x1fGetGenomeAssemblySequenceRegion\x12\x35.ensembl_metadata.GenomeAssemblySequenceRegionRequest\x1a..ensembl_metadata.GenomeAssemblySequenceRegion\"\x00\x12X\n\x15GetDatasetsListByUUID\x12!.ensembl_metadata.DatasetsRequest\x1a\x1a.ensembl_metadata.Datasets\"\x00\x12\x62\n\x15GetDatasetInformation\x12\'.ensembl_metadata.GenomeDatatypeRequest\x1a\x1e.ensembl_metadata.DatasetInfos\"\x00\x12j\n\x16GetOrganismsGroupCount\x12\'.ensembl_metadata.OrganismsGroupRequest\x1a%.ensembl_metadata.OrganismsGroupCount\"\x00\x12X\n\x12GetGenomeUUIDByTag\x12\".ensembl_metadata.GenomeTagRequest\x1a\x1c.ensembl_metadata.GenomeUUID\"\x00\x12N\n\x0bGetFTPLinks\x12!.ensembl_metadata.FTPLinksRequest\x1a\x1a.ensembl_metadata.FTPLinks\"\x00\x12\x66\n\x17GetReleaseVersionByUUID\x12\'.ensembl_metadata.ReleaseVersionRequest\x1a .ensembl_metadata.ReleaseVersion\"\x00\x12z\n\x19GetAttributesValuesByUUID\x12\x30.ensembl_metadata.DatasetAttributesValuesRequest\x1a).ensembl_metadata.DatasetAttributesValues\"\x00\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'ensembl.production.metadata.grpc.ensembl_metadata_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  _globals['_DATASETS_DATASETSENTRY']._options = None
  _globals['_DATASETS_DATASETSENTRY']._serialized_options = b'8\001'
  _globals['_GENOME']._serialized_start=78
  _globals['_GENOME']._serialized_end=393
  _globals['_SPECIES']._serialized_start=396
  _globals['_SPECIES']._serialized_end=549
  _globals['_ASSEMBLYINFO']._serialized_start=552
  _globals['_ASSEMBLYINFO']._serialized_end=734
  _globals['_SUBSPECIES']._serialized_start=736
  _globals['_SUBSPECIES']._serialized_end=815
  _globals['_ATTRIBUTESTATISTICS']._serialized_start=817
  _globals['_ATTRIBUTESTATISTICS']._serialized_end=916
  _globals['_TOPLEVELSTATISTICSBYUUID']._serialized_start=918
  _globals['_TOPLEVELSTATISTICSBYUUID']._serialized_end=1024
  _globals['_TOPLEVELSTATISTICS']._serialized_start=1026
  _globals['_TOPLEVELSTATISTICS']._serialized_end=1143
  _globals['_ASSEMBLY']._serialized_start=1146
  _globals['_ASSEMBLY']._serialized_end=1324
  _globals['_TAXON']._serialized_start=1326
  _globals['_TAXON']._serialized_end=1422
  _globals['_RELEASE']._serialized_start=1425
  _globals['_RELEASE']._serialized_end=1581
  _globals['_ORGANISM']._serialized_start=1584
  _globals['_ORGANISM']._serialized_end=1806
  _globals['_ATTRIBUTE']._serialized_start=1808
  _globals['_ATTRIBUTE']._serialized_end=1883
  _globals['_ATTRIBUTESINFO']._serialized_start=1886
  _globals['_ATTRIBUTESINFO']._serialized_end=2303
  _globals['_DATASETINFOS']._serialized_start=2306
  _globals['_DATASETINFOS']._serialized_end=2598
  _globals['_DATASETINFOS_DATASETINFO']._serialized_start=2433
  _globals['_DATASETINFOS_DATASETINFO']._serialized_end=2598
  _globals['_GENOMESEQUENCE']._serialized_start=2600
  _globals['_GENOMESEQUENCE']._serialized_end=2713
  _globals['_ASSEMBLYREGION']._serialized_start=2715
  _globals['_ASSEMBLYREGION']._serialized_end=2829
  _globals['_GENOMEASSEMBLYSEQUENCEREGION']._serialized_start=2831
  _globals['_GENOMEASSEMBLYSEQUENCEREGION']._serialized_end=2945
  _globals['_DATASETS']._serialized_start=2948
  _globals['_DATASETS']._serialized_end=3120
  _globals['_DATASETS_DATASETSENTRY']._serialized_start=3041
  _globals['_DATASETS_DATASETSENTRY']._serialized_end=3120
  _globals['_GENOMEUUID']._serialized_start=3122
  _globals['_GENOMEUUID']._serialized_end=3155
  _globals['_ORGANISMSGROUP']._serialized_start=3157
  _globals['_ORGANISMSGROUP']._serialized_end=3278
  _globals['_ORGANISMSGROUPCOUNT']._serialized_start=3280
  _globals['_ORGANISMSGROUPCOUNT']._serialized_end=3391
  _globals['_FTPLINK']._serialized_start=3393
  _globals['_FTPLINK']._serialized_end=3438
  _globals['_FTPLINKS']._serialized_start=3440
  _globals['_FTPLINKS']._serialized_end=3492
  _globals['_RELEASEVERSION']._serialized_start=3494
  _globals['_RELEASEVERSION']._serialized_end=3535
  _globals['_DATASETATTRIBUTEVALUE']._serialized_start=3537
  _globals['_DATASETATTRIBUTEVALUE']._serialized_end=3609
  _globals['_DATASETATTRIBUTESVALUES']._serialized_start=3611
  _globals['_DATASETATTRIBUTESVALUES']._serialized_end=3722
  _globals['_GENOMEUUIDREQUEST']._serialized_start=3724
  _globals['_GENOMEUUIDREQUEST']._serialized_end=3789
  _globals['_GENOMEBYKEYWORDREQUEST']._serialized_start=3791
  _globals['_GENOMEBYKEYWORDREQUEST']._serialized_end=3857
  _globals['_GENOMEBYSPECIFICKEYWORDREQUEST']._serialized_start=3860
  _globals['_GENOMEBYSPECIFICKEYWORDREQUEST']._serialized_end=4117
  _globals['_GENOMENAMEREQUEST']._serialized_start=4119
  _globals['_GENOMENAMEREQUEST']._serialized_end=4204
  _globals['_ASSEMBLYIDREQUEST']._serialized_start=4206
  _globals['_ASSEMBLYIDREQUEST']._serialized_end=4273
  _globals['_ASSEMBLYACCESSIONIDREQUEST']._serialized_start=4275
  _globals['_ASSEMBLYACCESSIONIDREQUEST']._serialized_end=4356
  _globals['_ORGANISMIDREQUEST']._serialized_start=4358
  _globals['_ORGANISMIDREQUEST']._serialized_end=4415
  _globals['_RELEASEREQUEST']._serialized_start=4417
  _globals['_RELEASEREQUEST']._serialized_end=4499
  _globals['_GENOMESEQUENCEREQUEST']._serialized_start=4501
  _globals['_GENOMESEQUENCEREQUEST']._serialized_end=4571
  _globals['_ASSEMBLYREGIONREQUEST']._serialized_start=4573
  _globals['_ASSEMBLYREGIONREQUEST']._serialized_end=4643
  _globals['_GENOMEASSEMBLYSEQUENCEREGIONREQUEST']._serialized_start=4645
  _globals['_GENOMEASSEMBLYSEQUENCEREGIONREQUEST']._serialized_end=4733
  _globals['_DATASETSREQUEST']._serialized_start=4735
  _globals['_DATASETSREQUEST']._serialized_end=4798
  _globals['_GENOMEDATATYPEREQUEST']._serialized_start=4800
  _globals['_GENOMEDATATYPEREQUEST']._serialized_end=4866
  _globals['_GENOMEINFOREQUEST']._serialized_start=4869
  _globals['_GENOMEINFOREQUEST']._serialized_end=5006
  _globals['_ORGANISMSGROUPREQUEST']._serialized_start=5008
  _globals['_ORGANISMSGROUPREQUEST']._serialized_end=5056
  _globals['_GENOMETAGREQUEST']._serialized_start=5058
  _globals['_GENOMETAGREQUEST']._serialized_end=5096
  _globals['_FTPLINKSREQUEST']._serialized_start=5098
  _globals['_FTPLINKSREQUEST']._serialized_end=5183
  _globals['_RELEASEVERSIONREQUEST']._serialized_start=5185
  _globals['_RELEASEVERSIONREQUEST']._serialized_end=5276
  _globals['_DATASETATTRIBUTESVALUESREQUEST']._serialized_start=5278
  _globals['_DATASETATTRIBUTESVALUESREQUEST']._serialized_end=5402
  _globals['_ENSEMBLMETADATA']._serialized_start=5405
  _globals['_ENSEMBLMETADATA']._serialized_end=7698
# @@protoc_insertion_point(module_scope)
