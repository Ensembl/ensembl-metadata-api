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

import ensembl.production.metadata.grpc.utils as utils
from ensembl.production.metadata.api.adaptors import GenomeAdaptor
from ensembl.production.metadata.api.adaptors.vep import VepAdaptor
from ensembl.production.metadata.grpc import ensembl_metadata_pb2_grpc
from ensembl.production.metadata.grpc.config import MetadataConfig

logger = logging.getLogger(__name__)


class EnsemblMetadataServicer(ensembl_metadata_pb2_grpc.EnsemblMetadataServicer):
    def __init__(self):
        self.genome_adaptor = utils.connect_to_db(
            adaptor_class=GenomeAdaptor,
            taxonomy_uri=MetadataConfig().taxon_uri
        )
        self.vep_adaptor = utils.connect_to_db(
            adaptor_class=VepAdaptor,
            file="all"
        )
        super().__init__()

    def GetSpeciesInformation(self, request, context):
        logger.debug(f"Received RPC for GetSpeciesInformation with request: {request}")
        return utils.get_species_information(self.genome_adaptor, request.genome_uuid)

    def GetAssemblyInformation(self, request, context):
        logger.debug(f"Received RPC for GetAssemblyInformation with request: {request}")
        return utils.get_assembly_information(self.genome_adaptor, request.assembly_uuid)

    def GetGenomesByAssemblyAccessionID(self, request, context):
        logger.debug(f"Received RPC for GetGenomesByAssemblyAccessionID with request: {request}")
        return utils.get_genomes_from_assembly_accession_iterator(
            self.genome_adaptor, request.assembly_accession, request.release_version
        )

    def GetSubSpeciesInformation(self, request, context):
        logger.debug(f"Received RPC for GetSubSpeciesInformation with request: {request}")
        return utils.get_sub_species_info(self.genome_adaptor, request.organism_uuid, request.group)

    def GetTopLevelStatistics(self, request, context):
        logger.debug(f"Received RPC for GetTopLevelStatistics with request: {request}")
        return utils.get_top_level_statistics(self.genome_adaptor, request.organism_uuid)

    def GetTopLevelStatisticsByUUID(self, request, context):
        logger.debug(f"Received RPC for GetTopLevelStatisticsByUUID with request: {request}")
        return utils.get_top_level_statistics_by_uuid(self.genome_adaptor, request.genome_uuid)

    def GetGenomeUUID(self, request, context):
        logger.debug(f"Received RPC for GetGenomeUUID with request: {request}")
        return utils.get_genome_uuid(self.genome_adaptor, production_name=request.production_name,
                                     assembly_name=request.assembly_name,
                                     use_default=request.use_default,
                                     genebuild_date=request.genebuild_date,
                                     release_version=request.release_version)

    def GetGenomeByUUID(self, request, context):
        logger.debug(f"Received RPC for GetGenomeByUUID with request: {request}")
        return utils.get_genome_by_uuid(self.genome_adaptor, request.genome_uuid, request.release_version)

    def GetAttributesByGenomeUUID(self, request, context):
        logger.debug(f"Received RPC for GetAttributesByGenomeUUID with request: {request}")
        return utils.get_attributes_by_genome_uuid(self.genome_adaptor, request.genome_uuid, request.release_version)

    def GetBriefGenomeDetailsByUUID(self, request, context):
        logger.debug(f"Received RPC for GetBriefGenomeDetailsByUUID with request: {request}")
        return utils.get_brief_genome_details_by_uuid(self.genome_adaptor, request.genome_uuid, request.release_version)


    def GetGenomesBySpecificKeyword(self, request, context):
        logger.debug(f"Received RPC for GetGenomesBySpecificKeyword with request: {request}")
        return utils.get_genomes_by_specific_keyword_iterator(
            self.genome_adaptor,
            request.tolid,
            request.assembly_accession_id,
            request.assembly_name,
            request.ensembl_name,
            request.common_name,
            request.scientific_name,
            request.scientific_parlance_name,
            request.species_taxonomy_id,
            request.release_version
        )

    def GetGenomesByReleaseVersion(self, request, context):
        logger.debug(f"Received RPC for GetGenomesByReleaseVersion with request: {request}")
        return utils.get_genomes_by_release_version_iterator(
            self.genome_adaptor,
            request.release_version
        )

    def GetGenomeByName(self, request, context):
        logger.debug(f"Received RPC for GetGenomeByName with request: {request}")
        return utils.get_genome_by_name(self.genome_adaptor, request.ensembl_name, request.site_name, request.release_version)

    def GetRelease(self, request, context):
        logger.debug(f"Received RPC for GetRelease with request: {request}")
        return utils.release_iterator(
            self.genome_adaptor, request.site_name, request.release_label, request.current_only
        )

    def GetReleaseByUUID(self, request, context):
        logger.debug(f"Received RPC for GetReleaseByUUID with request: {request}")
        return utils.release_by_uuid_iterator(self.genome_adaptor, request.genome_uuid)

    def GetGenomeSequence(self, request, context):
        logger.debug(f"Received RPC for GetGenomeSequence with request: {request}")
        return utils.genome_sequence_iterator(
            self.genome_adaptor, request.genome_uuid, request.chromosomal_only
        )

    def GetAssemblyRegion(self, request, context):
        logger.debug(f"Received RPC for GetAssemblyRegion with request: {request}")
        return utils.assembly_region_iterator(
            self.genome_adaptor, request.genome_uuid, request.chromosomal_only
        )

    def GetGenomeAssemblySequenceRegion(self, request, context):
        logger.debug(f"Received RPC for GetGenomeAssemblySequenceRegion with request: {request}")
        return utils.genome_assembly_sequence_region(
            self.genome_adaptor, request.genome_uuid, request.sequence_region_name
        )

    def GetDatasetsListByUUID(self, request, context):
        logger.debug(f"Received RPC for GetDatasetsListByUUID with request: {request}")
        return utils.get_datasets_list_by_uuid(
            self.genome_adaptor, request.genome_uuid, request.release_version
        )

    def GetDatasetInformation(self, request, context):
        # TODO: this method can merged with the GetDatasetsListByUUID above...
        logger.debug(f"Received RPC for GetDatasetInformation with request: {request}")
        return utils.get_dataset_by_genome_and_dataset_type(
            self.genome_adaptor, request.genome_uuid, request.dataset_type
        )

    def GetOrganismsGroupCount(self, request, context):
        logger.debug(f"Received RPC for GetOrganismsGroupCount with request: {request}")
        return utils.get_organisms_group_count(
            self.genome_adaptor, request.release_label
        )

    def GetGenomeUUIDByTag(self, request, context):
        logger.debug(f"Received RPC for GetGenomeUUIDByTag with request: {request}")
        return utils.get_genome_uuid_by_tag(self.genome_adaptor, request.genome_tag)

    def GetFTPLinks(self, request, context):
        return utils.get_ftp_links(self.genome_adaptor, request.genome_uuid, request.dataset_type, request.release_version)

    def GetReleaseVersionByUUID(self, request, context):
        logger.debug(f"Received RPC for GetReleaseVersionByUUID with request: {request}")
        return utils.get_release_version_by_uuid(
            self.genome_adaptor, request.genome_uuid, request.dataset_type, request.release_version
        )

    def GetAttributesValuesByUUID(self, request, context):
        logger.debug(f"Received RPC for GetAttributesByUUID with request: {request}")
        attribute_names = list(request.attribute_name) if request.attribute_name else None
        return utils.get_attributes_values_by_uuid(
            self.genome_adaptor, request.genome_uuid, request.dataset_type,
            request.release_version, attribute_names, request.latest_only
        )

    def GetVepFilePathsByUUID(self, request, context):
        logger.debug(f"Received RPC for GetVepFilePathsByUUID with request: {request}")
        return utils.get_vep_paths_by_uuid(
            self.vep_adaptor, request.genome_uuid
        )

    def GetGenomeGroupsWithReference(self, request, context):
        logger.debug(f"Received RPC for GetGenomeGroupsWithReference with request: {request}")
        return utils.get_genome_groups_by_reference(
            self.genome_adaptor, request.group_type
        )

