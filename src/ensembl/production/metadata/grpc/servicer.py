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
from ensembl.production.metadata.grpc import ensembl_metadata_pb2_grpc

logger = logging.getLogger(__name__)


class EnsemblMetadataServicer(ensembl_metadata_pb2_grpc.EnsemblMetadataServicer):
    def __init__(self):
        self.db = utils.connect_to_db()
        super().__init__()

    def GetSpeciesInformation(self, request, context):
        logger.debug(f"Received RPC for GetSpeciesInformation with request: {request}")
        return utils.get_species_information(self.db, request.genome_uuid)

    def GetAssemblyInformation(self, request, context):
        logger.debug(f"Received RPC for GetAssemblyInformation with request: {request}")
        return utils.get_assembly_information(self.db, request.assembly_uuid)

    def GetGenomesByAssemblyAccessionID(self, request, context):
        logger.debug(f"Received RPC for GetGenomesByAssemblyAccessionID with request: {request}")
        return utils.get_genomes_from_assembly_accession_iterator(
            self.db, request.assembly_accession, request.release_version
        )

    def GetSubSpeciesInformation(self, request, context):
        logger.debug(f"Received RPC for GetSubSpeciesInformation with request: {request}")
        return utils.get_sub_species_info(self.db, request.organism_uuid, request.group)

    def GetTopLevelStatistics(self, request, context):
        logger.debug(f"Received RPC for GetTopLevelStatistics with request: {request}")
        return utils.get_top_level_statistics(self.db, request.organism_uuid, request.group)

    def GetTopLevelStatisticsByUUID(self, request, context):
        logger.debug(f"Received RPC for GetTopLevelStatisticsByUUID with request: {request}")
        return utils.get_top_level_statistics_by_uuid(self.db, request.genome_uuid)

    def GetGenomeUUID(self, request, context):
        logger.debug(f"Received RPC for GetGenomeUUID with request: {request}")
        return utils.get_genome_uuid(self.db, production_name=request.production_name,
                                     assembly_name=request.assembly_name,
                                     use_default=request.use_default,
                                     genebuild_date=request.genebuild_date,
                                     release_version=request.release_version)

    def GetGenomeByUUID(self, request, context):
        logger.debug(f"Received RPC for GetGenomeByUUID with request: {request}")
        return utils.get_genome_by_uuid(self.db, request.genome_uuid, request.release_version)

    def GetGenomesByKeyword(self, request, context):
        logger.debug(f"Received RPC for GetGenomesByKeyword with request: {request}")
        return utils.get_genomes_by_keyword_iterator(
            self.db, request.keyword, request.release_version
        )

    def GetGenomeByName(self, request, context):
        logger.debug(f"Received RPC for GetGenomeByName with request: {request}")
        return utils.get_genome_by_name(self.db, request.ensembl_name, request.site_name, request.release_version)

    def GetRelease(self, request, context):
        logger.debug(f"Received RPC for GetRelease with request: {request}")
        return utils.release_iterator(
            self.db, request.site_name, request.release_version, request.current_only
        )

    def GetReleaseByUUID(self, request, context):
        logger.debug(f"Received RPC for GetReleaseByUUID with request: {request}")
        return utils.release_by_uuid_iterator(self.db, request.genome_uuid)

    def GetGenomeSequence(self, request, context):
        logger.debug(f"Received RPC for GetGenomeSequence with request: {request}")
        return utils.genome_sequence_iterator(
            self.db, request.genome_uuid, request.chromosomal_only
        )

    def GetAssemblyRegion(self, request, context):
        logger.debug(f"Received RPC for GetAssemblyRegion with request: {request}")
        return utils.assembly_region_iterator(
            self.db, request.genome_uuid, request.chromosomal_only
        )

    def GetGenomeAssemblySequenceRegion(self, request, context):
        logger.debug(f"Received RPC for GetGenomeAssemblySequenceRegion with request: {request}")
        return utils.genome_assembly_sequence_region(
            self.db, request.genome_uuid, request.sequence_region_name
        )

    def GetDatasetsListByUUID(self, request, context):
        logger.debug(f"Received RPC for GetDatasetsListByUUID with request: {request}")
        return utils.get_datasets_list_by_uuid(
            self.db, request.genome_uuid, request.release_version
        )

    def GetDatasetInformation(self, request, context):
        logger.debug(f"Received RPC for GetDatasetInformation with request: {request}")
        return utils.get_dataset_by_genome_and_dataset_type(
            self.db, request.genome_uuid, request.dataset_type
        )

    def GetOrganismsGroupCount(self, request, context):
        logger.debug(f"Received RPC for GetOrganismsGroupCount with request: {request}")
        return utils.get_organisms_group_count(
            self.db, request.release_version
        )

    def GetGenomeUUIDByTag(self, request, context):
        logger.debug(f"Received RPC for GetGenomeUUIDByTag with request: {request}")
        return utils.get_genome_uuid_by_tag(self.db, request.genome_tag)

    def GetFTPLinks(self, request, context):
        return utils.get_ftp_links(self.db, request.genome_uuid, request.dataset_type, request.release_version)

    def GetReleaseVersionByUUID(self, request, context):
        logger.debug(f"Received RPC for GetReleaseVersionByUUID with request: {request}")
        return utils.get_release_version_by_uuid(
            self.db, request.genome_uuid, request.dataset_type, request.release_version
        )
