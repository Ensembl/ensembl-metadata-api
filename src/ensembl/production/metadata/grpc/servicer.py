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
from ensembl.production.metadata.grpc import ensembl_metadata_pb2_grpc

import ensembl.production.metadata.grpc.utils as utils


class EnsemblMetadataServicer(ensembl_metadata_pb2_grpc.EnsemblMetadataServicer):
    def __init__(self):
        self.db = utils.connect_to_db()

    def GetSpeciesInformation(self, request, context):
        return utils.get_species_information(self.db, request.genome_uuid)

    def GetAssemblyInformation(self, request, context):
        return utils.get_assembly_information(self.db, request.assembly_uuid)

    def GetGenomesByAssemblyAccessionID(self, request, context):
        return utils.get_genomes_from_assembly_accession_iterator(
            self.db, request.assembly_accession, request.release_version
        )

    def GetSubSpeciesInformation(self, request, context):
        return utils.get_sub_species_info(self.db, request.organism_uuid, request.group)

    def GetTopLevelStatistics(self, request, context):
        return utils.get_top_level_statistics(self.db, request.organism_uuid, request.group)

    def GetTopLevelStatisticsByUUID(self, request, context):
        return utils.get_top_level_statistics_by_uuid(self.db, request.genome_uuid)

    def GetGenomeUUID(self, request, context):
        return utils.get_genome_uuid(self.db, request.production_name, request.assembly_name, request.use_default)

    def GetGenomeByUUID(self, request, context):
        return utils.get_genome_by_uuid(self.db, request.genome_uuid, request.release_version)

    def GetGenomesByKeyword(self, request, context):
        return utils.get_genomes_by_keyword_iterator(
            self.db, request.keyword, request.release_version
        )

    def GetGenomeByName(self, request, context):
        return utils.get_genome_by_name(
            self.db, request.ensembl_name, request.site_name, request.release_version
        )

    def GetRelease(self, request, context):
        return utils.release_iterator(
            self.db, request.site_name, request.release_version, request.current_only
        )

    def GetReleaseByUUID(self, request, context):
        return utils.release_by_uuid_iterator(self.db, request.genome_uuid)

    def GetGenomeSequence(self, request, context):
        return utils.genome_sequence_iterator(
            self.db, request.genome_uuid, request.chromosomal_only
        )

    def GetAssemblyRegion(self, request, context):
        return utils.assembly_region_iterator(
            self.db, request.genome_uuid, request.chromosomal_only
        )

    def GetGenomeAssemblySequenceRegion(self, request, context):
        return utils.genome_assembly_sequence_region(
            self.db, request.genome_uuid, request.sequence_region_name
        )

    def GetDatasetsListByUUID(self, request, context):
        return utils.get_datasets_list_by_uuid(
            self.db, request.genome_uuid, request.release_version
        )

    def GetDatasetInformation(self, request, context):
        return utils.get_dataset_by_genome_and_dataset_type(
            self.db, request.genome_uuid, request.dataset_type
        )

    def GetOrganismsGroupCount(self, request, context):
        return utils.get_organisms_group_count(
            self.db, request.release_version
        )

    def GetGenomeUUIDByTag(self, request, context):
        return utils.get_genome_uuid_by_tag(self.db, request.genome_tag)
