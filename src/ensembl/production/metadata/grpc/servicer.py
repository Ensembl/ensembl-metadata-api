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

from ensembl.production.metadata.grpc.utils import connect_to_db, get_species_information, get_assembly_information, \
    get_genomes_from_assembly_accession_iterator, get_sub_species_info, get_karyotype_information, \
    get_top_level_statistics, get_top_level_statistics_by_uuid, get_genome_uuid, get_genome_by_uuid, \
    get_genomes_by_keyword_iterator, get_genome_by_name, release_iterator, release_by_uuid_iterator, \
    genome_sequence_iterator, get_datasets_list_by_uuid, get_dataset_by_genome_and_dataset_type, \
    genome_assembly_sequence_iterator, genome_assembly_sequence_region_iterator, get_organisms_group_count


class EnsemblMetadataServicer(ensembl_metadata_pb2_grpc.EnsemblMetadataServicer):
    def __init__(self):
        self.db = connect_to_db()

    def GetSpeciesInformation(self, request, context):
        return get_species_information(self.db, request.genome_uuid)

    def GetAssemblyInformation(self, request, context):
        return get_assembly_information(self.db, request.assembly_uuid)

    def GetGenomesByAssemblyAccessionID(self, request, context):
        return get_genomes_from_assembly_accession_iterator(
            self.db, request.assembly_accession
        )

    def GetSubSpeciesInformation(self, request, context):
        return get_sub_species_info(self.db, request.organism_uuid, request.group)

    def GetKaryotypeInformation(self, request, context):
        return get_karyotype_information(self.db, request.genome_uuid)

    def GetTopLevelStatistics(self, request, context):
        return get_top_level_statistics(self.db, request.organism_uuid, request.group)

    def GetTopLevelStatisticsByUUID(self, request, context):
        return get_top_level_statistics_by_uuid(self.db, request.genome_uuid)

    def GetGenomeUUID(self, request, context):
        return get_genome_uuid(self.db, request.ensembl_name, request.assembly_name, request.use_default)

    def GetGenomeByUUID(self, request, context):
        return get_genome_by_uuid(self.db, request.genome_uuid, request.release_version)

    def GetGenomesByKeyword(self, request, context):
        return get_genomes_by_keyword_iterator(
            self.db, request.keyword, request.release_version
        )

    def GetGenomeByName(self, request, context):
        return get_genome_by_name(
            self.db, request.ensembl_name, request.site_name, request.release_version
        )

    def GetRelease(self, request, context):
        return release_iterator(
            self.db, request.site_name, request.release_version, request.current_only
        )

    def GetReleaseByUUID(self, request, context):
        return release_by_uuid_iterator(self.db, request.genome_uuid)

    def GetGenomeSequence(self, request, context):
        return genome_sequence_iterator(
            self.db, request.genome_uuid, request.chromosomal_only
        )

    def GetGenomeAssemblySequence(self, request, context):
        return genome_assembly_sequence_iterator(
            self.db, request.genome_uuid, request.chromosomal_only
        )

    def GetGenomeAssemblySequenceRegion(self, request, context):
        return genome_assembly_sequence_region_iterator(
            self.db, request.genome_uuid,
            request.sequence_region_name,
            request.chromosomal_only
        )

    def GetDatasetsListByUUID(self, request, context):
        return get_datasets_list_by_uuid(
            self.db, request.genome_uuid, request.release_version
        )

    def GetDatasetInformation(self, request, context):
        return get_dataset_by_genome_and_dataset_type(
            self.db, request.genome_uuid, request.dataset_type
        )

    def GetOrganismsGroupCount(self, request, context):
        return get_organisms_group_count(
            self.db, request.release_version
        )
