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
import grpc
import logging

from ensembl_metadata_pb2 import (
    GenomeUUIDRequest,
    GenomeNameRequest,
    ReleaseRequest,
    GenomeSequenceRequest,
    AssemblyIDRequest,
    GenomeByKeywordRequest,
    AssemblyAccessionIDRequest,
    OrganismIDRequest,
    DatasetsRequest,
    GenomeDatatypeRequest,
    GenomeInfoRequest
)

import ensembl.production.metadata.ensembl_metadata_pb2_grpc as ensembl_metadata_pb2_grpc


def get_genome(stub, genome_request):
    if isinstance(genome_request, GenomeUUIDRequest):
        genome = stub.GetGenomeByUUID(genome_request)
        print(genome)
    elif isinstance(genome_request, GenomeNameRequest):
        genome = stub.GetGenomeByName(genome_request)
        print(genome)
    else:
        print("Unrecognised request message")
        return

    if genome.genome_uuid == '':
        print("No genome")
        return


def get_genomes_by_keyword(stub, genome_request):
    genome_uuids = [
        genome.genome_uuid for genome in stub.GetGenomesByKeyword(genome_request)
    ]
    if not genome_uuids:
        print("No genomes")
        return


def get_genomes(stub):
    request1 = GenomeUUIDRequest(genome_uuid="a73351f7-93e7-11ec-a39d-005056b38ce3")
    request2 = GenomeUUIDRequest(genome_uuid="rhubarb")
    request3 = GenomeNameRequest(ensembl_name="accipiter_gentilis", site_name="rapid")
    request4 = GenomeNameRequest(
        ensembl_name="accipiter_gentilis", site_name="rapid", release_version=13.0
    )
    request5 = GenomeNameRequest(
        ensembl_name="banana", site_name="plants", release_version=104.0
    )
    request6 = GenomeByKeywordRequest(keyword="Human")
    request7 = GenomeByKeywordRequest(keyword="Bigfoot")
    print("**** Valid UUID ****")
    get_genome(stub, request1)
    print("**** Invalid UUID ****")
    get_genome(stub, request2)
    print("**** Name, no release ****")
    get_genome(stub, request3)
    print("**** Name, past release ****")
    get_genome(stub, request4)
    print("**** Invalid name ****")
    get_genome(stub, request5)
    print("**** Valid keyword, no release ****")
    get_genomes_by_keyword(stub, request6)
    print("**** Invalid keyword ****")
    get_genomes_by_keyword(stub, request7)


def list_genome_sequences(stub):
    request1 = GenomeSequenceRequest(
        genome_uuid="a73351f7-93e7-11ec-a39d-005056b38ce3", chromosomal_only=True
    )
    genome_sequences1 = stub.GetGenomeSequence(request1)
    print("**** Only chromosomes ****")
    for seq in genome_sequences1:
        print(seq)

    request2 = GenomeSequenceRequest(genome_uuid="a73351f7-93e7-11ec-a39d-005056b38ce3")
    genome_sequences2 = stub.GetGenomeSequence(request2)
    print("**** All sequences ****")
    for seq in genome_sequences2:
        print(seq)

    request3 = GenomeSequenceRequest(genome_uuid="garbage")
    genome_sequences3 = stub.GetGenomeSequence(request3)
    print("**** Invalid UUID ****")
    for seq in genome_sequences3:
        print(seq)


def list_releases(stub):
    request1 = ReleaseRequest()
    releases1 = stub.GetRelease(request1)
    print("**** All releases ****")
    for release in releases1:
        print(release)

    request2 = ReleaseRequest(site_name=["rapid"])
    releases2 = stub.GetRelease(request2)
    print("**** All Rapid releases ****")
    for release in releases2:
        print(release)

    request3 = ReleaseRequest(site_name=["rapid"], current_only=1)
    releases3 = stub.GetRelease(request3)
    print("**** Current Rapid release ****")
    for release in releases3:
        print(release)

    request4 = ReleaseRequest(release_version=[1])
    releases4 = stub.GetRelease(request4)
    print("**** Version 14 ****")
    for release in releases4:
        print(release)

    request5 = ReleaseRequest(release_version=[79])
    releases5 = stub.GetRelease(request5)
    print("**** Version 79 ****")
    for release in releases5:
        print(release)

    request6 = ReleaseRequest(release_version=[1])
    releases6 = stub.GetRelease(request6)
    print("**** Versions 14 and 15 ****")
    for release in releases6:
        print(release)


def list_releases_by_uuid(stub):
    request1 = GenomeUUIDRequest(genome_uuid="a73351f7-93e7-11ec-a39d-005056b38ce3")
    releases1 = stub.GetReleaseByUUID(request1)
    print("**** Release for Narwhal ****")
    for release in releases1:
        print(release)


def get_species_information_by_uuid(stub):
    request1 = GenomeUUIDRequest(genome_uuid="a73351f7-93e7-11ec-a39d-005056b38ce3")
    releases1 = stub.GetSpeciesInformation(request1)
    print("**** Species information ****")
    print(releases1)


def get_assembly_information(stub):
    request1 = AssemblyIDRequest(assembly_uuid="3142a1c4-0f6a-11ee-bfb4-005056b370fb")
    releases1 = stub.GetAssemblyInformation(request1)
    print("**** Assembly information ****")
    print(releases1)


def get_genomes_by_assembly_accession(stub):
    request1 = AssemblyAccessionIDRequest(assembly_accession="GCA_000001405.28")
    genomes1 = stub.GetGenomesByAssemblyAccessionID(request1)
    print("**** Genomes from assembly accession information ****")
    for genome in genomes1:
        print(genome)

    request2 = AssemblyAccessionIDRequest(assembly_accession=None)
    genomes2 = stub.GetGenomesByAssemblyAccessionID(request2)
    print("**** Genomes from null assembly accession ****")
    print(list(genomes2))


def get_sub_species_info(stub):
    request1 = OrganismIDRequest(organism_uuid="317e356e-0f6a-11ee-bfb4-005056b370fb")
    releases1 = stub.GetSubSpeciesInformation(request1)
    print("**** Sub species information ****")
    print(releases1)


def get_grouping_info(stub):
    request1 = OrganismIDRequest(organism_uuid="317e356e-0f6a-11ee-bfb4-005056b370fb")
    releases1 = stub.GetGroupingInformation(request1)
    print("**** Grouping information ****")
    print(releases1)


def get_karyotype_information(stub):
    request1 = GenomeUUIDRequest(genome_uuid="a73351f7-93e7-11ec-a39d-005056b38ce3")
    releases1 = stub.GetKaryotypeInformation(request1)
    print("**** Karyotype ****")
    print(releases1)


def get_top_level_statistics(stub):
    request1 = OrganismIDRequest(organism_uuid="317e356e-0f6a-11ee-bfb4-005056b370fb")
    releases1 = stub.GetTopLevelStatistics(request1)
    print("**** Top level statistics ****")
    print(releases1)


def get_top_level_statistics_by_uuid(stub):
    genome_request = GenomeUUIDRequest(
        genome_uuid="a73351f7-93e7-11ec-a39d-005056b38ce3"
    )
    toplevel_stats_by_uuid_request = stub.GetTopLevelStatisticsByUUID(genome_request)
    print("**** Top level statistics by UUID ****")
    print(toplevel_stats_by_uuid_request)


def get_datasets_list_by_uuid(stub):
    request1 = DatasetsRequest(
        genome_uuid="a73351f7-93e7-11ec-a39d-005056b38ce3", release_version=108.0
    )
    datasets = stub.GetDatasetsListByUUID(request1)
    print(datasets)


def get_dataset_infos_by_dataset_type(stub):
    request1 = GenomeDatatypeRequest(
        genome_uuid="a7335667-93e7-11ec-a39d-005056b38ce3", dataset_type="assembly"
    )
    datasets1 = stub.GetDatasetInformation(request1)
    print(datasets1.dataset_infos)


def get_genome_uuid(stub):
    request1 = GenomeInfoRequest(
        ensembl_name="homo_sapiens", assembly_name="GRCh37.p13"
    )
    genome_uuid = stub.GetGenomeUUID(request1)
    print(genome_uuid)


def run():
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = ensembl_metadata_pb2_grpc.EnsemblMetadataStub(channel)
        print("---------------Get Species Information-----------")
        get_species_information_by_uuid(stub)
        print("---------------Get Assembly Information-----------")
        get_assembly_information(stub)
        print(
            "---------------Get Genome Information from assembly accession-----------"
        )
        get_genomes_by_assembly_accession(stub)
        print("---------------Get Subspecies Information-----------")
        get_sub_species_info(stub)
        print("---------------Get Grouping Information-----------")
        get_grouping_info(stub)
        print("---------------Get Karyotype Information-----------")
        get_karyotype_information(stub)
        print("---------------Get Top Level Statistics-----------")
        get_top_level_statistics(stub)
        print("---------------Get Top Level Statistics By UUID-----------")
        get_top_level_statistics_by_uuid(stub)
        print("-------------- Get Genomes --------------")
        get_genomes(stub)
        print("-------------- List Sequences --------------")
        list_genome_sequences(stub)
        print("-------------- List Releases --------------")
        list_releases(stub)
        print("-------------- List Releases for Genome --------------")
        list_releases_by_uuid(stub)
        print("---------------Get Datasets List-----------")
        get_datasets_list_by_uuid(stub)
        print("-------------- List Dataset information for Genome --------------")
        get_dataset_infos_by_dataset_type(stub)
        print("-------------- Get Genome UUID --------------")
        get_genome_uuid(stub)


if __name__ == "__main__":
    logging.basicConfig()
    run()
