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
    GenomeInfoRequest,
    OrganismsGroupRequest,
    AssemblyRegionRequest,
    GenomeAssemblySequenceRegionRequest,
    GenomeTagRequest,
    FTPLinksRequest
)

import ensembl.production.metadata.grpc.ensembl_metadata_pb2_grpc as ensembl_metadata_pb2_grpc


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
    if isinstance(genome_request, GenomeByKeywordRequest):
        genomes = stub.GetGenomesByKeyword(genome_request)
        for genome in genomes:
            print(genome)


def get_genomes(stub):
    request1 = GenomeUUIDRequest(genome_uuid="9caa2cae-d1c8-4cfc-9ffd-2e13bc3e95b1")
    request2 = GenomeUUIDRequest(genome_uuid="rhubarb")
    request3 = GenomeNameRequest(ensembl_name="129S1_SvImJ_v1", site_name="Ensembl")
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
        genome_uuid="2afef36f-3660-4b8c-819b-d1e5a77c9918", chromosomal_only=True
    )
    genome_sequences1 = stub.GetGenomeSequence(request1)
    print("**** Only chromosomes ****")
    for seq in genome_sequences1:
        print(seq)

    request2 = GenomeSequenceRequest(genome_uuid="2afef36f-3660-4b8c-819b-d1e5a77c9918")
    genome_sequences2 = stub.GetGenomeSequence(request2)
    print("**** All sequences ****")
    for seq in genome_sequences2:
        print(seq)

    request3 = GenomeSequenceRequest(genome_uuid="garbage")
    genome_sequences3 = stub.GetGenomeSequence(request3)
    print("**** Invalid UUID ****")
    for seq in genome_sequences3:
        print(seq)


def list_genome_assembly_sequences(stub):
    request1 = AssemblyRegionRequest(
        genome_uuid="2afef36f-3660-4b8c-819b-d1e5a77c9918",
        chromosomal_only=False
    )
    genome_assembly_sequences1 = stub.GetAssemblyRegion(request1)

    request2 = AssemblyRegionRequest(
        genome_uuid="2afef36f-3660-4b8c-819b-d1e5a77c9918",
        chromosomal_only=True
    )
    genome_assembly_sequences2 = stub.GetAssemblyRegion(request2)
    print("**** Chromosomal and non-chromosomal ****")
    for seq in genome_assembly_sequences1:
        print(seq)

    print("**** Chromosomal_only ****")
    for seq in genome_assembly_sequences2:
        print(seq)


def list_genome_assembly_sequences_region(stub):
    request1 = GenomeAssemblySequenceRegionRequest(
        genome_uuid="9caa2cae-d1c8-4cfc-9ffd-2e13bc3e95b1",
        sequence_region_name="HG03540#1#h1tg000001l"
    )
    genome_assembly_sequences_region1 = stub.GetGenomeAssemblySequenceRegion(request1)
    print("**** Non-chromosomal ****")
    print(genome_assembly_sequences_region1)

    request2 = GenomeAssemblySequenceRegionRequest(
        genome_uuid="2afef36f-3660-4b8c-819b-d1e5a77c9918",
        sequence_region_name="3"
    )
    genome_assembly_sequences_region2 = stub.GetGenomeAssemblySequenceRegion(request2)
    print("**** Chromosomal ****")
    print(genome_assembly_sequences_region2)


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
    request1 = GenomeUUIDRequest(genome_uuid="9caa2cae-d1c8-4cfc-9ffd-2e13bc3e95b1")
    releases1 = stub.GetSpeciesInformation(request1)
    print("**** Species information ****")
    print(releases1)


def get_assembly_information(stub):
    request1 = AssemblyIDRequest(assembly_uuid="9d2dc346-358a-4c70-8fd8-3ff194246a76")
    releases1 = stub.GetAssemblyInformation(request1)
    print("**** Assembly information ****")
    print(releases1)


def get_genomes_by_assembly_accession(stub):
    request1 = AssemblyAccessionIDRequest(assembly_accession="GCA_001624185.1")
    genomes1 = stub.GetGenomesByAssemblyAccessionID(request1)
    print("**** Genomes from assembly accession information ****")
    for genome in genomes1:
        print(genome)

    request2 = AssemblyAccessionIDRequest(assembly_accession=None)
    genomes2 = stub.GetGenomesByAssemblyAccessionID(request2)
    print("**** Genomes from null assembly accession ****")
    print(list(genomes2))


def get_sub_species_info(stub):
    request1 = OrganismIDRequest(
        organism_uuid="86dd50f1-421e-4829-aca5-13ccc9a459f6",
        group="EnsemblPlants"
    )
    releases1 = stub.GetSubSpeciesInformation(request1)
    print("**** Sub species information ****")
    print(releases1)


def get_top_level_statistics(stub):
    request1 = OrganismIDRequest(
        organism_uuid="86dd50f1-421e-4829-aca5-13ccc9a459f6",
        group="EnsemblPlants"
    )
    releases1 = stub.GetTopLevelStatistics(request1)
    print("**** Top level statistics ****")
    print(releases1)


def get_top_level_statistics_by_uuid(stub):
    genome_request = GenomeUUIDRequest(
        genome_uuid="a7335667-93e7-11ec-a39d-005056b38ce3"
    )
    toplevel_stats_by_uuid_request = stub.GetTopLevelStatisticsByUUID(genome_request)
    print("**** Top level statistics by UUID ****")
    print(toplevel_stats_by_uuid_request)


def get_datasets_list_by_uuid(stub):
    request1 = DatasetsRequest(
        genome_uuid="9caa2cae-d1c8-4cfc-9ffd-2e13bc3e95b1"
    )
    request2 = DatasetsRequest(
        genome_uuid="9caa2cae-d1c8-4cfc-9ffd-2e13bc3e95b1", release_version=108.0
    )
    print("**** Release not specified ****")
    datasets1 = stub.GetDatasetsListByUUID(request1)
    print(datasets1)
    print("**** Release specified ****")
    datasets2 = stub.GetDatasetsListByUUID(request2)
    print(datasets2)


def get_dataset_infos_by_dataset_type(stub):
    request1 = GenomeDatatypeRequest(
        genome_uuid="9caa2cae-d1c8-4cfc-9ffd-2e13bc3e95b1", dataset_type="assembly"
    )
    datasets1 = stub.GetDatasetInformation(request1)
    print(datasets1.dataset_infos)


def get_genome_uuid(stub):
    request1 = GenomeInfoRequest(
        ensembl_name="homo_sapiens_37", assembly_name="GRCh37.p13"
    )
    genome_uuid1 = stub.GetGenomeUUID(request1)
    request2 = GenomeInfoRequest(
        ensembl_name="homo_sapiens_37", assembly_name="GRCh37", use_default=True
    )
    genome_uuid2 = stub.GetGenomeUUID(request2)
    request3 = GenomeInfoRequest(
        ensembl_name="homo_sapiens_37", assembly_name="GRCh37.p13", use_default=True
    )
    genome_uuid3 = stub.GetGenomeUUID(request3)

    print("**** Using assembly_name ****")
    print(genome_uuid1)
    print("**** Using assembly_default ****")
    print(genome_uuid2)
    print("**** Using assembly_default (No results) ****")
    print(genome_uuid3)


def get_organisms_group_count(stub):
    request = OrganismsGroupRequest()
    organisms_group_count = stub.GetOrganismsGroupCount(request)
    print(organisms_group_count)


def get_genome_uuid_by_tag(stub):
    request1 = GenomeTagRequest(genome_tag="grch37")
    genome_uuid1 = stub.GetGenomeUUIDByTag(request1)
    request2 = GenomeTagRequest(genome_tag="grch38")
    genome_uuid2 = stub.GetGenomeUUIDByTag(request2)
    request3 = GenomeTagRequest(genome_tag="r64-1-1")
    genome_uuid3 = stub.GetGenomeUUIDByTag(request3)
    request4 = GenomeTagRequest(genome_tag="foo")
    genome_uuid4 = stub.GetGenomeUUIDByTag(request4)

    print("**** Genome Tag: grch37 ****")
    print(genome_uuid1)
    print("**** Genome Tag: grch38 ****")
    print(genome_uuid2)
    print("**** Genome Tag: r64-1-1 ****")
    print(genome_uuid3)
    print("**** Genome Tag: foo ****")
    print(genome_uuid4)

def get_ftp_links(stub):
    request1 = FTPLinksRequest(genome_uuid="b997075a-292d-4e15-bfe5-23dca5a57b26", dataset_type="all")
    links = stub.GetFTPLinks(request1)
    print("**** FTP Links ****")
    print(links)

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
        print("---------------Get Top Level Statistics-----------")
        get_top_level_statistics(stub)
        print("---------------Get Top Level Statistics By UUID-----------")
        get_top_level_statistics_by_uuid(stub)
        print("-------------- Get Genomes --------------")
        get_genomes(stub)
        print("-------------- List Genome Sequences --------------")
        list_genome_sequences(stub)
        print("-------------- List Genome Assembly Sequences --------------")
        list_genome_assembly_sequences(stub)
        print("-------------- List Region Info for Given Sequence Name --------------")
        list_genome_assembly_sequences_region(stub)
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
        print("-------------- Get Organisms Group Count --------------")
        get_organisms_group_count(stub)
        print("-------------- Get Genome UUID By Tag --------------")
        get_genome_uuid_by_tag(stub)
        print("-------------- Get FTP Links by Genome UUID and dataset --------------")
        get_ftp_links(stub)


if __name__ == "__main__":
    logging.basicConfig()
    run()
