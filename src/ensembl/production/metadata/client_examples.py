import grpc
import logging

from ensembl_metadata_pb2 import \
    GenomeUUIDRequest, GenomeNameRequest, \
    ReleaseRequest, GenomeSequenceRequest
import ensembl_metadata_pb2_grpc


def get_genome(stub, genome_request):
    if isinstance(genome_request, GenomeUUIDRequest):
        genome = stub.GetGenomeByUUID(genome_request)
    elif isinstance(genome_request, GenomeNameRequest):
        genome = stub.GetGenomeByName(genome_request)
    else:
        print("Unrecognised request message")
        return

    if genome.genome_uuid == '':
        print("No genome")
        return

    print(genome)


def get_genomes(stub):
    request1 = GenomeUUIDRequest(genome_uuid='dc8cbfd9-9bd4-11eb-b85b-0028f81f0374')
    request2 = GenomeUUIDRequest(genome_uuid='rhubarb')
    request3 = GenomeNameRequest(ensembl_name='accipiter_gentilis', site_name='rapid')
    request4 = GenomeNameRequest(ensembl_name='accipiter_gentilis', site_name='rapid', release_version=13)
    request5 = GenomeNameRequest(ensembl_name='banana', site_name='plants', release_version=104)
    print('**** Valid UUID ****')
    get_genome(stub, request1)
    print('**** Invalid UUID ****')
    get_genome(stub, request2)
    print('**** Name, no release ****')
    get_genome(stub, request3)
    print('**** Name, past release ****')
    get_genome(stub, request4)
    print('**** Invalid name ****')
    get_genome(stub, request5)


def list_genome_sequences(stub):
    request1 = GenomeSequenceRequest(genome_uuid='dc8cf6ef-9bd4-11eb-b85b-0028f81f0374', chromosomal_only=True)
    genome_sequences1 = stub.GetGenomeSequence(request1)
    print('**** Only chromosomes ****')
    for seq in genome_sequences1:
        print(seq)

    request2 = GenomeSequenceRequest(genome_uuid='dc8cf6ef-9bd4-11eb-b85b-0028f81f0374')
    genome_sequences2 = stub.GetGenomeSequence(request2)
    print('**** All sequences ****')
    for seq in genome_sequences2:
        print(seq)

    request3 = GenomeSequenceRequest(genome_uuid='garbage')
    genome_sequences3 = stub.GetGenomeSequence(request3)
    print('**** Invalid UUID ****')
    for seq in genome_sequences3:
        print(seq)


def list_releases(stub):
    request1 = ReleaseRequest()
    releases1 = stub.GetRelease(request1)
    print('**** All releases ****')
    for release in releases1:
        print(release)

    request2 = ReleaseRequest(site_name=['rapid'])
    releases2 = stub.GetRelease(request2)
    print('**** All Rapid releases ****')
    for release in releases2:
        print(release)

    request3 = ReleaseRequest(site_name=['rapid'], current_only=1)
    releases3 = stub.GetRelease(request3)
    print('**** Current Rapid release ****')
    for release in releases3:
        print(release)

    request4 = ReleaseRequest(release_version=[14])
    releases4 = stub.GetRelease(request4)
    print('**** Version 14 ****')
    for release in releases4:
        print(release)

    request5 = ReleaseRequest(release_version=[79])
    releases5 = stub.GetRelease(request5)
    print('**** Version 79 ****')
    for release in releases5:
        print(release)

    request6 = ReleaseRequest(release_version=[14, 15])
    releases6 = stub.GetRelease(request6)
    print('**** Versions 14 and 15 ****')
    for release in releases6:
        print(release)


def list_releases_by_uuid(stub):
    request1 = GenomeUUIDRequest(genome_uuid='dc8cbfd9-9bd4-11eb-b85b-0028f81f0374')
    releases1 = stub.GetReleaseByUUID(request1)
    print('**** Release for Narwhal ****')
    for release in releases1:
        print(release)


def run():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = ensembl_metadata_pb2_grpc.EnsemblMetadataStub(channel)
        print("-------------- Get Genomes --------------")
        get_genomes(stub)
        print("-------------- List Sequences --------------")
        list_genome_sequences(stub)
        print("-------------- List Releases --------------")
        list_releases(stub)
        print("-------------- List Releases for Genome --------------")
        list_releases_by_uuid(stub)


if __name__ == '__main__':
    logging.basicConfig()
    run()
