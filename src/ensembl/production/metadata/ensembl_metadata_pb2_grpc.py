# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

import ensembl_metadata_pb2 as ensembl__metadata__pb2


class EnsemblMetadataStub(object):
    """Metadata for the genomes in Ensembl.
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.GetGenomeByUUID = channel.unary_unary(
                '/ensembl_metadata.EnsemblMetadata/GetGenomeByUUID',
                request_serializer=ensembl__metadata__pb2.GenomeUUIDRequest.SerializeToString,
                response_deserializer=ensembl__metadata__pb2.Genome.FromString,
                )
        self.GetSpeciesInformation = channel.unary_unary(
                '/ensembl_metadata.EnsemblMetadata/GetSpeciesInformation',
                request_serializer=ensembl__metadata__pb2.GenomeUUIDRequest.SerializeToString,
                response_deserializer=ensembl__metadata__pb2.Species.FromString,
                )
        self.GetAssemblyInformation = channel.unary_unary(
                '/ensembl_metadata.EnsemblMetadata/GetAssemblyInformation',
                request_serializer=ensembl__metadata__pb2.AssemblyIDRequest.SerializeToString,
                response_deserializer=ensembl__metadata__pb2.AssemblyInfo.FromString,
                )
        self.GetSubSpeciesInformation = channel.unary_unary(
                '/ensembl_metadata.EnsemblMetadata/GetSubSpeciesInformation',
                request_serializer=ensembl__metadata__pb2.OrganismIDRequest.SerializeToString,
                response_deserializer=ensembl__metadata__pb2.SubSpecies.FromString,
                )
        self.GetGroupingInformation = channel.unary_unary(
                '/ensembl_metadata.EnsemblMetadata/GetGroupingInformation',
                request_serializer=ensembl__metadata__pb2.OrganismIDRequest.SerializeToString,
                response_deserializer=ensembl__metadata__pb2.Grouping.FromString,
                )
        self.GetGenomeByName = channel.unary_unary(
                '/ensembl_metadata.EnsemblMetadata/GetGenomeByName',
                request_serializer=ensembl__metadata__pb2.GenomeNameRequest.SerializeToString,
                response_deserializer=ensembl__metadata__pb2.Genome.FromString,
                )
        self.GetRelease = channel.unary_stream(
                '/ensembl_metadata.EnsemblMetadata/GetRelease',
                request_serializer=ensembl__metadata__pb2.ReleaseRequest.SerializeToString,
                response_deserializer=ensembl__metadata__pb2.Release.FromString,
                )
        self.GetReleaseByUUID = channel.unary_stream(
                '/ensembl_metadata.EnsemblMetadata/GetReleaseByUUID',
                request_serializer=ensembl__metadata__pb2.GenomeUUIDRequest.SerializeToString,
                response_deserializer=ensembl__metadata__pb2.Release.FromString,
                )
        self.GetGenomeSequence = channel.unary_stream(
                '/ensembl_metadata.EnsemblMetadata/GetGenomeSequence',
                request_serializer=ensembl__metadata__pb2.GenomeSequenceRequest.SerializeToString,
                response_deserializer=ensembl__metadata__pb2.GenomeSequence.FromString,
                )


class EnsemblMetadataServicer(object):
    """Metadata for the genomes in Ensembl.
    """

    def GetGenomeByUUID(self, request, context):
        """Retrieve genome by its UUID.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetSpeciesInformation(self, request, context):
        """Get species information for a genome UUID
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetAssemblyInformation(self, request, context):
        """Get assembly information
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetSubSpeciesInformation(self, request, context):
        """Get subspecies information
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetGroupingInformation(self, request, context):
        """Get grouping information
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetGenomeByName(self, request, context):
        """Retrieve genome by Ensembl name and site, and optionally release.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetRelease(self, request, context):
        """Retrieve release details.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetReleaseByUUID(self, request, context):
        """Retrieve release details for a genome.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetGenomeSequence(self, request, context):
        """Retrieve sequence metadata for a genome's assembly.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_EnsemblMetadataServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'GetGenomeByUUID': grpc.unary_unary_rpc_method_handler(
                    servicer.GetGenomeByUUID,
                    request_deserializer=ensembl__metadata__pb2.GenomeUUIDRequest.FromString,
                    response_serializer=ensembl__metadata__pb2.Genome.SerializeToString,
            ),
            'GetSpeciesInformation': grpc.unary_unary_rpc_method_handler(
                    servicer.GetSpeciesInformation,
                    request_deserializer=ensembl__metadata__pb2.GenomeUUIDRequest.FromString,
                    response_serializer=ensembl__metadata__pb2.Species.SerializeToString,
            ),
            'GetAssemblyInformation': grpc.unary_unary_rpc_method_handler(
                    servicer.GetAssemblyInformation,
                    request_deserializer=ensembl__metadata__pb2.AssemblyIDRequest.FromString,
                    response_serializer=ensembl__metadata__pb2.AssemblyInfo.SerializeToString,
            ),
            'GetSubSpeciesInformation': grpc.unary_unary_rpc_method_handler(
                    servicer.GetSubSpeciesInformation,
                    request_deserializer=ensembl__metadata__pb2.OrganismIDRequest.FromString,
                    response_serializer=ensembl__metadata__pb2.SubSpecies.SerializeToString,
            ),
            'GetGroupingInformation': grpc.unary_unary_rpc_method_handler(
                    servicer.GetGroupingInformation,
                    request_deserializer=ensembl__metadata__pb2.OrganismIDRequest.FromString,
                    response_serializer=ensembl__metadata__pb2.Grouping.SerializeToString,
            ),
            'GetGenomeByName': grpc.unary_unary_rpc_method_handler(
                    servicer.GetGenomeByName,
                    request_deserializer=ensembl__metadata__pb2.GenomeNameRequest.FromString,
                    response_serializer=ensembl__metadata__pb2.Genome.SerializeToString,
            ),
            'GetRelease': grpc.unary_stream_rpc_method_handler(
                    servicer.GetRelease,
                    request_deserializer=ensembl__metadata__pb2.ReleaseRequest.FromString,
                    response_serializer=ensembl__metadata__pb2.Release.SerializeToString,
            ),
            'GetReleaseByUUID': grpc.unary_stream_rpc_method_handler(
                    servicer.GetReleaseByUUID,
                    request_deserializer=ensembl__metadata__pb2.GenomeUUIDRequest.FromString,
                    response_serializer=ensembl__metadata__pb2.Release.SerializeToString,
            ),
            'GetGenomeSequence': grpc.unary_stream_rpc_method_handler(
                    servicer.GetGenomeSequence,
                    request_deserializer=ensembl__metadata__pb2.GenomeSequenceRequest.FromString,
                    response_serializer=ensembl__metadata__pb2.GenomeSequence.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'ensembl_metadata.EnsemblMetadata', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class EnsemblMetadata(object):
    """Metadata for the genomes in Ensembl.
    """

    @staticmethod
    def GetGenomeByUUID(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/ensembl_metadata.EnsemblMetadata/GetGenomeByUUID',
            ensembl__metadata__pb2.GenomeUUIDRequest.SerializeToString,
            ensembl__metadata__pb2.Genome.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetSpeciesInformation(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/ensembl_metadata.EnsemblMetadata/GetSpeciesInformation',
            ensembl__metadata__pb2.GenomeUUIDRequest.SerializeToString,
            ensembl__metadata__pb2.Species.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetAssemblyInformation(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/ensembl_metadata.EnsemblMetadata/GetAssemblyInformation',
            ensembl__metadata__pb2.AssemblyIDRequest.SerializeToString,
            ensembl__metadata__pb2.AssemblyInfo.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetSubSpeciesInformation(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/ensembl_metadata.EnsemblMetadata/GetSubSpeciesInformation',
            ensembl__metadata__pb2.OrganismIDRequest.SerializeToString,
            ensembl__metadata__pb2.SubSpecies.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetGroupingInformation(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/ensembl_metadata.EnsemblMetadata/GetGroupingInformation',
            ensembl__metadata__pb2.OrganismIDRequest.SerializeToString,
            ensembl__metadata__pb2.Grouping.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetGenomeByName(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/ensembl_metadata.EnsemblMetadata/GetGenomeByName',
            ensembl__metadata__pb2.GenomeNameRequest.SerializeToString,
            ensembl__metadata__pb2.Genome.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetRelease(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_stream(request, target, '/ensembl_metadata.EnsemblMetadata/GetRelease',
            ensembl__metadata__pb2.ReleaseRequest.SerializeToString,
            ensembl__metadata__pb2.Release.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetReleaseByUUID(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_stream(request, target, '/ensembl_metadata.EnsemblMetadata/GetReleaseByUUID',
            ensembl__metadata__pb2.GenomeUUIDRequest.SerializeToString,
            ensembl__metadata__pb2.Release.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetGenomeSequence(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_stream(request, target, '/ensembl_metadata.EnsemblMetadata/GetGenomeSequence',
            ensembl__metadata__pb2.GenomeSequenceRequest.SerializeToString,
            ensembl__metadata__pb2.GenomeSequence.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
