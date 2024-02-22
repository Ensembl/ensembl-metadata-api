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
from concurrent import futures
import grpc
import logging

from grpc_reflection.v1alpha import reflection

from ensembl.production.metadata.grpc.config import MetadataConfig
from ensembl.production.metadata.grpc import ensembl_metadata_pb2_grpc, ensembl_metadata_pb2
from ensembl.production.metadata.grpc.servicer import EnsemblMetadataServicer

logger = logging.getLogger(__name__)

def serve():
    cfg = MetadataConfig()
    log_level = logging.DEBUG if cfg.debug_mode else logging.WARNING

    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    ensembl_metadata_pb2_grpc.add_EnsemblMetadataServicer_to_server(
        EnsemblMetadataServicer(), server
    )
    SERVICE_NAMES = (
        ensembl_metadata_pb2.DESCRIPTOR.services_by_name['EnsemblMetadata'].full_name,
        reflection.SERVICE_NAME
    )
    reflection.enable_server_reflection(SERVICE_NAMES, server)
    server.add_insecure_port("[::]:50051")
    server.start()
    try:
        logger.info(f"Starting GRPC Server from {cfg.metadata_uri}")
        server.wait_for_termination()
        yield server
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt caught, stopping the server...")
        server.stop(grace=0)  # Immediately stop the server
        logger.info("gRPC server has shut down gracefully")


if __name__ == "__main__":
    logger.info("gRPC server starting on port 50051...")
    logger.info(f"DEBUG: {cfg.debug_mode}")
    serve()
