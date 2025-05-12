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
from concurrent import futures

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from grpc_reflection.v1alpha import reflection

from ensembl.production.metadata.grpc import ensembl_metadata_pb2_grpc, ensembl_metadata_pb2
from ensembl.production.metadata.grpc.config import MetadataConfig
from ensembl.production.metadata.grpc.servicer import EnsemblMetadataServicer

logger = logging.getLogger(__name__)


def serve():
    cfg = MetadataConfig()
    log_level = logging.DEBUG if cfg.debug_mode else logging.WARNING

    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Creating gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    # Registering Ensembl metadata service
    ensembl_metadata_pb2_grpc.add_EnsemblMetadataServicer_to_server(
        EnsemblMetadataServicer(), server
    )

    # Adding Health Check Service
    health_servicer = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)

    # Setting service health status to "SERVING"
    health_servicer.set('', health_pb2.HealthCheckResponse.SERVING)  # Global health check
    health_servicer.set('EnsemblMetadata', health_pb2.HealthCheckResponse.SERVING)

    # Enabling reflection
    SERVICE_NAMES = (
        ensembl_metadata_pb2.DESCRIPTOR.services_by_name['EnsemblMetadata'].full_name,
        health_pb2.DESCRIPTOR.services_by_name['Health'].full_name,
        reflection.SERVICE_NAME
    )
    reflection.enable_server_reflection(SERVICE_NAMES, server)

    # Starting the server
    server.add_insecure_port(f"[::]:{cfg.service_port}")
    server.start()

    try:
        logger.info(f"Starting GRPC Server on {cfg.service_port} DEBUG: {cfg.debug_mode}")
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt caught, stopping the server...")
        server.stop(grace=0)  # Immediately stop the server
        logger.info("gRPC server has shut down gracefully")


if __name__ == "__main__":
    logger.info(f"gRPC server starting...")
    serve()
