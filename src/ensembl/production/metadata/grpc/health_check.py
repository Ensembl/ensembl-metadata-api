import grpc
from grpc_health.v1 import health_pb2, health_pb2_grpc

"""
Python script to test the health check

Usage:
    python src/ensembl/production/metadata/grpc/health_check.py 

Expected responses:
    HealthCheckResponse.SERVING (1) -> Service is running normally.
    HealthCheckResponse.NOT_SERVING (2) -> Service is unhealthy.
    Error message → The server is not reachable or the service name is incorrect.
"""

GRPC_SERVER_ADDRESS = "localhost:50051"

def check_health(service_name=""):
    channel = grpc.insecure_channel(GRPC_SERVER_ADDRESS)
    health_stub = health_pb2_grpc.HealthStub(channel)

    request = health_pb2.HealthCheckRequest(service=service_name)
    try:
        response = health_stub.Check(request)
        print(f"Health status of '{service_name or 'Global'}': {response.status}")
    except grpc.RpcError as e:
        print(f"Error checking health: {e.details()} (Code: {e.code()})")

if __name__ == "__main__":
    check_health("")  # Global health check
    check_health("EnsemblMetadata")  # Specific service health check
