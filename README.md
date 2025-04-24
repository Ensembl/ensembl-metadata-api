# Ensembl Metadata API / gRPC API

[![Build Status](https://travis-ci.com/Ensembl/ensembl-metadata-api.svg?branch=main)](https://travis-ci.com/Ensembl/ensembl-metadata-api)

## Overview

The Ensembl Metadata API provides a comprehensive solution for interacting with the Ensembl Metadata database. This repository is modular, containing three main components: the API (ORM), gRPC service, and the updater. Additionally, it includes a collection of scripts for auxiliary tasks.

## Features

- **SQLAlchemy ORM**: Simplifies database operations and schema management.
- **gRPC Service**: Enables efficient, language-neutral communication with the database.
- **Updater**: Keeps the metadata up-to-date with tools for synchronization and migration.
- **Scripts**: Utility scripts to support the main functionalities.

---

## Repository Structure

### [API (ORM)](https://github.com/Ensembl/ensembl-metadata-api/tree/main/src/ensembl/production/metadata/api)
This module provides an SQLAlchemy ORM layer for interacting with the Ensembl Metadata database. It abstracts database access, making it easier to query and manipulate metadata.

### [gRPC](https://github.com/Ensembl/ensembl-metadata-api/tree/main/src/ensembl/production/metadata/grpc)
The gRPC module enables remote procedure calls to interact with the metadata database. It includes:
- Protobuf definitions for service and message contracts.
- Server implementation for serving metadata queries.
- Client examples to demonstrate how to interact with the gRPC service.

### [Updater](https://github.com/Ensembl/ensembl-metadata-api/tree/main/src/ensembl/production/metadata/updater)
This module is responsible for updating the metadata database. It includes tools and scripts to synchronize or migrate metadata, ensuring the database remains current.

### [Scripts](https://github.com/Ensembl/ensembl-metadata-api/tree/main/src/ensembl/production/metadata/scripts)
A collection of utility scripts that complement the main functionalities of the repository. These scripts can assist in tasks such as testing, data exploration, and more.

---

## System Requirements

- Python 3.10+
- MySQL Client

---

## Installation

### Clone the Repository

```bash
git clone -b main https://github.com/Ensembl/ensembl-metadata-api.git
cd ensembl-metadata-api
```

### Setup Virtual Environment

```bash
python -m venv venv
source venv/bin/activate
```

### Install Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
pip install -e .
```

---

## Environment Variables

Set the following environment variables:

```bash
export METADATA_URI=mysql+pymysql://<username>:<password>@<host>:<port>/<database_name>
export TAXONOMY_URI=mysql+pymysql://<username>:<password>@<host>:<port>/<database_name>
export DEBUG=True
```

Replace `<username>`, `<password>`, `<host>`, `<port>`, and `<database_name>` with the appropriate values for your database configurations.

---

## gRPC Setup

### Generate/Update Protobuf Files

If you need to regenerate the protobuf files:

```bash
python -m grpc_tools.protoc -Iprotos --python_out=src --grpc_python_out=src protos/ensembl/production/metadata/grpc/ensembl_metadata.proto
```

### Run the Server

```bash
PYTHONPATH='src' python3 src/ensembl/production/metadata/grpc/service.py
```

### Test gRPC Using grpcui

`grpcui` is a web-based gRPC user interface that makes it easy to test gRPC endpoints interactively. For more details, visit the official [grpcui repository](https://github.com/fullstorydev/grpcui).

#### Installing grpcui

- **MacOS**: Use Homebrew to install:
  ```bash
  brew install grpcui
  ```

- **Other Platforms**: Install using Go:
  ```bash
  go install github.com/fullstorydev/grpcui/cmd/grpcui@latest
  ```

#### Running grpcui

1. Start the `grpcui` interface by pointing it to your gRPC server (replace `<server_address>` with the actual address, e.g., `localhost:50051`):
   ```bash
   grpcui -plaintext <server_address>
   ```

2. Open the web browser to interact with the gRPC endpoints and test your service.

---

## Run the Tests

```bash
PYTHONPATH='src' pytest src/tests/ --server 'mysql://root:toor1234@localhost:3306/?local_infile=1'
```

---

## Development

### Linting and Type Checking

```bash
pylint src
mypy src
```

### Automatic Formatting

```bash
black src
```

---

### How to Add a New gRPC Method

This section outlines the step-by-step process for adding a new method to the gRPC API. Below is an example of how to implement a method called `GetGenomeUUIDByTag`:

#### 1. Define the Method in `ensembl_metadata.proto`
Start by defining the new method, request message, and response message in the `ensembl_metadata.proto` file under the **EnsemblMetadata** service.

**Example:**

```proto
service EnsemblMetadata {
    rpc GetGenomeUUIDByTag(GenomeTagRequest) returns (GenomeUUID) {}
}

message GenomeTagRequest {
    string genome_tag = 1; // Mandatory
}

message GenomeUUID {
    string genome_uuid = 1;
}
```

Regenerate the protobuf files after making changes (see step 7).

#### 2. Implement the Method in `servicer.py`
Define the implementation of the method in the `EnsemblMetadataServicer` class.

**Example:**

```python
import logging
from ensembl.production.metadata.grpc import ensembl_metadata_pb2_grpc
import ensembl.production.metadata.grpc.utils as utils

logger = logging.getLogger(__name__)

class EnsemblMetadataServicer(ensembl_metadata_pb2_grpc.EnsemblMetadataServicer):
    ...
    def GetGenomeUUIDByTag(self, request, context):
        logger.debug(f"Received RPC for GetGenomeUUIDByTag with request: {request}")
        return utils.get_genome_uuid_by_tag(self.genome_adaptor, request.genome_tag)
```

#### 3. Implement the Supporting Logic in `utils.py`
Create or update a utility function to handle the business logic and/or database interaction. This function prepares the payload for the response.

**Example:**

```python
import logging
import ensembl.production.metadata.grpc.protobuf_msg_factory as msg_factory

logger = logging.getLogger(__name__)

def get_genome_uuid_by_tag(db_conn, genome_tag):
    if not genome_tag:
        logger.warning("Missing or Empty Genome tag field.")
        return msg_factory.create_genome_uuid()

    genome_uuid_result = db_conn.fetch_genomes(genome_tag=genome_tag)
    if len(genome_uuid_result) == 0:
        logger.error(f"No Genome UUID found for tag: {genome_tag}")
    else:
        if len(genome_uuid_result) > 1:
            logger.warning(f"Multiple results returned: {genome_uuid_result}")
        response_data = msg_factory.create_genome_uuid(
            {"genome_uuid": genome_uuid_result[0].Genome.genome_uuid}
        )
        return response_data
    return msg_factory.create_genome_uuid()
```

#### 4. Add the Response Factory in `protobuf_msg_factory.py`
Define the response creation logic in the `protobuf_msg_factory.py` file. This ensures consistency in response structure.

**Example:**

```python
from ensembl.production.metadata.grpc import ensembl_metadata_pb2

def create_genome_uuid(data=None):
    if data is None:
        return ensembl_metadata_pb2.GenomeUUID()

    genome_uuid = ensembl_metadata_pb2.GenomeUUID(
        genome_uuid=data["genome_uuid"]
    )
    return genome_uuid
```

#### 5. Update ORM Logic (if applicable)
If the new method requires changes to the ORM, such as adding a new query or modifying an existing one, implement those changes at this stage. For the above example, you used (but didn't have to modify) the `fetch_genomes()` function in the ORM layer.

#### 6. Add a Client Function
Add a client function to interact with the newly created gRPC method. This function can be included in `client_examples.py` script.

**Example:**

```python
from ensembl.production.metadata.grpc.ensembl_metadata_pb2 import GenomeTagRequest

def get_genome_uuid_by_tag(stub):
    request1 = GenomeTagRequest(genome_tag="grch37")
    genome_uuid1 = stub.GetGenomeUUIDByTag(request1)
    print("**** Genome Tag: grch37 ****")
    print(genome_uuid1)
```

#### 7. Regenerate Protobuf Files
After updating the `.proto` file, regenerate the Python protobuf files.

**Command:**

```bash
python -m grpc_tools.protoc -Iprotos --python_out=src --grpc_python_out=src protos/ensembl/production/metadata/grpc/ensembl_metadata.proto
```

#### 8. Write Tests
Add tests to ensure the new method behaves as expected. Depending on the changes, you may need to update one or both of the following:

- `src/tests/test_grpc_*.py` (for gRPC server/client tests)
- `src/tests/test_protobuf_msg_factory.py` (for message factory tests)

#### 9. Final Validation
- Run the tests to ensure everything is working as expected:
  ```bash
  PYTHONPATH='src' pytest src/tests/
  ```
- Verify that the gRPC service starts and the new method is accessible.

By following these steps, you can methodically add and test a new gRPC method within the Ensembl Metadata API.

---

## Docker Support

### Build Docker Image

```bash
docker build -t ensembl-metadata-service .
```

### Run Docker Container

```bash
docker run -t -i -e METADATA_URI=<URI> -e TAXONOMY_URI=<URI> -p 80:80 ensembl-metadata-api
```

---

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request for any improvements or bug fixes.

---

## License

[MIT License](LICENSE)