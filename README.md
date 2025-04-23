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

### Run the Tests

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