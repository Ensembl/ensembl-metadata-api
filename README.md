# Ensembl Metadata API / GRPC API 
# SQLAlchemy ORM for the Ensembl Metadata database.
# GRPC Service protofile to interact with metadata database through GRPC

[![Build Status](https://travis-ci.com/Ensembl/ensembl-metadata-api.svg?branch=main)](https://travis-ci.com/Ensembl/ensembl-metadata-api)

## System Requirements

- Python 3.8+
- MySQL Client

## Usage

Clone the repository:
```
git clone -b main https://github.com/Ensembl/ensembl-metadata-api.git
```

Install the environment (with pyenv)

```
cd ensembl-metadata-api
pyenv virtualenv 3.8 ensembl_metadata_api
pyenv local ensembl_metadata_api
pip install --upgrade pip
pip install -r requirements.txt
```

## Related repositories

[ensembl-metadata](https://github.com/Ensembl/ensembl-metadata): Legacy Ensembl Metadata database and Perl API

[ensembl-metadata-admin](https://github.com/Ensembl/ensembl-metadata-admin): Django ORM for the Ensembl Metadata database


## Development

Install the development environment (with pyenv)

```
cd ensembl-metadata-api
pyenv virtualenv 3.8 ensembl_metadata_api
pyenv local ensembl_metadata_api
pip install --upgrade pip
pip install -r requirements-dev.txt
```

Install the development environment (with mkvirtualenv)

```
cd ensembl-metadata-api
mkvirtualenv ensembl_metadata_api -p python3.8
workon ensembl_metadata_api
pip install --upgrade pip
pip install -r requirements-dev.txt
```
To generate client and server files
(Remember to run these after adding a new method in ensembl_metadata.proto)
```
python3 -m grpc_tools.protoc -Iprotos --python_out=src --grpc_python_out=src protos/ensembl/production/metadata/grpc/ensembl_metadata.proto --pyi_out ./src/
```

Start the server script

```
PYTHONPATH='src' python3 src/ensembl/production/metadata/grpc/service.py
```

Start the client script
```
PYTHONPATH='src' python3 src/ensembl/production/metadata/grpc/client_examples.py
```

### Testing

Run test suite:
```
cd ensembl-metadata-api
coverage run -m pytest --server mysql://ensembl@localhost:3306/?local_infile=1
```

### Automatic Formatting
```
cd ensembl-metadata-api
black --check src
```
Use `--diff` to print a diff of what Black would change, without actually changing the files.

To actually reformat all files contained in `src`:
```
cd ensembl-metadata-api
black src
PYTHONPATH='src' pytest
```

To run tests, calculate and display testing coverage stats:
```
cd ensembl-metadata-api
coverage run -m pytest
coverage report -m
```

#### Explore test DB content

As for now, some of the test DB sqlite content is different from what's in MySQL metadata DB (e.g. release `version` in `ensembl_release`)

> `test.db` created when running tests is deleted once tests are executed.

To take a look at the test data you can create a temporary `sampledb.db` importing `tables.sql` content using the command:

```
cat tables.sql | sqlite3 sampledb.db
```

You can then open `sampledb.db` using [DB Browser for SQLite](https://sqlitebrowser.org/dl/).

### Automatic Formatting
```
cd ensembl-metadata-api
black --check src tests
```
Use `--diff` to print a diff of what Black would change, without actually changing the files.

To actually reformat all files contained in `src` and `test`:
```
cd ensembl-metadata-api
black src tests
```

### Linting and type checking
```
cd ensembl-metadata-api
pylint src
mypy src
```
Pylint will check the code for syntax, name errors and formatting style.
Mypy will use type hints to statically type check the code.

cd ensembl-metadata-service
pylint src tests
mypy src tests
```
Pylint will check the code for syntax, name errors and formatting style.
Mypy will use type hints to statically type check the code.

### To build docker image
```
docker build -t ensembl-metadata-api .
```

### To run docker container
```
docker run -t -i -e METADATA_URI=<URI> -e TAXONOMY_URI=<URI> -p 80:80 ensembl-metadata-api
```
