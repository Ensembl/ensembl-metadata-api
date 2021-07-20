# Ensembl Metadata API

[![Build Status](https://travis-ci.com/Ensembl/ensembl-metadata-api.svg?branch=main)](https://travis-ci.com/Ensembl/ensembl-metadata-api)

SQLAlchemy ORM for the Ensembl Metadata database.

## System Requirements

- Python 3.8+
- MySQL Client

## Usage

Clone this template:
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

[ensembl-metadata-registry](https://github.com/Ensembl/ensembl-metadata-registry): GUI for the Ensembl Metadata database

[ensembl-metadata-service](https://github.com/Ensembl/ensembl-metadata-service): gRPC layer for the Ensembl Metadata database


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

### Testing

Run test suite:
```
cd ensembl-metadata-api
pytest
```

To run tests, calculate and display testing coverage stats:
```
cd ensembl-metadata-api
coverage run -m pytest
coverage report -m
```

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
pylint src tests
mypy src tests
```
Pylint will check the code for syntax, name errors and formatting style.
Mypy will use type hints to statically type check the code.
