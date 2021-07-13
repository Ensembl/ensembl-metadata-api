# Ensembl Python Template

[![Documentation Status](https://readthedocs.org/projects/template-python/badge/?version=latest)](http://template-python.readthedocs.io/en/latest/?badge=latest) [![Build Status](https://travis-ci.com/Ensembl/templat-python.svg?branch=master)](https://travis-ci.com/Ensembl/template-python)

Example Ensembl Python Project.

This repo structure can be forked and used as the base template for developing a
Python package (app or library).
It contains boilerplate code and configuration for unit testing,
linting, type checking, automatic documentation and CI/CD.


## Requirements

- pyenv (with pyenv-virtualenv plugin)
- Python 3.8+
- Python packages
  - Test
    - pytest
    - pylint
    - mypy
  - Docs
    - Sphinx
    - mock
    - sphinx-rtd-theme
  - Dev
    - ipython
    - black


## Getting Started

This template is a simple but fully functioning Python project.

#### Template Project Layout

```
.
├── .gitignore               <- File patterns to be ignored by Git
├── .travis.yml              <- Travis CI instructions
├── LICENSE                  <- License text
├── MANIFEST.in              <- Files to be included in source distribution
├── NOTICE                   <- Notice file (required by License)
├── README.md                <- This file
├── VERSION                  <- Version file (try using semantic versioning)
├── docs                     <- Folder with boilerplate for auto-generated Sphinx docs
│   ├── Makefile
│   ├── _static
│   │   └── style.css
│   ├── _templates
│   │   └── layout.html
│   ├── conf.py              <- Sphinx configuration
│   ├── index.rst
│   ├── install.rst
│   └── license.rst
├── pylintrc                 <- Pylint (linter) configuration
├── pyproject.toml           <- Various other Python tools packaging configuration
├── requirements-dev.txt     <- Third-party packages required for development environment
├── requirements-doc.txt     <- Third-party packages required for building Sphinx docs
├── requirements-test.txt    <- Third-party packages required for running tests
├── requirements.txt         <- Third-party packages required for this project
├── scripts                  <- Utility scripts
│   └── build_docs.sh        <- Script to generate modules files and build docs
├── setup.py                 <- Module executed by pip when installing this project
├── src                      <- Root folder for this project source code
│   ├── ensembl
│   │   ├── __init__.py
│   │   └── hello_world.py
└── tests                    <- Root folder for tests code
    └── test_hello.py
```

#### Creating a new project

Clone this template:
```
git clone --depth 1 -b master https://github.com/Ensembl/template-python.git
```

Remove Git repository data and rename the main folder:
```
rm -rf template-python/.git
mv template-python <NEW_PROJECT_NAME>
cd <NEW_PROJECT_NAME>
git init
```

The following files (or folder names) will need to be modified:
- `NOTICE`: Substitute "Ensembl Python Template" with your project's name
- `setup.py`: Change package name and other settings
- `docs/conf.py`: Change accordigly to project's name
- `docs/install.rst`: Change installation instructions accordingly
- `README.md`: Write a meaningful README for your project

Once done with the basic customisation, create the initial commit:
```
cd <NEW_PROJECT_NAME>
git add .
git commit -m 'Initial commit'
```

#### Installing the development environment (with Pyenv)

```
pyenv virtualenv 3.8 <VIRTUAL-ENVIRONMENT-NAME>
cd <NEW_PROJECT_NAME>
pyenv local <VIRTUAL-ENVIRONMENT-NAME>
pip install -r requirements-dev.txt
pip install -e .
```

#### Testing

Run test suite:
```
cd <NEW_PROJECT_NAME>
pytest
```

To run tests, calculate and display testing coverage stats:
```
cd <NEW_PROJECT_NAME>
coverage run -m pytest
coverage report -m
```


#### Generate documentation
```
cd <NEW_PROJECT_NAME>
./scripts/build_docs.sh
```
Open automatically generated documentation page at `docs/_build/html/index.html`


#### Automatic Formatting
```
cd <NEW_PROJECT_NAME>
black --check src tests
```
Use `--diff` to print a diff of what Black would change, without actually changing the files.

To actually reformat all files contained in `src` and `test`:
```
cd <NEW_PROJECT_NAME>
black src tests
```

#### Linting and type checking
```
cd <NEW_PROJECT_NAME>
pylint src tests
mypy src tests
```
Pylint will check the code for syntax, name errors and formatting style.
Mypy will use type hints to statically type check the code.

It should be relatively easy (and definitely useful) to integrate both `pylint` and `mypy`
in your IDE/Text editor.


## Resources

#### Python Documentation
- [Official Python Docs](https://docs.python.org/3/)

#### Python distributions and virtual environments management
- [Pyenv docs](https://github.com/pyenv/pyenv#readme)
- [Pyenv virtualenv docs](https://github.com/pyenv/pyenv-virtualenv#readme)

#### Auto-generating documentation
- [Spinx Docs](https://www.sphinx-doc.org/en/master/index.html)

#### Linting, type checking and formatting
- [Pylint](https://www.pylint.org/)
- [Mypy](https://mypy.readthedocs.io/en/stable/)
- [Black](https://black.readthedocs.io/en/stable/)
- [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings)

#### Testing
- [pytest](https://docs.pytest.org/en/6.2.x/)
- [Coverage](https://coverage.readthedocs.io/)

#### Development tools
- [IPython](https://ipython.org/)

#### Distributing
- [Packaging Python](https://packaging.python.org/tutorials/packaging-projects/)
- [Apache 2.0 License](https://www.apache.org/licenses/LICENSE-2.0#apply)
- [Semantic Versioning](https://semver.org/)

