#!/bin/bash

sphinx-apidoc -o docs src/ensembl
cd docs
make html
