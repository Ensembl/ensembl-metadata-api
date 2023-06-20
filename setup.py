import os
from pathlib import Path

from setuptools import find_namespace_packages, setup

with open('README.md') as f:
    readme = f.read()

with open('VERSION') as f:
    version = f.read()


def import_requirements():
    """Import ``requirements.txt`` file located at the root of the repository."""
    with open(Path(__file__).parent / 'requirements.txt') as file:
        return [line.rstrip() for line in file.readlines()]


setup(
    name='ensembl-metadata-service',
    version=os.getenv('CI_COMMIT_TAG', version),
    description='Ensembl Metadata Service',
    long_description=readme,
    author='Alisha Aneja,Daniel Poppleton,Marc Chakiachvili,Sanjay boddu,Bilal El Houdaigui',
    author_email='aaneja@ebi.ac.uk,danielp@ebi.ac.uk,mchakiachvili@ebi.ac.uk,sboddu@ebi.ac.uk,bilal@ebi.ac.uk',
    url='https://github.com/Ensembl/ensembl-metadata-service',
    download_url='https://github.com/Ensembl/ensembl-metadata-service',
    license='Apache License 2.0',
    packages=find_namespace_packages(where='src'),
    package_dir={'': 'src'},
    include_package_data=True,
    python_requires='>=3.8',
    install_requires=[
        'grpcio',
        'protobuf==3.20'
    ],
    extras_require={
        'service': import_requirements(),
    },
    classifiers=[
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        "Programming Language :: Python :: 3.8",
    ]
)
