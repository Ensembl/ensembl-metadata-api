# See the NOTICE file distributed with this work for additional information
#   regarding copyright ownership.
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#       http://www.apache.org/licenses/LICENSE-2.0
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
import os
from pathlib import Path

from setuptools import find_namespace_packages, setup

with open(Path(__file__).parent / 'README.md') as f:
    readme = f.read()
with open(Path(__file__).parent / 'VERSION') as f:
    version = f.read()


def import_requirements():
    """Import ``requirements.txt`` file located at the root of the repository."""
    with open(Path(__file__).parent / 'requirements.txt') as file:
        return [line.rstrip() for line in file.readlines()]


setup(
    name='ensembl_metadata_api',
    version=os.getenv('CI_COMMIT_TAG', version),
    description='Ensembl Metadata API',
    long_description=readme,
    author='Daniel Poppleton,Marc Chakiachvili,Vinay Kaikala',
    author_email='danielp@ebi.ac.uk,mchakiachvili@ebi.ac.uk,vinay@ebi.ac.uk',
    url='https://www.ensembl.org',
    download_url='https://github.com/Ensembl/ensembl-metadata-api',
    license='Apache License 2.0',
    packages=find_namespace_packages(where='src', include=['ensembl.*']),
    package_dir={'': 'src'},
    include_package_data=True,
    python_requires='>=3.8',
    install_requires=import_requirements(),
    classifiers=[
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        "Programming Language :: Python :: 3.8",
    ]
)
