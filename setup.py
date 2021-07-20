from setuptools import find_namespace_packages, setup


with open('README.md') as f:
    readme = f.read()


with open('VERSION') as f:
    version = f.read()


setup(
    name='ensembl_metadata_api',
    version=version,
    description='Ensembl Metadata API',
    long_description=readme,
    author='Ensembl',
    author_email='dev@ensembl.org',
    url='https://www.ensembl.org',
    download_url='https://github.com/Ensembl/ensembl-metadata-api',
    license='Apache License 2.0',
    packages=find_namespace_packages(where='src', include=['ensembl.*']),
    package_dir={'': 'src'},
    include_package_data=True,
    python_requires='>=3.8',
    classifiers=[
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        "Programming Language :: Python :: 3.8",
    ]
)
