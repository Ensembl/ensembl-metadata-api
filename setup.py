"""Build script for setuptools."""


from setuptools import setup, find_packages


with open('README.md') as f:
    readme = f.read()


with open('VERSION') as f:
    version = f.read()

setup(
    name='ensembl_template_py',
    version=version,
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    description="Ensembl Python Template",
    include_package_data=True,
    long_description=readme,
    author='Ensembl',
    author_email='dev@ensembl.org',
    url='https://www.ensembl.org',
    download_url='https://github.com/Ensembl/template-python',
    license="Apache License 2.0",
    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3.8",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ]
)
