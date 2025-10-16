import os
import shutil
import tempfile
from pathlib import Path

import pytest
import sqlalchemy as db
from _pytest.config import Config
from ensembl.utils.database import DBConnection
from grpc_reflection.v1alpha import reflection

from ensembl.production.metadata.api.adaptors import GenomeAdaptor
from ensembl.production.metadata.api.adaptors import ReleaseAdaptor
from ensembl.production.metadata.api.adaptors.vep import VepAdaptor
from ensembl.production.metadata.api.factories.datasets import DatasetFactory
from ensembl.production.metadata.api.factories.genomes import GenomeFactory
from ensembl.production.metadata.grpc import ensembl_metadata_pb2


def pytest_addoption(parser):
    """Add custom command line options."""
    parser.addoption(
        "--use-sqlite-dbs",
        action="store_true",
        default=False,
        help="Use pre-converted SQLite .db files instead of creating temporary databases",
    )


def pytest_configure(config: Config) -> None:
    pytest.dbs_dir = Path(__file__).parent / "databases"


@pytest.fixture(scope="session")
def use_sqlite_dbs(request):
    """Check if we should use SQLite .db files."""
    return request.config.getoption("--use-sqlite-dbs")


@pytest.fixture(scope="module")
def test_dbs(request, use_sqlite_dbs):
    """
    Test database fixture - supports both SQLite and MySQL.

    With --use-sqlite-dbs: Uses pre-converted .db files (creates temporary copies for isolation)
    Without flag: Creates temporary MySQL databases using UnitTestDB

    Note: SQLite databases are copied to temp directories to ensure test isolation.
          Changes made during tests won't affect the original .db files.
    """
    from ensembl.utils.database import UnitTestDB

    db_configs = request.param if hasattr(request, "param") else []
    test_databases = {}
    temp_resources = []  # Track resources for cleanup

    for db_config in db_configs:
        src_path = db_config["src"]
        db_name = src_path.name

        if use_sqlite_dbs:
            # Use pre-converted SQLite .db files with temporary copies for isolation
            sqlite_file = src_path.parent / f"{db_name}.db"

            if not sqlite_file.exists():
                raise FileNotFoundError(
                    f"SQLite database not found: {sqlite_file}\n"
                    f"Please convert it first using your conversion script."
                )

            # Create temporary copy to ensure test isolation
            temp_dir = tempfile.mkdtemp(prefix=f"pytest_{db_name}_")
            temp_db_file = Path(temp_dir) / f"{db_name}_test.db"

            print(f"\n>>> Using SQLite database: {sqlite_file}")
            print(f"    (temporary copy: {temp_db_file})")

            shutil.copy2(sqlite_file, temp_db_file)

            # Create connection to temporary copy
            db_url = f"sqlite:///{temp_db_file}"
            test_databases[db_name] = type("TestDB", (object,), {
                "dbc": DBConnection(db_url),
                "drop": lambda: None,  # Add no-op drop method for consistency
            })()

            temp_resources.append((temp_db_file, temp_dir))

        else:
            # Use MySQL with UnitTestDB (creates temporary databases from dumps)
            server_url = request.config.getoption(
                "--server", "mysql://ensembl@localhost:3306/?local_infile=1"
            )
            print(f"\n>>> Creating temporary MySQL database from: {src_path}")
            test_db = UnitTestDB(server_url, dump_dir=src_path, name=db_name)
            test_databases[db_name] = test_db

    yield test_databases

    # Cleanup
    if use_sqlite_dbs:
        # Close SQLite connections and remove temporary files
        for db_name, test_db in test_databases.items():
            if hasattr(test_db.dbc, 'dispose'):
                test_db.dbc.dispose()

        # Remove temporary files and directories
        for temp_file, temp_dir in temp_resources:
            try:
                if temp_file.exists():
                    temp_file.unlink()
                if Path(temp_dir).exists():
                    shutil.rmtree(temp_dir)
                print(f">>> Cleaned up temporary SQLite copy: {temp_dir}")
            except Exception as e:
                print(f"Warning: Failed to cleanup {temp_dir}: {e}")
    else:
        # Drop temporary MySQL databases
        for db_name, test_db in test_databases.items():
            if hasattr(test_db, "drop"):
                test_db.drop()


@pytest.fixture(scope="module", autouse=True)
def engine(test_dbs):
    os.environ["METADATA_URI"] = test_dbs["ensembl_genome_metadata"].dbc.url
    os.environ["TAXONOMY_URI"] = test_dbs["ncbi_taxonomy"].dbc.url
    yield db.create_engine(test_dbs["ensembl_genome_metadata"].dbc.url)


@pytest.fixture(scope="function")
def genome_conn(test_dbs):
    genome_conn = GenomeAdaptor(
        metadata_uri=test_dbs["ensembl_genome_metadata"].dbc.url,
        taxonomy_uri=test_dbs["ncbi_taxonomy"].dbc.url,
    )
    yield genome_conn


@pytest.fixture(scope="function")
def vep_conn(test_dbs):
    vep_conn = VepAdaptor(metadata_uri=test_dbs["ensembl_genome_metadata"].dbc.url, file="all")
    yield vep_conn


@pytest.fixture(scope="function")
def allow_unreleased(request):
    """Set ALLOWED_UNRELEASED environment variable, this fixture must be used with `parametrize`"""
    from ensembl.production.metadata.grpc.config import cfg

    cfg.allow_unreleased = request.param
    yield cfg


@pytest.fixture(scope="class")
def release_conn(test_dbs):
    release_conn = ReleaseAdaptor(metadata_uri=test_dbs["ensembl_genome_metadata"].dbc.url)
    yield release_conn


@pytest.fixture(scope="class")
def genome_factory():
    return GenomeFactory()


@pytest.fixture(scope="function")
def dataset_factory(test_dbs):
    yield DatasetFactory(test_dbs["ensembl_genome_metadata"].dbc.url)


@pytest.fixture(scope="module")
def grpc_add_to_server():
    from ensembl.production.metadata.grpc.ensembl_metadata_pb2_grpc import (
        add_EnsemblMetadataServicer_to_server,
    )

    return add_EnsemblMetadataServicer_to_server


@pytest.fixture(scope="module")
def grpc_servicer(test_dbs, engine):
    from ensembl.production.metadata.grpc.servicer import EnsemblMetadataServicer

    return EnsemblMetadataServicer()


@pytest.fixture(scope="module")
def grpc_server(_grpc_server, grpc_addr, grpc_add_to_server, grpc_servicer):
    grpc_add_to_server(grpc_servicer, _grpc_server)
    SERVICE_NAMES = (
        ensembl_metadata_pb2.DESCRIPTOR.services_by_name["EnsemblMetadata"].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(SERVICE_NAMES, _grpc_server)
    _grpc_server.add_insecure_port(grpc_addr)
    _grpc_server.start()
    yield _grpc_server
    _grpc_server.stop(grace=None)
