#!/usr/bin/env python3
"""
Convert MySQL database to SQLite using SQLAlchemy.
Uses reflection to automatically handle schema and data copying.
"""

import argparse
from pathlib import Path

from sqlalchemy import create_engine, MetaData, inspect, Integer, String, Text, Float, Boolean, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool


def convert_mysql_types_to_sqlite(metadata):
    """
    Convert MySQL-specific types to SQLite-compatible types.
    Modifies the metadata in place.
    """
    type_mapping = {
        "TINYINT": Integer,
        "SMALLINT": Integer,
        "MEDIUMINT": Integer,
        "INT": Integer,
        "BIGINT": Integer,
        "DECIMAL": Float,
        "FLOAT": Float,
        "DOUBLE": Float,
        "VARCHAR": String,
        "CHAR": String,
        "TEXT": Text,
        "MEDIUMTEXT": Text,
        "LONGTEXT": Text,
        "ENUM": String,
        "SET": String,
    }

    for table_name, table in metadata.tables.items():
        for column in table.columns:
            type_name = type(column.type).__name__.upper()

            # Handle TINYINT(1) as Boolean
            if type_name == "TINYINT":
                # Check if it's TINYINT(1) which is typically used for boolean
                if hasattr(column.type, "display_width") and column.type.display_width == 1:
                    column.type = Boolean()
                else:
                    column.type = Integer()
            elif type_name in type_mapping:
                # Get length/precision if available
                if hasattr(column.type, "length") and column.type.length:
                    column.type = type_mapping[type_name](length=column.type.length)
                else:
                    column.type = type_mapping[type_name]()

    return metadata


def remove_indexes(metadata):
    """
    Remove all indexes from metadata (except primary key constraints).
    Useful for test databases where indexes aren't needed.
    """
    for table_name, table in metadata.tables.items():
        # Create a list of indexes to remove (can't modify during iteration)
        indexes_to_remove = [idx for idx in table.indexes]

        # Remove each index
        for idx in indexes_to_remove:
            table.indexes.remove(idx)

    return metadata


def convert_database(mysql_url, sqlite_path, batch_size=1000, keep_indexes=False):
    """
    Convert a MySQL database to SQLite using SQLAlchemy reflection.

    Args:
        mysql_url: MySQL connection URL (e.g., mysql://user:pass@host:port/dbname)
        sqlite_path: Path to output SQLite database file
        batch_size: Number of rows to copy per batch
        keep_indexes: Whether to keep indexes (default: False, since not needed for tests)
    """
    print(f"\nConverting database to: {sqlite_path}")

    # Create engines
    mysql_engine = create_engine(mysql_url, poolclass=NullPool)

    # Remove existing SQLite file if it exists
    sqlite_file = Path(sqlite_path)
    if sqlite_file.exists():
        sqlite_file.unlink()
        print(f"✓ Removed existing SQLite file")

    sqlite_engine = create_engine(f"sqlite:///{sqlite_path}")

    # Reflect MySQL schema
    print("Reflecting MySQL schema...")
    mysql_metadata = MetaData()
    mysql_metadata.reflect(bind=mysql_engine)

    print(f"✓ Found {len(mysql_metadata.tables)} tables")

    # Convert MySQL types to SQLite-compatible types
    print("Converting MySQL types to SQLite types...")
    convert_mysql_types_to_sqlite(mysql_metadata)
    print("✓ Types converted")

    # Remove indexes unless user wants to keep them
    if not keep_indexes:
        print("Removing indexes (not needed for unit tests)...")
        remove_indexes(mysql_metadata)
        print("✓ Indexes removed")

    # Create SQLite schema
    print("Creating SQLite schema...")
    mysql_metadata.create_all(sqlite_engine)
    print("✓ Schema created")

    # Get inspector to check for foreign keys
    inspector = inspect(mysql_engine)

    # Disable foreign key checks in SQLite during data load
    with sqlite_engine.begin() as conn:
        conn.execute(text("PRAGMA foreign_keys = OFF"))

    # Copy data table by table
    print("\nCopying data...")

    # Create sessions
    MySQLSession = sessionmaker(bind=mysql_engine)
    SQLiteSession = sessionmaker(bind=sqlite_engine)

    mysql_session = MySQLSession()
    sqlite_session = SQLiteSession()

    try:
        for table_name in mysql_metadata.tables:
            table = mysql_metadata.tables[table_name]
            print(f"  Copying {table_name}...", end=" ", flush=True)

            # Count rows in MySQL
            count = mysql_session.execute(table.select()).rowcount
            if count == -1:  # Some drivers don't support rowcount on select
                # Get actual count
                result = mysql_session.execute(table.select())
                rows = result.fetchall()
                count = len(rows)

                # Insert in batches
                total_inserted = 0
                for i in range(0, count, batch_size):
                    batch = rows[i: i + batch_size]
                    if batch:
                        sqlite_session.execute(table.insert(), [dict(row._mapping) for row in batch])
                        total_inserted += len(batch)

                sqlite_session.commit()
                print(f"✓ {total_inserted} rows")
            else:
                # Stream and batch insert
                result = mysql_session.execute(table.select())
                total_inserted = 0

                while True:
                    batch = result.fetchmany(batch_size)
                    if not batch:
                        break

                    sqlite_session.execute(table.insert(), [dict(row._mapping) for row in batch])
                    total_inserted += len(batch)

                sqlite_session.commit()
                print(f"✓ {total_inserted} rows")

        # Re-enable foreign keys
        with sqlite_engine.begin() as conn:
            conn.execute(text("PRAGMA foreign_keys = ON"))

        print(f"\n{'=' * 60}")
        print(f"✓ Successfully converted to {sqlite_path}")
        return True

    except Exception as e:
        print(f"\n✗ Error during conversion: {e}")
        import traceback

        traceback.print_exc()
        sqlite_session.rollback()
        return False
    finally:
        mysql_session.close()
        sqlite_session.close()
        mysql_engine.dispose()
        sqlite_engine.dispose()


def main():
    parser = argparse.ArgumentParser(
        description="Convert MySQL database to SQLite using SQLAlchemy",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s mysql://user:pass@host:port/test_core_1 core_1.db
  %(prog)s mysql://user:pass@host:port/test_compara_db compara_db.db
  %(prog)s mysql://user:pass@host/test_core_1 ./sqlite_dbs/core_1.db --batch-size 5000

The script uses SQLAlchemy to reflect the MySQL schema and copy all data to SQLite.
This preserves table structures, indexes, and relationships automatically.
        """,
    )
    parser.add_argument("mysql_url", help="MySQL connection URL (mysql://user:password@host:port/database)")
    parser.add_argument("sqlite_path", help="Output SQLite database file path")
    parser.add_argument(
        "-b", "--batch-size", type=int, default=1000, help="Number of rows to copy per batch (default: 1000)"
    )
    parser.add_argument(
        "-k",
        "--keep-indexes",
        action="store_true",
        help="Keep indexes in SQLite (default: False, indexes removed for faster tests)",
    )

    args = parser.parse_args()

    # Check if SQLAlchemy is installed
    try:
        import sqlalchemy
    except ImportError:
        print("✗ Error: SQLAlchemy is not installed")
        print("Install it with: pip install sqlalchemy")
        exit(1)

    # Create output directory if needed
    output_path = Path(args.sqlite_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    success = convert_database(args.mysql_url, args.sqlite_path, args.batch_size, args.keep_indexes)
    exit(0 if success else 1)


if __name__ == "__main__":
    main()
