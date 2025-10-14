#!/usr/bin/env python3
"""
Load a test database into MySQL from directory structure.
Reads table.sql schema file and tab-separated .txt data files.
"""

import argparse
import csv
from pathlib import Path
from urllib.parse import urlparse

import mysql.connector
from mysql.connector import Error


def parse_mysql_uri(uri):
    """Parse MySQL URI and return connection parameters."""
    parsed = urlparse(uri)

    return {
        "host": parsed.hostname,
        "port": parsed.port or 3306,
        "user": parsed.username,
        "password": parsed.password,
        "database": None,  # We'll create databases ourselves
    }


def create_database(cursor, db_name, drop_existing=False):
    """Create database, optionally dropping it first if it exists."""
    try:
        if drop_existing:
            print(f"Dropping existing database '{db_name}' if it exists...")
            cursor.execute(f"DROP DATABASE IF EXISTS `{db_name}`")
            print(f"✓ Database dropped")

        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
        print(f"✓ Database '{db_name}' ready")
        return True
    except Error as e:
        print(f"✗ Error creating database: {e}")
        return False


def load_schema(cursor, schema_file):
    """Load SQL schema from file."""
    with open(schema_file, "r", encoding="utf-8") as f:
        schema_sql = f.read()

    # Split into individual statements (handle multi-statement SQL)
    statements = [s.strip() for s in schema_sql.split(";") if s.strip()]

    for statement in statements:
        try:
            cursor.execute(statement)
        except Error as e:
            print(f"✗ Error executing statement: {e}")
            print(f"  Statement: {statement[:100]}...")
            raise

    print(f"✓ Schema loaded")


def get_table_columns(cursor, table_name):
    """Get column names for a table."""
    cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
    return [row[0] for row in cursor.fetchall()]


def load_table_data(cursor, table_name, txt_file):
    """Load data from tab-separated file into table."""

    # Get column information
    columns = get_table_columns(cursor, table_name)
    column_count = len(columns)

    # Prepare INSERT statement
    placeholders = ",".join(["%s"] * column_count)
    insert_sql = f"INSERT INTO `{table_name}` VALUES ({placeholders})"

    rows_inserted = 0
    with open(txt_file, "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="\t")

        for row in reader:
            # Handle MySQL NULL representation and clean data
            cleaned_row = []
            for val in row:
                if val == "\\N":
                    cleaned_row.append(None)
                else:
                    # Strip trailing commas and whitespace
                    cleaned_row.append(val.rstrip(",").strip() if val else val)

            try:
                cursor.execute(insert_sql, cleaned_row)
                rows_inserted += 1
            except Error as e:
                print(f"⚠ Warning: Error inserting row into {table_name}: {e}")
                print(f"  Row data: {cleaned_row}")

    return rows_inserted


def load_database(db_dir, mysql_uri, db_name=None, drop_existing=False):
    """Load a database directory into MySQL."""
    db_path = Path(db_dir)

    if not db_path.exists():
        print(f"✗ Error: Directory {db_dir} does not exist")
        return False

    if not db_path.is_dir():
        print(f"✗ Error: {db_dir} is not a directory")
        return False

    # Use provided database name or default to test_<directory_name>
    if not db_name:
        db_name = f"test_{db_path.name}"

    print(f"\nLoading database from: {db_path}")
    print(f"Target database name: {db_name}\n")

    # Check for schema file
    schema_file = db_path / "table.sql"
    if not schema_file.exists():
        print(f"✗ Error: No table.sql found in {db_path}")
        return False

    # Parse MySQL connection
    try:
        connection_params = parse_mysql_uri(mysql_uri)
    except Exception as e:
        print(f"✗ Error parsing MySQL URI: {e}")
        print("Expected format: mysql://user:password@host:port/")
        return False

    try:
        # Connect to MySQL server
        conn = mysql.connector.connect(**connection_params)
        cursor = conn.cursor()

        # Create and use database
        if not create_database(cursor, db_name, drop_existing):
            return False

        cursor.execute(f"USE `{db_name}`")

        # Disable foreign key checks during data load
        cursor.execute("SET FOREIGN_KEY_CHECKS=0")
        print(f"✓ Foreign key checks disabled")

        # Load schema
        load_schema(cursor, schema_file)
        conn.commit()

        # Load data from all .txt files
        txt_files = sorted(db_path.glob("*.txt"))

        if not txt_files:
            print(f"⚠ No data files found")

        for txt_file in txt_files:
            table_name = txt_file.stem

            # Check if table exists
            cursor.execute("SHOW TABLES LIKE %s", (table_name,))
            if not cursor.fetchone():
                print(f"⚠ Table '{table_name}' not found in schema, skipping {txt_file.name}")
                continue

            rows = load_table_data(cursor, table_name, txt_file)
            conn.commit()
            print(f"✓ Loaded {rows} rows into {table_name}")

        # Re-enable foreign key checks
        cursor.execute("SET FOREIGN_KEY_CHECKS=1")
        print(f"\n✓ Foreign key checks re-enabled")

        cursor.close()
        conn.close()

        print(f"\n{'=' * 60}")
        print(f"✓ Successfully loaded database: {db_name}")
        return True

    except Error as e:
        print(f"✗ MySQL Error: {e}")
        return False
    except Exception as e:
        print(f"✗ Error: {e}")
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load a test database into MySQL from directory structure",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s core_1 mysql://root:password@localhost:3306/
  %(prog)s /path/to/core_1 mysql://user:pass@db.example.com:3306/
  %(prog)s core_1 mysql://root:password@localhost:3306/ --name my_test_db
  %(prog)s core_1 mysql://root:password@localhost:3306/ --drop

The script will create a database named 'test_<directory_name>' by default,
or use the name specified with --name. Use --drop to drop and recreate the
database if it already exists.
        """,
    )
    parser.add_argument("directory", help="Directory containing table.sql and .txt data files")
    parser.add_argument("mysql_uri", help="MySQL connection URI (mysql://user:password@host:port/)")
    parser.add_argument("-n", "--name", help="Database name (default: test_<directory_name>)", default=None)
    parser.add_argument("-d", "--drop", action="store_true", help="Drop database if it exists before loading")

    args = parser.parse_args()

    # Check if mysql-connector-python is installed
    try:
        import mysql.connector
    except ImportError:
        print("✗ Error: mysql-connector-python is not installed")
        print("Install it with: pip install mysql-connector-python")
        exit(1)

    success = load_database(args.directory, args.mysql_uri, args.name, args.drop)
    exit(0 if success else 1)
