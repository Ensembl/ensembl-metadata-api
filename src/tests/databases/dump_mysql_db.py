#!/usr/bin/env python3
"""
Dump MySQL database to table.sql and .txt files.
Creates the same format that load_mysql_db.py expects.
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
        "database": parsed.path.lstrip("/"),
    }


def get_table_create_statement(cursor, table_name):
    """Get the CREATE TABLE statement for a table."""
    cursor.execute(f"SHOW CREATE TABLE `{table_name}`")
    result = cursor.fetchone()
    return result[1]  # Second column is the CREATE TABLE statement


def dump_schema(cursor, output_dir):
    """Dump all table schemas to table.sql."""
    # Get all tables in database
    cursor.execute("SHOW TABLES")
    tables = [row[0] for row in cursor.fetchall()]

    schema_file = output_dir / "table.sql"

    with open(schema_file, "w", encoding="utf-8") as f:
        for table_name in tables:
            create_stmt = get_table_create_statement(cursor, table_name)
            f.write(create_stmt)
            f.write(";\n\n")

    print(f"✓ Exported schema for {len(tables)} tables to {schema_file}")
    return tables


def dump_table_data(cursor, table_name, output_dir):
    """Dump table data to a tab-separated .txt file."""
    output_file = output_dir / f"{table_name}.txt"

    # Get all data from table
    cursor.execute(f"SELECT * FROM `{table_name}`")
    rows = cursor.fetchall()

    if not rows:
        # Create empty file for consistency
        output_file.touch()
        return 0

    # Write to TSV file
    with open(output_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter="\t", lineterminator="\n")

        for row in rows:
            # Convert None to \N (MySQL NULL representation)
            converted_row = ["\\N" if val is None else str(val) for val in row]
            writer.writerow(converted_row)

    return len(rows)


def dump_database(mysql_url, output_dir, overwrite=False):
    """
    Dump MySQL database to table.sql and .txt files.

    Args:
        mysql_url: MySQL connection URL (mysql://user:pass@host:port/database)
        output_dir: Output directory for schema and data files
        overwrite: Whether to overwrite existing directory
    """
    output_path = Path(output_dir)

    # Check if output directory exists
    if output_path.exists():
        if not overwrite:
            print(f"✗ Error: Directory {output_dir} already exists. Use --overwrite to replace it.")
            return False
        print(f"⚠ Overwriting existing directory: {output_dir}")
    else:
        output_path.mkdir(parents=True, exist_ok=True)
        print(f"✓ Created output directory: {output_dir}")

    # Parse connection parameters
    try:
        conn_params = parse_mysql_uri(mysql_url)
        db_name = conn_params["database"]

        if not db_name:
            print("✗ Error: No database specified in URL")
            print("Expected format: mysql://user:password@host:port/database_name")
            return False

        print(f"\nDumping database: {db_name}")
        print(f"MySQL Server: {conn_params['host']}:{conn_params['port']}")
        print(f"Output directory: {output_dir}\n")
    except Exception as e:
        print(f"✗ Error parsing MySQL URI: {e}")
        print("Expected format: mysql://user:password@host:port/database_name")
        return False

    try:
        # Connect to MySQL
        conn = mysql.connector.connect(**conn_params)
        cursor = conn.cursor()

        # Dump schema
        tables = dump_schema(cursor, output_path)

        # Dump data for each table
        print("\nExporting table data...")
        total_rows = 0

        for table_name in tables:
            rows = dump_table_data(cursor, table_name, output_path)
            total_rows += rows
            print(f"  ✓ {table_name}: {rows} rows")

        cursor.close()
        conn.close()

        print(f"\n{'=' * 60}")
        print(f"✓ Successfully dumped database: {db_name}")
        print(f"  - Schema: table.sql")
        print(f"  - Data: {len(tables)} tables, {total_rows} total rows")
        print(f"  - Location: {output_path.absolute()}")
        return True

    except Error as e:
        print(f"✗ MySQL Error: {e}")
        return False
    except Exception as e:
        print(f"✗ Error: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Dump MySQL database to table.sql and .txt data files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s mysql://user:pass@host:port/my_database ./output_dir
  %(prog)s mysql://ensadmin:ensembl@mysql-server:4508/test_core_1 databases/core_1
  %(prog)s mysql://user:pass@host/testdb ./testdb --overwrite

The script creates:
  - table.sql: Complete schema for all tables
  - <table_name>.txt: Tab-separated data for each table (no headers)

This format is compatible with load_mysql_db.py for re-importing.
        """,
    )
    parser.add_argument("mysql_url", help="MySQL connection URL (mysql://user:password@host:port/database)")
    parser.add_argument("output_dir", help="Output directory for schema and data files")
    parser.add_argument(
        "-o", "--overwrite", action="store_true", help="Overwrite output directory if it exists"
    )

    args = parser.parse_args()

    success = dump_database(args.mysql_url, args.output_dir, args.overwrite)
    exit(0 if success else 1)


if __name__ == "__main__":
    main()
