#!/usr/bin/env python3
import argparse
from urllib.parse import urlparse
import os
import duckdb

# This will copy a DB by iterating over all tables and copying everything,
# schema as well as data

# Requirements: Just the Python duckdb package

def main():
    description = "Creates a DuckDB-format file from the metadata DB"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--dbhost", help='Metadata database hostname')
    parser.add_argument("--dbport", help='Metadata database port')
    parser.add_argument("--dbuser", help='Metadata database read-only user')
    parser.add_argument("--dbname", help='Metadata database name')
    parser.add_argument("--outfile", help='Name of DuckDB format output file')
    args = vars(parser.parse_args())

    dbhost = args.get("dbhost") or "mysql-ens-production-1"
    dbport = args.get("dbport") or "4721"
    dbuser = args.get("dbuser") or "ensro"
    dbname = args.get("dbname") or "ensembl_genome_metadata"
    outfile = args.get("outfile") or "duck_meta.db"

    if os.environ.get('METADATA_DB') is not None:
        db = urlparse(os.environ.get('METADATA_DB'))
        dbhost = db.hostname
        dbport = db.port
        dbuser = db.username
        dbname = db.path[1:]

    print(
        (
            f"Starting import from MySQL metadata DB"
            f" ({dbuser}@{dbhost}:{dbport}/{dbname})"
            f" into DuckDB native format (file: {outfile})."
        )
    )

    con = duckdb.connect()

    # The mem limit below is not honored for the 'CREATE AS FROM' operation.
    # For the current metadata DB, this needs a bit more than 12GB to run
    # We may want to consider to dump from metadata to CSV, then read that
    # Alternatively, do our own loop with OFFSET + LIMIT (or similar) to copy data
    con.execute("SET memory_limit = '8GB'")
    con.execute(f"ATTACH 'host={dbhost} user={dbuser} port={dbport} database={dbname}' AS metadb (TYPE mysql)")
    con.execute(f"ATTACH '{outfile}' as duck_meta")
    con.execute("use metadb")

    con.execute("show tables")

    results = con.fetchall()
    for res in results:
        tbl = res[0]
        if tbl.startswith("vw_"):
            continue
        print(f"Importing from table {tbl}")
        con.execute(f"DROP TABLE IF EXISTS duck_meta.{tbl}")
        con.execute(f"CREATE TABLE duck_meta.{tbl} AS FROM metadb.{tbl}")

    print("Done")


if __name__ == "__main__":
    main()
