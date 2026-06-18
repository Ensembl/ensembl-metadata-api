#!/usr/bin/env python3
import argparse
from urllib.parse import urlparse
import os
import duckdb

# This will copy a DB by iterating over all tables and copying everything,
# schema as well as data, except for genome-linked tables that are filtered
# to released Ensembl releases only.

# Requirements: Just the Python duckdb package


def released_genomes_query():
    # A genome is treated as released when it is connected through
    # genome_release to an ensembl_release row whose status is Released.
    return """
        SELECT DISTINCT gr.genome_id
        FROM metadb.genome_release gr
        JOIN metadb.ensembl_release er ON er.release_id = gr.release_id
        WHERE er.status = 'Released'
    """


def released_releases_query():
    # Keep the release status filter in one place so the dependent table
    # copy queries stay aligned with the release table itself.
    return """
        SELECT er.release_id
        FROM metadb.ensembl_release er
        WHERE er.status = 'Released'
    """


def released_genome_dataset_query():
    # Genome datasets are retained only for released genomes. Rows attached to
    # a specific release are additionally limited to released releases.
    return f"""
        SELECT gd.dataset_id
        FROM metadb.genome_dataset gd
        WHERE gd.genome_id IN ({released_genomes_query()})
          AND (
              gd.release_id IS NULL
              OR gd.release_id IN ({released_releases_query()})
          )
    """


def copy_query(tbl):
    released_genomes = released_genomes_query()
    released_releases = released_releases_query()
    released_genome_datasets = released_genome_dataset_query()

    # Most tables are independent lookup/taxonomy tables and can be copied as
    # before. The entries below constrain release/genome-owned data so the
    # DuckDB dump does not include unreleased genomes or their release rows.
    filtered_copy_queries = {
        "ensembl_release": """
            SELECT *
            FROM metadb.ensembl_release
            WHERE status = 'Released'
        """,
        "genome_release": f"""
            SELECT gr.*
            FROM metadb.genome_release gr
            WHERE gr.release_id IN ({released_releases})
        """,
        "genome": f"""
            SELECT g.*
            FROM metadb.genome g
            WHERE g.genome_id IN ({released_genomes})
        """,
        "genome_dataset": f"""
            SELECT gd.*
            FROM metadb.genome_dataset gd
            WHERE gd.genome_id IN ({released_genomes})
              AND (
                  gd.release_id IS NULL
                  OR gd.release_id IN ({released_releases})
              )
        """,
        "dataset": f"""
            SELECT d.*
            FROM metadb.dataset d
            WHERE d.dataset_id IN ({released_genome_datasets})
        """,
        "dataset_attribute": f"""
            SELECT da.*
            FROM metadb.dataset_attribute da
            WHERE da.dataset_id IN ({released_genome_datasets})
        """,
        "assembly": f"""
            SELECT a.*
            FROM metadb.assembly a
            WHERE a.assembly_id IN (
                SELECT g.assembly_id
                FROM metadb.genome g
                WHERE g.genome_id IN ({released_genomes})
            )
        """,
        "assembly_sequence": f"""
            SELECT ase.*
            FROM metadb.assembly_sequence ase
            WHERE ase.assembly_id IN (
                SELECT g.assembly_id
                FROM metadb.genome g
                WHERE g.genome_id IN ({released_genomes})
            )
        """,
        "sequence_alias": f"""
            SELECT sa.*
            FROM metadb.sequence_alias sa
            WHERE sa.assembly_sequence_id IN (
                SELECT ase.assembly_sequence_id
                FROM metadb.assembly_sequence ase
                WHERE ase.assembly_id IN (
                    SELECT g.assembly_id
                    FROM metadb.genome g
                    WHERE g.genome_id IN ({released_genomes})
                )
            )
        """,
        "organism": f"""
            SELECT o.*
            FROM metadb.organism o
            WHERE o.organism_id IN (
                SELECT g.organism_id
                FROM metadb.genome g
                WHERE g.genome_id IN ({released_genomes})
            )
        """,
        "genome_group_member": f"""
            SELECT ggm.*
            FROM metadb.genome_group_member ggm
            WHERE ggm.genome_id IN ({released_genomes})
              AND (
                  ggm.release_id IS NULL
                  OR ggm.release_id IN ({released_releases})
              )
        """,
        "genome_group": f"""
            SELECT gg.*
            FROM metadb.genome_group gg
            WHERE gg.genome_group_id IN (
                SELECT ggm.genome_group_id
                FROM metadb.genome_group_member ggm
                WHERE ggm.genome_id IN ({released_genomes})
                  AND (
                      ggm.release_id IS NULL
                      OR ggm.release_id IN ({released_releases})
                  )
            )
        """,
        "organism_group_member": f"""
            SELECT ogm.*
            FROM metadb.organism_group_member ogm
            WHERE ogm.organism_id IN (
                SELECT g.organism_id
                FROM metadb.genome g
                WHERE g.genome_id IN ({released_genomes})
            )
        """,
        "organism_group": f"""
            SELECT og.*
            FROM metadb.organism_group og
            WHERE og.organism_group_id IN (
                SELECT ogm.organism_group_id
                FROM metadb.organism_group_member ogm
                WHERE ogm.organism_id IN (
                    SELECT g.organism_id
                    FROM metadb.genome g
                    WHERE g.genome_id IN ({released_genomes})
                )
            )
        """,
    }

    return filtered_copy_queries.get(tbl, f"FROM metadb.{tbl}")


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
        con.execute(f"CREATE TABLE duck_meta.{tbl} AS {copy_query(tbl)}")

    print("Done")


if __name__ == "__main__":
    main()
