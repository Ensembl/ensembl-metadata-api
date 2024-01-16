import argparse
import logging
import mysql.connector

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
def parse_arguments():
    parser = argparse.ArgumentParser(description='Database Patch Script')
    # Arguments for the core databases server
    parser.add_argument('--host', type=str, required=True, help='Core databases server host')
    parser.add_argument('--port', type=int, default=3306, help='Core databases server port (default 3306)')
    parser.add_argument('--user', type=str, required=True, help='Core databases server user')
    parser.add_argument('--password', type=str, required=True, help='Core databases server password')
    parser.add_argument('--mode', type=str, choices=['update', 'check', 'delete', 'full'], required=True,
                        help='Script mode')
    # Arguments for the metadata database server
    parser.add_argument('--meta_host', type=str, required=True, help='Metadata database server host')
    parser.add_argument('--meta_port', type=int, default=3306, help='Metadata database server port (default 3306)')
    parser.add_argument('--meta_user', type=str, required=True, help='Metadata database server user')
    parser.add_argument('--meta_password', type=str, required=True, help='Metadata database server password')
    parser.add_argument('--meta_database', type=str, required=True, help='Metadata database name')

    # Additional arguments for update mode
    parser.add_argument('--patch_file', type=str, help='Output file for SQL patches (required for update mode)')
    parser.add_argument('--database', type=str, required=False, help='Database name (required for update mode)')

    return parser.parse_args()


def generate_update_patch(host, port, user, password, database, patch_file):
    try:
        connection = mysql.connector.connect(host=host, port=port, user=user, password=password, database=database)
        cursor = connection.cursor()
        query = """SELECT DISTINCT ds.name, g.genome_uuid
FROM dataset_source ds
JOIN dataset d ON ds.dataset_source_id = d.dataset_source_id
JOIN genome_dataset gd ON d.dataset_id = gd.dataset_id
JOIN genome g ON gd.genome_id = g.genome_id
WHERE ds.type = 'core'"""
        cursor.execute(query)
        rows = cursor.fetchall()

        with open(patch_file, 'w') as file:
            for row in rows:
                source_name, genome_uuid = row
                # Write SQL commands to the patch file instead of executing them
                file.write(f"USE {source_name};\n")
                file.write("DELETE FROM meta WHERE meta_key = 'genome.genome_uuid' AND species_id = 1;\n")
                file.write(
                    f"INSERT INTO meta (species_id, meta_key, meta_value) VALUES (1, 'genome.genome_uuid', '{genome_uuid}');\n\n")

        cursor.close()
    except mysql.connector.Error as e:
        logging.error(f"Error: {e}")
    finally:
        connection.close()


def generate_delete_patch(core_host, core_port, core_user, core_password, meta_host, meta_port, meta_user, meta_password, metadata_database, patch_file):
    mismatches = check_databases(core_host, core_port, core_user, core_password, meta_host, meta_port, meta_user, meta_password, metadata_database)
    mismatches = [db_name for db_name, status in mismatches if status == 'mismatch']

    try:
        with open(patch_file, 'w') as file:
            for db_name in mismatches:
            # Write SQL commands to the patch file to delete mismatched entries
                file.write(f"USE {db_name};\n")
                file.write("DELETE FROM meta WHERE meta_key = 'genome.genome_uuid' AND species_id = 1;\n\n")
    except Exception as e:
        logging.error(f"Error while writing delete patch: {e}")

def check_databases(core_host, core_port, core_user, core_password, meta_host, meta_port, meta_user, meta_password, metadata_database):
    try:
        core_conn = mysql.connector.connect(host=core_host, port=core_port, user=core_user, password=core_password)
        core_cursor = core_conn.cursor()
        core_cursor.execute("SHOW DATABASES LIKE '%core%'")
        core_databases = core_cursor.fetchall()
        meta_conn = mysql.connector.connect(host=meta_host, port=meta_port, user=meta_user, password=meta_password, database=metadata_database)
        meta_cursor = meta_conn.cursor()

        results = []
        for (db_name,) in core_databases:
            core_cursor.execute(f"USE {db_name};")
            core_cursor.execute("SELECT meta_value FROM meta WHERE meta_key = 'genome.genome_uuid' AND species_id = 1")
            core_uuid = core_cursor.fetchone()

            if core_uuid:
                core_uuid = core_uuid[0]
                meta_cursor.execute(f"SELECT genome_uuid FROM genome WHERE genome_uuid = '{core_uuid}'")
                metadata_uuid = meta_cursor.fetchone()

                if metadata_uuid and metadata_uuid[0] == core_uuid:
                    results.append((db_name, 'match'))
                else:
                    results.append((db_name, 'mismatch'))
            else:
                results.append((db_name, 'absent'))

        return results

    except mysql.connector.Error as e:
        logging.error(f"Error: {e}")
    finally:
        if core_cursor and core_conn:
            core_cursor.close()
            core_conn.close()
        if meta_cursor and meta_conn:
            meta_cursor.close()
            meta_conn.close()
    return []

def main():
    args = parse_arguments()

    if args.mode == 'update':
        if not args.patch_file or not args.database:
            raise ValueError("Patch file name and database name are required for update mode")
        generate_update_patch(args.meta_host, args.meta_port, args.meta_user, args.meta_password, args.meta_database, args.patch_file)
    elif args.mode == 'check':
        results = check_databases(args.host, args.port, args.user, args.password, args.meta_host, args.meta_port, args.meta_user,
                                  args.meta_password, args.meta_database)
        for db_name, status in results:
            print(f"{db_name}: {status}")
    elif args.mode == 'delete':
        generate_delete_patch(args.host, args.port, args.user, args.password, args.meta_host, args.meta_port, args.meta_user,
                              args.meta_password, args.meta_database, args.patch_file)
    elif args.mode == 'full':
        print ("generating update patch")
        generate_update_patch(args.meta_host, args.meta_port, args.meta_user, args.meta_password, args.meta_database, args.patch_file)
        print ("generating delete patch")
        generate_delete_patch(args.host, args.port, args.user, args.password, args.meta_host, args.meta_port, args.meta_user,
                              args.meta_password, args.meta_database, args.patch_file)
        print("checking results")
        results = check_databases(args.host, args.port, args.user, args.password, args.meta_host, args.meta_port, args.meta_user,
                                  args.meta_password, args.meta_database)
        absent_cores = [db_name for db_name, status in results if status == 'absent']
        print("Cores without genome_uuids:", absent_cores)

if __name__ == "__main__":
    main()
