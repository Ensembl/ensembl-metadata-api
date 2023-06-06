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
import pytest
import MySQLdb

host = os.getenv('MYSQL_HOST', "localhost")
port = os.getenv('MYSQL_PORT', 3306)
password = os.getenv('MYSQL_PASSWORD', "")
user = os.getenv('MYSQL_USER', "ensembl")


@pytest.fixture(scope="session", autouse=True)
def setup_database():
    sql_files = [
        f'{os.path.dirname(__file__)}/sql/ensembl_genome_metadata.sql',
        f'{os.path.dirname(__file__)}/sql/ncbi_taxonomy.sql',
        f'{os.path.dirname(__file__)}/sql/core_test_databases.sql',
    ]
    # Connect to the database
    # Connect to the MySQL host
    conn = MySQLdb.connect(
        host=host,
        user=user,
        password=password
    )
    cursor = conn.cursor()

    # Read the SQL script file
    for sql_file in sql_files:
        with open(sql_file, "r") as file:
            sql_script = file.read()

        sql_statements = sql_script.split(';')
        # Execute each statement
        for statement in sql_statements:
            if statement.strip():  # Skip empty statements
                cursor.execute(statement)
        # Execute the SQL script
        # cursor.executemany(sql_script)
        # Commit the changes and close the connection
        conn.commit()
    conn.close()
