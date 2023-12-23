import os
from datetime import datetime

import mysql.connector

from base_migration import MigrationBase


class MySQLMigration(MigrationBase):
    cwd = os.path.abspath(__file__).split('/')  # path prefix
    cwd = "/".join(cwd[:-1])
    is_any_migration_executed = False

    def __init__(self, host, user, password, database):
        self.mysql_connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        self.mysql_cursor = self.mysql_connection.cursor()

    def insert_state(self, id_and_name):
        insert_query = "INSERT INTO migration_states (id, codename, execution_date) VALUES (%s, %s, %s)"
        data_to_insert = (id_and_name[0], id_and_name[1], datetime.now())
        self.mysql_cursor.execute(insert_query, data_to_insert)

    def execute_migration(self, sql_file):
        with open(MySQLMigration.cwd + "/migration_files/mysql_migration_files/" + sql_file, "r") as file:
            sql_statements = file.read()

        # Split the SQL statements into individual queries
        queries = sql_statements.split(';')

        # Execute each query
        for query in queries:
            if query.strip():  # Check if the query is not empty
                self.mysql_cursor.execute(query)
        self.is_any_migration_executed = True

    def get_migrations_history(self):
        migrations_history = []
        query = "select * from migration_states"
        self.mysql_cursor.execute(query)
        migrations = self.mysql_cursor.fetchall()
        for migration in migrations:
            migrations_history.append(migration[1])

        return migrations_history

    def create_index_if_not_exists(self):
        # Define the SQL query
        create_table_query = """
        CREATE TABLE IF NOT EXISTS migration_states (
            id INT AUTO_INCREMENT PRIMARY KEY,
            codename VARCHAR(255),
            execution_date DATETIME
        )
        """
        # Execute the SQL query
        self.mysql_cursor.execute(create_table_query)

    def apply_migration(self, sql_migration_files):
        self.create_index_if_not_exists()
        self.backup_database()
        migrations_history = self.get_migrations_history()
        migration_name = ""
        pk = len(migrations_history)
        try:
            for sql_file in sql_migration_files:
                migration_name = sql_file.split(".")[0]
                if not (migrations_history and (migration_name in migrations_history)):
                    self.execute_migration(sql_file)
                    self.insert_state((pk, migration_name))
                    pk += 1
                    print(f"{migration_name} ran successfully")
        except Exception as ex:
            # If an exception is caught, handle the error
            print(f"Error in migration: {migration_name}\n{str(ex)}\n")

    def backup_database(self):
        if self.is_any_migration_executed:
            # Get a list of all tables in the database
            self.mysql_cursor.execute("SHOW TABLES")
            tables = self.mysql_cursor.fetchall()

            new_directory_name = str(datetime.now())

            # Specify the output folder path
            output_folder_path = "backups/mysql_backup/"
            os.makedirs(output_folder_path + new_directory_name)
            output_folder_path = "backups/mysql_backup/" + new_directory_name + '/'

            # Iterate through each table and export data to separate JSON files
            for table in tables:
                table_name = table[0]

                # Retrieve data from the table
                self.mysql_cursor.execute(f"SELECT * FROM {table_name}")
                rows = self.mysql_cursor.fetchall()

                output_file_path = f"{output_folder_path}{table_name}.sql"

                # Generate SQL insert statements and write to the output file
                with open(output_file_path, "w") as output_file:
                    for row in rows:
                        values = ", ".join([f"'{value}'" if isinstance(value, str) else str(value) for value in row])
                        insert_statement = f"INSERT INTO {table_name} VALUES ({values});\n"
                        output_file.write(insert_statement)

                print(f"Data from table '{table_name}' exported to '{output_file_path}'.")

    def close_connection(self):
        self.mysql_connection.commit()
        self.mysql_cursor.close()
        self.mysql_connection.close()

# # Check if the connection is successful
# if db_connection.is_connected():
#     print("Connected to MySQL database!")
