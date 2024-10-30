import json
import os
from central_logger import get_logger

from datetime import datetime

from bson import ObjectId, json_util
from pymongo import MongoClient

from base_migration import MigrationBase


class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        return json.JSONEncoder.default(self, o)


class MongoMigration(MigrationBase):
    logger = get_logger(__name__)
    cwd = os.path.abspath(__file__).split('/')  # path prefix
    cwd = "/".join(cwd[:-1])
    backup_flag = False

    def __init__(self, mongo_client_url, database):
        self.client = MongoClient(mongo_client_url)
        self.database = self.client[database]

    def execute_migration(self, script_file_name):
        path = MongoMigration.cwd + "/migration_files/mongo_migration_files/"
        with open(path + script_file_name, "r") as file:
            exec(file.read())

    def insert_state(self, migration_name):
        # Insert a document into the migration_files collection to record the applied migration
        migration_doc = {
            "migration": migration_name,
            "execution_date": datetime.now()
        }
        self.database["migration_files"].insert_one(migration_doc)

    def get_migrations_history(self):
        return self.database["migration_files"].distinct("migration")

    def apply_migration(self, file_names):
        codename_history = self.get_migrations_history()
        migration_name = ""
        try:
            for script_file in file_names:
                migration_name = script_file.split('.')[0]
                if not (codename_history and (migration_name in codename_history)):
                    if self.backup_flag:
                        self.backup_database([])
                    self.execute_migration(script_file)
                    self.insert_state(migration_name)
                    self.logger.info(f"{migration_name} executed successfully!")

        except Exception as ex:
            # If an exception is caught, handle the error
            self.logger.exception(f"Error in migration: {migration_name}\n{str(ex)}\n")

    def backup_database(self, selected_tables):
        self.logger.info("===== *** Started mongodb backup process *** =====")

        if len(selected_tables) == 0:
            selected_tables = self.database.list_collection_names()
        else:
            selected_tables = selected_tables

        new_directory_name = str(datetime.now())

        # Specify the output folder path
        output_folder_path = MongoMigration.cwd + "backups/mongo_backup/"
        os.makedirs(output_folder_path + new_directory_name)
        output_folder_path = output_folder_path + new_directory_name + '/'

        for collection_name in selected_tables:
            collection = self.database[collection_name]
            collection_data = list(collection.find())

            # Generate the output file path based on the collection name
            output_file_path = f"{output_folder_path}{collection_name}-{datetime.now()}.json"

            # Write data to the output file in JSON format
            with open(output_file_path, "w") as output_file:
                json.dump(collection_data, output_file, indent=4, cls=JSONEncoder, default=json_util.default)

            self.logger.info(f"Data from collection '{collection_name}' exported to '{output_file_path}'.")
        self.backup_flag = False
        self.logger.info("===== *** Finished mongodb backup process *** =====")

    def close_connection(self):
        self.client.close()
