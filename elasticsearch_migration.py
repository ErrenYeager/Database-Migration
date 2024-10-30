import json
import os
from datetime import datetime
from central_logger import get_logger

from elasticsearch import Elasticsearch

from base_migration import MigrationBase


class ElasticsearchMigration(MigrationBase):
    logger = get_logger(__name__)
    cwd = os.path.abspath(__file__).split('/')  # path prefix
    cwd = "/".join(cwd[:-1])
    backup_flag = False

    def __init__(self, elastic_host_url):
        self.INDEX_NAME = "migration_states"
        self.elasticsearch = Elasticsearch([elastic_host_url])

    def execute_migration(self, migration_file_name):
        path = ElasticsearchMigration.cwd + "/migration_files/elastic_migration_files/"
        with open(path + migration_file_name, "r") as file:
            exec(file.read())

    def insert_state(self, migration_name):
        document_data = {
            "migration": migration_name,
            "execution_date": datetime.now()
        }

        # Index the document
        response = self.elasticsearch.index(index=self.INDEX_NAME, body=document_data)

        # Check if the document was successfully indexed
        if response['result'] == 'created':
            self.logger.info(f"Migration state named {migration_name} indexed successfully.")
        else:
            self.logger.error(f"Migration state named {migration_name} indexing failed!")
            raise Exception(f"Migration state named {migration_name} indexing failed!")

    def get_migrations_history(self):
        migrations_history = []
        query_for_migrations = {
            "query": {
                "match_all": {}
            }
        }
        res = self.elasticsearch.search(index=self.INDEX_NAME, query=query_for_migrations["query"], size=10000)
        for hit in res['hits']['hits']:
            migrations_history.append(hit['_source']["migration"])

        return migrations_history

    def create_index_if_not_exists(self):
        # Create the migration_files index if not exists
        if not self.elasticsearch.indices.exists(index=self.INDEX_NAME):
            index_mapping = {
                "mappings": {
                    "properties": {
                        "migration": {
                            "type": "text"
                        },
                        "execution_date": {
                            "type": "date"
                        }
                    }
                }
            }
            self.elasticsearch.indices.create(index=self.INDEX_NAME, body=index_mapping)
            self.logger.info(f"Index {self.INDEX_NAME} created successfully.")

    def backup_database(self):
        self.logger.info("===== *** Started elasticsearch backup process *** =====")
        database_indices = self.elasticsearch.indices.get_alias().keys()
        new_directory_name = str(datetime.now())
        # Specify the output folder path
        output_folder_path = ElasticsearchMigration.cwd + "backups/elastic_backup/"
        os.makedirs(output_folder_path + new_directory_name)
        output_folder_path = output_folder_path + new_directory_name + '/'

        for index_name in database_indices:
            # Search for all documents in the index
            query = {"query": {"match_all": {}}}
            all_docs = []
            while True:
                results = self.elasticsearch.search(index=index_name, body=query, size=5000)
                # Extract the documents from the search results
                documents = [hit["_source"] for hit in results["hits"]["hits"]]
                all_docs.extend(documents)
                if len(documents) < 10000:
                    break

                query["from"] = query.get("from", 0) + 10000

            # Generate the output file path based on the index name
            output_file_path = f"{output_folder_path}{index_name}.json"

            # Serialize and write data to the output file in JSON format
            with open(output_file_path, "w") as output_file:
                json.dump(documents, output_file, indent=4)

            self.logger.info(f"Data from index '{index_name}' exported to '{output_file_path}'.")
        self.backup_flag = False
        self.logger.info("===== *** Finished elasticsearch backup process *** =====")

    def close_connection(self):
        self.elasticsearch.close()

    def apply_migration(self, migration_files):
        self.create_index_if_not_exists()
        migrations_history = self.get_migrations_history()
        migration_name = ""
        try:
            for migration_file_name in migration_files:
                migration_name = migration_file_name.split(".")[0]
                if not (migrations_history and (migration_name in migrations_history)):
                    if self.backup_flag:
                        self.backup_database()
                    self.execute_migration(migration_file_name)
                    self.insert_state(migration_name)
                    self.logger.info(f"{migration_name} executed successfully!")
        except Exception as ex:
            # If an exception is caught, handle the error
            self.logger.exception(f"Error in migration: {migration_name}\n{str(ex)}\n")
