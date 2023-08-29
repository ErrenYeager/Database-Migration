import json
import os
from datetime import datetime

from elasticsearch import Elasticsearch

from base_migration import MigrationBase


class ElasticsearchMigration(MigrationBase):

    def __init__(self, elastic_host_url):
        self.INDEX_NAME = "migrations"
        self.elasticsearch = Elasticsearch([elastic_host_url])

    def execute_migration(self, migration_file_name):
        path = "migration_files/elastic_migration_files/"
        with open(path + migration_file_name, "r") as file:
            migration_script = file.read()

        # Split the script into individual operations using a delimiter
        code_pieces = migration_script.split('\n')

        for code_piece in code_pieces:
            exec(code_piece.strip())

    def insert_state(self, migration_name):
        document_data = {
            "migration": migration_name,
            "execution_date": datetime.now()
        }

        # Index the document
        response = self.elasticsearch.index(index=self.INDEX_NAME, body=document_data)

        # Check if the document was successfully indexed
        if response['result'] == 'created':
            print("Document indexed successfully.")
        else:
            print("Document indexing failed.")

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

    def apply_migration(self, migration_files):
        self.backup_database()
        migrations_history = self.get_migrations_history()
        migration_name = ""
        try:
            for migration_file_name in migration_files:
                migration_name = migration_file_name.split(".")[0]
                if not (migrations_history and (migration_name in migrations_history)):
                    self.execute_migration(migration_file_name)
                    self.insert_state(migration_name)
                    print(f"{migration_name} ran successfully")
        except Exception as ex:
            # If an exception is caught, handle the error
            print(f"Error in migration: {migration_name}\n{str(ex)}\n")

    def close_connection(self):
        self.elasticsearch.close()

    def backup_database(self):
        database_indices = self.elasticsearch.indices.get_alias().keys()

        new_directory_name = str(datetime.now())
        # Specify the output folder path
        output_folder_path = "backups/elastic_backup/"
        os.makedirs(output_folder_path + new_directory_name)
        output_folder_path = output_folder_path + new_directory_name + '/'

        for index_name in database_indices:
            # Search for all documents in the index
            query = {"query": {"match_all": {}}}
            all_docs = []
            while True:
                results = self.elasticsearch.search(index=index_name, body=query, size=10000)
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

            print(f"Data from index '{index_name}' exported to '{output_file_path}'.")

#
# # Connect to Elasticsearch
# # Replace "localhost" with the hostname or IP address of your Elasticsearch server
# # Replace 9200 with the port number of your Elasticsearch instance
# es = Elasticsearch(
#     ['http://elastic:passw0rd@localhost:9200']
# )
#
# if es.ping():
#     print("Connected to Elasticsearch")
# else:
#     print("Connection failed")
#     raise Exception("Connection to Elasticsearch failed!")
#
# INDEX_NAME = "migration_files"
# # Get a list of all files in the directory
# script_names = [f for f in os.listdir("elastic_migration_files") if
#                 os.path.isfile(os.path.join("elastic_migration_files", f))]
#
# # Create the migration_files index if not exists
# if not es.indices.exists(index=INDEX_NAME):
#     index_mapping = {
#         "mappings": {
#             "properties": {
#                 "migration": {
#                     "type": "text"
#                 },
#                 "executed_at": {
#                     "type": "date"
#                 }
#             }
#         }
#     }
#     es.indices.create(index="migration_files", body=index_mapping)
#     print(f"Index {INDEX_NAME} created successfully.")
# else:
#     print(f"Index {INDEX_NAME} already exists.")
