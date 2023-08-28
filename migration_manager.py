import os

import elasticsearch_migration as elastic_migration
import mongodb_migration as mongo_migration
import mysql_migration as mysql_migration


class MigrationManager:
    def __init__(self):
        self.mysql = mysql_migration.MySQLMigration(host="localhost",
                                                    user="root",
                                                    password="my-secret-pw",
                                                    database="migrations")
        self.mongo = mongo_migration.MongoMigration("mongodb://localhost:27017/", "migration")
        self.elastic = elastic_migration.ElasticsearchMigration('http://elastic:passw0rd@localhost:9200')

    def get_migration_files(self):
        sql_migrations = [f for f in os.listdir("migration_files/mysql_migration_files") if
                          os.path.isfile(os.path.join("migration_files/mysql_migration_files", f))]

        mongo_migrations = [f for f in os.listdir("migration_files/mongo_migration_files") if
                            os.path.isfile(os.path.join("migration_files/mongo_migration_files", f))]

        elastic_migrations = [f for f in os.listdir("migration_files/elastic_migration_files") if
                              os.path.isfile(os.path.join("migration_files/elastic_migration_files", f))]

        return sql_migrations, mongo_migrations, elastic_migrations

    def execute(self, sql_migration_files, mongo_migration_files, elastic_migration_files):
        self.mysql.apply_migration(sql_migration_files)
        self.mongo.apply_migration(mongo_migration_files)
        self.elastic.apply_migration(elastic_migration_files)

    def close_all_connections(self):
        self.mysql.close_connection()
        self.elastic.close_connection()
        self.mongo.close_connection()


if __name__ == '__main__':
    manager = MigrationManager()
    sql_files, mongo_files, elastic_files = manager.get_migration_files()
    manager.execute(sql_files, mongo_files, elastic_files)
    manager.close_all_connections()
