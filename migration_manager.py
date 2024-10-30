import os

from central_logger import get_logger
import elasticsearch_migration as elastic_migration
import mongodb_migration as mongo_migration
import mysql_migration as mysql_migration


class MigrationManager:
    logger = get_logger(__name__)
    cwd = os.path.abspath(__file__).split('/')  # path prefix
    cwd = "/".join(cwd[:-1])

    def __init__(self):
        self.mysql = mysql_migration.MySQLMigration(host=os.environ['DB_HOST'],
                                                    user=os.environ['DB_USER'],
                                                    password=os.environ['DB_PASS'],
                                                    database=os.environ['DB_NAME'])

        self.mongo = mongo_migration.MongoMigration(
            f"mongodb://{os.environ['MONGO_DB_USER']}:{os.environ['MONGO_DB_PASS']}@{os.environ['MONGO_DB_HOST']}:{os.environ['MONGO_DB_PORT']}",
            os.environ['MONGO_DB_NAME'])

        self.elastic = elastic_migration.ElasticsearchMigration(
            f"http://{os.environ['ELASTICSEARCH_USERNAME']}:{os.environ['ELASTICSEARCH_PASSWORD']}@{os.environ['ELASTICSEARCH_HOST']}:{os.environ['ELASTICSEARCH_PORT_ONE']}")

    def get_migration_files(self):
        sql_migrations = [f for f in os.listdir(MigrationManager.cwd + "/migration_files/mysql_migration_files") if
                          os.path.isfile(
                              os.path.join(MigrationManager.cwd + "/migration_files/mysql_migration_files", f))]

        mongo_migrations = [f for f in os.listdir(MigrationManager.cwd + "/migration_files/mongo_migration_files") if
                            os.path.isfile(
                                os.path.join(MigrationManager.cwd + "/migration_files/mongo_migration_files", f))]

        elastic_migrations = [f for f in os.listdir(MigrationManager.cwd + "/migration_files/elastic_migration_files")
                              if
                              os.path.isfile(
                                  os.path.join(MigrationManager.cwd + "/migration_files/elastic_migration_files", f))]

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
    manager.logger.info("========= Migration module initialized and started! =========")
    sql_files, mongo_files, elastic_files = manager.get_migration_files()
    manager.execute(sql_files, mongo_files, elastic_files)
    manager.close_all_connections()
    manager.logger.info("========= Migration module done! =========")
