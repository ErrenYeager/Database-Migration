from abc import ABC, abstractmethod


class MigrationBase(ABC):
    @abstractmethod
    def insert_state(self, to_be_inserted_data):
        pass

    @abstractmethod
    def execute_migration(self, file):
        pass

    @abstractmethod
    def get_migrations_history(self):
        pass

    @abstractmethod
    def backup_database(self, selected_tables=[]):
        pass

    @abstractmethod
    def close_connection(self):
        pass

    @abstractmethod
    def apply_migration(self, script_files):
        pass
