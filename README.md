# Database Migration Project

This project provides a flexible and extensible framework for performing database migrations across various database systems, including MySQL, MongoDB, and Elasticsearch.

## Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [Usage](#usage)
- [Requirements](#requirements)


## Overview

The project is structured around the concept of a `MigrationBase` abstract class, which serves as a foundation for creating specific migration modules for different database systems. The project includes the following modules:

- `MongoMigration`: Handles MongoDB migrations using .py based migration scripts.
- `ElasticsearchMigration`: Manages Elasticsearch migrations with .py based migration scripts.
- `MySQLMigration`: Provides capabilities for MySQL migrations using SQL files.

A `MigrationManager` class is also included to orchestrate and execute migrations across the different modules.

## Installation

1. Clone the repository to your local machine:

   ```shell
   git clone https://github.com/ErrenYeager/databasemigration.git

## Usage

### Migration Modules

- Implement migration logic by extending the `MigrationBase` abstract class.
- For MongoDB migrations, use the `MongoMigration` module with python scripts.
- For Elasticsearch migrations, utilize the `ElasticsearchMigration` module with python scripts.
- For MySQL migrations, use the `MySQLMigration` module with SQL migration files.

### MigrationManager

- Use the `MigrationManager` class to run migrations across all modules.

## Requirements

- Python 3.x
- The following Python packages are required and can be installed using `pip`:
  - `mysql-connector-python~=8.1.0`
  - `pymongo~=4.5.0`
  - `elasticsearch~=8.9.0`


