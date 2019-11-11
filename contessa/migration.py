# Python script that will apply the migrations up to head
from contessa.db import Connector
from packaging import version

import alembic.config
import os
import click
import contessa

ALEMBIC_TABLE = 'alembic_version'

here = os.path.dirname(os.path.abspath(__file__))
alembic_ini_path = os.path.join(here, "alembic.ini")


class MigrationsResolver:
    def __init__(self, migrations_map, package_version, url, schema):
        self.versions_migrations = migrations_map
        self.package_version = package_version
        self.url = url
        self.schema = schema
        self.conn = Connector(self.url)

    def schema_exists(self):
        result = self.conn.get_records(
            f"""
            SELECT EXISTS (
               SELECT 1
               FROM   information_schema.schemata
               WHERE  schema_name = '{self.schema}'
            );
            """)
        return result.first()[0]

    def migrations_table_exists(self):
        result = self.conn.get_records(
            f"""
            SELECT EXISTS (
               SELECT 1
               FROM   information_schema.tables
               WHERE  table_schema = '{self.schema}'
               AND    table_name = '{ALEMBIC_TABLE}'
           );
            """)
        return result.first()[0]

    def get_applied_migration(self):
        if self.migrations_table_exists() is False:
            return None
        version = self.conn.get_records(f"select * from {self.schema}.{ALEMBIC_TABLE}")
        return version.first()[0]

    def is_on_head(self):
        if self.migrations_table_exists() is False:
            return False
        current = self.get_applied_migration()
        return self.versions_migrations[self.package_version] == current

    def get_migration_to_head(self):
        if self.package_version not in self.versions_migrations.keys():
            raise Exception(f'No migration exists for the Contessa version {self.package_version}')

        if self.is_on_head():
            return None

        if self.migrations_table_exists() is False:
            return 'upgrade', self.versions_migrations[self.package_version]

        migrations_versions = dict(map(reversed, self.versions_migrations.items()))
        applied_migration = self.get_applied_migration()
        applied_package = migrations_versions[applied_migration]

        if version.parse(applied_package) < version.parse(self.package_version):
            return 'upgrade', self.versions_migrations[self.package_version]
        if version.parse(applied_package) > version.parse(self.package_version):
            return 'downgrade', self.versions_migrations[self.package_version]

    def migrate(self):
        migrations = self.get_migration_to_head()

        alembic_args = [
            "-x",
            f"sqlalchemy_url={self.url}",
            "-x",
            f"schema={self.schema}",
            "-c",
            alembic_ini_path,
            migrations[0],
            migrations[1],
        ]

        alembic.config.main(argv=alembic_args)


migration_map = {
    "0.1.4": "54f8985b0ee5",
    "0.1.5": "480e6618700d"
}

@click.command()
@click.option(
    '-u',
    '--url',
    help='Connection string to database in the form driver://user:pass@localhost/dbname.',
    required=True)
@click.option(
    '-s',
    '--schema',
    help='Schema containing tables to migrate.',
    required=True)
def main(url, schema):
    migration = MigrationsResolver(migration_map, contessa.__version__, url, schema)
    migration.migrate()


if __name__ == '__main__':
    main()
