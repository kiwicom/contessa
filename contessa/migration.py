# Python script that will apply the migrations up to head
from contessa.db import Connector
from packaging.version import parse as pv

ALEMBIC_TABLE = "alembic_version"


class MigrationsResolver:
    """
    Migrations helper class for the Contessa migrations.
    """

    def __init__(self, migrations_map, package_version, url, schema):
        """
        :param migrations_map: map of package versions and their migrations.
        In form of dictionary {'0.1.4':'A', '0.1.5':'B'}
        :param package_version: the version of the package planned to be migrated
        :param url: the database url where the Alembic migration table is present or planned to be created
        :param schema: the database schema where the Alembic migration table is present or planned to be created
        """
        self.versions_migrations = migrations_map
        self.package_version = package_version
        self.url = url
        self.schema = schema
        self.conn = Connector(self.url)

    def schema_exists(self):
        """
        Check if schema with the Alembic migration table exists.
        :return: Return true if schema with the Alembic migration exists.
        """
        result = self.conn.get_records(
            f"""
            SELECT EXISTS (
               SELECT 1
               FROM   information_schema.schemata
               WHERE  schema_name = '{self.schema}'
            );
            """
        )
        return result.first()[0]

    def migrations_table_exists(self):
        """
        Check if the Alembic versions table exists.
        """
        result = self.conn.get_records(
            f"""
            SELECT EXISTS (
               SELECT 1
               FROM   information_schema.tables
               WHERE  table_schema = '{self.schema}'
               AND    table_name = '{ALEMBIC_TABLE}'
           );
            """
        )
        return result.first()[0]

    def get_applied_migration(self):
        """
        Get the current applied migration in the target schema.
        """
        if self.migrations_table_exists() is False:
            return None
        version = self.conn.get_records(f"select * from {self.schema}.{ALEMBIC_TABLE}")
        return version.first()[0]

    def is_on_head(self):
        """
        Check if the current applied migration is valid for the Contessa version.
        """
        if self.migrations_table_exists() is False:
            return False
        current = self.get_applied_migration()

        fallback_package_version = self.get_fallback_version()
        return self.versions_migrations[fallback_package_version] == current

    def get_fallback_version(self):
        """
        Get fallback version in the case for the Contessa package version do not exist migration.
        The last package version containing the migration is returned.
        """
        keys = list(self.versions_migrations.keys())
        if self.package_version in self.versions_migrations.keys():
            return self.package_version
        if pv(self.package_version) < pv(keys[0]):
            return list(self.versions_migrations.keys())[0]
        if pv(self.package_version) > pv(keys[-1]):
            return list(self.versions_migrations.keys())[-1]

        result = keys[0]
        for k in keys[1:]:
            if pv(k) <= pv(self.package_version):
                result = k
            else:
                return result

    """
    Get the migration command for alembic. Migration command is a tupple of type of migration and migration hash.
    E.g. ('upgrade', 'dfgdfg5b0ee5') or ('downgrade', 'dfgdfg5b0ee5')
    """

    def get_migration_to_head(self):
        if self.is_on_head():
            return None

        fallback_version = self.get_fallback_version()

        if self.migrations_table_exists() is False:
            return "upgrade", self.versions_migrations[fallback_version]

        migrations_versions = dict(map(reversed, self.versions_migrations.items()))
        applied_migration = self.get_applied_migration()
        applied_package = migrations_versions[applied_migration]

        if pv(applied_package) < pv(fallback_version):
            return "upgrade", self.versions_migrations[fallback_version]
        if pv(applied_package) > pv(fallback_version):
            return "downgrade", self.versions_migrations[fallback_version]
