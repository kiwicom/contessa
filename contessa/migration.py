# Python script that will apply the migrations up to head
from contessa.db import Connector
from packaging.version import parse as pv

ALEMBIC_TABLE = 'alembic_version'


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

        fallback_package_version = self.get_fallback_version()
        return self.versions_migrations[fallback_package_version] == current

    def get_fallback_version(self):
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

    def get_migration_to_head(self):
        if self.is_on_head():
            return None

        fallback_version = self.get_fallback_version()

        if self.migrations_table_exists() is False:
            return 'upgrade', self.versions_migrations[fallback_version]

        migrations_versions = dict(map(reversed, self.versions_migrations.items()))
        applied_migration = self.get_applied_migration()
        applied_package = migrations_versions[applied_migration]

        if pv(applied_package) < pv(fallback_version):
            return 'upgrade', self.versions_migrations[fallback_version]
        if pv(applied_package) > pv(fallback_version):
            return 'downgrade', self.versions_migrations[fallback_version]


