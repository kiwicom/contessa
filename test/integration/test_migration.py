import unittest

from contessa.db import Connector
from test.integration.conftest import TEST_DB_URI
from contessa.models import DQBase
from contessa.migration import MigrationsResolver

DATA_QUALITY_SCHEMA = "data_quality_test"
ALEMBIC_TABLE = "alembic_version"
DATA_QUALITY_TABLE_1 = "quality_check_example_table"
DATA_QUALITY_TABLE_2 = "quality_check_another_table"
SQLALCHEMY_URL = "postgresql://postgres:postgres@postgres:5432/test_db"


def get_quality_table_creation_script(schema, table_name):
    return f"""create table {schema}.{table_name}
                    (
                        attribute text,
                        rule_name text,
                        rule_description text,
                        total_records integer,
                        failed integer,
                        median_30_day_failed double precision,
                        failed_percentage double precision,
                        passed integer,
                        median_30_day_passed double precision,
                        passed_percentage double precision,
                        status text,
                        time_filter text,
                        task_ts timestamp with time zone not null,
                        created_at timestamp with time zone default now() not null,
                        id bigserial not null
                            constraint {table_name}_pkey
                                primary key,
                        constraint {table_name}_unique_quality_check
                            unique (attribute, rule_name, task_ts, time_filter)
                    );
                """


class TestMigrationsResolverInit(unittest.TestCase):
    def setUp(self):
        """
        Init a temporary table with some data.
        """

        sql = [
            f"DROP SCHEMA IF EXISTS {DATA_QUALITY_SCHEMA} CASCADE;"
            f"CREATE SCHEMA IF NOT EXISTS {DATA_QUALITY_SCHEMA};",
            get_quality_table_creation_script(
                DATA_QUALITY_SCHEMA, DATA_QUALITY_TABLE_1
            ),
            get_quality_table_creation_script(
                DATA_QUALITY_SCHEMA, DATA_QUALITY_TABLE_2
            ),
        ]
        self.conn = Connector(TEST_DB_URI)
        for s in sql:
            self.conn.execute(s)

    def tearDown(self):
        """
        Drop all created tables.
        """
        self.conn.execute(f"DROP schema {DATA_QUALITY_SCHEMA} CASCADE;")
        DQBase.metadata.clear()

    def test_migration_table_exists_init(self):
        versions_migrations = {"0.1.4": "54f8985b0ee5", "0.1.5": "480e6618700d"}

        m = MigrationsResolver(
            versions_migrations, "0.1.4", SQLALCHEMY_URL, DATA_QUALITY_SCHEMA
        )
        migration_table_exists = m.migrations_table_exists()

        assert migration_table_exists is False

    def test_get_current_migration_init(self):
        versions_migrations = {"0.1.4": "54f8985b0ee5", "0.1.5": "480e6618700d"}

        m = MigrationsResolver(
            versions_migrations, "0.1.4", SQLALCHEMY_URL, DATA_QUALITY_SCHEMA
        )
        current = m.get_applied_migration()

        assert current is None

    def test_is_on_head_init(self):
        versions_migrations = {"0.1.4": "54f8985b0ee5", "0.1.5": "480e6618700d"}

        m = MigrationsResolver(
            versions_migrations, "0.1.4", SQLALCHEMY_URL, DATA_QUALITY_SCHEMA
        )
        is_on_head = m.is_on_head()

        is_on_head is False

    def test_get_migrations_to_head__is_before_init(self):
        versions_migrations = {
            "0.1.2": "w5rtyuret457",
            "0.1.3": "dfgdfg5b0ee5",
            "0.1.4": "54f8985b0ee5",
            "0.1.5": "480e6618700d",
            "0.1.6": "3w4er8y50yyd",
            "0.1.7": "034hfa8943hr",
        }

        m = MigrationsResolver(
            versions_migrations, "0.1.7", SQLALCHEMY_URL, DATA_QUALITY_SCHEMA
        )
        migrations = m.get_migration_to_head()
        assert migrations[0] is "upgrade"
        assert migrations[1] is "034hfa8943hr"


class TestMigrationsResolver(unittest.TestCase):
    def setUp(self):
        """
        Init a temporary table with some data.
        """

        sql = [
            f"DROP SCHEMA IF EXISTS {DATA_QUALITY_SCHEMA} CASCADE;"
            f"CREATE SCHEMA IF NOT EXISTS {DATA_QUALITY_SCHEMA};",
            get_quality_table_creation_script(
                DATA_QUALITY_SCHEMA, DATA_QUALITY_TABLE_1
            ),
            get_quality_table_creation_script(
                DATA_QUALITY_SCHEMA, DATA_QUALITY_TABLE_2
            ),
            f"""
                    create table {DATA_QUALITY_SCHEMA}.{ALEMBIC_TABLE}
                        (
                            version_num varchar(32) not null
                                constraint alembic_version_pkc
                                    primary key
                        );
                        INSERT INTO {DATA_QUALITY_SCHEMA}.{ALEMBIC_TABLE} (version_num) VALUES ('54f8985b0ee5');
                    """,
        ]
        self.conn = Connector(TEST_DB_URI)
        for s in sql:
            self.conn.execute(s)

    def tearDown(self):
        """
        Drop all created tables.
        """
        self.conn.execute(f"DROP schema data_quality_test CASCADE;")
        DQBase.metadata.clear()

    def test_schema_exists(self):
        versions_migrations = {"0.1.4": "54f8985b0ee5", "0.1.5": "480e6618700d"}

        m = MigrationsResolver(
            versions_migrations, "0.1.4", SQLALCHEMY_URL, DATA_QUALITY_SCHEMA
        )
        schema_exists = m.schema_exists()

        assert schema_exists

        m = MigrationsResolver(
            versions_migrations, "0.1.4", SQLALCHEMY_URL, "not_existing_schema"
        )
        schema_exists = m.schema_exists()

        assert schema_exists is False

    def test_migration_table_exists(self):
        versions_migrations = {"0.1.4": "54f8985b0ee5", "0.1.5": "480e6618700d"}

        m = MigrationsResolver(
            versions_migrations, "0.1.4", SQLALCHEMY_URL, DATA_QUALITY_SCHEMA
        )
        migration_table_exists = m.migrations_table_exists()

        assert migration_table_exists

    def test_get_current_migration(self):
        versions_migrations = {"0.1.4": "54f8985b0ee5", "0.1.5": "480e6618700d"}

        m = MigrationsResolver(
            versions_migrations, "0.1.4", SQLALCHEMY_URL, DATA_QUALITY_SCHEMA
        )
        current = m.get_applied_migration()

        assert current == "54f8985b0ee5"

    def test_is_on_head(self):
        versions_migrations = {"0.1.4": "54f8985b0ee5", "0.1.5": "480e6618700d"}

        m = MigrationsResolver(
            versions_migrations, "0.1.4", SQLALCHEMY_URL, DATA_QUALITY_SCHEMA
        )
        is_on_head = m.is_on_head()

        assert is_on_head

    def test_is_on_head_no_on_head(self):
        versions_migrations = {"0.1.4": "54f8985b0ee5", "0.1.5": "480e6618700d"}

        m = MigrationsResolver(
            versions_migrations, "0.1.5", SQLALCHEMY_URL, DATA_QUALITY_SCHEMA
        )
        is_on_head = m.is_on_head()

        assert is_on_head is False

    def test_is_on_head_with_fallback(self):
        versions_migrations = {"0.1.4": "54f8985b0ee5", "0.1.6": "480e6618700d"}

        m = MigrationsResolver(
            versions_migrations, "0.1.5", SQLALCHEMY_URL, DATA_QUALITY_SCHEMA
        )
        is_on_head = m.is_on_head()

        assert is_on_head

    def test_get_migrations_to_head__already_on_head(self):
        versions_migrations = {"0.1.4": "54f8985b0ee5", "0.1.5": "480e6618700d"}

        m = MigrationsResolver(
            versions_migrations, "0.1.4", SQLALCHEMY_URL, DATA_QUALITY_SCHEMA
        )
        migrations = m.get_migration_to_head()
        assert migrations is None

    def test_get_migrations_to_head__package_greather_than_map_max(self):
        versions_migrations = {"0.1.4": "54f8985b0ee5", "0.1.5": "480e6618700d"}

        m = MigrationsResolver(
            versions_migrations, "0.1.6", SQLALCHEMY_URL, DATA_QUALITY_SCHEMA
        )
        migrations = m.get_migration_to_head()
        assert migrations[0] is "upgrade"
        assert migrations[1] is "480e6618700d"

    def test_get_migrations_to_head__is_down_from_head(self):
        versions_migrations = {
            "0.1.2": "w5rtyuret457",
            "0.1.3": "dfgdfg5b0ee5",
            "0.1.4": "54f8985b0ee5",
            "0.1.5": "480e6618700d",
            "0.1.6": "3w4er8y50yyd",
            "0.1.7": "034hfa8943hr",
        }

        m = MigrationsResolver(
            versions_migrations, "0.1.7", SQLALCHEMY_URL, DATA_QUALITY_SCHEMA
        )
        migrations = m.get_migration_to_head()
        assert migrations[0] is "upgrade"
        assert migrations[1] is "034hfa8943hr"

    def test_get_migrations_to_head__is_down_from_head_with_fallback(self):
        versions_migrations = {
            "0.1.2": "w5rtyuret457",
            "0.1.3": "dfgdfg5b0ee5",
            "0.1.4": "54f8985b0ee5",
            "0.1.5": "sdfatferbvg3",
            "0.1.8": "480e6618700d",
            "0.1.9": "3w4er8y50yyd",
        }

        m = MigrationsResolver(
            versions_migrations, "0.1.7", SQLALCHEMY_URL, DATA_QUALITY_SCHEMA
        )
        migrations = m.get_migration_to_head()
        assert migrations[0] is "upgrade"
        assert migrations[1] is "sdfatferbvg3"

    def test_get_migrations_to_head__is_up_from_head(self):
        versions_migrations = {
            "0.1.2": "w5rtyuret457",
            "0.1.3": "dfgdfg5b0ee5",
            "0.1.4": "54f8985b0ee5",
            "0.1.5": "480e6618700d",
            "0.1.6": "3w4er8y50yyd",
            "0.1.7": "034hfa8943hr",
        }

        m = MigrationsResolver(
            versions_migrations, "0.1.2", SQLALCHEMY_URL, DATA_QUALITY_SCHEMA
        )
        migrations = m.get_migration_to_head()
        assert migrations[0] is "downgrade"
        assert migrations[1] is "w5rtyuret457"

    def test_get_migrations_to_head__is_up_from_head_with_fallback(self):
        versions_migrations = {
            "0.1.1": "w5rtyuret457",
            "0.1.3": "dfgdfg5b0ee5",
            "0.1.4": "54f8985b0ee5",
            "0.1.5": "480e6618700d",
            "0.1.6": "3w4er8y50yyd",
            "0.1.7": "034hfa8943hr",
        }

        m = MigrationsResolver(
            versions_migrations, "0.1.2", SQLALCHEMY_URL, DATA_QUALITY_SCHEMA
        )
        migrations = m.get_migration_to_head()
        assert migrations[0] is "downgrade"
        assert migrations[1] is "w5rtyuret457"
