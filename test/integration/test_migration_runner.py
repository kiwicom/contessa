import unittest

from alembic.config import Config

import contessa.migration_runner as migration
from contessa import ConsistencyChecker
from contessa.alembic.packages_migrations import migration_map

from contessa.db import Connector
from contessa.models import TIME_FILTER_DEFAULT

from test.conftest import FakedDatetime
from test.integration.conftest import TEST_DB_URI, ALEMBIC_INI_PATH
from contessa.models import (
    DQBase,
    QualityCheck,
    ConsistencyCheck,
    ResultTable,
    Table,
)

DATA_QUALITY_SCHEMA = "data_quality_test"


class MigrationTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.conn = Connector(TEST_DB_URI)
        cls.alembic_cfg = Config(ALEMBIC_INI_PATH)

        migration_table_name = cls.alembic_cfg.get_main_option("version_table")
        cls.migration_table = Table(DATA_QUALITY_SCHEMA, migration_table_name)

    def migrate_to_latest(self):
        """
        Mock sqlalchemy version table to be migration 1 before latest.
        Then apply latest migration.
        """
        idx = list(migration_map.keys())[-2]
        before_last = migration_map[idx]

        self.conn.execute(
            f"create table {self.migration_table.fullname}(version_num text);"
        )
        self.conn.execute(
            f"insert into {self.migration_table.fullname}(version_num) values('{before_last}')"
        )

        last_version = list(migration_map.keys())[-1]
        try:
            migration.main(
                ["-u", TEST_DB_URI, "-s", DATA_QUALITY_SCHEMA, "-v", last_version, "-f"]
            )
        except SystemExit as e:
            print(e.args[0])

    def migrate_to(self, version):
        try:
            migration.main(
                ["-u", TEST_DB_URI, "-s", DATA_QUALITY_SCHEMA, "-v", version, "-f"]
            )
        except SystemExit as e:
            print(e.args[0])

    def fullname(self, table_name, cls):
        """
        Prefix schema name to table name.
        """
        return f"{DATA_QUALITY_SCHEMA}.{cls._table_prefix}_{table_name}"


def get_quality_table_creation_script_0_1_4(table: ResultTable):
    return f"""create table {table.fullname}
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
                            constraint {table.table_name}_pkey
                                primary key
                    );
                """


def get_quality_table_creation_script_0_1_4_invalid(table):
    return f"""create table {table.fullname}
                    (
                        attribute text,
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
                            constraint {table.table_name}_pkey
                                primary key
                    );
                """


class TestMigrationsFrom014to024(MigrationTestCase):
    """
    Test of migration from v 0.1.4. to 0.2.4
    """

    def setUp(self):
        """
        Init a temporary table with some data.
        """
        self.DATA_QUALITY_TABLE_1 = ResultTable(
            DATA_QUALITY_SCHEMA, "example_table", QualityCheck
        )
        self.DATA_QUALITY_TABLE_2 = ResultTable(
            DATA_QUALITY_SCHEMA, "another_table", QualityCheck
        )

        sql = [
            f"DROP SCHEMA IF EXISTS {DATA_QUALITY_SCHEMA} CASCADE;",
            f"CREATE SCHEMA IF NOT EXISTS {DATA_QUALITY_SCHEMA};",
            get_quality_table_creation_script_0_1_4(self.DATA_QUALITY_TABLE_1),
            get_quality_table_creation_script_0_1_4(self.DATA_QUALITY_TABLE_2),
            f"""
            INSERT INTO {self.DATA_QUALITY_TABLE_1.fullname} (attribute, rule_name, rule_description, total_records, failed, median_30_day_failed, failed_percentage, passed, median_30_day_passed, passed_percentage, status, time_filter, task_ts, created_at, id) VALUES ('src', 'not_null', 'True when data is null.', 41136, 0, null, 0, 41136, null, 100, 'valid', null, '2019-11-09 00:00:00.000000', '2019-11-12 12:41:28.391365', 75597);
            INSERT INTO {self.DATA_QUALITY_TABLE_1.fullname} (attribute, rule_name, rule_description, total_records, failed, median_30_day_failed, failed_percentage, passed, median_30_day_passed, passed_percentage, status, time_filter, task_ts, created_at, id) VALUES ('dst', 'not_null', 'True when data is null.', 41136, 0, null, 0, 41136, null, 100, 'valid', null, '2019-11-09 00:00:00.000000', '2019-11-12 12:41:28.391365', 75598);
            INSERT INTO {self.DATA_QUALITY_TABLE_1.fullname} (attribute, rule_name, rule_description, total_records, failed, median_30_day_failed, failed_percentage, passed, median_30_day_passed, passed_percentage, status, time_filter, task_ts, created_at, id) VALUES ('departure_time', 'not_null', 'True when data is null.', 41136, 0, null, 0, 41136, null, 100, 'valid', null, '2019-11-09 00:00:00.000000', '2019-11-12 12:41:28.391365', 75599);
            """,
        ]
        for s in sql:
            self.conn.execute(s)

    def tearDown(self):
        """
        Drop all created tables.
        """
        self.conn.execute(f"DROP schema {DATA_QUALITY_SCHEMA} CASCADE;")
        DQBase.metadata.clear()

    def test_migration_table_is_created(self):
        self.migrate_to("0.2.4")

        migration_table_exists = self.conn.get_records(
            f"""
            SELECT EXISTS (
               SELECT 1
               FROM   information_schema.tables
               WHERE  table_schema = '{self.migration_table.schema_name}'
               AND    table_name = '{self.migration_table.table_name}'
            );            
            """
        )

        assert migration_table_exists

    def test_migration_to_0_2_4(self):
        self.migrate_to("0.2.4")

        rule_type_exists_result = self.conn.get_records(
            f"""
            SELECT EXISTS (
               SELECT 1
               FROM information_schema.columns
               WHERE table_schema='{self.DATA_QUALITY_TABLE_1.schema_name}' and
                     table_name='{self.DATA_QUALITY_TABLE_1.table_name}' and
                     column_name='rule_type'
            );
            """
        )

        assert rule_type_exists_result.first()[0]

        rule_type_exists_result = self.conn.get_records(
            f"""
                    SELECT EXISTS (
                       SELECT 1
                       FROM information_schema.columns
                       WHERE table_schema='{self.DATA_QUALITY_TABLE_2.schema_name}' and
                             table_name='{self.DATA_QUALITY_TABLE_2.table_name}' and
                             column_name='rule_type'
                    );
                    """
        )

        assert rule_type_exists_result.first()[0]

        rule_name_exists_result = self.conn.get_records(
            f"""
                    SELECT EXISTS (
                       SELECT 1
                       FROM information_schema.columns
                       WHERE table_schema='{self.DATA_QUALITY_TABLE_1.schema_name}' and
                             table_name='{self.DATA_QUALITY_TABLE_1.table_name}' and
                             column_name='rule_name'
                    );
                    """
        )

        assert rule_name_exists_result.first()[0]

        rule_name_exists_result = self.conn.get_records(
            f"""
                            SELECT EXISTS (
                               SELECT 1
                               FROM information_schema.columns
                               WHERE table_schema='{self.DATA_QUALITY_TABLE_2.schema_name}' and
                                     table_name='{self.DATA_QUALITY_TABLE_2.table_name}' and
                                     column_name='rule_name'
                            );
                            """
        )

        assert rule_name_exists_result.first()[0]

        rule_type_is_filled_result = self.conn.get_records(
            f"""
                SELECT COUNT(*)
                FROM {self.DATA_QUALITY_TABLE_1.fullname}
                WHERE rule_type='not_null'
            """
        )

        assert rule_type_is_filled_result.first()[0] == 3


class TestMigrations014to024Transaction(MigrationTestCase):
    """
    Test of migration from v 0.1.4. to 0.2.4 transaction
    """

    def setUp(self):
        """
        Init a temporary table with some data.
        """
        self.DATA_QUALITY_TABLE_1 = ResultTable(
            DATA_QUALITY_SCHEMA, "example_table", QualityCheck
        )
        self.DATA_QUALITY_TABLE_2 = ResultTable(
            DATA_QUALITY_SCHEMA, "another_table", QualityCheck
        )

        sql = [
            f"DROP SCHEMA IF EXISTS {DATA_QUALITY_SCHEMA} CASCADE;"
            f"CREATE SCHEMA IF NOT EXISTS {DATA_QUALITY_SCHEMA};",
            get_quality_table_creation_script_0_1_4(self.DATA_QUALITY_TABLE_1),
            get_quality_table_creation_script_0_1_4_invalid(self.DATA_QUALITY_TABLE_2),
            f"""
            INSERT INTO {self.DATA_QUALITY_TABLE_1.fullname} (attribute, rule_name, rule_description, total_records, failed, median_30_day_failed, failed_percentage, passed, median_30_day_passed, passed_percentage, status, time_filter, task_ts, created_at, id) VALUES ('src', 'not_null', 'True when data is null.', 41136, 0, null, 0, 41136, null, 100, 'valid', null, '2019-11-09 00:00:00.000000', '2019-11-12 12:41:28.391365', 75597);
            INSERT INTO {self.DATA_QUALITY_TABLE_1.fullname} (attribute, rule_name, rule_description, total_records, failed, median_30_day_failed, failed_percentage, passed, median_30_day_passed, passed_percentage, status, time_filter, task_ts, created_at, id) VALUES ('dst', 'not_null', 'True when data is null.', 41136, 0, null, 0, 41136, null, 100, 'valid', null, '2019-11-09 00:00:00.000000', '2019-11-12 12:41:28.391365', 75598);
            INSERT INTO {self.DATA_QUALITY_TABLE_1.fullname} (attribute, rule_name, rule_description, total_records, failed, median_30_day_failed, failed_percentage, passed, median_30_day_passed, passed_percentage, status, time_filter, task_ts, created_at, id) VALUES ('departure_time', 'not_null', 'True when data is null.', 41136, 0, null, 0, 41136, null, 100, 'valid', null, '2019-11-09 00:00:00.000000', '2019-11-12 12:41:28.391365', 75599);
            """,
        ]
        for s in sql:
            self.conn.execute(s)

    def tearDown(self):
        """
        Drop all created tables.
        """
        self.conn.execute(f"DROP schema {DATA_QUALITY_SCHEMA} CASCADE;")
        DQBase.metadata.clear()

    def test_migration_to_0_2_4_transaction(self):
        try:
            self.migrate_to("0.2.4")
        except:
            pass

        rule_type_exists_result = self.conn.get_records(
            f"""
            SELECT EXISTS (
               SELECT 1
               FROM information_schema.columns
               WHERE table_schema='{self.DATA_QUALITY_TABLE_1.schema_name}' and
                     table_name='{self.DATA_QUALITY_TABLE_1.table_name}' and
                     column_name='rule_type'
            );
            """
        )

        assert rule_type_exists_result.first()[0] is False

        rule_type_exists_result = self.conn.get_records(
            f"""
                    SELECT EXISTS (
                       SELECT 1
                       FROM information_schema.columns
                       WHERE table_schema='{self.DATA_QUALITY_TABLE_2.schema_name}' and
                             table_name='{self.DATA_QUALITY_TABLE_2.table_name}' and
                             column_name='rule_type'
                    );
                    """
        )

        assert rule_type_exists_result.first()[0] is False

        rule_type_is_filled_result = self.conn.get_records(
            f"""
                SELECT COUNT(*)
                FROM {self.DATA_QUALITY_TABLE_1.fullname}
                WHERE rule_name='not_null'
            """
        )

        assert rule_type_is_filled_result.first()[0] == 3


class TestMigrationTo025(MigrationTestCase):
    def ddl_quality_check_0_2_4(self, table: ResultTable):
        return f"""create table {table.fullname}
                        (
                            attribute text,
                            rule_type text,
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
                                constraint {table.table_name}_pkey
                                    primary key
                        );
                    """

    def ddl_consistency_check_0_2_4(self, table: ResultTable):
        return f"""create table {table.fullname}
                            (
                                type text,
                                name text,
                                description text,
                                left_table text,
                                right_table text,
                                status text,
                                time_filter text,
                                task_ts timestamp with time zone not null,
                                created_at timestamp with time zone default now() not null,
                                id bigserial not null
                                    constraint {table.table_name}_pkey
                                        primary key
                            );
                        """

    def setUp(self):
        """
        Init a temporary table with some data.
        """
        self.QUALITY_TABLE_1 = ResultTable(DATA_QUALITY_SCHEMA, "table_1", QualityCheck)
        self.CONSISTENCY_TABLE_1 = ResultTable(
            DATA_QUALITY_SCHEMA, "table_2", ConsistencyCheck
        )
        sql = [
            f"DROP SCHEMA IF EXISTS {DATA_QUALITY_SCHEMA} CASCADE;",
            f"CREATE SCHEMA IF NOT EXISTS {DATA_QUALITY_SCHEMA};",
            self.ddl_quality_check_0_2_4(self.QUALITY_TABLE_1),
            self.ddl_consistency_check_0_2_4(self.CONSISTENCY_TABLE_1),
            f"""
                INSERT INTO {self.QUALITY_TABLE_1.fullname}(
                    attribute, rule_name, rule_type, rule_description, total_records,
                    time_filter, task_ts)
                VALUES('a', 'stuff', 'not_null', 'This is the rule.', 10, NULL,
                    \'{FakedDatetime.now().isoformat()}\');
            """
            f"""
                INSERT INTO {self.CONSISTENCY_TABLE_1.fullname}(
                    type, name, description, left_table, right_table, status, time_filter, task_ts
                )
                VALUES(
                    \'{ConsistencyChecker.COUNT}\', 'hello', 'aa', 'tmp.a', 'tmp.b', 'hello',
                    NULL, \'{FakedDatetime.now().isoformat()}\'
                );
            """,
        ]
        for s in sql:
            self.conn.execute(s)

    def tearDown(self):
        """
        Drop all created tables.
        """
        self.conn.execute(f"DROP schema {DATA_QUALITY_SCHEMA} CASCADE;")
        DQBase.metadata.clear()

    def _validate_migration(self, metadata, should_be):
        count = 0
        for name, is_nullable, column_default in metadata:
            count += 1
            assert should_be[name]["is_nullable"] == is_nullable
            if name == "time_filter":
                assert should_be["time_filter"]["column_default"] == column_default
        assert count == len(should_be.keys())

    def _get_metadata(self, cols, table: ResultTable):
        cols_str = ",".join((f"'{c}'" for c in cols))
        return self.conn.execute(
            f"""
            select column_name, is_nullable, column_default
            from information_schema."columns"
            where column_name in ({cols_str})
                and table_name = '{table.table_name}'
                and table_schema = '{table.schema_name}'
        """
        )

    def test_migration_upgrade_to_0_2_5(self):
        """
        NOT NULL set for - attribute, rule_name, rule_type, time_filter.
                         - type, name, left_table, right_table, time_filter
        DEFAULT set for - time_filter (TIME_FILTER_DEFAULT)
        """
        data = self.conn.get_records(
            f"select time_filter from {self.QUALITY_TABLE_1.fullname}"
        )
        assert [d[0] for d in data] == [None]

        data = self.conn.get_records(
            f"select time_filter from {self.CONSISTENCY_TABLE_1.fullname}"
        )
        assert [d[0] for d in data] == [None]

        self.migrate_to_latest()

        # quality check - attribute, rule_name, rule_type, time_filter
        metadata = self._get_metadata(
            ["attribute", "rule_name", "rule_type", "time_filter"], self.QUALITY_TABLE_1
        )
        should_be = {
            "attribute": {"is_nullable": "NO"},
            "rule_name": {"is_nullable": "NO"},
            "rule_type": {"is_nullable": "NO"},
            "time_filter": {
                "is_nullable": "NO",
                "column_default": f"'{TIME_FILTER_DEFAULT}'::text",
            },
        }
        self._validate_migration(metadata, should_be)
        data = self.conn.get_records(
            f"select time_filter from {self.QUALITY_TABLE_1.fullname}"
        )
        assert [d[0] for d in data] == [TIME_FILTER_DEFAULT]

        # consistency check - name, type, left_table, right_table, time_filter
        metadata = self._get_metadata(
            ["type", "name", "left_table", "right_table", "time_filter"],
            self.CONSISTENCY_TABLE_1,
        )
        should_be = {
            "type": {"is_nullable": "NO"},
            "name": {"is_nullable": "NO"},
            "left_table": {"is_nullable": "NO"},
            "right_table": {"is_nullable": "NO"},
            "time_filter": {
                "is_nullable": "NO",
                "column_default": f"'{TIME_FILTER_DEFAULT}'::text",
            },
        }
        self._validate_migration(metadata, should_be)
        data = self.conn.get_records(
            f"select time_filter from {self.CONSISTENCY_TABLE_1.fullname}"
        )
        assert [d[0] for d in data] == [TIME_FILTER_DEFAULT]

    def test_migration_downgrade_to_0_2_4(self):
        self.migrate_to_latest()
        self.migrate_to("0.2.4")

        # quality check
        metadata = self._get_metadata(
            ["attribute", "rule_name", "rule_type", "time_filter"], self.QUALITY_TABLE_1
        )

        should_be = {
            "attribute": {"is_nullable": "YES"},
            "rule_name": {"is_nullable": "YES"},
            "rule_type": {"is_nullable": "YES"},
            "time_filter": {"is_nullable": "YES", "column_default": None},
        }
        self._validate_migration(metadata, should_be)
        data = self.conn.get_records(
            f"select time_filter from {self.QUALITY_TABLE_1.fullname}"
        )
        assert [d[0] for d in data] == [None]

        # consistency check
        metadata = self._get_metadata(
            ["type", "name", "left_table", "right_table", "time_filter"],
            self.CONSISTENCY_TABLE_1,
        )

        should_be = {
            "type": {"is_nullable": "YES"},
            "name": {"is_nullable": "YES"},
            "left_table": {"is_nullable": "YES"},
            "right_table": {"is_nullable": "YES"},
            "time_filter": {"is_nullable": "YES", "column_default": None},
        }
        self._validate_migration(metadata, should_be)
        data = self.conn.get_records(
            f"select time_filter from {self.CONSISTENCY_TABLE_1.fullname}"
        )
        assert [d[0] for d in data] == [None]
