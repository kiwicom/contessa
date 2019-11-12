import unittest
import pytest
import contessa.migration_runner as migration

from contessa.db import Connector
from test.integration.conftest import TEST_DB_URI
from contessa.models import DQBase

DATA_QUALITY_SCHEMA = "data_quality_test"
ALEMBIC_TABLE = "alembic_version"
DATA_QUALITY_TABLE_1 = "quality_check_example_table"
DATA_QUALITY_TABLE_2 = "quality_check_another_table"
SQLALCHEMY_URL = "postgresql://postgres:postgres@postgres:5432/test_db"


def get_quality_table_creation_script_0_1_4(schema, table_name):
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


# test of migration from v 0.1.4. to 0.1.5
class TestMigrationsResolver(unittest.TestCase):
    def setUp(self):
        """
        Init a temporary table with some data.
        """

        sql = [
            f"DROP SCHEMA IF EXISTS {DATA_QUALITY_SCHEMA} CASCADE;"
            f"CREATE SCHEMA IF NOT EXISTS {DATA_QUALITY_SCHEMA};",
            get_quality_table_creation_script_0_1_4(
                DATA_QUALITY_SCHEMA, DATA_QUALITY_TABLE_1
            ),
            get_quality_table_creation_script_0_1_4(
                DATA_QUALITY_SCHEMA, DATA_QUALITY_TABLE_2
            ),
            f"""
            INSERT INTO {DATA_QUALITY_SCHEMA}.{DATA_QUALITY_TABLE_1} (attribute, rule_name, rule_description, total_records, failed, median_30_day_failed, failed_percentage, passed, median_30_day_passed, passed_percentage, status, time_filter, task_ts, created_at, id) VALUES ('src', 'not_null', 'True when data is null.', 41136, 0, null, 0, 41136, null, 100, 'valid', null, '2019-11-09 00:00:00.000000', '2019-11-12 12:41:28.391365', 75597);
            INSERT INTO {DATA_QUALITY_SCHEMA}.{DATA_QUALITY_TABLE_1} (attribute, rule_name, rule_description, total_records, failed, median_30_day_failed, failed_percentage, passed, median_30_day_passed, passed_percentage, status, time_filter, task_ts, created_at, id) VALUES ('dst', 'not_null', 'True when data is null.', 41136, 0, null, 0, 41136, null, 100, 'valid', null, '2019-11-09 00:00:00.000000', '2019-11-12 12:41:28.391365', 75598);
            INSERT INTO {DATA_QUALITY_SCHEMA}.{DATA_QUALITY_TABLE_1} (attribute, rule_name, rule_description, total_records, failed, median_30_day_failed, failed_percentage, passed, median_30_day_passed, passed_percentage, status, time_filter, task_ts, created_at, id) VALUES ('departure_time', 'not_null', 'True when data is null.', 41136, 0, null, 0, 41136, null, 100, 'valid', null, '2019-11-09 00:00:00.000000', '2019-11-12 12:41:28.391365', 75599);
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

    def test_migration_to_0_1_5(self):
        try:
            migration.main(
                ["-u", SQLALCHEMY_URL, "-s", DATA_QUALITY_SCHEMA, "-v", "0.1.5"]
            )

        except SystemExit as e:
            print(e.args[0])

        rule_type_exists_result = self.conn.get_records(
            f"""
            SELECT EXISTS (
               SELECT 1
               FROM information_schema.columns
               WHERE table_schema='{DATA_QUALITY_SCHEMA}' and
                     table_name='{DATA_QUALITY_TABLE_1}' and
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
                       WHERE table_schema='{DATA_QUALITY_SCHEMA}' and
                             table_name='{DATA_QUALITY_TABLE_1}' and
                             column_name='rule_name'
                    );
                    """
        )

        assert rule_name_exists_result.first()[0]

        rule_type_is_filled_result = self.conn.get_records(
            f"""
                SELECT COUNT(*)
                FROM data_quality_test.quality_check_example_table
                WHERE rule_type='not_null'
            """
        )

        assert rule_type_is_filled_result.first()[0] == 3
