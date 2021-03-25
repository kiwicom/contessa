from contessa.db import Connector
from contessa.models import DQBase
from test.conftest import FakedDatetime
from test.integration.conftest import TEST_DB_URI
import unittest
from unittest import mock

from contessa import ContessaRunner


class TestDataQualityOperator(unittest.TestCase):
    def setUp(self):
        """
        Init a temporary table with some data.
        """
        self.table_name = "booking_all_v2"
        self.ts_nodash = (
            FakedDatetime.now().isoformat().replace("-", "").replace(":", "")
        )
        self.tmp_table_name = f"{self.table_name}_{self.ts_nodash}"
        self.now = FakedDatetime.now()

        sql = [
            "DROP SCHEMA if exists tmp CASCADE;",
            "DROP SCHEMA if exists data_quality CASCADE;",
            "CREATE SCHEMA IF NOT EXISTS tmp;",
            "CREATE SCHEMA IF NOT EXISTS data_quality;",
            "CREATE SCHEMA IF NOT EXISTS hello;",
            f"""
                    CREATE TABLE IF NOT EXISTS tmp.{self.table_name}(
                      id SERIAL PRIMARY KEY,
                      src text,
                      dst text,
                      price int,
                      turnover_after_refunds double precision,
                      initial_price double precision,
                      created_at timestamptz
                    )
                """,
            f"""
                    CREATE TABLE IF NOT EXISTS tmp.{self.tmp_table_name}(
                      id SERIAL PRIMARY KEY,
                      src text,
                      dst text,
                      price int,
                      turnover_after_refunds double precision,
                      initial_price double precision,
                      created_at timestamptz
                    )
                """,
            f"""
                    INSERT INTO tmp.{self.tmp_table_name}
                        (src, dst, price, turnover_after_refunds, initial_price, created_at)
                    VALUES
                        ('BTS', NULL, 1, 100, 11, '2018-09-12T11:50:00'),
                        -- this is older than 30 days.
                        -- not in stats when time_filter = `created_at`
                        (NULL, 'PEK', 33, 1.1, 13, '2018-01-12T15:50:00'),
                        ('VIE', 'JFK', 4, 5.5, 23.4, '2018-09-11T11:50:00'),
                        ('VIE', 'VIE', 4, 0.0, 0.0, '2018-09-11T11:50:00')
                """,
            f"""
                INSERT INTO tmp.{self.table_name}
                    (src, dst, price, turnover_after_refunds, initial_price, created_at)
                VALUES
                    ('BTS', NULL, 1, 100, 11, '2018-09-12T13:00:00'),
                    -- this is older than 30 days.
                    -- not in stats when time_filter = `created_at`
                    (NULL, 'PEK', 33, 1.1, 13, '2018-01-12T13:00:00'),
                    ('VIE', 'JFK', 4, 5.5, 23.4, '2018-09-11T13:00:00'),
                    ('VIE', 'VIE', 4, 0.0, 0.0, '2018-09-11T13:00:00')
            """,
        ]
        self.conn = Connector(TEST_DB_URI)
        for s in sql:
            self.conn.execute(s)

        self.contessa_runner = ContessaRunner(TEST_DB_URI)

    def tearDown(self):
        """
        Drop all created tables.
        """
        self.conn.execute(f"DROP schema tmp CASCADE;")
        self.conn.execute(f"DROP schema data_quality CASCADE;")
        self.conn.execute(f"DROP schema hello CASCADE;")
        DQBase.metadata.clear()

    @mock.patch("contessa.executor.datetime", FakedDatetime)
    def test_execute_tmp(self):
        sql = """
            SELECT
              CASE WHEN src = 'BTS' and dst is null THEN false ELSE true END as res
            from {{ table_fullname }}
        """
        rules = [
            {
                "name": "not_null_name",
                "type": "not_null",
                "column": "dst",
                "time_filter": "created_at",
            },
            {
                "name": "gt_name",
                "type": "gt",
                "column": "price",
                "value": 10,
                "time_filter": "created_at",
            },
            {
                "name": "sql_name",
                "type": "sql",
                "sql": sql,
                "column": "src_dst",
                "description": "test sql rule",
            },
            {"name": "not_name", "type": "not", "column": "src", "value": "dst"},
        ]
        self.contessa_runner.run(
            check_table={"schema_name": "tmp", "table_name": self.tmp_table_name},
            result_table={"schema_name": "data_quality", "table_name": self.table_name},
            raw_rules=rules,
            context={"task_ts": self.now},
        )

        rows = self.conn.get_records(
            f"""
            SELECT * from data_quality.quality_check_{self.table_name}
            order by created_at
        """
        ).fetchall()
        self.assertEqual(len(rows), 4)

        notnull_rule = rows[0]
        self.assertEqual(notnull_rule["failed"], 1)
        self.assertEqual(notnull_rule["passed"], 2)
        self.assertEqual(notnull_rule["attribute"], "dst")
        self.assertEqual(notnull_rule["task_ts"].timestamp(), self.now.timestamp())

        gt_rule = rows[1]
        self.assertEqual(gt_rule["failed"], 3)
        self.assertEqual(gt_rule["passed"], 0)
        self.assertEqual(gt_rule["attribute"], "price")

        sql_rule = rows[2]
        self.assertEqual(sql_rule["failed"], 1)
        self.assertEqual(sql_rule["passed"], 3)
        self.assertEqual(sql_rule["attribute"], "src_dst")

        not_column_rule = rows[3]
        self.assertEqual(not_column_rule["failed"], 1)
        self.assertEqual(not_column_rule["passed"], 3)
        self.assertEqual(not_column_rule["attribute"], "src")

    @mock.patch("contessa.executor.datetime", FakedDatetime)
    def test_execute_dst(self):
        sql = """
            SELECT
              CASE WHEN src = 'BTS' AND dst is null THEN false ELSE true END as res
            FROM {{ table_fullname }}
            WHERE created_at BETWEEN
                timestamptz '{{task_ts}}' - INTERVAL '1 hour'
                AND
                timestamptz '{{task_ts}}' + INTERVAL '1 hour'
        """
        rules = [
            {
                "name": "not_null_name",
                "type": "not_null",
                "column": "dst",
                "time_filter": "created_at",
            },
            {
                "name": "sql_name",
                "type": "sql",
                "sql": sql,
                "column": "src_dst",
                "description": "test sql rule",
            },
        ]
        self.contessa_runner.run(
            check_table={"schema_name": "tmp", "table_name": self.tmp_table_name},
            result_table={"schema_name": "data_quality", "table_name": self.table_name},
            raw_rules=rules,
            context={"task_ts": self.now},
        )

        rows = self.conn.get_records(
            f"""
            SELECT * from data_quality.quality_check_{self.table_name}
            order by created_at
        """
        ).fetchall()
        self.assertEqual(len(rows), 2)

        notnull_rule = rows[0]
        self.assertEqual(notnull_rule["failed"], 1)
        self.assertEqual(notnull_rule["passed"], 2)
        self.assertEqual(notnull_rule["attribute"], "dst")
        self.assertEqual(notnull_rule["task_ts"].timestamp(), self.now.timestamp())

        sql_rule = rows[1]
        self.assertEqual(sql_rule["failed"], 1)
        self.assertEqual(sql_rule["passed"], 0)
        self.assertEqual(sql_rule["attribute"], "src_dst")

    def test_different_schema(self):
        rules = [
            {
                "name": "not_nul_name",
                "type": "not_null",
                "column": "dst",
                "time_filter": "created_at",
            }
        ]
        self.contessa_runner.run(
            check_table={"schema_name": "tmp", "table_name": self.tmp_table_name},
            result_table={"schema_name": "hello", "table_name": "abcde"},
            raw_rules=rules,
            context={"task_ts": self.now},
        )
        rows = self.conn.get_records(
            f"""
                SELECT 1 from hello.quality_check_abcde
            """
        ).fetchall()
        self.assertEqual(len(rows), 1)
