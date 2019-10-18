from contessa.db import Connector
from contessa.models import DQBase
from test.conftest import FakedDatetime
from test.integration.conftest import TEST_DB_URI
import unittest
from unittest import mock

from contessa import ConsistencyChecker


class TestDataQualityOperator(unittest.TestCase):
    def setUp(self):
        """
        Init a temporary table with some data.
        """
        self.left_table_name = "raw_booking"
        self.right_table_name = "booking"
        self.result_table_name = "consistency_booking"
        self.ts_nodash = (
            FakedDatetime.now().isoformat().replace("-", "").replace(":", "")
        )
        self.now = FakedDatetime.now()

        sql = [
            "DROP SCHEMA if exists tmp CASCADE;",
            "DROP SCHEMA if exists data_quality CASCADE;",
            "CREATE SCHEMA IF NOT EXISTS tmp;",
            "CREATE SCHEMA IF NOT EXISTS data_quality;",
            "CREATE SCHEMA IF NOT EXISTS hello;",
            f"""
                    CREATE TABLE IF NOT EXISTS tmp.{self.left_table_name}(
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
                    CREATE TABLE IF NOT EXISTS hello.{self.right_table_name}(
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
                INSERT INTO tmp.{self.left_table_name}
                    (src, dst, price, turnover_after_refunds, initial_price, created_at)
                VALUES
                    ('BTS', NULL, 1, 100, 11, '2018-09-12T11:50:00'),
                    (NULL, 'PEK', 33, 1.1, 13, '2018-01-12T15:50:00'),
                    ('VIE', 'JFK', 4, 5.5, 23.4, '2018-09-11T11:50:00'),
                    ('VIE', 'VIE', 4, 0.0, 0.0, '2018-09-11T11:50:00')
                """,
            f"""
                INSERT INTO hello.{self.right_table_name}
                    (src, dst, price, turnover_after_refunds, initial_price, created_at)
                VALUES
                    ('BTS', NULL, 1, 100, 11, '2018-09-12T11:50:00'),
                    (NULL, 'PEK', 33, 1.1, 13, '2018-01-12T15:50:00'),
                    ('VIE', 'JFK', 4, 5.5, 23.4, '2018-09-11T11:50:00')
            """,
        ]
        self.conn = Connector(TEST_DB_URI)
        for s in sql:
            self.conn.execute(s)

        self.consistency_checker = ConsistencyChecker(TEST_DB_URI)

    def tearDown(self):
        """
        Drop all created tables.
        """
        self.conn.execute(f"DROP schema tmp CASCADE;")
        self.conn.execute(f"DROP schema data_quality CASCADE;")
        self.conn.execute(f"DROP schema hello CASCADE;")
        DQBase.metadata.clear()

    @mock.patch("contessa.executor.datetime", FakedDatetime)
    def test_execute_consistency_false(self):
        self.consistency_checker.run(
            self.consistency_checker.COUNT,
            left_check_table={"schema_name": "tmp", "table_name": self.left_table_name},
            right_check_table={
                "schema_name": "hello",
                "table_name": self.right_table_name,
            },
            result_table={
                "schema_name": "data_quality",
                "table_name": self.result_table_name,
                "use_prefix": False,
            },
            context={"task_ts": self.now},
        )
        rows = self.conn.get_pandas_df(
            f"""
            SELECT * from data_quality.{self.result_table_name}
            order by created_at
        """
        )
        self.assertEqual(rows.iloc[0]["status"], "false")

    @mock.patch("contessa.executor.datetime", FakedDatetime)
    def test_execute_consistency_true(self):
        # add missing record to the right table
        self.conn.execute(
            f"""
            INSERT INTO hello.{self.right_table_name}
                (src, dst, price, turnover_after_refunds, initial_price, created_at)
            VALUES
                ('VIE', 'VIE', 4, 0.0, 0.0, '2018-09-11T11:50:00')
        """
        )

        self.consistency_checker.run(
            self.consistency_checker.COUNT,
            left_check_table={"schema_name": "tmp", "table_name": self.left_table_name},
            right_check_table={
                "schema_name": "hello",
                "table_name": self.right_table_name,
            },
            result_table={
                "schema_name": "data_quality",
                "table_name": self.result_table_name,
                "use_prefix": False,
            },
            context={"task_ts": self.now},
        )

        rows = self.conn.get_pandas_df(
            f"""
            SELECT * from data_quality.{self.result_table_name}
            order by created_at
        """
        )
        self.assertEqual(rows.iloc[0]["status"], "true")
