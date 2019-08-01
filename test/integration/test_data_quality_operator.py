from datetime import datetime, timedelta
from test.conftest import FakedDatetime
import unittest
from unittest import mock

from plugins.platform.contessa import DataQualityOperator

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import TaskInstance
from airflow.operators.postgres_operator import PostgresOperator


class TestDataQualityOperator(unittest.TestCase):
    def setUp(self):
        """
        Init a temporary table with some data.
        """
        self.dag = DAG(
            dag_id="test_dataquality",
            start_date=datetime.now(),
            schedule_interval=timedelta(hours=1),
        )
        self.table_name = "booking_all_v2"
        now = FakedDatetime.now().isoformat().replace("-", "").replace(":", "")

        pg_op = PostgresOperator(
            task_id="pg_test",
            dag=self.dag,
            postgres_conn_id="test_db",
            sql=[
                "DROP SCHEMA if exists tmp CASCADE;",
                "DROP SCHEMA if exists data_quality CASCADE;",
                "CREATE SCHEMA IF NOT EXISTS tmp;",
                "CREATE SCHEMA IF NOT EXISTS data_quality;",
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
                    CREATE TABLE IF NOT EXISTS tmp.{self.table_name}_{now}(
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
                    INSERT INTO tmp.{self.table_name}_{now}
                        (src, dst, price, turnover_after_refunds, initial_price, created_at)
                    VALUES
                        ('BTS', NULL, 1, 100, 11, '2018-09-12T13:00:00'),
                        -- this is older than 30 days.
                        -- not in stats when time_filter = `created_at`
                        (NULL, 'PEK', 33, 1.1, 13, '2018-01-12T13:00:00'),
                        ('VIE', 'JFK', 4, 5.5, 23.4, '2018-09-11T13:00:00'),
                        ('VIE', 'VIE', 4, 0.0, 0.0, '2018-09-11T13:00:00')
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
            ],
        )
        self._run_ti(pg_op)
        self.hook = PostgresHook("test_db")

    def tearDown(self):
        """
        Drop all created tables.
        """
        self.hook.run(f"DROP schema tmp CASCADE;")
        self.hook.run(f"DROP schema data_quality CASCADE;")

    def _run_ti(self, op):
        ti = TaskInstance(task=op, execution_date=FakedDatetime.now())
        result = op.execute(ti.get_template_context())
        return result

    @mock.patch("plugins.platform.contessa.executor.datetime", FakedDatetime)
    def test_execute_tmp(self):
        sql = """
            SELECT
              CASE WHEN src = 'BTS' and dst is null THEN false ELSE true END as res
            from {{ tmp_table_name }}
        """
        rules = [
            {"name": "not_null", "column": "dst", "time_filter": "created_at"},
            {"name": "gt", "column": "price", "value": 10, "time_filter": "created_at"},
            {"name": "sql", "sql": sql, "description": "test sql rule"},
            {"name": "not_column", "column": "src", "column2": "dst"},
        ]
        dq = DataQualityOperator(
            task_id="dq_test",
            dag=self.dag,
            conn_id="test_db",
            table_name=self.table_name,
            schema_name="tmp",
            rules=rules,
        )
        self._run_ti(dq)

        rows = self.hook.get_pandas_df(
            f"""
            SELECT * from data_quality.quality_check_{self.table_name}
            order by created_at
        """
        )
        self.assertEqual(rows.shape[0], 4)

        notnull_rule = rows.loc[0]
        self.assertEqual(notnull_rule["failed"], 1)
        self.assertEqual(notnull_rule["passed"], 2)
        self.assertEqual(notnull_rule["attribute"], "dst")
        self.assertEqual(notnull_rule["turnover_after_refunds_sum"], 105.5)
        self.assertEqual(notnull_rule["initial_price_sum"], 34.4)

        gt_rule = rows.loc[1]
        self.assertEqual(gt_rule["failed"], 3)
        self.assertEqual(gt_rule["passed"], 0)
        self.assertEqual(gt_rule["attribute"], "price")
        self.assertEqual(gt_rule["turnover_after_refunds_sum"], 105.5)
        self.assertEqual(gt_rule["initial_price_sum"], 34.4)

        sql_rule = rows.loc[2]
        self.assertEqual(sql_rule["failed"], 1)
        self.assertEqual(sql_rule["passed"], 3)
        self.assertEqual(sql_rule["attribute"], None)
        self.assertEqual(sql_rule["turnover_after_refunds_sum"], 106.6)
        self.assertEqual(sql_rule["initial_price_sum"], 47.4)

        not_column_rule = rows.loc[3]
        self.assertEqual(not_column_rule["failed"], 1)
        self.assertEqual(not_column_rule["passed"], 3)
        self.assertEqual(not_column_rule["attribute"], "src")
        self.assertEqual(not_column_rule["turnover_after_refunds_sum"], 106.6)
        self.assertEqual(not_column_rule["initial_price_sum"], 47.4)

    @mock.patch("plugins.platform.contessa.executor.datetime", FakedDatetime)
    def test_execute_dst(self):
        sql = """
            SELECT
              CASE WHEN src = 'BTS' and dst is null THEN false ELSE true END as res
            from {{ dst_table_name }}
            where created_at between timestamptz '{{ts_nodash}}' and timestamptz '{{ts_nodash}}' + interval '{{dag.schedule_interval}}'
        """
        rules = [
            {"name": "not_null", "column": "dst", "time_filter": "created_at"},
            {"name": "sql", "sql": sql, "description": "test sql rule"},
        ]
        dq = DataQualityOperator(
            task_id="dq_test",
            dag=self.dag,
            conn_id="test_db",
            table_name=self.table_name,
            schema_name="tmp",
            rules=rules,
        )
        self._run_ti(dq)

        rows = self.hook.get_pandas_df(
            f"""
            SELECT * from data_quality.quality_check_{self.table_name}
            order by created_at
        """
        )
        self.assertEqual(rows.shape[0], 2)

        notnull_rule = rows.loc[0]
        self.assertEqual(notnull_rule["failed"], 1)
        self.assertEqual(notnull_rule["passed"], 2)
        self.assertEqual(notnull_rule["attribute"], "dst")
        self.assertEqual(notnull_rule["turnover_after_refunds_sum"], 105.5)
        self.assertEqual(notnull_rule["initial_price_sum"], 34.4)

        sql_rule = rows.loc[1]
        self.assertEqual(sql_rule["failed"], 1)
        self.assertEqual(sql_rule["passed"], 0)
        self.assertEqual(sql_rule["attribute"], None)
        self.assertEqual(sql_rule["turnover_after_refunds_sum"], 106.6)
        self.assertEqual(sql_rule["initial_price_sum"], 47.4)
