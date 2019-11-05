from datetime import datetime, timedelta

import pandas as pd

from contessa.executor import PandasExecutor, SqlExecutor
from contessa.models import Table
from contessa.rules import NotNullRule


def test_compose_kwargs_sql_executor(dummy_contessa, ctx):
    t = Table(**{"schema_name": "tmp", "table_name": "hello_world"})
    e = SqlExecutor(t, dummy_contessa.conn, ctx)
    rule = NotNullRule("not_null_name", "not_null", "src", time_filter="created_at")
    kwargs = e.compose_kwargs(rule)
    expected = {"conn": dummy_contessa.conn}
    assert kwargs == expected


def test_compose_kwargs_sql_executor_time_filter(dummy_contessa, ctx):
    t = Table(**{"schema_name": "tmp", "table_name": "hello_world"})
    e = SqlExecutor(t, dummy_contessa.conn, ctx)

    rule = NotNullRule("not_null_name", "not_null", "src", time_filter="created_at")
    time_filter = e.compose_where_time_filter(rule)
    computed_datetime = (ctx["task_ts"] - timedelta(days=30)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    expected = f"created_at BETWEEN '{computed_datetime} UTC'::timestamptz AND '{ctx['task_ts']} UTC'::timestamptz"
    assert time_filter == expected, "time_filter is string"

    rule = NotNullRule(
        "not_null_name", "not_null", "src", time_filter=[{"column": "created_at"}]
    )
    time_filter = e.compose_where_time_filter(rule)
    computed_datetime = (ctx["task_ts"] - timedelta(days=30)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    expected = f"created_at BETWEEN '{computed_datetime} UTC'::timestamptz AND '{ctx['task_ts']} UTC'::timestamptz"
    assert time_filter == expected, "time_filter has only column"

    rule = NotNullRule(
        "not_null_name",
        "not_null",
        "src",
        time_filter=[
            {"column": "created_at", "days": 10},
            {"column": "updated_at", "days": 1},
        ],
    )
    time_filter = e.compose_where_time_filter(rule)
    computed_created = (ctx["task_ts"] - timedelta(days=10)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    computed_updated = (ctx["task_ts"] - timedelta(days=1)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    expected = (
        f"created_at BETWEEN '{computed_created} UTC'::timestamptz AND '{ctx['task_ts']} UTC'::timestamptz AND "
        f"updated_at BETWEEN '{computed_updated} UTC'::timestamptz AND '{ctx['task_ts']} UTC'::timestamptz"
    )
    assert time_filter == expected, "time_filter has 2 members"


def test_compose_kwargs_pd_executor(dummy_contessa, ctx):
    t = Table(**{"schema_name": "tmp", "table_name": "hello_world"})
    e = PandasExecutor(t, dummy_contessa.conn, ctx)
    rule = NotNullRule("not_null_name", "not_null", "src", time_filter="created_at")
    df = pd.DataFrame([{"created_at": datetime(2017, 10, 10)}])
    e.conn.get_pandas_df = lambda x: df
    kwargs = e.compose_kwargs(rule)
    expected = {"df": df}
    assert kwargs.keys() == expected.keys()
