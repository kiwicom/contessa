from datetime import datetime, timedelta

from contessa.executor import SqlExecutor
from contessa.failed_examples import default_example_selector
from contessa.models import Table
from contessa.rules import NotNullRule
from contessa.time_filter import TimeFilter, TimeFilterColumn, TimeFilterConjunction


def test_compose_kwargs_sql_executor(dummy_contessa, ctx):
    t = Table(**{"schema_name": "tmp", "table_name": "hello_world"})
    e = SqlExecutor(t, dummy_contessa.conn, ctx)
    rule = NotNullRule("not_null_name", "not_null", "src", time_filter="created_at")
    kwargs = e.compose_kwargs(rule)
    expected = {
        "conn": dummy_contessa.conn,
        "example_selector": default_example_selector,
    }
    assert kwargs == expected


def test_compose_kwargs_sql_executor_time_filter(dummy_contessa, ctx):
    t = Table(**{"schema_name": "tmp", "table_name": "hello_world"})
    e = SqlExecutor(t, dummy_contessa.conn, ctx)

    rule = NotNullRule("not_null_name", "not_null", "src", time_filter="created_at")
    time_filter = e.compose_where_time_filter(rule)
    computed_datetime = (ctx["task_ts"] - timedelta(days=30)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    expected = f"(created_at >= '{computed_datetime} UTC'::timestamptz AND created_at < '{ctx['task_ts']} UTC'::timestamptz)"
    assert time_filter == expected, "time_filter is string"

    rule = NotNullRule(
        "not_null_name", "not_null", "src", time_filter=[{"column": "created_at"}]
    )
    time_filter = e.compose_where_time_filter(rule)
    computed_datetime = (ctx["task_ts"] - timedelta(days=30)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    expected = f"(created_at >= '{computed_datetime} UTC'::timestamptz AND created_at < '{ctx['task_ts']} UTC'::timestamptz)"
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
        f"(created_at >= '{computed_created} UTC'::timestamptz AND created_at < '{ctx['task_ts']} UTC'::timestamptz) OR "
        f"(updated_at >= '{computed_updated} UTC'::timestamptz AND updated_at < '{ctx['task_ts']} UTC'::timestamptz)"
    )
    assert time_filter == expected, "time_filter has 2 members"


def test_direct_time_filter_usage(dummy_contessa, ctx):
    t = Table(**{"schema_name": "tmp", "table_name": "hello_world"})
    e = SqlExecutor(t, dummy_contessa.conn, ctx)

    rule = NotNullRule(
        "not_null_name",
        "not_null",
        "src",
        time_filter=TimeFilter(
            columns=[
                TimeFilterColumn("created_at", since=timedelta(days=10), until="now"),
                TimeFilterColumn("updated_at", since=timedelta(days=1)),
            ],
            conjunction=TimeFilterConjunction.AND,
        ),
    )
    time_filter = e.compose_where_time_filter(rule)
    computed_created = (ctx["task_ts"] - timedelta(days=10)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    computed_updated = (ctx["task_ts"] - timedelta(days=1)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    expected = (
        f"(created_at >= '{computed_created} UTC'::timestamptz AND created_at < '{ctx['task_ts']} UTC'::timestamptz) AND "
        f"(updated_at >= '{computed_updated} UTC'::timestamptz)"
    )
    assert time_filter == expected, "TimeFilter type can be used directly"
