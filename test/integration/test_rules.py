import pandas as pd
import pytest
from datetime import datetime

from contessa.executor import refresh_executors
from contessa.models import Table
from contessa.rules import (
    CustomSqlRule,
    GteRule,
    GtRule,
    NotColumnRule,
    NotNullRule,
    NotRule,
    LtRule,
    LteRule,
    EqRule,
)


@pytest.fixture(scope="module")
def df():
    return pd.DataFrame(
        [
            {"src": None, "dst": "abcd", "value": 4},
            {"src": "aa", "dst": "aa", "value": 66},
        ]
    )


@pytest.mark.parametrize(
    "rule, expected",
    [
        (GtRule("gt", "value", 4), [False, False, True, False, False]),
        (NotNullRule("not_null", "value"), [True, True, True, False, True]),
        (GteRule("gte", "value", 4), [False, True, True, False, True]),
        (NotRule("not", "value", 4), [True, False, True, True, False]),
        (LtRule("lt", "value", 4), [True, False, False, False, False]),
        (LteRule("lte", "value", 4), [True, True, False, False, True]),
        (EqRule("eq", "value", 4), [False, True, False, False, True]),
    ],
)
def test_one_column_rule_sql(rule, expected, conn, ctx):
    conn.execute(
        """
            drop table if exists public.tmp_table;

            create table public.tmp_table(
              value int
            );

            insert into public.tmp_table(value)
            values (1), (4), (5), (NULL), (4)
        """
    )
    refresh_executors(
        Table(schema_name="public", table_name="tmp_table"), conn, context=ctx
    )

    results = rule.apply(conn)
    expected = pd.Series(expected, name=rule.column)
    assert list(expected) == list(results)


@pytest.mark.parametrize(
    "rule, expected",
    [
        (NotColumnRule("not_column", "value1", "value2"), [True, False, False]),
        (NotColumnRule("not_column", "value4", "value1"), [False, True, False]),
        (NotColumnRule("not_column", "value1", "value3"), [False, False, False]),
        (NotColumnRule("not_column", "value2", "value3"), [True, False, False]),
    ],
)
def test_not_column_rule(rule, expected, conn, ctx):
    conn.execute(
        """
        drop table if exists public.tmp_table;

        create table public.tmp_table(
          value1 int,
          value2 int,
          value3 int,
          value4 int
        );

        insert into public.tmp_table(value1, value2, value3, value4)
        values (1, 2, 1, 1), (1, 1, 1, NULL), (1, 1, 1, 1)
    """
    )
    refresh_executors(
        Table(schema_name="public", table_name="tmp_table"), conn, context=ctx
    )

    results = rule.apply(conn)
    expected = pd.Series(expected, name=rule.column)
    assert list(expected) == list(results)


def test_sql_apply(conn, ctx):
    conn.execute(
        """
        drop table if exists public.tmp_table;

        create table public.tmp_table(
          src text,
          dst text
        );

        insert into public.tmp_table(src, dst)
        values ('bts', 'abc'), ('aaa', NULL)
    """
    )
    refresh_executors(
        Table(schema_name="public", table_name="tmp_table"), conn, context=ctx
    )

    sql = """
        select
        src = 'aaa'
        from {{ table_fullname }}
    """
    rule = CustomSqlRule("sql_test", sql, "example description")
    results = rule.apply(conn)
    expected = pd.Series([False, True])
    assert list(expected) == list(results)
    conn.execute("""DROP TABLE tmp_table;""")


def test_sql_apply_extra_ctx(conn, ctx):
    ctx["dst_table"] = "public.dst_table"
    conn.execute(
        """
        drop table if exists public.dst_table;

        create table public.dst_table(
          src text,
          dst text,
          created timestamptz
        );

        insert into public.dst_table(src, dst, created)
        values ('bts', 'abc', '2018-09-12T12:00:00'), ('aaa', NULL, '2018-09-12T12:00:00'), ('aaa', NULL, '2019-07-31T12:00:00')
    """
    )
    refresh_executors(Table("public", "dst_table"), conn, context=ctx)

    sql = """
        select
        src = 'aaa'
        from {{ dst_table }}
        where created between timestamptz '{{task_ts}}' and timestamptz '{{task_ts}}' + interval '60 seconds'
    """
    rule = CustomSqlRule("sql_test", sql, "example description")
    results = rule.apply(conn)
    expected = pd.Series([False, True])
    assert list(expected) == list(results)
    conn.execute("""DROP TABLE public.dst_table;""")
