import pandas as pd
import pytest
from datetime import datetime

from contessa.executor import refresh_executors
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
def test_one_column_rule_sql(rule, expected, conn):
    conn.execute(
        """
            drop table if exists tmp_table;

            create table tmp_table(
              value int
            );

            insert into tmp_table(value)
            values (1), (4), (5), (NULL), (4)
        """
    )
    refresh_executors("", "tmp_table", conn)

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
def test_not_column_rule(rule, expected, conn):
    conn.execute(
        """
        drop table if exists tmp_table;

        create table tmp_table(
          value1 int,
          value2 int,
          value3 int,
          value4 int
        );

        insert into tmp_table(value1, value2, value3, value4)
        values (1, 2, 1, 1), (1, 1, 1, NULL), (1, 1, 1, 1)
    """
    )
    refresh_executors("", "tmp_table", conn)

    results = rule.apply(conn)
    expected = pd.Series(expected, name=rule.column)
    assert list(expected) == list(results)


def test_sql_apply(conn):
    conn.execute(
        """
        drop table if exists tmp_table;

        create table tmp_table(
          src text,
          dst text
        );

        insert into tmp_table(src, dst)
        values ('bts', 'abc'), ('aaa', NULL)
    """
    )
    refresh_executors("", "tmp_table", conn)

    sql = """
        select
        src = 'aaa'
        from {{tmp_table_name}}
    """
    rule = CustomSqlRule("sql_test", sql, "example description")
    results = rule.apply(conn)
    expected = pd.Series([False, True])
    assert list(expected) == list(results)
    conn.execute("""DROP TABLE tmp_table;""")


def test_sql_apply_destination(conn):
    conn.execute(
        """
        drop table if exists dst_table;

        create table dst_table(
          src text,
          dst text,
          created timestamptz
        );

        insert into dst_table(src, dst, created)
        values ('bts', 'abc', '2019-07-31T11:00:00'), ('aaa', NULL, '2019-07-31T11:00:00'), ('aaa', NULL, '2019-07-31T12:00:00')
    """
    )
    refresh_executors("", "dst_table", conn, datetime(2019, 7, 31, 11, 0, 0))

    sql = """
        select
        src = 'aaa'
        from {{dst_table_name}}
        where created between timestamptz '{{task_time}}' and timestamptz '{{task_time}}' + interval '60 seconds'
    """
    rule = CustomSqlRule("sql_test", sql, "example description")
    results = rule.apply(conn)
    expected = pd.Series([False, True])
    assert list(expected) == list(results)
    conn.execute("""DROP TABLE dst_table;""")
