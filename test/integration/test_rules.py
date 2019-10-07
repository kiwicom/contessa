import pandas as pd
import pytest

from contessa.executor import refresh_executors, SqlExecutor
from contessa.models import Table
from contessa.rules import (
    SqlRule,
    CustomSqlRule,
    GteRule,
    GtRule,
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
        (
            GtRule("gt", "value", "value2"),
            [False, False, True, False, False],
        ),  # test another col
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
              value int,
              value2 int
            );

            insert into public.tmp_table(value, value2)
            values (1, 2), (4, 5), (5, 3), (NULL, NULL), (4, 11)
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
        (GtRule("gt", "value", 4, condition="conditional is TRUE"), [False, False]),
        (
            NotNullRule("not_null", "value", condition="conditional is TRUE"),
            [True, True],
        ),
        (GteRule("gte", "value", 4, condition="conditional is TRUE"), [False, True]),
        (NotRule("not", "value", 4, condition="conditional is TRUE"), [True, False]),
        (LtRule("lt", "value", 4, condition="conditional is TRUE"), [True, False]),
        (LteRule("lte", "value", 4, condition="conditional is TRUE"), [True, True]),
        (EqRule("eq", "value", 4, condition="conditional is TRUE"), [False, True]),
        (
            LteRule("lte", "date", "now()", condition="conditional is FALSE"),
            [False, False, True],
        ),
    ],
)
def test_one_column_rule_sql_condition(rule, expected, conn, ctx):
    conn.execute(
        """
            drop table if exists public.tmp_table;

            create table public.tmp_table(
              value int,
              conditional boolean,
              date timestamptz
            );

            insert into public.tmp_table(value, conditional, date)
            values (1, TRUE, NULL), (4, TRUE, NULL), (5, FALSE, NULL), (NULL, FALSE, NULL), (4, FALSE, '2019-10-02T13:30:00+0020')
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
        (NotRule("not", "value1", "value2"), [True, False, False]),
        (LteRule("lte", "value4", "value1"), [True, False, True]),
        (EqRule("eq", "value1", "value3"), [True, True, True]),
        (GtRule("gte", "value2", "value3"), [True, False, False]),
    ],
)
def test_cmp_with_other_col_rules(rule, expected, conn, ctx):
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


def test_new_rule(conn, ctx):
    class CountSqlRule(SqlRule):
        executor_cls = SqlExecutor

        def __init__(self, name, count, description = None, **kwargs):
            super().__init__(name, description = description, **kwargs)
            self.count = count

        def get_sql_parameters(self):
            context = super().get_sql_parameters()
            context.update({"target_count": self.count})
            return context

        @property
        def sql(self):
            return """
                SELECT COUNT(*) = {{target_count}} FROM {{table_fullname}}
            """

    conn.execute(
        """
        drop table if exists public.tmp_table;
    
        create table public.tmp_table(
          a text,
          b text
        );
    
        insert into public.tmp_table(a, b)
        values ('bts', 'abc'), ('aaa', NULL)
    """
    )

    refresh_executors(
        Table(schema_name="public", table_name="tmp_table"), conn, context=ctx
    )
    rule = CountSqlRule("count", 2)
    results = rule.apply(conn)
    expected = pd.Series([True])
    assert list(expected) == list(results)

    rule = CountSqlRule("count", 2, condition="a = 'bts'")
    results = rule.apply(conn)
    expected = pd.Series([False])
    assert list(expected) == list(results)
    conn.execute("""DROP TABLE tmp_table;""")
