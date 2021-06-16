import jinja2
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
from contessa.utils import AggregatedResult


@pytest.mark.parametrize(
    "rule, expected",
    [
        (
            GtRule("gt_name", "gt", "value", "value2"),
            AggregatedResult(total_records=5, failed=3, passed=1),
        ),  # test another col
        (
            NotNullRule("not_null_name", "not_null", "value"),
            AggregatedResult(total_records=5, failed=1, passed=4),
        ),
        (
            GteRule("gte_name", "gte", "value", 4),
            AggregatedResult(total_records=5, failed=1, passed=3),
        ),
        (
            NotRule("not_name", "not", "value", 4),
            AggregatedResult(total_records=5, failed=2, passed=3),
        ),
        (
            LtRule("lt_name", "lt", "value", 4),
            AggregatedResult(total_records=5, failed=3, passed=1),
        ),
        (
            LteRule("lte_name", "lte", "value", 4),
            AggregatedResult(total_records=5, failed=1, passed=3),
        ),
        (
            EqRule("eq_name", "eq", "value", 4),
            AggregatedResult(total_records=5, failed=3, passed=2),
        ),
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
    assert (expected.total_records, expected.failed, expected.passed) == (
        results.total_records,
        results.failed,
        results.passed,
    )


@pytest.mark.parametrize(
    "rule, expected",
    [
        (
            GtRule("gt_name", "gt", "value", 4, condition="conditional is TRUE"),
            AggregatedResult(total_records=2, failed=2, passed=0),
        ),
        (
            NotNullRule(
                "not_null_name", "not_null", "value", condition="conditional is TRUE"
            ),
            AggregatedResult(total_records=2, failed=0, passed=2),
        ),
        (
            GteRule("gte_name", "gte", "value", 4, condition="conditional is TRUE"),
            AggregatedResult(total_records=2, failed=1, passed=1),
        ),
        (
            NotRule("not_name", "not", "value", 4, condition="conditional is TRUE"),
            AggregatedResult(total_records=2, failed=1, passed=1),
        ),
        (
            LtRule("lt_name", "lt", "value", 4, condition="conditional is TRUE"),
            AggregatedResult(total_records=2, failed=1, passed=1),
        ),
        (
            LteRule("lte_name", "lte", "value", 4, condition="conditional is TRUE"),
            AggregatedResult(total_records=2, failed=0, passed=2),
        ),
        (
            EqRule("eq_name", "eq", "value", 4, condition="conditional is TRUE"),
            AggregatedResult(total_records=2, failed=1, passed=1),
        ),
        (
            LteRule(
                "lte_name", "lte", "date", "now()", condition="conditional is FALSE"
            ),
            AggregatedResult(total_records=3, failed=0, passed=1),
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
    assert (expected.total_records, expected.failed, expected.passed) == (
        results.total_records,
        results.failed,
        results.passed,
    )


@pytest.mark.parametrize(
    "rule, expected",
    [
        (
            NotRule("not_name", "not", "value1", "value2"),
            AggregatedResult(total_records=3, failed=2, passed=1),
        ),
        (
            LteRule("lte_name", "lte", "value4", "value1"),
            AggregatedResult(total_records=3, failed=0, passed=2),
        ),
        (
            EqRule("eq_name", "eq", "value1", "value3"),
            AggregatedResult(total_records=3, failed=0, passed=3),
        ),
        (
            GtRule("gte_name", "gte", "value2", "value3"),
            AggregatedResult(total_records=3, failed=2, passed=1),
        ),
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
    assert (expected.total_records, expected.failed, expected.passed) == (
        results.total_records,
        results.failed,
        results.passed,
    )


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
    rule = CustomSqlRule("sql_test_name", "sql_test", "src", sql, "example description")
    results = rule.apply(conn)
    assert results.total_records == 2
    assert results.failed == 1
    assert results.passed == 1
    conn.execute("""DROP TABLE tmp_table;""")


def test_sql_apply_only_failures(conn, ctx):
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
        select src
        from {{ table_fullname }}
        where src != 'aaa'
    """
    rule = CustomSqlRule(
        "sql_test_name",
        "sql_test",
        "src",
        sql,
        "example description",
        only_failures_mode=True,
    )
    results = rule.apply(conn)
    assert results.total_records == 0
    assert results.failed == 1
    assert results.passed == 0
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
    rule = CustomSqlRule(
        "sql_test_name", "sql_test", "col1", sql, "example description"
    )
    results = rule.apply(conn)
    assert results.total_records == 2
    assert results.failed == 1
    assert results.passed == 1
    conn.execute("""DROP TABLE public.dst_table;""")


def test_new_rule(conn, ctx):
    class CountSqlRule(SqlRule):
        executor_cls = SqlExecutor

        def __init__(self, name, type, count, description=None, **kwargs):
            super().__init__(name, type, description=description, **kwargs)
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
    rule = CountSqlRule("count_name", "count", 2)
    results = rule.apply(conn)
    expected = AggregatedResult(total_records=1, failed=0, passed=1)
    assert (expected.failed, expected.passed) == (results.failed, results.passed)

    rule = CountSqlRule("count_name", "count", 2, condition="a = 'bts'")
    results = rule.apply(conn)
    expected = AggregatedResult(total_records=1, failed=1, passed=0)
    assert (expected.failed, expected.passed) == (results.failed, results.passed)
    conn.execute("""DROP TABLE tmp_table;""")


def test_sql_standard_formatting(conn, ctx):
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
        select dst
        from {{ dst_table }}
        where dst LIKE 'a%'
    """
    rule = CustomSqlRule(
        "sql_test_name",
        "sql_test",
        None,
        sql,
        "example description",
        only_failures_mode=True,
    )
    results = rule.apply(conn)
    assert results.total_records == 0  # unknown because of only_failures_mode
    assert results.failed == 1
    assert results.passed == 0


def test_sql_missing_jinja_param(conn, ctx):
    ctx["dst_table"] = "public.dst_table"
    refresh_executors(Table("public", "dst_table"), conn, context=ctx)

    sql = """
        select dst
        from {{ dst_table }}
        where dst LIKE '{{ missing_pattern }}'
    """
    rule = CustomSqlRule(
        "sql_test_name",
        "sql_test",
        None,
        sql,
        "example description",
        only_failures_mode=True,
    )

    with pytest.raises(jinja2.exceptions.UndefinedError):
        rule.apply(conn)
