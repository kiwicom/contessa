from contessa import ContessaRunner
from contessa.executor import refresh_executors
from contessa.models import Table
from test.utils import normalize_str
from contessa.rules import SqlRule


def test_rule_context_formatted_in_where():
    class TestRule(SqlRule):
        @property
        def sql(self):
            return "select a, b, c from {{table_fullname}}_{{ts_nodash}}"

    r = TestRule(
        name="test_rule_name",
        type="test_rule_type",
        condition="created_at >= '{{ts_nodash}}'::timestamptz - interval '10 minutes'",
        description="Greater than 0 when bags <> 0",
    )
    check_table = Table("raw", "table")
    context = ContessaRunner.get_context(check_table, {"ts_nodash": "20190101T000000"})

    # executor holds context of run, so set it
    refresh_executors(check_table, "", context)

    result = r.sql_with_where
    expected = """
		select a, b, c
		from raw.table_20190101T000000
		where created_at >= '20190101T000000'::timestamptz - interval '10 minutes'
	"""
    assert normalize_str(result) == normalize_str(expected)
