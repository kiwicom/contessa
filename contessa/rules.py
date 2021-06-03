import logging
import re
from itertools import islice

import jinja2

from contessa.base_rules import Rule
from contessa.db import Connector
from contessa.executor import get_executor, SqlExecutor
from contessa.failed_examples import ExampleSelector, default_example_selector
from contessa.utils import AggregatedResult


class SqlRule(Rule):
    """
    Rule that executes a custom sql that is custom written.
    It can use context from Executor.
    """

    executor_cls = SqlExecutor
    only_failures_mode = False

    def get_sql_parameters(self):
        e = get_executor(self.__class__)
        return e.context

    @property
    def sql(self):
        """
        SQL query to perform on column.
        Can use context from Executor.
        """
        return f""

    def render_sql(self, sql):
        """
        Replace some parameters in query.
        :return str, formatted sql
        """
        t = jinja2.Template(sql)
        ctx = self.get_sql_parameters()
        rendered = t.render(**ctx)
        rendered = re.sub(r"%", "%%", rendered)
        return rendered

    @property
    def sql_with_where(self):
        """
        Adds `where` statement with time filter and/or user-defined condition to SQL statement.
        Could be tricky, you need to format your SQL query so WHERE statement fits to the end of it
        :return:
        """
        e = get_executor(SqlRule)
        where_clause = "WHERE "
        where_time_filter = e.compose_where_time_filter(self)
        where_condition = e.compose_where_condition(self)
        if where_time_filter == "" and where_condition == "":
            where_clause = ""
        elif where_time_filter != "" and where_condition != "":
            where_clause = f"{where_clause} {where_time_filter} AND {where_condition}"
        else:
            where_clause = f"{where_clause} {where_time_filter} {where_condition}"
        final_sql = f"{self.sql} {where_clause}"
        return self.render_sql(final_sql)

    def apply(
        self,
        conn: Connector,
        example_selector: ExampleSelector = default_example_selector,
    ):
        """
        Execute a formatted sql. Check if it returns column full of booleans representing validity that is needed
        to do a quality check. If yes, stream results and return aggregated results
        :return: AggregatedResult
        """
        sql = self.sql_with_where
        logging.debug(sql)

        failed = passed = total = 0
        failed_rows = set()

        with conn.engine.connect() as con:
            result = con.execution_options(stream_results=True).execute(sql)
            for row in result:
                if self.only_failures_mode:
                    failed += 1
                    failed_rows.add(tuple(row))
                else:
                    if not isinstance(row[0], bool) and not row[0] is None:
                        raise ValueError(
                            f"Your query for rule `{self.name}` of type `{self.type}` does not return list of booleans in column `valid`."
                        )
                    total += 1
                    if row[0] is True:
                        passed += 1
                    if row[0] is False:
                        failed += 1
                        failed_rows.add(tuple(islice(row.values(), 1, None)))

        failed_examples = example_selector.select_examples(failed_rows)

        return AggregatedResult(
            total_records=0 if self.only_failures_mode else total,
            failed=failed,
            passed=passed,
            failed_example=list(failed_examples),
        )


class OneColumnRuleSQL(SqlRule):
    def __init__(
        self, name, type, column, description, only_failures_mode=False, **kwargs
    ):
        if description == "" or description is None:
            raise TypeError("Description is mandatory")
        super().__init__(name, type, description=description, **kwargs)
        self.column = column
        self.only_failures_mode = only_failures_mode

    @property
    def attribute(self):
        return self.column

    def get_sql_parameters(self):
        context = super().get_sql_parameters()
        context.update({"target_column": self.column})
        if hasattr(self, "value"):
            context.update({"value": self.value})
        return context

    def __str__(self):
        tf = f"- {self.time_filter}" or ""
        return f"Rule {self.name} - {self.type} - {self.attribute} {tf}"


class CustomSqlRule(OneColumnRuleSQL):
    def __init__(self, name, type, column, sql, description, **kwargs):
        super().__init__(name, type, column, description, **kwargs)
        self.custom_sql = sql

    @property
    def sql(self):
        return self.custom_sql


class NotNullRule(OneColumnRuleSQL):
    def __init__(
        self, name, type, column, description="True when data is null.", **kwargs
    ):
        super().__init__(name, type, column, description=description, **kwargs)

    @property
    def sql(self):
        return """
            SELECT
                {{target_column}} IS NOT NULL,
                {{target_column}}
            FROM {{table_fullname}}
        """


class GtRule(OneColumnRuleSQL):
    def __init__(
        self,
        name,
        type,
        column,
        value,
        description="True when data is greater than input value.",
        **kwargs,
    ):
        super().__init__(name, type, column, description=description, **kwargs)
        self.value = value

    @property
    def sql(self):
        return """
            SELECT
                {{target_column}} > {{value}},
                {{target_column}}
            FROM {{table_fullname}}
        """


class GteRule(OneColumnRuleSQL):
    def __init__(
        self,
        name,
        type,
        column,
        value,
        description="True when data is greater or even than input value.",
        **kwargs,
    ):
        super().__init__(name, type, column, description=description, **kwargs)
        self.value = value

    @property
    def sql(self):
        return """
            SELECT
                {{target_column}} >= {{value}},
                {{target_column}}
            FROM {{table_fullname}}
        """


class NotRule(OneColumnRuleSQL):
    def __init__(
        self,
        name,
        type,
        column,
        value,
        description="True when data is not input value.",
        **kwargs,
    ):
        super().__init__(name, type, column, description=description, **kwargs)
        self.value = value

    @property
    def sql(self):
        return """
            SELECT
                {{target_column}} is distinct from {{value}},
                {{target_column}}
            FROM {{table_fullname}}
        """


class LtRule(OneColumnRuleSQL):
    def __init__(
        self,
        name,
        type,
        column,
        value,
        description="True when data is less than input value.",
        **kwargs,
    ):
        super().__init__(name, type, column, description=description, **kwargs)
        self.value = value

    @property
    def sql(self):
        return """
            SELECT
                {{target_column}} < {{value}},
                {{target_column}}
            FROM {{table_fullname}}
        """


class LteRule(OneColumnRuleSQL):
    def __init__(
        self,
        name,
        type,
        column,
        value,
        description="True when data is less or even than input value.",
        **kwargs,
    ):
        super().__init__(name, type, column, description=description, **kwargs)
        self.value = value

    @property
    def sql(self):
        return """
            SELECT
                {{target_column}} <= {{value}},
                {{target_column}}
            FROM {{table_fullname}}
        """


class EqRule(OneColumnRuleSQL):
    def __init__(
        self,
        name,
        type,
        column,
        value,
        description="True when data is the same as input value.",
        **kwargs,
    ):
        super().__init__(name, type, column, description=description, **kwargs)
        self.value = value

    @property
    def sql(self):
        return """
            SELECT
                {{target_column}} IS NOT DISTINCT FROM {{value}},
                {{target_column}}
            FROM {{table_fullname}}
        """


NOT_NULL = "not_null"
NOT_COLUMN = "not_column"
GT = "gt"
GTE = "gte"
NOT = "not"
SQL = "sql"
LT = "lt"
LTE = "lte"
EQ = "eq"

RULES = {
    NOT_NULL: NotNullRule,
    GT: GtRule,
    GTE: GteRule,
    NOT: NotRule,
    SQL: CustomSqlRule,
    LT: LtRule,
    LTE: LteRule,
    EQ: EqRule,
}


def get_rule_cls(key):
    aval_rules = RULES.keys()
    if key not in aval_rules:
        raise ValueError(
            f"I dont know this kind of rule - '{key}'. "
            f"Possible rules are - {list(aval_rules)}"
        )
    return RULES[key]
