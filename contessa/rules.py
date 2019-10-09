import jinja2
import pandas as pd

from contessa.base_rules import Rule
from contessa.db import Connector
from contessa.executor import get_executor, SqlExecutor


class SqlRule(Rule):
    """
    Rule that executes a custom sql that is custom written.
    It can use context from Executor.get_context
    """

    executor_cls = SqlExecutor

    def get_sql_parameters(self):
        e = get_executor(self.__class__)
        return e.get_context()

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

    def apply(self, conn: Connector):
        """
        Execute a formatted sql. Check if it returns list of booleans that is needed
        to do a quality check. If yes, return pd.Series.
        :return: pd.Series
        """
        sql = self.sql_with_where
        results = [
            r for r in conn.get_records(sql)
        ]  # returns generator, so get it to memory

        is_list_of_bool = all(
            (len(r) == 1 and isinstance(r[0], (bool, type(None))) for r in results)
        )
        if not is_list_of_bool:
            raise ValueError(
                f"Your query for rule `{self.name}` does not return list of booleans or Nones."
            )
        return pd.Series([bool(r[0]) for r in results])


class OneColumnRuleSQL(SqlRule):
    executor_cls = SqlExecutor

    def __init__(self, name, column, description, **kwargs):
        if description == "" or description is None:
            raise TypeError("Description is mandatory")
        super().__init__(name, description=description, **kwargs)
        self.column = column

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
        return f"Rule {self.name} - {self.attribute} {tf}"


class CustomSqlRule(SqlRule):
    def __init__(self, name, sql, description, **kwargs):
        super().__init__(name, description=description, **kwargs)
        self.custom_sql = sql

    @property
    def sql(self):
        return self.custom_sql


class NotNullRule(OneColumnRuleSQL):
    def __init__(self, name, column, description="True when data is null.", **kwargs):
        super().__init__(name, column, description=description, **kwargs)

    @property
    def sql(self):
        return """
            SELECT {{target_column}} IS NOT NULL FROM {{table_fullname}}
        """


class GtRule(OneColumnRuleSQL):
    def __init__(
        self,
        name,
        column,
        value,
        description="True when data is greater than input value.",
        **kwargs,
    ):
        super().__init__(name, column, description=description, **kwargs)
        self.value = value

    @property
    def sql(self):
        return """
            SELECT {{target_column}} > {{value}} FROM {{table_fullname}}
        """


class GteRule(OneColumnRuleSQL):
    def __init__(
        self,
        name,
        column,
        value,
        description="True when data is greater or even than input value.",
        **kwargs,
    ):
        super().__init__(name, column, description=description, **kwargs)
        self.value = value

    @property
    def sql(self):
        return """
            SELECT {{target_column}} >= {{value}} FROM {{table_fullname}}
        """


class NotRule(OneColumnRuleSQL):
    def __init__(
        self,
        name,
        column,
        value,
        description="True when data is not input value.",
        **kwargs,
    ):
        super().__init__(name, column, description=description, **kwargs)
        self.value = value

    @property
    def sql(self):
        return """
            SELECT {{target_column}} is distinct from {{value}}
            FROM {{table_fullname}}
        """


class LtRule(OneColumnRuleSQL):
    def __init__(
        self,
        name,
        column,
        value,
        description="True when data is less than input value.",
        **kwargs,
    ):
        super().__init__(name, column, description=description, **kwargs)
        self.value = value

    @property
    def sql(self):
        return """
            SELECT {{target_column}} < {{value}} FROM {{table_fullname}}
        """


class LteRule(OneColumnRuleSQL):
    def __init__(
        self,
        name,
        column,
        value,
        description="True when data is less or even than input value.",
        **kwargs,
    ):
        super().__init__(name, column, description=description, **kwargs)
        self.value = value

    @property
    def sql(self):
        return """
            SELECT {{target_column}} <= {{value}} FROM {{table_fullname}}
        """


class EqRule(OneColumnRuleSQL):
    def __init__(
        self,
        name,
        column,
        value,
        description="True when data is the same as input value.",
        **kwargs,
    ):
        super().__init__(name, column, description=description, **kwargs)
        self.value = value

    @property
    def sql(self):
        return """
            SELECT {{target_column}} IS NOT DISTINCT FROM {{value}} FROM {{table_fullname}}
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


def get_rule_cls(name):
    aval_rules = RULES.keys()
    if name not in aval_rules:
        raise ValueError(
            f"I dont know this kind of rule - '{name}'. "
            f"Possible rules are - {list(aval_rules)}"
        )
    return RULES[name]
