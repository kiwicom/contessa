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

    def format_sql(self):
        """
        Replace some parameters in query.
        :return str, formatted sql
        """
        sql = (
            self.sql.replace("{{ ", "{")
            .replace("{{", "{")
            .replace(" }}", "}")
            .replace("}}", "}")
        )
        formatted_sql = sql.format(**self.get_sql_parameters())
        return formatted_sql

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
        return f"{self.format_sql()} {where_clause}"

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

    def __init__(self, name, column, **kwargs):
        super().__init__(name, **kwargs)
        self.column = column

    @property
    def attribute(self):
        return self.column

    def get_sql_parameters(self):
        context = super().get_sql_parameters()
        context.update({"target_column": self.column})
        return context

    def __str__(self):
        tf = f"- {self.time_filter}" or ""
        return f"Rule {self.name} - {self.attribute} {tf}"


class CustomSqlRule(SqlRule):
    def __init__(self, name, sql, description, **kwargs):
        super().__init__(name, **kwargs)
        self.custom_sql = sql
        self.description = description

    @property
    def sql(self):
        return self.custom_sql


class NotNullRule(OneColumnRuleSQL):
    description = "True when data is null."

    @property
    def sql(self):
        return f"""
            SELECT {{target_column}} IS NOT NULL FROM {{table_fullname}}
        """


class GtRule(OneColumnRuleSQL):
    description = "True when data is greater than input value."

    def __init__(self, name, column, value, **kwargs):
        super().__init__(name, column, **kwargs)
        self.value = value

    @property
    def sql(self):
        return f"""
            SELECT {{target_column}} > {self.value} FROM {{table_fullname}}
        """


class GteRule(OneColumnRuleSQL):
    description = "True when data is greater or even than input value."

    def __init__(self, name, column, value, **kwargs):
        super().__init__(name, column, **kwargs)
        self.value = value

    @property
    def sql(self):
        return f"""
            SELECT {{target_column}} >= {self.value} FROM {{table_fullname}}
        """


class NotRule(OneColumnRuleSQL):
    description = "True when data is not input value."

    def __init__(self, name, column, value, **kwargs):
        super().__init__(name, column, **kwargs)
        self.value = value

    @property
    def sql(self):
        return f"""
            SELECT {{target_column}} is distinct from {self.value}
            FROM {{table_fullname}}
        """


class LtRule(OneColumnRuleSQL):
    description = "True when data is less than input value."

    def __init__(self, name, column, value, **kwargs):
        super().__init__(name, column, **kwargs)
        self.value = value

    @property
    def sql(self):
        return f"""
            SELECT {{target_column}} < {self.value} FROM {{table_fullname}}
        """


class LteRule(OneColumnRuleSQL):
    description = "True when data is less or even than input value."

    def __init__(self, name, column, value, **kwargs):
        super().__init__(name, column, **kwargs)
        self.value = value

    @property
    def sql(self):
        return f"""
            SELECT {{target_column}} <= {self.value} FROM {{table_fullname}}
        """


class EqRule(OneColumnRuleSQL):
    description = "True when data is the same as input value."

    def __init__(self, name, column, value, **kwargs):
        super().__init__(name, column, **kwargs)
        self.value = value

    @property
    def sql(self):
        return f"""
            SELECT {{target_column}} IS NOT DISTINCT FROM {self.value} FROM {{table_fullname}}
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
