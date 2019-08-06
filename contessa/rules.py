import pandas as pd

from contessa.base_rules import Rule
from contessa.db import Connector
from contessa.executor import get_executor, SqlExecutor


class SqlRule(Rule):
    """
    Rule that executes a custom sql that is written by anyone.
    It can use variable `tmp_table_name`

    :param name: str
    :param sql: str, SQL to execute
    """

    executor_cls = SqlExecutor

    def get_sql_parameters(self):
        e = get_executor(self.__class__)
        params = {"tmp_table_name": e.tmp_table, "dst_table_name": e.dst_table}
        params.update(e.context)
        return params

    @property
    def sql(self):
        """
        SQL query to perform on column.
        Can consist parameters: {{dst_table_name}}, {{tmp_table_name}} and {{target_column}}.
        :return:
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
        formated_sql = sql.format(**self.get_sql_parameters())
        return formated_sql

    @property
    def sql_with_time_filter(self):
        """
        Adds time filter to SQL statement.
        Could be tricky, you need to format your SQL query so WHERE statement fits to the end of it
        :return:
        """
        e = get_executor(SqlRule)
        where_time_filter = e.compose_where_filter(self)
        return f"{self.format_sql()} {where_time_filter}"

    def apply(self, conn: Connector):
        """
        Execute a formatted sql. Check if it returns list of booleans that is needed
        to do a quality check. If yes, return pd.Series.
        :return: pd.Series
        """
        sql = self.sql_with_time_filter
        results = conn.get_records(sql)
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
            SELECT {{target_column}} IS NOT NULL FROM {{tmp_table_name}}
        """


class GtRule(OneColumnRuleSQL):
    description = "True when data is greater than input value."

    def __init__(self, name, column, value, **kwargs):
        super().__init__(name, column, **kwargs)
        self.value = value

    @property
    def sql(self):
        return f"""
            SELECT {{target_column}} > {self.value} FROM {{tmp_table_name}}
        """


class GteRule(OneColumnRuleSQL):
    description = "True when data is greater or even than input value."

    def __init__(self, name, column, value, **kwargs):
        super().__init__(name, column, **kwargs)
        self.value = value

    @property
    def sql(self):
        return f"""
            SELECT {{target_column}} >= {self.value} FROM {{tmp_table_name}}
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
            FROM {{tmp_table_name}}
        """


class NotColumnRule(OneColumnRuleSQL):
    description = (
        "True when data in one column is different from data in another column."
    )

    def __init__(self, name, column, column2, **kwargs):
        super().__init__(name, column, **kwargs)
        self.column2 = column2

    @property
    def sql(self):
        return f"""
            SELECT {{target_column}} is distinct from {self.column2}
            FROM {{tmp_table_name}}
        """


class LtRule(OneColumnRuleSQL):
    description = "True when data is less than input value."

    def __init__(self, name, column, value, **kwargs):
        super().__init__(name, column, **kwargs)
        self.value = value

    @property
    def sql(self):
        return f"""
            SELECT {{target_column}} < {self.value} FROM {{tmp_table_name}}
        """


class LteRule(OneColumnRuleSQL):
    description = "True when data is less or even than input value."

    def __init__(self, name, column, value, **kwargs):
        super().__init__(name, column, **kwargs)
        self.value = value

    @property
    def sql(self):
        return f"""
            SELECT {{target_column}} <= {self.value} FROM {{tmp_table_name}}
        """


class EqRule(OneColumnRuleSQL):
    description = "True when data is the same as input value."

    def __init__(self, name, column, value, **kwargs):
        super().__init__(name, column, **kwargs)
        self.value = value

    @property
    def sql(self):
        return f"""
            SELECT {{target_column}} IS NOT DISTINCT FROM {self.value} FROM {{tmp_table_name}}
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
    NOT_COLUMN: NotColumnRule,
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
