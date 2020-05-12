import logging
from typing import Dict, Optional, List

import jinja2
import sqlalchemy
from datetime import datetime

from contessa.db import Connector
from contessa.models import (
    create_default_check_class,
    Table,
    ResultTable,
    ConsistencyCheck,
)
from contessa.settings import TIME_FILTER_DEFAULT
from contessa.utils import compose_where_time_filter


class ConsistencyChecker:
    """
    Checks consistency of the sync between two tables.
    """

    model_cls = ConsistencyCheck

    COUNT = "count"
    DIFF = "difference"

    def __init__(self, left_conn_uri_or_engine, right_conn_uri_or_engine=None):
        self.left_conn_uri_or_engine = left_conn_uri_or_engine
        self.left_conn = Connector(left_conn_uri_or_engine)
        if right_conn_uri_or_engine is None:
            self.right_conn_uri_or_engine = self.left_conn_uri_or_engine
            self.right_conn = self.left_conn
        else:
            self.right_conn_uri_or_engine = right_conn_uri_or_engine
            self.right_conn = Connector(right_conn_uri_or_engine)

    def run(
        self,
        method: str,
        left_check_table: Dict,
        right_check_table: Dict,
        result_table: Dict,
        columns: Optional[List[str]] = None,
        time_filter: str = TIME_FILTER_DEFAULT,
        left_custom_sql: str = None,
        right_custom_sql: str = None,
        context: Optional[Dict] = None,
    ):
        if left_custom_sql and right_custom_sql:
            if columns or time_filter:
                raise ValueError(
                    "When using custom sqls you cannot change 'columns' or 'time_filter' attribute"
                )

        left_check_table = Table(**left_check_table)
        right_check_table = Table(**right_check_table)
        result_table = ResultTable(**result_table, model_cls=self.model_cls)
        context = self.get_context(left_check_table, right_check_table, context)

        result = self.do_consistency_check(
            method,
            columns,
            time_filter,
            left_check_table,
            right_check_table,
            left_custom_sql,
            right_custom_sql,
            context,
        )

        quality_check_class = create_default_check_class(result_table)
        self.right_conn.ensure_table(quality_check_class.__table__)
        self.upsert(quality_check_class, result)

    @staticmethod
    def get_context(
        left_check_table: Table,
        right_check_table: Table,
        context: Optional[Dict] = None,
    ) -> Dict:
        """
        Construct context to pass to executors. User context overrides defaults.
        """
        ctx_defaults = {
            "left_table_fullname": left_check_table.fullname,
            "right_table_fullname": right_check_table.fullname,
            "task_ts": datetime.now(),
        }
        ctx_defaults.update(context)
        return ctx_defaults

    def do_consistency_check(
        self,
        method: str,
        columns: Optional[List[str]],
        time_filter: str,
        left_check_table: Table,
        right_check_table: Table,
        left_sql: str = None,
        right_sql: str = None,
        context: Dict = None,
    ):
        """
        Run quality check for all rules. Use `qc_cls` to construct objects that will be inserted
        afterwards.
        """
        if not left_sql or not right_sql:
            if method == self.COUNT:
                if columns:
                    column = f"count({', '.join(columns)})"
                else:
                    column = "count(*)"
            elif method == self.DIFF:
                if columns:
                    column = ", ".join(columns)
                else:
                    # List the columns explicitly in case column order of compared tables is not the same.
                    column = ", ".join(
                        sorted(
                            self.right_conn.get_column_names(right_check_table.fullname)
                        )
                    )
            else:
                raise NotImplementedError(f"Method {method} not implemented")

        if not left_sql:
            left_sql = self.construct_default_query(
                left_check_table.fullname, column, time_filter, context
            )
        left_result = self.run_query(self.left_conn, left_sql, context)
        if not right_sql:
            right_sql = self.construct_default_query(
                right_check_table.fullname, column, time_filter, context
            )
        right_result = self.run_query(self.right_conn, right_sql, context)

        return {
            "check": {"type": method, "description": "", "name": "consistency",},
            "status": "valid"
            if self.compare_results(left_result, right_result)
            else "invalid",
            "left_table_name": left_check_table.fullname,
            "right_table_name": right_check_table.fullname,
            "time_filter": time_filter,
            "context": context,
        }

    def compare_results(self, left_result, right_result):
        return set(left_result) == set(right_result)

    def construct_default_query(self, table_name, column, time_filter, context):
        time_filter = compose_where_time_filter(time_filter, context["task_ts"])
        query = f"""
            SELECT {column}
            FROM {table_name}
            {f'WHERE {time_filter}' if time_filter else ''}
        """
        return query

    def render_sql(self, sql, context):
        """
        Replace some parameters in query.
        :return str, formatted sql
        """
        t = jinja2.Template(sql)
        rendered = t.render(**context)
        return rendered

    def run_query(self, conn: Connector, query: str, context):
        query = self.render_sql(query, context)
        logging.info(query)
        result = [r._row for r in conn.get_records(query)]
        return result

    def upsert(self, dc_cls, result):
        obj = dc_cls()
        obj.init_row(**result)
        self.right_conn.upsert(
            [obj,]
        )
