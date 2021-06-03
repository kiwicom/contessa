import logging
from typing import Dict, Optional, List, Union

import jinja2
from datetime import datetime

from contessa.db import Connector
from contessa.failed_examples import default_example_selector, ExampleSelector
from contessa.models import (
    create_default_check_class,
    Table,
    ResultTable,
    ConsistencyCheck,
    CheckResult,
)
from contessa.time_filter import (
    TimeFilter,
    TimeFilterConjunction,
    TimeFilterColumn,
    parse_time_filter,
)
from contessa.utils import AggregatedResult


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
        result_table: Optional[Dict] = None,
        columns: Optional[List[str]] = None,
        time_filter: Optional[Union[str, List[Dict], TimeFilter]] = None,
        left_custom_sql: str = None,
        right_custom_sql: str = None,
        context: Optional[Dict] = None,
        example_selector: ExampleSelector = default_example_selector,
    ) -> Union[CheckResult, ConsistencyCheck]:
        if left_custom_sql and right_custom_sql:
            if columns or time_filter:
                raise ValueError(
                    "When using custom sqls you cannot change 'columns' or 'time_filter' attribute"
                )

        time_filter = parse_time_filter(time_filter)

        left_check_table = Table(**left_check_table)
        right_check_table = Table(**right_check_table)
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
            example_selector,
        )

        if result_table:
            result_table = ResultTable(**result_table, model_cls=self.model_cls)
            quality_check_class = create_default_check_class(result_table)
            self.right_conn.ensure_table(quality_check_class.__table__)
            self.upsert(quality_check_class, result)
            return result

        obj = CheckResult()
        obj.init_row_consistency(**result)
        return obj

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
        if context:
            ctx_defaults.update(context)
        return ctx_defaults

    def do_consistency_check(
        self,
        method: str,
        columns: Optional[List[str]],
        time_filter: Optional[TimeFilter],
        left_check_table: Table,
        right_check_table: Table,
        left_sql: str = None,
        right_sql: str = None,
        context: Dict = None,
        example_selector: ExampleSelector = default_example_selector,
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

        results = self.compare_results(
            left_result, right_result, method, example_selector
        )

        return {
            "check": {"type": method, "description": "", "name": "consistency",},
            "results": results,
            "left_table_name": left_check_table.fullname,
            "right_table_name": right_check_table.fullname,
            "time_filter": time_filter,
            "context": context,
        }

    def compare_results(self, left_result, right_result, method, example_selector):
        if method == self.COUNT:
            left_count = left_result[0][0]
            right_count = right_result[0][0]
            passed = min(left_count, right_count)
            failed = (left_count - passed) - (right_count - passed)
            return AggregatedResult(
                total_records=max(left_count, right_count),
                failed=failed,
                passed=passed,
            )

        elif method == self.DIFF:
            left_set = set(left_result)
            right_set = set(right_result)
            common = left_set.intersection(right_set)
            passed = len(common)
            failed = (len(left_set) - len(common)) + (len(right_set) - len(common))
            failed_examples = example_selector.select_examples(
                left_set.symmetric_difference(right_set)
            )
            return AggregatedResult(
                total_records=failed + passed,
                failed=failed,
                passed=passed,
                failed_example=list(failed_examples),
            )

        else:
            raise NotImplementedError(f"Method {method} not implemented")

    def construct_default_query(
        self,
        table_name: str,
        column: str,
        time_filter: Optional[TimeFilter],
        context: Dict,
    ):
        if time_filter:
            if context.get("task_ts"):
                time_filter.now = context["task_ts"]
            time_filter = time_filter.sql
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
        logging.debug(query)
        result = [tuple(r.values()) for r in conn.get_records(query)]
        return result

    def upsert(self, dc_cls, result):
        obj = dc_cls()
        obj.init_row(**result)
        self.right_conn.upsert(
            [obj,]
        )

    def construct_automatic_time_filter(
        self, left_check_table: Dict, created_at_column=None, updated_at_column=None,
    ) -> TimeFilter:
        left_check_table = Table(**left_check_table)

        if created_at_column is None and updated_at_column is None:
            raise ValueError("Automatic time filter need at least one time column")

        since_column = updated_at_column or created_at_column
        since_sql = f"SELECT min({since_column}) FROM {left_check_table.fullname}"
        logging.info(since_sql)
        since = self.left_conn.get_records(since_sql).scalar()

        return TimeFilter(
            columns=[TimeFilterColumn(since_column, since=since),],
            conjunction=TimeFilterConjunction.AND,
        )
