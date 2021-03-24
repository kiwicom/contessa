from datetime import datetime, timedelta
import abc
import logging
from typing import Dict

from contessa.db import Connector
from contessa.failed_examples import ExampleSelector, default_example_selector
from contessa.models import Table


class Executor(metaclass=abc.ABCMeta):
    """
    Class that execute a rule and gives a proper kwargs to the `Rule.apply()` method.
    """

    def __init__(
        self,
        check_table: Table,
        conn: Connector,
        context: Dict,
        example_selector: ExampleSelector = default_example_selector,
    ):
        self.conn = conn
        self.check_table = check_table
        self.context = context
        self.example_selector = example_selector

    def compose_where_time_filter(self, rule):
        """
        Composes WHERE statement, which filters records by time_filter`.
        Rule attribute `time_filter` filters
        only data that were updated/created/confirmed in last 30 days.
        :return: str, WHERE `time_filter` filter statement
        """
        if rule.time_filter:
            if self.context.get("task_ts"):
                rule.time_filter.now = self.context["task_ts"]
            return rule.time_filter.sql
        return ""

    def compose_where_condition(self, rule):
        """
        Composes WHERE statement, which filters records by user-provided condition with Rule attribute `condition`.
        Very simple for now, ready for possible extensions later.
        :return: str, WHERE `condition` filter statement
        """
        condition = rule.condition
        if condition:
            return condition
        else:
            return ""

    def execute(self, rule):
        """
        Main entrypoint for Executor class. Composing kwargs specific for the executor
        (e.g. SqlExecutor) and get the results from rule.
        :param rule: Rule
        :return: AggregatedResult
        """
        kwargs = self.compose_kwargs(rule)
        results = rule.apply(**kwargs)
        return results

    @abc.abstractmethod
    def compose_kwargs(self, rule):
        raise NotImplementedError


class SqlExecutor(Executor):
    def compose_kwargs(self, rule):
        return {
            "conn": self.conn,
            "example_selector": self.example_selector,
        }


# singleton of executors
executors = None


def refresh_executors(
    check_table: Table,
    conn: Connector,
    context: Dict,
    example_selector: ExampleSelector = default_example_selector,
):
    """
    Use this to re-init the executor classes that are used to execute rules. To have right
    data from new table in Executor class.

    NOTE: This would fail if it 2 quality checks would run in 2 threads or async manner.
    Should exists a pool of executors that are mapped using schema and table_names.

    """
    global executors
    executors = {
        SqlExecutor: SqlExecutor(check_table, conn, context, example_selector),
    }
    logging.info("Successfully initialized SqlExecutor.")


def get_executor(rule):
    """
    Return instance of Executor for a specific Rule.
    :param rule: Rule
    :return: Executor
    """
    return executors[rule.executor_cls]
