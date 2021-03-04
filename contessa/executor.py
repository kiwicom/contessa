import abc
from datetime import datetime, timedelta
import logging
from typing import Dict

import pandas as pd

from contessa.db import Connector
from contessa.models import Table


class Executor(metaclass=abc.ABCMeta):
    """
    Class that execute a rule and gives a proper kwargs to the `Rule.apply()` method.
    It also holds the `raw_df` and `filtered_df`.
    """

    date_columns = ("created_at", "updated_at", "confirmed_at")

    def __init__(self, check_table: Table, conn: Connector, context: Dict):
        self.conn = conn
        self.check_table = check_table
        self.context = context
        self._raw_df = None

    def matched_cols(self, cols):
        for col in cols:
            if col in self.date_columns:
                yield col

    @property
    def raw_df(self):
        """
        Cache raw df of temporary table we are doing quality check on.
        Casts datetime fields to python datetime.
        :return pd.Dataframe
        """
        if self._raw_df is not None:
            return self._raw_df
        self._raw_df = self.conn.get_pandas_df(
            f"select * from {self.check_table.fullname}"
        )

        # cast datetime cols to python datetime
        # pandas issue...
        for col in self.matched_cols(self._raw_df.columns):
            try:
                self._raw_df[col] = self._raw_df[col].apply(
                    lambda d: pd.to_datetime(str(d)).replace(tzinfo=None)
                )
            except:
                logging.warning(
                    f"Wrong date in `{col}` of {self.check_table.fullname}. Probably all Nones."
                )
        return self._raw_df

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

    def filter_df(self, rule):
        """
        Filter the `raw_df` to be give it to apply method. Rule attribute `time_filter` filters
        only data that were updated/created/confirmed in last 30 days.
        :return: pd.Dataframe
        """
        if rule.time_filter:
            raise NotImplementedError(
                "time_filter is not supported with PandasExecutor"
            )
        return self.raw_df

    def execute(self, rule):
        """
        Main entrypoint for Executor class. Composing kwargs specific for the executor
        (e.g. PandasExecutor) and get the results from rule.
        :param rule: Rule
        :return: pd.Series
        """
        kwargs = self.compose_kwargs(rule)
        results = rule.apply(**kwargs)
        return results

    @abc.abstractmethod
    def compose_kwargs(self, rule):
        raise NotImplementedError


class PandasExecutor(Executor):
    def compose_kwargs(self, rule):
        return {"df": self.filter_df(rule)}


class SqlExecutor(Executor):
    def compose_kwargs(self, rule):
        return {"conn": self.conn}


# singleton of executors
executors = None


def refresh_executors(check_table: Table, conn: Connector, context: Dict):
    """
    Use this to re-init the executor classes that are used to execute rules. To have right
    data from new table in Executor class.

    NOTE: This would fail if it 2 quality checks would run in 2 threads or async manner.
    Should exists a pool of executors that are mapped using schema and table_names.

    """
    global executors
    executors = {
        PandasExecutor: PandasExecutor(check_table, conn, context),
        SqlExecutor: SqlExecutor(check_table, conn, context),
    }
    logging.info("Successfully inited PandasExecutor and SqlExecutor.")


def get_executor(rule):
    """
    Return instance of Executor for a specific Rule.
    :param rule: Rule
    :return: Executor
    """
    return executors[rule.executor_cls]
