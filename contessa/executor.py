import abc
from datetime import datetime, timedelta
import logging

import pandas as pd

from contessa.db import Connector


class Executor(metaclass=abc.ABCMeta):
    """
    Class that execute a rule and gives a proper kwargs to the `Rule.apply()` method.
    It also holds the `raw_df` and `filtered_df`.
    """

    date_columns = ("created_at", "updated_at", "confirmed_at")

    def __init__(
        self,
        schema_name: str,
        dst_table_name: str,
        conn: Connector,
        task_time: datetime = None,
    ):
        self.conn = conn
        self.schema_name = schema_name

        # todo - rename
        self.dst_table_name = dst_table_name

        # todo - if None - provide default, should it be now() ?
        self.task_time = task_time

        self._raw_df = None

    def matched_cols(self, cols):
        for col in cols:
            if col in self.date_columns:
                yield col

    def context(self):
        return {
            "tmp_table_name": self.tmp_table,
            "table_name": self.table_name,
            "task_time": self.ts_nodash(),
        }

    def ts_nodash(self):
        if not self.task_time:
            return ""
        return self.task_time.strftime("%Y%m%dT%H%M%S")

    # todo - this should be solely constructed in Airflow as it is
    # todo - specific to kiwi and airflow
    @property
    def tmp_table(self):
        tmp_table_name = f"{self.dst_table_name}_{self.ts_nodash()}"
        return (
            f"{self.schema_name}.{tmp_table_name}"
            if self.schema_name
            else tmp_table_name
        )

    @property
    def table_name(self):
        return (
            f"{self.schema_name}.{self.dst_table_name}"
            if self.schema_name
            else self.dst_table_name
        )

    @property
    def raw_df(self):
        """
        Cache raw df of temporary table we are doing quality check on.
        Casts datetime fields to python datetime.
        :return pd.Dataframe
        """
        if self._raw_df is not None:
            return self._raw_df
        self._raw_df = self.conn.get_pandas_df(f"select * from {self.table_name}")

        # cast datetime cols to python datetime
        # pandas issue...
        for col in self.matched_cols(self._raw_df.columns):
            try:
                self._raw_df[col] = self._raw_df[col].apply(
                    lambda d: pd.to_datetime(str(d)).replace(tzinfo=None)
                )
            except:
                logging.warning(
                    f"Wrong date in `{col}` of {self.tmp_table}. Probably all Nones."
                )
        return self._raw_df

    def compose_where_filter(self, rule):
        """
        Composes WHERE statement, which filters records by time_filter`.
        Rule attribute `time_filter` filters
        only data that were updated/created/confirmed in last 30 days.
        :return: str, WHERE `time_filter` filter statement
        """
        time_filter_column = rule.time_filter
        if time_filter_column:
            past = (datetime.now() - timedelta(days=30)).strftime(
                "%Y-%m-%d %H:%M:%S UTC"
            )
            return f"""WHERE {time_filter_column} >= '{past}'::timestamp"""
        else:
            return ""

    def filter_df(self, rule):
        """
        Filter the `raw_df` to be give it to apply method. Rule attribute `time_filter` filters
        only data that were updated/created/confirmed in last 30 days.
        :return: pd.Dataframe
        """
        md = rule.time_filter
        if md:
            past = datetime.now() - timedelta(days=30)
            selector = self.raw_df[md] >= past
            filtered_df = self.raw_df[selector]
        else:
            filtered_df = self.raw_df
        return filtered_df

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


def refresh_executors(
    schema_name: str, dst_table_name: str, conn: Connector, task_time=None
):
    """
    Use this to re-init the executor classes that are used to execute rules. To have right
    data from new table in Executor class.

    NOTE: This would fail if it 2 quality checks would run in 2 threads or async manner.
    Should exists a pool of executors that are mapped using schema and table_names.

    """
    global executors
    executors = {
        PandasExecutor: PandasExecutor(schema_name, dst_table_name, conn, task_time),
        SqlExecutor: SqlExecutor(schema_name, dst_table_name, conn, task_time),
    }
    logging.info("Successfully inited PandasExecutor and SqlExecutor.")


def get_executor(rule):
    """
    Return instance of Executor for a specific Rule.
    :param rule: Rule
    :return: Executor
    """
    return executors[rule.executor_cls]
