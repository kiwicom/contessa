from datetime import datetime

import pandas as pd

from contessa.executor import PandasExecutor, SqlExecutor
from contessa.models import Table
from contessa.rules import NotNullRule


def test_compose_kwargs_sql_executor(dummy_contessa):
    t = Table(**{"schema_name": "tmp", "table_name": "hello_world"})
    e = SqlExecutor(t, dummy_contessa.conn)
    rule = NotNullRule("not_null", "src", time_filter="created_at")
    kwargs = e.compose_kwargs(rule)
    expected = {"conn": dummy_contessa.conn}
    assert kwargs == expected


def test_compose_kwargs_pd_executor(dummy_contessa):
    t = Table(**{"schema_name": "tmp", "table_name": "hello_world"})
    e = PandasExecutor(t, dummy_contessa.conn)
    rule = NotNullRule("not_null", "src", time_filter="created_at")
    df = pd.DataFrame([{"created_at": datetime(2017, 10, 10)}])
    e.conn.get_pandas_df = lambda x: df
    kwargs = e.compose_kwargs(rule)
    expected = {"df": df}
    assert kwargs.keys() == expected.keys()
