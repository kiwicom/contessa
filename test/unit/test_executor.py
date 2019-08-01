from datetime import datetime
from test.conftest import FakedDatetime

import pandas as pd
import pytest

from contessa.executor import Executor, PandasExecutor, SqlExecutor
from contessa.rules import NotNullRule


@pytest.fixture(autouse=True)
def db(hook):
    hook.run(
        """
        create table tmp.hello_world(
          src text,
          dst text,
          value double PRECISION,
          created_at timestamptz default NULL
        );

        insert into tmp.hello_world(src, dst, value, created_at)
        values ('a', 'b', 3, '2018-09-12T13:00:00'), ('aa', 'bb', 55, NULL);
    """
    )
    yield

    hook.run("DROP table tmp.hello_world")


@pytest.fixture
def e(hook):
    class ConcreteExecutor(Executor):
        def compose_kwargs(self, rule):
            return {}

    return ConcreteExecutor("tmp", "", "hello_world", hook, {})


def test_executor_raw_df(e):
    df = e.raw_df
    expected = pd.DataFrame(
        [("a", "b", 3.0, datetime(2018, 9, 12, 13)), ("aa", "bb", 55.0, None)],
        columns=["src", "dst", "value", "created_at"],
    )
    assert df.equals(expected)


def test_executor_filter_df(e, monkeypatch):
    rule = NotNullRule("not_null", "src", time_filter="created_at")
    monkeypatch.setattr("plugins.platform.contessa.executor.datetime", FakedDatetime)
    df = e.filter_df(rule)
    expected = pd.DataFrame(
        [("a", "b", 3.0, datetime(2018, 9, 12, 13))],
        columns=["src", "dst", "value", "created_at"],
    )
    assert df.equals(expected)


def test_compose_kwargs_sql_executor(hook):
    e = SqlExecutor("tmp", "", "hello_world", hook, {})
    rule = NotNullRule("not_null", "src", time_filter="created_at")
    kwargs = e.compose_kwargs(rule)
    expected = {"hook": ""}
    assert kwargs.keys() == expected.keys()


def test_compose_kwargs_pd_executor(hook):
    e = PandasExecutor("tmp", "", "hello_world", hook, {})
    rule = NotNullRule("not_null", "src", time_filter="created_at")
    kwargs = e.compose_kwargs(rule)
    expected = {"df": ""}
    assert kwargs.keys() == expected.keys()
