from datetime import datetime

import pandas as pd
import pytest

from contessa.executor import Executor
from contessa.models import Table
from contessa.rules import NotNullRule
from test.conftest import FakedDatetime


@pytest.fixture(autouse=True)
def db(conn):
    conn.execute(
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

    conn.execute("DROP table tmp.hello_world")


@pytest.fixture
def e(conn, ctx):
    class ConcreteExecutor(Executor):
        def compose_kwargs(self, rule):
            return {}

    return ConcreteExecutor(Table("tmp", "hello_world"), conn, ctx)


def test_executor_raw_df(e):
    df = e.raw_df
    expected = pd.DataFrame(
        [("a", "b", 3.0, datetime(2018, 9, 12, 13)), ("aa", "bb", 55.0, None)],
        columns=["src", "dst", "value", "created_at"],
    )
    assert df.equals(expected)


def test_executor_filter_df(e, monkeypatch):
    rule = NotNullRule("not_null_name", "not_null", "src", time_filter="created_at")
    monkeypatch.setattr("contessa.executor.datetime", FakedDatetime)
    df = e.filter_df(rule)
    expected = pd.DataFrame(
        [("a", "b", 3.0, datetime(2018, 9, 12, 13))],
        columns=["src", "dst", "value", "created_at"],
    )
    assert df.equals(expected)
