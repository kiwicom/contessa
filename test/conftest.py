from datetime import datetime, timedelta

import pandas as pd
import pytest

from contessa.rules import NotNullRule
from contessa.session import init_session

from kw_postgres_hook import KwPostgresHook


class FakedDatetime(datetime):
    @classmethod
    def now(cls, **kwargs):
        return cls(2018, 9, 12, 12, 0, 0)

    @classmethod
    def today(cls):
        return cls(2018, 9, 12, 12, 0, 0)


@pytest.fixture(scope="session")
def rule():
    return NotNullRule("not_null", "src")


@pytest.fixture(scope="session")
def results():
    return pd.Series([True, True, False, False, True], name="src")


@pytest.fixture()
def hook():
    h = KwPostgresHook("test_db")
    init_session(h.get_sqlalchemy_engine())

    schemas = ["tmp", "temporary", "data_quality", "raw"]
    create_queries = [f"create schema if not exists {s}" for s in schemas]
    drop_queries = [f"drop schema if exists {s} cascade" for s in schemas]

    h.run(create_queries)

    yield h

    h.run(drop_queries)
