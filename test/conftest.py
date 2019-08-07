from datetime import datetime

import os
import pandas as pd
import pytest

from contessa.rules import NotNullRule

from contessa.db import Connector

TEST_DB_URI = os.environ.get("TEST_DB_URI")

if not TEST_DB_URI:
    raise ValueError("To run integration test set `TEST_DB_URI` env var.")


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
def conn():
    h = Connector(TEST_DB_URI)

    schemas = ["tmp", "temporary", "data_quality", "raw"]
    create_queries = [f"create schema if not exists {s}" for s in schemas]
    drop_queries = [f"drop schema if exists {s} cascade" for s in schemas]

    for c in create_queries:
        h.execute(c)

    yield h

    for d in drop_queries:
        h.execute(d)
