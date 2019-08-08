import os

import pytest

from contessa.db import Connector

TEST_DB_URI = os.environ.get("TEST_DB_URI")

if not TEST_DB_URI:
    raise ValueError("To run integration test set `TEST_DB_URI` env var.")


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
