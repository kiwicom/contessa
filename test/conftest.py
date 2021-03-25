from datetime import datetime

import pytest

from contessa.rules import NotNullRule


class FakedDatetime(datetime):
    @classmethod
    def now(cls, **kwargs):
        return cls(2018, 9, 12, 12, 0, 0)

    @classmethod
    def today(cls):
        return cls(2018, 9, 12, 12, 0, 0)


@pytest.fixture(scope="session")
def rule():
    return NotNullRule("not_null_name", "not_null", "src")


@pytest.fixture(scope="session")
def ctx():
    return {"task_ts": FakedDatetime.now(), "table_fullname": "public.tmp_table"}
