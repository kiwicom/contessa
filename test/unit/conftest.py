import pytest
from sqlalchemy import create_engine

from contessa import ContessaRunner


@pytest.fixture(scope="session")
def dummy_engine():
    # should be lazy so nevermind it doesn't exists, shouldn't be used in unit tests anyway
    return create_engine("postgresql://:@postgres:5432")


@pytest.fixture(scope="session")
def dummy_contessa(dummy_engine):
    return ContessaRunner(dummy_engine)
