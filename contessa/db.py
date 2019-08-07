from typing import Union, List

from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
import pandas.io.sql as pdsql
from sqlalchemy.orm import sessionmaker


class Connector:
    """
    Wrapping sqlachemy engine. Holds some useful methods.
    """

    def __init__(self, conn_uri_or_engine: Union[str, Engine]):
        if isinstance(conn_uri_or_engine, str):
            self.engine = create_engine(conn_uri_or_engine)
        elif isinstance(conn_uri_or_engine, Engine):
            self.engine = conn_uri_or_engine
        else:
            cls_name = self.__class__.__name__
            raise ValueError(
                f"You can only pass conn str or sqlalchemy `Engine` to `{cls_name}`."
            )
        self.Session = sessionmaker(bind=self.engine)

    def make_session(self):
        return self.Session()

    def get_records(self, sql, params=None):
        """
        Just proxy with better name if used.
        """
        return self.execute(sql, params)

    def execute(self, sql: [List, str], params=None):
        """
        Execute sql, if there are some results, return them.
        """
        params = params or {}
        with self.engine.connect() as conn:
            rs = conn.execute(sql, **params)
        return rs

    def get_pandas_df(self, sql):
        return pdsql.read_sql(sql, con=self.engine)
