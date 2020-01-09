from typing import Union, List
import logging

from sqlalchemy import create_engine, Table, UniqueConstraint
from sqlalchemy.dialects.postgresql import insert
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

    def ensure_table(self, table: Table):
        """
        Create table for given table class if it doesn't exists.
        """
        table.create(bind=self.engine, checkfirst=True)
        logging.info(f"Created table {table.name}.")

    @staticmethod
    def model2dict(obj):
        """
        Model instance dict contains all the cols, but also internal _sa_instance_state.
        """
        a = obj.__dict__.copy()
        a.pop("_sa_instance_state", None)
        return a

    def upsert(self, objs):
        """
        Insert on conflict do update.
        """
        logging.info(f"Upserting {len(objs)} results.")

        data = []
        for o in objs:
            data.append(self.model2dict(o))

        table = objs[0].__table__
        stmt = insert(table).values(data)
        conflicting_cols = get_unique_constraint_names(table)
        excluded_set = {k: getattr(stmt.excluded, k) for k in data[0].keys()}

        on_update_stmt = stmt.on_conflict_do_update(
            index_elements=conflicting_cols, set_=excluded_set
        )

        session = self.make_session()
        try:
            session.execute(on_update_stmt)
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def get_column_names(self, table_full_name: str) -> List:
        schema_query = f"""
                SELECT
                    column_name
                FROM information_schema.columns
                WHERE concat(table_schema, '.', table_name) = '{table_full_name}'
                ORDER BY ordinal_position
            """

        return [col[0] for col in self.get_records(schema_query)]


def get_unique_constraint_names(table):
    """
    Doesn't make sense if there are multiple unique constraints, as nothing indicate which one
    to pick. If there is only 1, return names of the columns.
    """
    unique_constraint = [
        u for u in table.constraints if isinstance(u, UniqueConstraint)
    ]
    if len(unique_constraint) == 0:
        return []
    elif len(unique_constraint) > 1:
        raise Exception(
            "'get_unique_constraint_names' can't be used for table with multiple contraints."
        )
    else:  # 1
        u = unique_constraint[0]
        return [c.name for c in u.columns]
