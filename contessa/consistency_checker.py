import logging
from typing import Dict, Optional

import sqlalchemy
from datetime import datetime

from contessa.db import Connector
from contessa.models import create_default_check_class, Table, ResultTable


class ConsistencyChecker:
    """
    Checks consistency of the sync between two tables.
    """

    COUNT = "count"
    DIFF = "difference"

    def __init__(self, left_conn_uri_or_engine, right_conn_uri_or_engine=None):
        self.left_conn_uri_or_engine = left_conn_uri_or_engine
        self.left_conn = Connector(left_conn_uri_or_engine)
        if right_conn_uri_or_engine is None:
            self.right_conn_uri_or_engine = self.left_conn_uri_or_engine
            self.right_conn = self.left_conn
        else:
            self.right_conn_uri_or_engine = right_conn_uri_or_engine
            self.right_conn = Connector(right_conn_uri_or_engine)

    def run(
        self,
        check_type: str,
        left_check_table: Dict,
        right_check_table: Dict,
        result_table: Dict,
        context: Optional[Dict] = None,
    ):
        left_check_table = Table(**left_check_table)
        right_check_table = Table(**right_check_table)
        result_table = ResultTable(**result_table)
        context = self.get_context(left_check_table, right_check_table, context)

        result = self.do_consistency_check(
            check_type, left_check_table, right_check_table, context
        )

        quality_check_class = create_default_check_class(
            result_table, check_type="consistency"
        )
        self.right_conn.ensure_table(quality_check_class.__table__)
        self.insert(quality_check_class, result)

    @staticmethod
    def get_context(
        left_check_table: Table,
        right_check_table: Table,
        context: Optional[Dict] = None,
    ) -> Dict:
        """
        Construct context to pass to executors. User context overrides defaults.
        """
        ctx_defaults = {
            "left_table_fullname": left_check_table.fullname,
            "right_table_fullname": right_check_table.fullname,
            "task_ts": datetime.now(),
        }
        ctx_defaults.update(context)
        return ctx_defaults

    def do_consistency_check(
        self,
        check_type: str,
        left_check_table: Table,
        right_check_table: Table,
        context: Dict = None,
    ):
        """
        Run quality check for all rules. Use `qc_cls` to construct objects that will be inserted
        afterwards.
        """
        left_result = self.run_query(
            left_check_table.fullname, self.left_conn, check_type
        )
        right_result = self.run_query(
            right_check_table.fullname, self.right_conn, check_type
        )
        return {
            "check": {"name": check_type, "description": ""},
            "status": left_result == right_result,
            "left_table_name": left_check_table.fullname,
            "right_table_name": right_check_table.fullname,
            "context": context,
        }

    def run_query(self, table_name: str, conn: Connector, type: str):
        if type == self.COUNT:
            column = "count(*)"
        else:
            column = "*"
        query = f"""
            SELECT { column }
            FROM { table_name }
        """
        result = [r for r in conn.get_records(query)]
        return result

    def insert(self, dc_cls, result):
        """
        Insert ConsistencyCheck objects using sqlalchemy. If there is integrity error, skip it.
        """
        logging.info(f"Inserting 1 result.")
        session = self.right_conn.make_session()
        obj = dc_cls()
        obj.init_row(**result)
        try:
            session.add(obj)
            session.commit()
        except sqlalchemy.exc.IntegrityError:
            ts = result.task_ts
            logging.info(
                f"This quality check ({ts}) was already done. Skipping it this time."
            )
            session.rollback()
        finally:
            session.close()
