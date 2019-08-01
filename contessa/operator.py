import logging

import sqlalchemy

from contessa.executor import get_executor, refresh_executors
from contessa.models import get_default_qc_class
from contessa.normalizer import RuleNormalizer
from contessa.rules import get_rule_cls
from contessa.session import init_session, make_session

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator


class DataQualityOperator(BaseOperator):
    """
    Run Data Quality check against a temporary table before load to public/raw table.
    1. Load rules by user
    2. Normalize them to be 1 rule == 1 check over 1 column and 1 time_filter
    3. Init sqlalchemy session and executors and quality check class.
    4. Create table of quality check if dont exists.
    5. Run all rules and get stats from them (number failed/passed etc., see models.QualityCheck)
    6. Save data

    :param task_id: str
    :param conn_id: str
    :param rules: list of dicts, supports various shortcuts, see RuleNormalizer
    :param table_name: str, name of the table we are trying to load.
           NOTE: not temporary one, name of tmp will be constructed in specific airflow task
    :param schema_name: str, name of schema of temporary table
    """

    special_qc_map = {"dummy": None}
    ui_color = "#f1f515"

    def __init__(
        self,
        task_id,
        conn_id,
        rules,
        table_name,
        schema_name="temporary",
        *args,
        **kwargs,
    ):
        super().__init__(task_id=task_id, *args, **kwargs)
        self.conn_id = conn_id

        # we are doing dq check on temporary tables, can be overridden, e.g. in tests
        self.schema_name = schema_name
        self.table_name = table_name
        self._rules_def = rules
        self.normalized_rules = RuleNormalizer().normalize(rules)

    def execute(self, context):
        hook = PostgresHook(self.conn_id)
        tmp_table_name = f"{self.table_name}_{context['ts_nodash']}"
        engine = hook.get_sqlalchemy_engine()
        init_session(engine)
        refresh_executors(
            self.schema_name, self.table_name, tmp_table_name, hook, context
        )
        quality_check_class = self.get_quality_check_class()
        self.ensure_table(quality_check_class, engine)

        rules = self.build_rules()
        objs = self.do_quality_check(quality_check_class, rules, context)

        self.insert(objs)

    def do_quality_check(self, dq_cls, rules, context):
        """
        Run quality check for all rules. Use `qc_cls` to construct objects that will be inserted
        afterwards.
        :param dq_cls: QualityCheck
        :param rules: list of Rule cls
        :param context: dict, airflow context
        :return: list of QualityCheck objs
        """
        ret = []
        for rule in rules:
            e = get_executor(rule)
            logging.info(f"Executing rule `{rule}`.")
            results = e.execute(rule)
            obj = dq_cls()
            obj.init_row(rule, results, context)
            ret.append(obj)
        return ret

    def insert(self, objs):
        """
        Insert QualityCheck objects using sqlalchemy. If there is integrity error, skip it.
        :param objs: list
        :return:
        """
        logging.info(f"Inserting {len(objs)} results.")
        session = make_session()
        try:
            session.add_all(objs)
            session.commit()
        except sqlalchemy.exc.IntegrityError:
            ts = objs[0].task_ts
            logging.info(
                f"This quality check ({ts}) was already done. Skipping it this time."
            )
            session.rollback()
        finally:
            session.close()

    def ensure_table(self, qc_cls, e):
        """
        Create table for QualityCheck class if it doesn't exists. E.g. quality_check_
        """
        try:
            qc_cls.__table__.create(bind=e)
            logging.info(f"Created table {qc_cls.__tablename__}.")
        except sqlalchemy.exc.ProgrammingError:
            logging.info(
                f"Table {qc_cls.__tablename__} already exists. Skipping creation."
            )

    def build_rules(self):
        """
        Construct rules classes from user definition that are dicts.
        Raises if there are bad arguments for a certain rule.
        :return: list of Rule objects
        """
        ret = []
        for rule_def in self.normalized_rules:
            rule_cls = self.pick_rule_cls(rule_def)
            try:
                r = rule_cls(**rule_def)
            except Exception as e:
                logging.error(f"For rule `{rule_cls.__name__}`. {e.args[0]}")
                raise
            else:
                ret.append(r)
        return ret

    def pick_rule_cls(self, rule_def):
        """
        Get rule class based on its name that was input by user.
        :param rule_def: dict
        :return: Rule class
        """
        return get_rule_cls(rule_def["name"])

    def get_quality_check_class(self):
        """
        QualityCheck can be different, e.g. `special_table` has specific quality_check.
        Or kind of generic one that computes number of passed/failed objects etc.
        So determine if is special or not and return the class.
        :return: QualityCheck cls
        """
        special_checks = self.special_qc_map.keys()
        if self.table_name in special_checks:
            quality_check_class = self.special_qc_map[self.table_name]
            logging.info(
                f"Using {quality_check_class.__name__} as quality check class."
            )
        else:
            quality_check_class = get_default_qc_class(self.table_name)
            logging.info("Using default QualityCheck class.")
        return quality_check_class
