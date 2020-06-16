import logging
from typing import List, Dict, Optional

from datetime import datetime

from contessa.base_rules import Rule
from contessa.db import Connector
from contessa.executor import get_executor, refresh_executors
from contessa.models import (
    create_default_check_class,
    Table,
    ResultTable,
    QualityCheck,
    CheckResult,
)
from contessa.normalizer import RuleNormalizer
from contessa.rules import get_rule_cls


class ContessaRunner:
    model_cls = QualityCheck

    def __init__(self, conn_uri_or_engine, special_qc_map=None):
        self.conn_uri_or_engine = conn_uri_or_engine
        self.conn = Connector(conn_uri_or_engine)

        # todo - allow cfg
        self.special_qc_map = special_qc_map or {}

    def run(
        self,
        raw_rules: List[Dict[str, str]],
        check_table: Dict,
        result_table: Optional[
            Dict
        ] = None,  # todo - docs for quality name, maybe defaults..
        context: Optional[Dict] = None,
    ):
        check_table = Table(**check_table)
        context = self.get_context(check_table, context)

        normalized_rules = self.normalize_rules(raw_rules)
        refresh_executors(check_table, self.conn, context)

        if result_table:
            result_table = ResultTable(**result_table, model_cls=self.model_cls)
            quality_check_class = self.get_quality_check_class(result_table)
            self.conn.ensure_table(quality_check_class.__table__)
        else:
            quality_check_class = CheckResult

        rules = self.build_rules(normalized_rules)
        objs = self.do_quality_checks(quality_check_class, rules, context)

        if result_table:
            self.conn.upsert(objs)
        return objs

    @staticmethod
    def get_context(check_table: Table, context: Optional[Dict] = None) -> Dict:
        """
        Construct context to pass to executors. User context overrides defaults.
        """
        ctx_defaults = {
            "table_fullname": check_table.fullname,
            "task_ts": datetime.now(),  # todo - is now() ok ?
        }
        if context:
            ctx_defaults.update(context)
        return ctx_defaults

    def normalize_rules(self, raw_rules):
        return RuleNormalizer.normalize(raw_rules)

    def do_quality_checks(self, dq_cls, rules: List[Rule], context: Dict = None):
        """
        Run quality check for all rules. Use `qc_cls` to construct objects that will be inserted
        afterwards.
        """
        ret = []
        for rule in rules:
            obj = self.apply_rule(context, dq_cls, rule)
            ret.append(obj)
        return ret

    def apply_rule(self, context, dq_cls, rule):
        e = get_executor(rule)
        logging.info(f"Executing rule `{rule}`.")
        results = e.execute(rule)
        obj = dq_cls()
        obj.init_row(rule, results, self.conn, context)
        return obj

    @staticmethod
    def build_rules(normalized_rules):
        """
        Construct rules classes from user definition that are dicts.
        Raises if there are bad arguments for a certain rule.
        :return: list of Rule objects
        """
        ret = []
        for rule_def in normalized_rules:
            rule_cls = ContessaRunner.pick_rule_cls(rule_def)
            try:
                r = rule_cls(**rule_def)
            except Exception as e:
                logging.error(f"For rule `{rule_cls.__name__}`. {e.args[0]}")
                raise
            else:
                ret.append(r)
        return ret

    @staticmethod
    def pick_rule_cls(rule_def):
        """
        Get rule class based on its type that was input by user.
        :param rule_def: dict
        :return: Rule class
        """
        return get_rule_cls(rule_def["type"])

    def get_quality_check_class(self, result_table: ResultTable):
        """
        QualityCheck can be different, e.g. `special_table` has specific quality_check.
        Or kind of generic one that computes number of passed/failed objects etc.
        So determine if is special or not and return the class.
        :return: QualityCheck cls
        """
        special_checks = self.special_qc_map.keys()
        if result_table.fullname in special_checks:
            quality_check_class = self.special_qc_map[result_table.fullname]
            logging.info(
                f"Using {quality_check_class.__name__} as quality check class."
            )
        else:
            quality_check_class = create_default_check_class(result_table)
            logging.info("Using default QualityCheck class.")
        return quality_check_class
