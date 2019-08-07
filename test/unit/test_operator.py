import pytest

from contessa import ContessaRunner
from contessa.models import QualityCheck
from contessa.rules import GtRule, NotNullRule
from test.conftest import TEST_DB_URI


def test_build_rules(contessa):
    rules = [
        {"name": "not_null", "columns": ["a", "b", "c"], "time_filter": "created_at"}
    ]
    contessa = ContessaRunner(TEST_DB_URI)
    normalized_rules = contessa.normalize_rules(rules)
    rules = contessa.build_rules(normalized_rules)
    expected = [
        NotNullRule("not_null", "a", time_filter="created_at"),
        NotNullRule("not_null", "b", time_filter="created_at"),
        NotNullRule("not_null", "c", time_filter="created_at"),
    ]

    expected_dicts = [e.__dict__ for e in expected]
    rules_dicts = [r.__dict__ for r in rules]
    assert expected_dicts == rules_dicts


@pytest.mark.parametrize(
    "rule_def, rule_cls",
    [({"name": "not_null"}, NotNullRule), ({"name": "gt"}, GtRule)],
)
def test_pick_rule(rule_def, rule_cls):
    contessa = ContessaRunner(TEST_DB_URI)
    assert contessa.pick_rule_cls(rule_def) == rule_cls


def test_not_known_rule():
    contessa = ContessaRunner(TEST_DB_URI)
    msg = "I dont know this kind of rule .*"
    with pytest.raises(ValueError, match=msg):
        contessa.pick_rule_cls({"name": "aa"})


def test_generic_typ_qc_class(dq):
    contessa = ContessaRunner(TEST_DB_URI)
    assert contessa.get_quality_check_class("mytable").__name__ == "MytableQualityCheck"
