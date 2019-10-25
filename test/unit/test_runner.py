import pytest

from contessa.models import ResultTable
from contessa.rules import GtRule, NotNullRule


def test_build_rules(dummy_contessa):
    rules = [
        {"name": "not_null_name", "type": "not_null", "columns": ["a", "b", "c"], "time_filter": "created_at"}
    ]
    normalized_rules = dummy_contessa.normalize_rules(rules)
    rules = dummy_contessa.build_rules(normalized_rules)
    expected = [
        NotNullRule("not_null_name", "not_null", "a", time_filter="created_at"),
        NotNullRule("not_null_name", "not_null", "b", time_filter="created_at"),
        NotNullRule("not_null_name", "not_null", "c", time_filter="created_at"),
    ]

    expected_dicts = [e.__dict__ for e in expected]
    rules_dicts = [r.__dict__ for r in rules]
    assert expected_dicts == rules_dicts


@pytest.mark.parametrize(
    "rule_def, rule_cls",
    [({"type": "not_null"}, NotNullRule), ({"type": "gt"}, GtRule)],
)
def test_pick_rule(rule_def, rule_cls, dummy_contessa):
    assert dummy_contessa.pick_rule_cls(rule_def) == rule_cls


def test_not_known_rule(dummy_contessa):
    msg = "I dont know this kind of rule .*"
    with pytest.raises(ValueError, match=msg):
        dummy_contessa.pick_rule_cls({"type": "aa"})


def test_generic_typ_qc_class(dummy_contessa):
    assert (
        dummy_contessa.get_quality_check_class(ResultTable("tmp", "mytable")).__name__
        == "TmpQualityCheckMytable"
    )


def test_generic_typ_qc_class_no_prefix(dummy_contessa):
    assert (
        dummy_contessa.get_quality_check_class(
            ResultTable("tmp", "mytable", use_prefix=False)
        ).__name__
        == "TmpMytable"
    )
