import pytest

from contessa import DataQualityOperator
from contessa.models import BookingQualityCheck
from contessa.rules import GtRule, NotNullRule


@pytest.fixture(scope="module")
def dq(dag):
    rules = [
        {"name": "not_null", "columns": ["a", "b", "c"], "time_filter": "created_at"}
    ]
    dq = DataQualityOperator(
        task_id="dq",
        conn_id="test_db",
        dag=dag,
        rules=rules,
        table_name="booking_all_v2",
    )
    return dq


def test_build_rules(dq):
    rules = dq.build_rules()
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
def test_pick_rule(rule_def, rule_cls, dq):
    assert dq.pick_rule_cls(rule_def) == rule_cls


def test_not_known_rule(dq):
    msg = "I dont know this kind of rule .*"
    with pytest.raises(ValueError, match=msg):
        dq.pick_rule_cls({"name": "aa"})


def test_get_quality_check_class(dq):
    assert dq.get_quality_check_class() == BookingQualityCheck


def test_generic_typ_qc_class(dq):
    dq.table_name = "mytable"
    assert dq.get_quality_check_class().__name__ == "MytableQualityCheck"
