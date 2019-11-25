import pytest

import contessa
from contessa import ContessaRunner


def test_attribute_in_custom_sql_rule():
    r = [
        {
            "name": "test_rule_name",
            "type": contessa.SQL,
            "sql": "select 1",
            "column": "col_name",
            "description": "some description",
        },
        {
            "name": "test_gt_rule",
            "type": contessa.GT,
            "value": 3,
            "column": "mycol",
            "description": "desc",
        },
    ]
    rules = ContessaRunner.build_rules(r)
    assert rules[0].attribute == "col_name"


def test_no_attribute_in_custom_sql_rule():
    r = [
        {
            "name": "test_rule_name",
            "type": contessa.SQL,
            "sql": "select 1",
            "description": "some description",
        }
    ]
    with pytest.raises(TypeError):
        ContessaRunner.build_rules(r)
