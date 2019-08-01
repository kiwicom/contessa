import operator

import pytest

from contessa.normalizer import RuleNormalizer


@pytest.mark.parametrize(
    "rules_def, normalized",
    [
        (
            [
                {
                    "name": "not_null",
                    "columns": ["a", "b", "c"],
                    "time_filters": ["c", "u"],
                }
            ],
            [
                {"name": "not_null", "column": "a", "time_filter": "c"},
                {"name": "not_null", "column": "b", "time_filter": "c"},
                {"name": "not_null", "column": "c", "time_filter": "c"},
                {"name": "not_null", "column": "a", "time_filter": "u"},
                {"name": "not_null", "column": "b", "time_filter": "u"},
                {"name": "not_null", "column": "c", "time_filter": "u"},
            ],
        ),
        (
            [{"name": "not_null", "columns": ["a", "b"]}],
            [
                {"name": "not_null", "column": "a", "time_filter": None},
                {"name": "not_null", "column": "b", "time_filter": None},
            ],
        ),
        (
            [{"name": "not_null", "column": "a", "time_filters": ["a", "b"]}],
            [
                {"name": "not_null", "column": "a", "time_filter": "a"},
                {"name": "not_null", "column": "a", "time_filter": "b"},
            ],
        ),
        (
            [{"name": "not_null", "column": "a", "time_filter": "a"}],
            [{"name": "not_null", "column": "a", "time_filter": "a"}],
        ),
    ],
)
def test_normalizer(rules_def, normalized):
    results = RuleNormalizer().normalize(rules_def)
    op = operator.itemgetter("name", "column", "time_filter")
    expected = sorted(normalized, key=op)
    results_sorted = sorted(results, key=op)
    assert expected == results_sorted
