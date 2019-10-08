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
                    "separate_time_filters": ["c", "u"],
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


def test_normalizer_separate_time_filters_bad_format():
    rules_def = [
        {"name": "not_null", "column": "a", "separate_time_filters": [{"column": "c"}]}
    ]
    with pytest.raises(ValueError):
        results = RuleNormalizer().normalize(rules_def)


@pytest.mark.parametrize(
    "rules_def, normalized",
    [
        (
            [
                {
                    "name": "not_null",
                    "column": "a",
                    "separate_time_filters": [
                        {"column": "a", "days": 10},
                        {"column": "b"},
                    ],
                }
            ],
            [
                {
                    "name": "not_null",
                    "column": "a",
                    "time_filter": [{"column": "a", "days": 10}],
                },
                {"name": "not_null", "column": "a", "time_filter": [{"column": "b"}]},
            ],
        ),
        (
            [
                {
                    "name": "not_null",
                    "column": "a",
                    "separate_time_filters": [
                        {"column": "a", "days": 10},
                        {"column": "b", "days": 5},
                    ],
                }
            ],
            [
                {
                    "name": "not_null",
                    "column": "a",
                    "time_filter": [{"column": "a", "days": 10}],
                },
                {
                    "name": "not_null",
                    "column": "a",
                    "time_filter": [{"column": "b", "days": 5}],
                },
            ],
        ),
    ],
)
def test_normalizer_separate_time_filters(rules_def, normalized):
    def sort_rules(rule):
        return (
            rule["name"],
            rule["column"],
            rule["time_filter"][0]["column"]
            if isinstance(rule["time_filter"], list)
            else rule["time_filter"]["column"],
        )

    results = RuleNormalizer().normalize(rules_def)
    expected = sorted(normalized, key=sort_rules)
    results_sorted = sorted(results, key=sort_rules)
    assert expected == results_sorted


@pytest.mark.parametrize(
    "rules_def, normalized",
    [
        (
            [
                {
                    "name": "not_null",
                    "columns": ["a", "b", "c"],
                    "condition": "d IN ('start', 'stay_end', 'stopover_end')",
                    "time_filter": None,
                }
            ],
            [
                {
                    "name": "not_null",
                    "column": "a",
                    "condition": "d IN ('start', 'stay_end', 'stopover_end')",
                    "time_filter": None,
                },
                {
                    "name": "not_null",
                    "column": "b",
                    "condition": "d IN ('start', 'stay_end', 'stopover_end')",
                    "time_filter": None,
                },
                {
                    "name": "not_null",
                    "column": "c",
                    "condition": "d IN ('start', 'stay_end', 'stopover_end')",
                    "time_filter": None,
                },
            ],
        )
    ],
)
def test_normalizer_condition(rules_def, normalized):
    results = RuleNormalizer().normalize(rules_def)
    op = operator.itemgetter("name", "column", "condition")
    expected = sorted(normalized, key=op)
    results_sorted = sorted(results, key=op)
    assert expected == results_sorted
