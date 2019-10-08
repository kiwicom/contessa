import itertools
import logging


class RuleNormalizer:
    """
    Class that split list values to normalized rules that checks one rule, one col,
    one time_filter with provided where condition.

    Example:
        {'name': 'not_null', 'columns': ['a', 'b', 'c'], 'separate_time_filters': ['c', 'u'], 'condition': 'd is TRUE'}

        will be transformed to

        {'name': 'not_null', 'column': 'a', 'time_filter': 'c', 'condition': 'd is TRUE'}
        {'name': 'not_null', 'column': 'b', 'time_filter': 'c', 'condition': 'd is TRUE'}
        {'name': 'not_null', 'column': 'c', 'time_filter': 'c', 'condition': 'd is TRUE'}
        {'name': 'not_null', 'column': 'a', 'time_filter': 'u', 'condition': 'd is TRUE'}
        {'name': 'not_null', 'column': 'b', 'time_filter': 'u', 'condition': 'd is TRUE'}
        {'name': 'not_null', 'column': 'c', 'time_filter': 'u', 'condition': 'd is TRUE'}

    This class should define what we can support as an input to be able to make Rule classes
    from it. Currently it supports:

        - multiple columns
        - multiple separate_time_filters

    """

    @classmethod
    def normalize(cls, rules_def):
        normalized = []
        for rule_def in rules_def:
            # if it is normalized, we skip it
            if not cls._should_normalize(rule_def):
                normalized.append(rule_def)
                continue
            # otherwise we split all the permutations of lists into separate rules
            permutations = cls._get_permutations(rule_def)
            new_rules = cls._split_permutations(permutations, rule_def)
            normalized.extend(new_rules)
        return normalized

    @staticmethod
    def _should_normalize(rule_def):
        if "columns" in rule_def:
            return True
        elif "separate_time_filters" in rule_def:
            if len(rule_def["separate_time_filters"]) <= 1:
                raise ValueError("Please use `time_filter` for one column.")
            return True
        return False

    @staticmethod
    def _get_permutations(rule_def):
        cols = rule_def.get("columns") or [rule_def.get("column")] or [None]
        time_filters = (
            rule_def.get("separate_time_filters")
            or [rule_def.get("time_filter")]
            or [None]
        )
        permutations = itertools.product(cols, time_filters)
        return permutations

    @staticmethod
    def _split_permutations(permutations, rule_def):
        new_rules = []
        for perm in permutations:
            # time_filter has to be one of list, str or None
            if isinstance(perm[1], dict):
                time_filter = [perm[1]]
            else:
                time_filter = perm[1]
            tmp = rule_def.copy()
            tmp["column"] = perm[0]
            tmp.pop("columns", None)
            tmp["time_filter"] = time_filter
            tmp.pop("separate_time_filters", None)
            new_rules.append(tmp)
        return new_rules
