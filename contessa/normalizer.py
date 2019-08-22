import itertools


class RuleNormalizer:
    """
    Class that split list values to normalized rules that checks one rule, one col,
    one time_filter with provided where condition.

    Example:
        {'name': 'not_null', 'columns': ['a', 'b', 'c'], 'time_filters': ['c', 'u'], 'condition': 'd is TRUE'}

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
        - multiple time_filters

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
        elif "time_filters" in rule_def and len(rule_def["time_filters"]) > 1:
            return True
        return False

    @staticmethod
    def _get_permutations(rule_def):
        cols = rule_def.get("columns") or [rule_def.get("column")] or [None]
        time_filters = (
            rule_def.get("time_filters") or [rule_def.get("time_filter")] or [None]
        )
        permutations = itertools.product(cols, time_filters)
        return permutations

    @staticmethod
    def _split_permutations(permutations, rule_def):
        new_rules = []
        for perm in permutations:
            tmp = rule_def.copy()
            tmp["column"] = perm[0]
            tmp.pop("columns", None)
            tmp["time_filter"] = perm[1]
            tmp.pop("time_filters", None)
            new_rules.append(tmp)
        return new_rules
