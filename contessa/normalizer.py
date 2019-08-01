import itertools


class RuleNormalizer:
    """
    Class that split list values to normalized rules that checks one rule, one col,
    one time_filter.

    Example:
        {'name': 'not_null', 'columns': ['a', 'b', 'c'], 'time_filters': ['c', 'u']}

        will be transformed to

        {'name': 'not_null', 'column': 'a', 'time_filter': 'c'}
        {'name': 'not_null', 'column': 'b', 'time_filter': 'c'}
        {'name': 'not_null', 'column': 'c', 'time_filter': 'c'}
        {'name': 'not_null', 'column': 'a', 'time_filter': 'u'}
        {'name': 'not_null', 'column': 'b', 'time_filter': 'u'}
        {'name': 'not_null', 'column': 'c', 'time_filter': 'u'}

    This class should define what we can support as an input to be able to make Rule classes
    from it. Currently it supports:

        - multiple columns
        - multiple time_filters

    """

    def normalize(self, rules_def):
        normalized = []
        for rule_def in rules_def:
            # if it is normalized, we skip it
            if not self._should_normalize(rule_def):
                normalized.append(rule_def)
                continue
            # otherwise we split all the permutations of lists into separate rules
            permutations = self._get_permutations(rule_def)
            new_rules = self._split_permutations(permutations, rule_def)
            normalized.extend(new_rules)
        return normalized

    def _should_normalize(self, rule_def):
        if "columns" in rule_def:
            return True
        elif "time_filters" in rule_def and len(rule_def["time_filters"]) > 1:
            return True
        return False

    def _get_permutations(self, rule_def):
        cols = rule_def.get("columns") or [rule_def.get("column")] or [None]
        time_filters = (
            rule_def.get("time_filters") or [rule_def.get("time_filter")] or [None]
        )
        permutations = itertools.product(cols, time_filters)
        return permutations

    def _split_permutations(self, permutations, rule_def):
        new_rules = []
        for perm in permutations:
            tmp = rule_def.copy()
            tmp["column"] = perm[0]
            tmp.pop("columns", None)
            tmp["time_filter"] = perm[1]
            tmp.pop("time_filters", None)
            new_rules.append(tmp)
        return new_rules
