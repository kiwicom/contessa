import abc


class Rule(metaclass=abc.ABCMeta):
    """
    Representation of one rule.
    Method apply define how it will be evaluated using the executor that will inject all the
    needed attributes to apply it (df, hook etc.).

    Attributes:
        executor_cls    Executor, indication of which executor class can execute this rule
        description     str, description of the rule

    :param name: str
    :param time_filter: str
    """

    executor_cls = None
    description = None

    def __init__(self, name, time_filter=None):
        self.name = name
        self.time_filter = time_filter

    @property
    def attribute(self):
        """What attributes(columns) is this rule using."""
        return None

    def apply(self, **kwargs):
        """
        Override this method to check a specific rule.
        """
        raise NotImplementedError

    def __str__(self):
        tf = f"- {self.time_filter}" or ""
        return f"Rule {self.name} {tf}"
