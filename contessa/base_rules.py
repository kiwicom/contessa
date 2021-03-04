import abc
from typing import Union, List, Dict, Optional

from contessa.time_filter import TimeFilter, parse_time_filter


class Rule(metaclass=abc.ABCMeta):
    """
    Representation of one rule.
    Method apply define how it will be evaluated using the executor that will inject all the
    needed attributes to apply it (df, connector etc.).

    Attributes:
        executor_cls    Executor, indication of which executor class can execute this rule
        description     str, description of the rule

    :param name: str
    :param type: str
    :param description: str
    :param time_filter: str
    :param condition: str
    """

    executor_cls = None
    description = None

    def __init__(
        self,
        name,
        type,
        description,
        time_filter: Optional[Union[str, List[Dict], TimeFilter]] = None,
        condition=None,
    ):
        self.name = name
        self.type = type
        self.time_filter = parse_time_filter(time_filter)
        self.condition = condition
        self.description = description

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
        tf = f" - {self.time_filter}" or ""
        return f"Rule {self.name} of type {self.type}{tf}"
