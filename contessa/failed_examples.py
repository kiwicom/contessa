from abc import abstractmethod
from itertools import islice

from typing import Set, Tuple


class ExampleSelector:
    @abstractmethod
    def select_examples(self, failed_rows: Set[Tuple]) -> Set[Tuple]:
        pass


class FirstNExampleSelector(ExampleSelector):
    def __init__(self, n):
        self.n = n

    def select_examples(self, failed_rows: Set[Tuple]) -> Set[Tuple]:
        return set(islice(failed_rows, self.n))


default_example_selector = FirstNExampleSelector(10)
