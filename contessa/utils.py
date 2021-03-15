from dataclasses import dataclass
from itertools import islice
from typing import Any, Iterable

from sqlalchemy.engine import RowProxy

failed_example_size = 10


@dataclass
class AggregatedResult:
    total_records: int
    failed: int
    passed: int
    failed_example: Any = None


def failed_example_from_result_rows(rows: Iterable[RowProxy]):
    rows_example = islice(rows, failed_example_size)
    return [{column: value for column, value in row.items()} for row in rows_example]
