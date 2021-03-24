from dataclasses import dataclass
from typing import Any


@dataclass
class AggregatedResult:
    total_records: int
    failed: int
    passed: int
    failed_example: Any = None
