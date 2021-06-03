"""
Contessa is data quality framework that can check various rules upon a table. It will store
aggregated results of checks (count of failed/passed etc.) in separate table.

It exposes `DataQualityOperator` that can be plugged in any airflow DAG and will do data quality
check on load in temporary table.
"""

__version__ = "0.2.11"

# Start ignoring PyUnusedCodeBear
from .consistency_checker import ConsistencyChecker
from .runner import ContessaRunner
from .rules import EQ, GT, GTE, LT, LTE, NOT, NOT_COLUMN, NOT_NULL, SQL

# Stop ignoring
