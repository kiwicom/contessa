# Contessa

Data-quality framework

# Short usage

```python
from contessa import ContessaRunner, NOT_NULL, GT, SQL
no_bags_sql = """
	SELECT CASE WHEN is_no_bags_booking = 'T' AND bags > 0 THEN false ELSE true END
	FROM {{table_fullname}};
"""
contessa = ContessaRunner("postgres://:@localhost:5432")

RULES = [
	{
        "name": NOT_NULL,
        "columns": ["status", "market", "src", "dst"], 
    },
    {
        "name": GT,
        "value": 0,
        "columns": ["initial_price", "turnover_before_refunds", ],
    },
    {
        "name": SQL,
        "sql": no_bags_sql,
        "description": "No bags booking should have bags = 0",
    },
]
ts_nodash = "20191010T101010"  # should be set dynamically (e.g. by airflow), just example here
contessa.run(
	raw_rules=RULES,
	check_table={"schema_name": "temporary", "table_name": f"my_table_{ts_nodash}"},
	result_table={"schema_name": "dq", "table_name": "my_table"},
)
```

This will result in table `dq.quality_check_my_table` with each row looking like:

```python
class QualityCheck:
    id = Column(BIGINT, primary_key=True)
    attribute = Column(TEXT)
    rule_name = Column(TEXT)
    rule_description = Column(TEXT)
    total_records = Column(INTEGER)

    failed = Column(INTEGER)
    median_30_day_failed = Column(DOUBLE_PRECISION)
    failed_percentage = Column(DOUBLE_PRECISION)

    passed = Column(INTEGER)
    median_30_day_passed = Column(DOUBLE_PRECISION)
    passed_percentage = Column(DOUBLE_PRECISION)

    status = Column(TEXT)
    time_filter = Column(TEXT)
    task_ts = Column(TIMESTAMP(timezone=True), nullable=False, index=True)
    created_at = Column(
        DateTime(timezone=True),
        server_default=text("NOW()"),
        nullable=False,
        index=True,
    )

```

# How to run tests

```bash
$ make test-up  # run postgres + app
$ make test args="/app/test -s"  # args for pytest
$ make test-down  # delete containers + volumes
```

In case of unit tests (you do not need db):
```
$ pytest test/unit/test_operator.py
```

# Context

Each run has its own context, mostly used for templating the final sql. This is its context: 

{
	"table_fullname" : "public.my_cool_table"
	"task_ts": passed by client or datetime.now()
}