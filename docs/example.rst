.. code-block:: python

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
    contessa.run(
        raw_rules=RULES,
        check_table={"schema_name": "public", "table_name": "bookings"},
        result_table={"schema_name": "dq", "table_name": "my_table"},
    )

This will result in table **dq.quality_check_my_table**. For model see :ref:`quality_check`
