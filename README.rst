Contessa
============================

|docs-badge|

.. |docs-badge| image:: https://readthedocs.org/projects/contessa/badge/?version=latest

Data Quality Framework

Quick Example
---------------------------

.. code-block:: python

    from contessa import ContessaRunner, NOT_NULL, GT, SQL
    no_bags_sql = """
        SELECT CASE WHEN is_no_bags_booking = 'T' AND bags > 0 THEN false ELSE true END
        FROM {{table_fullname}};
    """
    contessa = ContessaRunner("postgres://:@localhost:5432")

    RULES = [
        {
            "name" : "Status and market null check"
            "type": NOT_NULL,
            "columns": ["status", "market", "src", "dst"], 
        },
        {
            "type": GT,
            "value": 0,
            "columns": ["initial_price", "turnover_before_refunds", ],
        },
        {
            "type": SQL,
            "sql": no_bags_sql,
            "description": "No bags booking should have bags = 0",
        },
    ]
    contessa.run(
        raw_rules=RULES,
        check_table={"schema_name": "public", "table_name": "bookings"},
        result_table={"schema_name": "dq", "table_name": "my_table"},
    )

How to run tests
---------------------------

.. code-block:: bash

    $ make test-up  # run postgres + app
    $ make test args="/app/test -s"  # args for pytest
    $ make test-down  # delete containers + volumes


In case of unit tests (you do not need db):

.. code-block:: bash

    $ pytest test/unit/test_operator.py

How to add docs
---------------------------

.. code-block:: bash

	$ pip3 install -r requirements-docs.txt
	$ python3 watchdogs.py

It will make html files with sphinx and serve a local webserver so that you can check it out.
It should also reload it :)

NOTE: If it doesn't work, build html manually. ``cd docs && make html``
