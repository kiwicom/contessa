.. index-start

Contessa
============================

|docs-badge| |build-badge| |pypi-badge| |license-badge|

.. |docs-badge| image:: https://readthedocs.org/projects/contessa/badge/?version=latest
   :target: https://contessa.readthedocs.io/en/latest/
.. |pypi-badge| image:: https://badge.fury.io/py/contessa.svg
   :target:  https://pypi.org/project/contessa/
.. |build-badge| image:: https://travis-ci.org/kiwicom/contessa.svg?branch=master
   :target: https://travis-ci.org/kiwicom/contessa
.. |license-badge| image:: https://img.shields.io/pypi/l/contessa.svg
   :target: https://opensource.org/licenses/MIT


Hello, welcome to Contessa!

Contessa is a **Data Quality** library that provides you an easy way to define, execute and
store quality rules for your data.

Instead of writing a lot of sql queries that look almost exactly the same, we're aiming for more
pragmatic approach - define rules programatically. This enables much more flexibility for the user and also for us as the creators of the lib.

We implement new Rules (incrementally) that should reflect Data Quality domain. From the start these are simple
rules like - NOT_NULL, GT (greater than) etc. We want to build on these simple rules and provide more complex Data Quality checkers out-of-the-box.

**Goals:**

- be database agnostic (to a reasonable degree), so you will define checks against any database (e.g. mysql vs. postgres) in the same way
- automatize data quality results e.g. from postgres table to Datadog dashboard
- programmatic approach to data-quality definition, which leads to:

  - dynamic composition of rules in a simple script using db or any 3rd party tool - e.g. take all tables, create NOT_NULl rule for all of them for each integer column

  - users can use special rules for data if needed, if not, they can go with generic solutions

  - automatizable testable parts of definitions when needed

- easier maintenance when number of checks scales too fast :)

Full docs_ here

.. _docs: https://contessa.readthedocs.io/en/latest/

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
            "name": "gt_0_prices",
            "value": 0,
            "columns": ["initial_price", "turnover_before_refunds", ],
        },
        {
            "type": SQL,
            "name": "no_bags_sql",
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

.. index-end

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
