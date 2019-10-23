..  _features:

Features
=========================

Here are list of features that may come in handy.

Time Filter
-------------------------

If you need to filter data you're checking by date this feature's for you. Each rule can define time interval for selection of suitable rows with parameter **time_filter**. It expects name of the column, optionally also length of the interval in *days* (with default 30).
It is possible to name multiple time-based columns, which will be joined with AND.

Format
````````````````````````

.. code-block:: json

    {
        "time_filter": [
            {"column": "a", "days": 10},
            {"column": "b", "days": 24},
        ],
    }

    # will select data that has `a` column between running time and running time minus 10 days AND `b` column
    # between running time and running time minus 24 days
    # then it will do the check against those filtered data only

Miscellaneous
````````````````````````

- For backward-compatibility is also supported simple format, which defines only name of the column. In this case default (30) for *days* is used.

.. code-block:: json

    {
        "time_filter": "a"
    }

- Parameter **separate_time_filters** is available.

  - Each one will be used separately. Meaning, it would actually create X rules and therefore X checks would be done against the data, each filtering the data by passed column. In case both *separate_time_filters* and *time_filter* are defined, only *separate_time_filters* would be considered.

.. code-block:: json

    {
        "name": NOT_NULL,
        "columns": ["a", "b", "c"],
        "separate_time_filters": [{"column": "c"}, {"column": "d"}]
    }
    # would check NOT_NULL while filtering by "c" column for last 30 days, write the result.
    # then apply another rule when it would filter by column "d"

Context
-------------------------

.. context-marker-start

Each run has its own context, mostly used for templating the final sql. This is its context: 

.. code-block:: json

    {
    	"table_fullname" : "public.my_cool_table",
    	"task_ts": "passed by client or datetime.now()"
    }

.. context-marker-end

Quality Check Result
-------------------------

.. quality-check-start

Result will be storred to ``result_table`` in your database. It's model is defined in :ref:`quality_check`.
Table name will be prefixed with ``quality_check``, in this example case the resulting table would be ``dq.quality_check_my_table``.

.. tip::
    
    You can suppress this behaviour by passing ``result_table={"schema_name": "dq", "table_name": "this_is_my_name", "use_prefix": False}``
    to ContessaRunner.

.. quality-check-end
