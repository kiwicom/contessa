Filtering based on time
==============================

Each rule can define also time interval for selection of suitable rows with parameter **time_filter**.
It expects at least name of the column, optionally also length of the interval in *days* (with default 30).
It is possible to name multiple time-based columns.

Format
------------------------------

.. code-block:: json

    {
        ...
        "time_filter": [
            {"column": "a", "days": 10},
            {"column": "b", "days": 24},
        ],
    }

How it works
------------------------------

Internally is this parameter used for constructing `WHERE` clause, with *task_ts* and (*task_ts* - *days*) as
interval borders. In case multiple columns are named, all would be joined with `AND` - therefore only rows which satisfy
all conditions at the same time will be eligible for further checking.


Miscellaneous
------------------------------

 - For backward-compatibility is also supported simple format, which defines only name of the column. In this case default
(30) for *days* is used.

.. code-block:: json

    "time_filter": "a"

 - Parallel to **columns** shortcut for defining multiple rules at once, parameter **separate_time_filters** is available.
   Each one will be used separately. In case both *separate_time_filters* and *time_filter* are defined, only
   *separate_time_filters* would be considered.

.. code-block:: json

    "separate_time_filters": [{"column": "c"}, {"column": "d"}]}



