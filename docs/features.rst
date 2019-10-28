..  _features:

Features
=========================

List of features that may come in handy.


Time Filter
-------------------------

If you need to filter data by date this feature's for you. Each rule can define time interval for selection of suitable rows with parameter **time_filter**. It expects name of the column, optionally also length of the interval in *days* (with default 30).
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

    # this will select data where column `a` is between running time and running time minus 10 days AND 
    # column `b` column is between running time and running time minus 24 days
    # then it will do the check against those filtered data only

Miscellaneous
````````````````````````

- (deprecated) Backward-compatible time filter format defined with column name only. This uses the default 30 days value.

.. code-block:: json

    {
        "time_filter": "a"
    }

- **separate_time_filters** parameter.

  - Creates a matrix of rules and filters - i.e. provided rule is applied with each separate_time_filters value separately - acts as a generator of rules. In case both *separate_time_filters* and *time_filter* are defined, only *separate_time_filters* would be considered.

.. code-block:: json

    {
        "name": NOT_NULL,
        "columns": ["a", "b", "c"],
        "separate_time_filters": [{"column": "c"}, {"column": "d"}]
    }
    # Checks NOT_NULL while filtering by column c and writes the result, 
    # then repeats the same check while filtering by column d and writes the result as a separate value.


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

Result will be storred to ``result_table`` in your database. Its model is defined in :ref:`quality_check`.
Table name will be prefixed with ``quality_check``, in this example case the resulting table would be ``dq.quality_check_my_table``.

.. tip::
    
    You can suppress this behaviour by passing ``result_table={"schema_name": "dq", "table_name": "this_is_my_name", "use_prefix": False}``
    to ContessaRunner.

.. quality-check-end

Debug Mode
-------------------------

Contessa uses python std logging module. For more verbosite enable DEBUG mode. Useful for checking queries Contessa builds.

.. code-block:: python

    # setup logging before using contessa
    logging.basicConfig(
     level=logging.DEBUG, 
     format= ‘[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s’,
     datefmt=‘%H:%M:%S’
 )
