How to write your first check
==============================

Basic idea of Contessa is to help you with defining data quality rules
for any data you have in your database. That can be from the abstact way, e.g. 
``column_A is not NULL``, to some specific ones, e.g. ``column_A + column_B > column_C*42``.

Let's create a script ``check_bookings.py`` and define first rules we wanna check on the 
booking table.

Basic Rules
--------------------------------------
.. code-block:: python

	from contessa import NOT_NULL
	
	RULES = [
	    {
	        "name": NOT_NULL,
	        "columns": [
	            "status",
	            "src",
	            "dst",
	            "src_city",
	            "src_country",
	            "src_continent",
	            "dst_city",
	            "dst_country",
	            "initial_price",
	        ],
	    },
	    {
	        "name": GT,
	        "value": 0,
	        "columns": ["full_price", "initial_price"],
	        "condition": "status IN ('closed', 'refunded')",
    	},
	]

Let's analyze what's happenning here:

1. Rules are defined as a list of dicts/objects.
2. You can group rules together - NOT_NULL has ``columns`` attribute, so you do not need to write
   separate rules or even 10 sqls that looks exactly the same.
3. You can define condition if really needed, e.g. in GT rule you can see status filtered.

Yes, definition could be yaml/json (we do not support that now), but now its defind in python script, which means you can do stuff like:

.. code-block:: python

	def get_cool_cols():
	  columns = []
  	  with connect() as conn:
  	    with conn.cursor() as cur:
	      cur.execute("select column_name from information_schema.columns where table_name = 'abc' and data_type = 'integer';")
	      columns = [c[0] for c in cur.fetchall()]
	  return columns

	RULES.append({
		"name": NOT_NULL,
		"columns": get_cool_cols()
	})

Which will create rule that checks if all integer columns in table 'abc' are not null. Of course, you can go much wilder, get all tables you have, get all columns in those tables and for each one crates a rule. You could even spawn multiple runners for each dbs you have or want to check.

.. note::
	
	Example is for Postgres. For other DB you need to get columns in other way, but point should be clear

Adding Custom Rule
--------------------------------------
Let's define a custom rule in SQL, because for example, Contessa doesn't have anything that could solve your problem.

.. code-block:: python

    {
        "name": SQL,
        "sql": """
             SELECT CASE WHEN created_at > NOW() OR updated_at > NOW() THEN false ELSE true END AS Status
             FROM {{table_fullname}}
        """,
        "description": "created_at OR updated_at should be less or equal NOW()",
    }

The point of rules is to return boolean of *False* or *True* values. 

- *False* - rule doesn't apply, check failed ☹️
- *True* - everything is OK and row passed your data-quality check with flying colors 🌈

If you can write sql that returns list of booleans, youre fine (yay).
In Custom Rule, we can see ``{{table_fullname}}`` being used.

.. include:: features.rst
  :start-after: context-marker-start
  :end-before: context-marker-end

How do I execute this stuff?
--------------------------------------

We still haven't talked about how it is finally executed, right? And whats the result ?
Let's look on the ContessaRunner then!

.. code-block:: python

	from contessa import ContessaRunner
	contessa = ContessaRunner("postgres://:@localhost:5432")
	contessa.run(
        raw_rules=RULES,
        check_table={"schema_name": "public", "table_name": "bookings"},
        result_table={"schema_name": "dq", "table_name": "my_table"},
    )

Yop, thats it. Runner goes against 1 db, take your rules, do a bit wrangling with normalization you do not
need to care about, it creates queries and finally execute them against the ``check_table``. 

Quality Check Result
--------------------------------------

.. include:: features.rst
  :start-after: quality-check-start
  :end-before: quality-check-end

Where to look next
--------------------------------------

Go ahead and test it all with your local db! Then you can schedule a cron to have this results regularly :)

If you wanna use Contessa even more, you can look at detailed description of:

- Specific :ref:`rules` Contessa provides
- Special :ref:`features`
