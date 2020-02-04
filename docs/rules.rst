..  _rules:

Rules
==============================

Usage of any generic or custom:

.. code-block:: python

  from contessa import GT, SQL, NOT_NULL

  rules = [{
    "type": NOT_NULL,
    "name": "not_nulls",
    "columns": ["a", "b", "c"],
  }, {
    "type": GT,
    "name": "gt_0",
    "value": 0,
    "columns": ["a", "b", "c"],
    "condition": "status IN ('closed', 'refunded')", # optional thingy in rules
  }, {
    "type": SQL,
    "name": "cool_sql",
    "sql": "SELECT CASE WHEN <something-cool> THEN false ELSE true END FROM {{table_fullname}}",
    "description": "more coolness",
    # condition doesnt make sense here :)
  }]

.. tip::

	value can be another col, e.g. - ``{"name": NOT, "column": "src", "value": "dst"}``

Check supported rules below.


Accuracy, Completeness and Conformity
--------------------------------------------

.. autoclass:: contessa.rules.NotNullRule
   :no-undoc-members:

   ``from contessa import NOT_NULL``

.. autoclass:: contessa.rules.GtRule
   :no-undoc-members:

   ``from contessa import GT``

.. autoclass:: contessa.rules.GteRule
   :no-undoc-members:

   ``from contessa import GTE``

.. autoclass:: contessa.rules.NotRule
   :no-undoc-members:

   ``from contessa import NOT``

.. autoclass:: contessa.rules.LtRule
   :no-undoc-members:

   ``from contessa import LT``

.. autoclass:: contessa.rules.LteRule
   :no-undoc-members:

   ``from contessa import LTE``

.. autoclass:: contessa.rules.EqRule
   :no-undoc-members:

   ``from contessa import EQ``


Custom Rules
-------------------------------------------

.. autoclass:: contessa.rules.CustomSqlRule
   :no-undoc-members:

   ``from contessa import SQL``


Consistency
--------------------------------------------

# comming soon
