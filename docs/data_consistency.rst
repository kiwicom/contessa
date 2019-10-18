Data consistency
==============================

Basic consistency-checks are a way to proof that data are consistent after the transfer/transform operation.
Typical check can be comparison of the number of rows between source and destination after the synchronisation.


Basic info
------------------------------

For this purpose, Contessa have *ConsistencyChecker* (alternative object to ContessaRunner).
It takes 2 tables (possibly from 2 different sources) and compares the results of the same query (typically `count(*)`)
Output of this check is simply boolean value, denoting whether the results are the same.
Result is then in form of **ConsistencyCheck** instance saved to the given result table, together with the table names
and name of the check.



