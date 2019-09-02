CONTESSA CHANGELOG
============================================

2019-09-02; 0.1.2;
--------------------------------------------
- introduce `condition` parameter to SqlRule
- "value" argument in rules can be column name. e.g. {"name": "not", "column": "src", "value": "dst"}


2019-08-13; 0.1.0;
--------------------------------------------
- first pypi release
- rules - EQ, GT, GTE, LT, LTE, NOT, NOT_COLUMN, NOT_NULL, SQL
- different check and result tables
