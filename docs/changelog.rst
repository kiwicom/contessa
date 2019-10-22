Contessa Changelog
============================================

2019-XX-XX; 0.2.0;
--------------------------------------------
- refactor rules to use jinja2 as templating system
- allow to configure time interval in `time_filter`

2019-10-09; 0.1.4;
--------------------------------------------
- rules description is mandatory and can be used in all rules


2019-09-04; 0.1.3;
--------------------------------------------
- `condition` parameter in SqlRule is templated now


2019-09-02; 0.1.2;
--------------------------------------------
- introduce `condition` parameter to SqlRule
- "value" argument in rules can be column name. e.g. {"name": "not", "column": "src", "value": "dst"}


2019-08-13; 0.1.0;
--------------------------------------------
- first pypi release
- rules - EQ, GT, GTE, LT, LTE, NOT, NOT_COLUMN, NOT_NULL, SQL
- different check and result tables
