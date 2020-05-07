Contessa Changelog
============================================

2019-XX-XX; 0.2.5;
--------------------------------------------

- remove `use_prefix` for result_table. User can't change name of the table
- remove Enum for Checks. Prefix of the table is in `_table_prefix` attribute on models
- change migration template. do migration for all the models separately if needed
- refactor migration tests. introduce MigrationTestCase class.
- updates of the quality checks are now possible. you can rerun same check and it will be updated.
- improvements to consistency checker:
    - add time filter
    - fix comparison of tables when they store rows in different order
    - allow to compare subset of columns
    - allow custom query for selecting results from table

*Migration needed*
- add new migration. Add nullables + default time_filter ('not_set') for QualityCheck and
  ConsistencyCheck models. Run this command:
  `contessa-migrate -u $DB_URI -s data_quality -v 0.2.5`


Note we're setting nullables for columns bellow. Make sure you don't have null values in them.

*QualityCheck*
- rule_type
- rule_name
- attribute
- time_filter (fix with 1. step)

*ConsistencyCheck*
- type
- name
- left_table
- right_table
- time_filter (fix with 1. step)


2019-04-12; 0.2.4;
--------------------------------------------
- *breaking change* - output data schema change - name renamed to type, added name. Do the :ref:`migration` before use this version. 
- refactor rules to use jinja2 as templating system
- allow to configure time interval in `time_filter`
- add docs - https://contessa.readthedocs.io/en/latest/
- add debug sql prints for rules
- add BigQuery support
- add ConsistencyChecker, supports rowcount comparison
- add "column" to CustomSQLRule. will be saved in quality check as "attribute"


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
