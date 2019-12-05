..  _migration:

Migration
=========================

Contessa stores quality check results to the database in the defined data model. 
These data or data model are sometimes changed and when it happens, we need to migrate data and data model to the new version.

When to migrate
-------------------------
The schema change is not present in all Contessa version changes. Please, be careful and read the changelog to identify when you need to do the migration.
Before the migration, we recommend some preparation steps.

How to prepare before migration
--------------------------------
- back up your current quality check result tables that you plan to migrate
- the migration can create, rename, add and delete columns and change the data - be sure consumers and services built on top of data are prepared for this change


How to migrate
-------------------------
- install a new version of Contessa (we assume this version contains migration)
- ensure you did preparation steps 
- run migration with ``contessa-migrate`` with appropriate arguments (use contessa-migrate --help for more info)

example of Contessa migration command

.. code-block:: console

    contessa-migrate \
        -u postgresql://postgres:postgres@localhost:5432/test_db \
        -s data_quality_schema \
        -v 0.1.5

The previous command executes migration on the schema ``data_quality_schema`` on all Contessa tables - all tables with the prefix ``quality_check``.
If you do the migration first time, the migration creates a ``contessa_alembic_version`` table inside your schema. This table 
contains info about the actual version of the data.

In the migration, you have to specify the version you'd like to migrate to. In the case you want to migrate to the version
that do not match with the current Contessa version, the migration fails.

Force migration
-------------------------
The forced migration is used in the case you'd like to migrate to the version that differs from your current version. 

.. code-block:: console

    contessa-migrate \
        -u postgresql://postgres:postgres@localhost:5432/test_db \
        -s data_quality_schema \
        -v 0.1.4 \
        -f

Be careful, in the case your Contessa package version and the migration version do not match, Contessa will not work properly.

