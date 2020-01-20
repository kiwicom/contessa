"""set_defaults_and_nullables

Revision ID: a179e5ca0ad2
Revises: 480e6618700d
Create Date: 2020-01-13 13:43:44.679965

"""
from typing import List

from alembic import op
import sqlalchemy as sa
from sqlalchemy import create_engine, inspect, Table, MetaData

from sqlalchemy.dialects.postgresql import TEXT
from contessa.models import QualityCheck, ConsistencyCheck


# revision identifiers, used by Alembic.
from contessa.settings import TIME_FILTER_DEFAULT

revision = "a179e5ca0ad2"
down_revision = "480e6618700d"
branch_labels = None
depends_on = None

config = None


def get_config():
    global config
    if config:
        return config

    from alembic import context

    config = context.config

    return config


def get(name):
    return get_config().get_main_option(name)


def get_quality_tables(table_prefix) -> List[str]:
    url = get("sqlalchemy.url")
    schema = get("schema")

    engine = create_engine(url)
    inspector = inspect(engine)

    all_tables = inspector.get_table_names(schema=schema)
    quality_tables = [x for x in all_tables if x.startswith(table_prefix)]

    return quality_tables


def set_time_filter_to_none(schema_name, table_name):
    qc_table = Table(
        table_name,
        MetaData(schema=schema_name),
        sa.Column("time_filter", TEXT),
        # Other columns not needed for the data migration
    )
    op.execute(
        qc_table.update()
        .where(qc_table.c.time_filter == TIME_FILTER_DEFAULT)
        .values({"time_filter": None})
    )


def set_time_filter_to_default(schema_name, table_name):
    qc_table = Table(
        table_name,
        MetaData(schema=schema_name),
        sa.Column("time_filter", TEXT),
        # Other columns not needed for the data migration
    )
    op.execute(
        qc_table.update()
        .where(qc_table.c.time_filter.is_(None))
        .values({"time_filter": TIME_FILTER_DEFAULT})
    )


def upgrade():
    schema = get("schema")
    not_null = lambda t, col: op.alter_column(t, col, nullable=False, schema=schema)

    print("Migration Quality Check")
    for table_name in get_quality_tables(QualityCheck._table_prefix):
        print(f"Migrate table {table_name}")
        not_null(table_name, "attribute")
        not_null(table_name, "rule_name")
        not_null(table_name, "rule_type")

        set_time_filter_to_default(schema, table_name)
        op.alter_column(
            table_name,
            "time_filter",
            nullable=False,
            server_default=TIME_FILTER_DEFAULT,
            schema=schema,
        )

    print("Migration Consistency Check")
    for table_name in get_quality_tables(ConsistencyCheck._table_prefix):
        print(f"Migrate table {table_name}")
        not_null(table_name, "type")
        not_null(table_name, "name")
        not_null(table_name, "left_table")
        not_null(table_name, "right_table")

        set_time_filter_to_default(schema, table_name)
        op.alter_column(
            table_name,
            "time_filter",
            nullable=False,
            server_default=TIME_FILTER_DEFAULT,
            schema=schema,
        )


def downgrade():
    schema = get("schema")
    null = lambda t, col: op.alter_column(t, col, nullable=True, schema=schema)

    for table_name in get_quality_tables(QualityCheck._table_prefix):
        print(f"Migrate table {table_name}")
        null(table_name, "attribute")
        null(table_name, "rule_name")
        null(table_name, "rule_type")

        op.alter_column(
            table_name, "time_filter", nullable=True, server_default=None, schema=schema
        )
        set_time_filter_to_none(schema, table_name)

    for table_name in get_quality_tables(ConsistencyCheck._table_prefix):
        print(f"Migrate table {table_name}")
        null(table_name, "type")
        null(table_name, "name")
        null(table_name, "left_table")
        null(table_name, "right_table")

        op.alter_column(
            table_name, "time_filter", nullable=True, server_default=None, schema=schema
        )
        set_time_filter_to_none(schema, table_name)
