"""change type to name

Revision ID: 480e6618700d
Revises: 54f8985b0ee5
Create Date: 2019-11-04 14:12:21.483875

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import create_engine, inspect

# revision identifiers, used by Alembic.
from contessa.models import QualityCheck, ConsistencyCheck

revision = "480e6618700d"
down_revision = "54f8985b0ee5"
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


def get_quality_tables(table_prefix):
    url = get("sqlalchemy.url")
    schema = get("schema")

    engine = create_engine(url)
    inspector = inspect(engine)

    all_tables = inspector.get_table_names(schema=schema)
    quality_tables = [x for x in all_tables if x.startswith(table_prefix)]

    return quality_tables


def upgrade():
    schema = get("schema")

    print("Migration Quality Check")
    for table in get_quality_tables(QualityCheck._table_prefix):
        print(f"Migrate table {table}")
        op.alter_column(
            table,
            "rule_name",
            nullable=False,
            new_column_name="rule_type",
            schema=schema,
        )

        op.add_column(table, sa.Column("rule_name", sa.TEXT), schema=schema)


def downgrade():
    schema = get("schema")

    print("Migration Quality Check")
    for table in get_quality_tables(QualityCheck._table_prefix):
        print(f"Migrate table {table}")

        op.drop_column(table, "rule_name", schema=schema)
        op.alter_column(
            table,
            "rule_type",
            nullable=False,
            new_column_name="rule_name",
            schema=schema,
        )
