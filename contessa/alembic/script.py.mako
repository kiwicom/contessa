"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}

"""
from typing import List

from alembic import op
import sqlalchemy as sa
from sqlalchemy import create_engine, inspect
from contessa.models import QualityCheck, ConsistencyCheck
${imports if imports else ""}

# revision identifiers, used by Alembic.
revision = ${repr(up_revision)}
down_revision = ${repr(down_revision)}
branch_labels = ${repr(branch_labels)}
depends_on = ${repr(depends_on)}

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


def upgrade():
    schema = get("schema")

    print("Migration Quality Check")
    for table in get_quality_tables(QualityCheck._table_prefix):
        print(f"Migrate table {table}")

    print("Migration Consistency Check")
    for table in get_quality_tables(ConsistencyCheck._table_prefix):
        print(f"Migrate table {table}")

    ${upgrades if upgrades else "pass"}


def downgrade():
    schema = get("schema")

    print("Migration Quality Check")
    for table in get_quality_tables(QualityCheck._table_prefix):
        print(f"Migrate table {table}")

    print("Migration Consistency Check")
    for table in get_quality_tables(ConsistencyCheck._table_prefix):
        print(f"Migrate table {table}")

    ${downgrades if downgrades else "pass"}
