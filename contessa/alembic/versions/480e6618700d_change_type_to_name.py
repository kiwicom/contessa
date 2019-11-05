"""change type to name

Revision ID: 480e6618700d
Revises: 54f8985b0ee5
Create Date: 2019-11-04 14:12:21.483875

"""
from alembic import op
from alembic import context
from sqlalchemy import create_engine
import sqlalchemy as sa
from sqlalchemy import inspect


# revision identifiers, used by Alembic.
revision = '480e6618700d'
down_revision = '54f8985b0ee5'
branch_labels = None
depends_on = None

config = context.config
url = config.get_main_option("sqlalchemy.url")
schema = config.get_main_option("schema")
table_prefix = 'quality_check'


def get_quality_tables():
    engine = create_engine(url)
    inspector = inspect(engine)

    all_tables = inspector.get_table_names(schema=schema)
    quality_tables = [x for x in all_tables if x.startswith(table_prefix)]

    return quality_tables


def upgrade():
    tables = get_quality_tables()

    for table in tables:
        print(f'Migrate table {table}')
        op.alter_column(table, 'rule_name', nullable=False, new_column_name='rule_type', schema=schema)

        op.add_column(
            table,
            sa.Column('rule_name', sa.VARCHAR(50)),
            schema=schema
        )


def downgrade():
    tables = get_quality_tables()

    for table in tables:
        print(f'Migrate table {table}')

        op.drop_column(table, 'rule_name', schema=schema)
        op.alter_column(table, 'rule_type', nullable=False, new_column_name='rule_name', schema=schema)