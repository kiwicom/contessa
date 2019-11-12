import click
import contessa
import alembic.config
import os

from contessa.migration import MigrationsResolver
from contessa.alembic.packages_migrations import migration_map

here = os.path.dirname(os.path.abspath(__file__))
alembic_ini_path = os.path.join(here, "alembic.ini")


@click.command()
@click.option(
    '-u',
    '--url',
    help='Connection string to database in the form driver://user:pass@localhost/dbname.',
    required=True)
@click.option(
    '-s',
    '--schema',
    help='Schema containing tables to migrate.',
    required=True)
@click.option(
    '-v',
    '--version',
    help='Version of package to migrate.',
    required=True)
def main(url, schema, version):
    if version != contessa.__version__:
        raise Exception(
            f"""
            Not possible execute migration to Contessa of version {version} because 
            your current Contessa version is {contessa.__version__}.
            """)

    migration = MigrationsResolver(migration_map, contessa.__version__, url, schema)
    command = migration.get_migration_to_head()

    alembic_args = [
        "-x",
        f"sqlalchemy_url={url}",
        "-x",
        f"schema={schema}",
        "-c",
        alembic_ini_path,
        command[0],
        command[1],
    ]

    alembic.config.main(argv=alembic_args)


if __name__ == '__main__':
    main()
