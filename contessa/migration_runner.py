import click
import contessa
import alembic.config
import os
import sys

from contessa.migration import MigrationsResolver
from contessa.alembic.packages_migrations import migration_map

here = os.path.dirname(os.path.abspath(__file__))
alembic_ini_path = os.path.join(here, "alembic.ini")


@click.command()
@click.option(
    "-u",
    "--url",
    help="Connection string to database in the form driver://user:pass@localhost/dbname.",
    required=True,
)
@click.option(
    "-s", "--schema", help="Schema containing tables to migrate.", required=True
)
@click.option("-v", "--version", help="Version of package to migrate.", required=True)
@click.option(
    "-f",
    "--force",
    help="""
    Force migration. Used for the package migration that differs from the current package version.""",
    is_flag=True,
)
def main(url, schema, version, force):
    if version != contessa.__version__ and not force:
        raise Exception(
            f"""
            Versions do not match. The migration is possible only to the current Contessa version.
            Use '{contessa.__version__}' as an input parameter.
            """
        )

    migration = MigrationsResolver(migration_map, version, url, schema)
    command = migration.get_migration_to_head()

    if command is None:
        print(
            f"Contessa database schema {schema} is already migrated to version {version}."
        )
        sys.exit()

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


if __name__ == "__main__":
    main()
