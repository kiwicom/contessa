# Python script that will apply the migrations up to head
import alembic.config
import os
import argparse


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    alembic_ini_path = os.path.join(here, "alembic.ini")

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-t",
        help="Migration type. Possible values: upgrade, downgrade.",
        default="upgrade",
    )

    parser.add_argument(
        "-v",
        help="""Version to upgrade/downgrade. 
        Possible value: hash of version (e.g 54f8985b0ee5). Default set to 'head' - last version.""",
        default="head",
    )

    parser.add_argument(
        "-u",
        help="In the form of driver://user:pass@localhost/dbname",
        default="driver://user:pass@localhost/dbname",
    )
    parser.add_argument("-s", help="Schema containing tables to migrate.")

    args = parser.parse_args()

    alembic_args = [
        "-x",
        f"sqlalchemy_url={args.u}",
        "-x",
        f"schema={args.s}",
        "-c",
        alembic_ini_path,
        args.t,
        args.v,
    ]

    alembic.config.main(argv=alembic_args)
