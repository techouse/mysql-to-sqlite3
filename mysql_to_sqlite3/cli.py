"""The command line interface of MySQLtoSQLite."""
import sys

import click
from tabulate import tabulate

from . import MySQLtoSQLite
from .click_utils import OptionEatAll
from .debug_info import info


@click.command()
@click.option(
    "-f",
    "--sqlite-file",
    type=click.Path(),
    default=None,
    help="SQLite3 database file",
    required=True,
)
@click.option(
    "-d", "--mysql-database", default=None, help="MySQL database name", required=True
)
@click.option("-u", "--mysql-user", default=None, help="MySQL user", required=True)
@click.option("-p", "--mysql-password", default=None, help="MySQL password")
@click.option(
    "-t",
    "--mysql-tables",
    cls=OptionEatAll,
    help="Transfer only these specific tables (space separated table names). "
    "Implies --without-foreign-keys which inhibits the transfer of foreign keys.",
)
@click.option(
    "-X", "--without-foreign-keys", is_flag=True, help="Do not transfer foreign keys."
)
@click.option(
    "-h", "--mysql-host", default="localhost", help="MySQL host. Defaults to localhost."
)
@click.option(
    "-P", "--mysql-port", type=int, default=3306, help="MySQL port. Defaults to 3306."
)
@click.option(
    "-c",
    "--chunk",
    type=int,
    default=200000,  # this default is here for performance reasons
    help="Chunk reading/writing SQL records",
)
@click.option("-l", "--log-file", type=click.Path(), help="Log file")
@click.option(
    "-V",
    "--vacuum",
    is_flag=True,
    help="Use the VACUUM command to rebuild the SQLite database file, "
    "repacking it into a minimal amount of disk space",
)
@click.option(
    "--use-buffered-cursors",
    is_flag=True,
    help="Use MySQLCursorBuffered for reading the MySQL database. This "
    "can be useful in situations where multiple queries, with small "
    "result sets, need to be combined or computed with each other.",
)
@click.option("-q", "--quiet", is_flag=True, help="Quiet. Display only errors.")
@click.version_option(
    message=tabulate(info(), headers=["software", "version"], tablefmt="github")
)
def cli(
    sqlite_file,
    mysql_user,
    mysql_password,
    mysql_database,
    mysql_tables,
    without_foreign_keys,
    mysql_host,
    mysql_port,
    chunk,
    log_file,
    vacuum,
    use_buffered_cursors,
    quiet,
):
    """Transfer MySQL to SQLite using the provided CLI options."""
    try:
        converter = MySQLtoSQLite(
            sqlite_file=sqlite_file,
            mysql_user=mysql_user,
            mysql_password=mysql_password,
            mysql_database=mysql_database,
            mysql_tables=mysql_tables,
            without_foreign_keys=without_foreign_keys
            or (mysql_tables is not None and len(mysql_tables) > 0),
            mysql_host=mysql_host,
            mysql_port=mysql_port,
            chunk=chunk,
            vacuum=vacuum,
            buffered=use_buffered_cursors,
            log_file=log_file,
            quiet=quiet,
        )
        converter.transfer()
    except KeyboardInterrupt:
        print("\nProcess interrupted. Exiting...")
        sys.exit(1)
    except Exception as err:  # pylint: disable=W0703
        print(err)
        sys.exit(1)
