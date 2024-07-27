"""The command line interface of MySQLtoSQLite."""

import os
import sys
import typing as t
from datetime import datetime

import click
from mysql.connector import CharacterSet
from tabulate import tabulate

from . import MySQLtoSQLite
from . import __version__ as package_version
from .click_utils import OptionEatAll, prompt_password, validate_positive_integer
from .debug_info import info
from .mysql_utils import mysql_supported_character_sets
from .sqlite_utils import CollatingSequences


_copyright_header: str = f"mysql2sqlite version {package_version} Copyright (c) 2019-{datetime.now().year} Klemen Tusar"


@click.command(
    name="mysql2sqlite",
    help=_copyright_header,
    no_args_is_help=True,
    epilog="For more information, visit https://github.com/techouse/mysql-to-sqlite3",
)
@click.option(
    "-f",
    "--sqlite-file",
    type=click.Path(),
    default=None,
    help="SQLite3 database file",
    required=True,
)
@click.option("-d", "--mysql-database", default=None, help="MySQL database name", required=True)
@click.option("-u", "--mysql-user", default=None, help="MySQL user", required=True)
@click.option(
    "-p",
    "--prompt-mysql-password",
    is_flag=True,
    default=False,
    callback=prompt_password,
    help="Prompt for MySQL password",
)
@click.option("--mysql-password", default=None, help="MySQL password")
@click.option(
    "-t",
    "--mysql-tables",
    type=tuple,
    cls=OptionEatAll,
    help="Transfer only these specific tables (space separated table names). "
    "Implies --without-foreign-keys which inhibits the transfer of foreign keys. "
    "Can not be used together with --exclude-mysql-tables.",
)
@click.option(
    "-e",
    "--exclude-mysql-tables",
    type=tuple,
    cls=OptionEatAll,
    help="Transfer all tables except these specific tables (space separated table names). "
    "Implies --without-foreign-keys which inhibits the transfer of foreign keys. "
    "Can not be used together with --mysql-tables.",
)
@click.option(
    "-L",
    "--limit-rows",
    type=int,
    callback=validate_positive_integer,
    default=0,
    help="Transfer only a limited number of rows from each table.",
)
@click.option(
    "-C",
    "--collation",
    type=click.Choice(
        [
            CollatingSequences.BINARY,
            CollatingSequences.NOCASE,
            CollatingSequences.RTRIM,
        ],
        case_sensitive=False,
    ),
    default=CollatingSequences.BINARY,
    show_default=True,
    help="Create datatypes of TEXT affinity using a specified collation sequence.",
)
@click.option(
    "-K",
    "--prefix-indices",
    is_flag=True,
    help="Prefix indices with their corresponding tables. "
    "This ensures that their names remain unique across the SQLite database.",
)
@click.option("-X", "--without-foreign-keys", is_flag=True, help="Do not transfer foreign keys.")
@click.option(
    "-Z",
    "--without-tables",
    is_flag=True,
    help="Do not transfer tables, data only.",
)
@click.option(
    "-W",
    "--without-data",
    is_flag=True,
    help="Do not transfer table data, DDL only.",
)
@click.option("-h", "--mysql-host", default="localhost", help="MySQL host. Defaults to localhost.")
@click.option("-P", "--mysql-port", type=int, default=3306, help="MySQL port. Defaults to 3306.")
@click.option(
    "--mysql-charset",
    metavar="TEXT",
    type=click.Choice(list(CharacterSet().get_supported()), case_sensitive=False),
    default="utf8mb4",
    show_default=True,
    help="MySQL database and table character set",
)
@click.option(
    "--mysql-collation",
    metavar="TEXT",
    type=click.Choice(
        [charset.collation for charset in mysql_supported_character_sets()],
        case_sensitive=False,
    ),
    default=None,
    help="MySQL database and table collation",
)
@click.option("-S", "--skip-ssl", is_flag=True, help="Disable MySQL connection encryption.")
@click.option(
    "-c",
    "--chunk",
    type=int,
    default=200000,  # this default is here for performance reasons
    help="Chunk reading/writing SQL records",
)
@click.option("-l", "--log-file", type=click.Path(), help="Log file")
@click.option("--json-as-text", is_flag=True, help="Transfer JSON columns as TEXT.")
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
@click.option("--debug", is_flag=True, help="Debug mode. Will throw exceptions.")
@click.version_option(message=tabulate(info(), headers=["software", "version"], tablefmt="github"))
def cli(
    sqlite_file: t.Union[str, "os.PathLike[t.Any]"],
    mysql_user: str,
    prompt_mysql_password: bool,
    mysql_password: str,
    mysql_database: str,
    mysql_tables: t.Optional[t.Sequence[str]],
    exclude_mysql_tables: t.Optional[t.Sequence[str]],
    limit_rows: int,
    collation: t.Optional[str],
    prefix_indices: bool,
    without_foreign_keys: bool,
    without_tables: bool,
    without_data: bool,
    mysql_host: str,
    mysql_port: int,
    mysql_charset: str,
    mysql_collation: str,
    skip_ssl: bool,
    chunk: int,
    log_file: t.Union[str, "os.PathLike[t.Any]"],
    json_as_text: bool,
    vacuum: bool,
    use_buffered_cursors: bool,
    quiet: bool,
    debug: bool,
) -> None:
    """Transfer MySQL to SQLite using the provided CLI options."""
    click.echo(_copyright_header)
    try:
        if mysql_collation:
            charset_collations: t.Tuple[str, ...] = tuple(
                cs.collation for cs in mysql_supported_character_sets(mysql_charset.lower())
            )
            if mysql_collation not in set(charset_collations):
                raise click.ClickException(
                    f"Error: Invalid value for '--collation' of charset '{mysql_charset}': '{mysql_collation}' "
                    f"""is not one of {"'" + "', '".join(charset_collations) + "'"}."""
                )

        # check if both mysql_skip_create_table and mysql_skip_transfer_data are True
        if without_tables and without_data:
            raise click.ClickException(
                "Error: Both -Z/--without-tables and -W/--without-data are set. There is nothing to do. Exiting..."
            )

        if mysql_tables and exclude_mysql_tables:
            raise click.UsageError("Illegal usage: --mysql-tables and --exclude-mysql-tables are mutually exclusive!")

        converter = MySQLtoSQLite(
            sqlite_file=sqlite_file,
            mysql_user=mysql_user,
            mysql_password=mysql_password or prompt_mysql_password,
            mysql_database=mysql_database,
            mysql_tables=mysql_tables,
            exclude_mysql_tables=exclude_mysql_tables,
            limit_rows=limit_rows,
            collation=collation,
            prefix_indices=prefix_indices,
            without_foreign_keys=without_foreign_keys or (mysql_tables is not None and len(mysql_tables) > 0),
            without_tables=without_tables,
            without_data=without_data,
            mysql_host=mysql_host,
            mysql_port=mysql_port,
            mysql_charset=mysql_charset,
            mysql_collation=mysql_collation,
            mysql_ssl_disabled=skip_ssl,
            chunk=chunk,
            json_as_text=json_as_text,
            vacuum=vacuum,
            buffered=use_buffered_cursors,
            log_file=log_file,
            quiet=quiet,
        )
        converter.transfer()
    except KeyboardInterrupt:
        if debug:
            raise
        click.echo("\nProcess interrupted. Exiting...")
        sys.exit(1)
    except Exception as err:  # pylint: disable=W0703
        if debug:
            raise
        click.echo(err)
        sys.exit(1)
