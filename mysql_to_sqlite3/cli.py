import sys

import click
from mysql_to_sqlite3 import MySQLtoSQLite


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
def cli(
    sqlite_file,
    mysql_user,
    mysql_password,
    mysql_database,
    mysql_host,
    mysql_port,
    chunk,
    log_file,
    vacuum,
    use_buffered_cursors,
):
    try:
        converter = MySQLtoSQLite(
            sqlite_file=sqlite_file,
            mysql_user=mysql_user,
            mysql_password=mysql_password,
            mysql_database=mysql_database,
            mysql_host=mysql_host,
            mysql_port=mysql_port,
            chunk=chunk,
            vacuum=vacuum,
            buffered=use_buffered_cursors,
            log_file=log_file,
        )
        converter.transfer()
    except KeyboardInterrupt:
        print("\nProcess interrupted. Exiting...")
        sys.exit(1)
    except Exception as err:
        print(err)
        sys.exit(1)
