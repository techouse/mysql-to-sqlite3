#!/usr/bin/env python
import argparse
import sys

from src.mysql_to_sqlite3 import MySQLtoSQLite

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-f", "--sqlite-file", dest="sqlite_file", default=None, help="SQLite3 db file"
    )
    parser.add_argument(
        "-u", "--mysql-user", dest="mysql_user", default=None, help="MySQL user"
    )
    parser.add_argument(
        "-p",
        "--mysql-password",
        dest="mysql_password",
        default=None,
        help="MySQL password",
    )
    parser.add_argument(
        "-d",
        "--mysql-database",
        dest="mysql_database",
        default=None,
        help="MySQL database name",
    )
    parser.add_argument(
        "--mysql-host",
        dest="mysql_host",
        default="localhost",
        help="MySQL host (default: localhost)",
    )
    parser.add_argument(
        "--mysql-port",
        dest="mysql_port",
        default="3306",
        help="MySQL port (default: 3306)",
    )
    parser.add_argument(
        "-c",
        "--chunk",
        dest="chunk",
        type=int,
        default=200000,  # this default is here for performance reasons
        help="Chunk reading/writing SQL records",
    )
    parser.add_argument("-l", "--log-file", dest="log_file", help="Log file")
    parser.add_argument(
        "-V",
        "--vacuum",
        dest="vacuum",
        action="store_true",
        help="Use the VACUUM command to rebuild the SQLite database file, "
             "repacking it into a minimal amount of disk space",
    )
    parser.add_argument(
        "--use-buffered-cursors",
        dest="buffered",
        action="store_true",
        help="Use MySQLCursorBuffered for reading the MySQL database. This "
             "can be useful in situations where multiple queries, with small "
             "result sets, need to be combined or computed with each other.",
    )
    args = parser.parse_args()

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    try:
        converter = MySQLtoSQLite(
            sqlite_file=args.sqlite_file,
            mysql_user=args.mysql_user,
            mysql_password=args.mysql_password,
            mysql_database=args.mysql_database,
            mysql_host=args.mysql_host,
            mysql_port=args.mysql_port,
            chunk=args.chunk,
            vacuum=args.vacuum,
            buffered=args.buffered,
            log_file=args.log_file,
        )
        converter.transfer()
    except KeyboardInterrupt:
        print("\nProcess interrupted. Exiting...")
        sys.exit(1)
    except Exception as err:
        print(err)
        sys.exit(1)
