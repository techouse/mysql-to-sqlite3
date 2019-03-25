#!/usr/bin/env python3
import logging
import re
import sqlite3
import sys
from math import ceil
from os.path import realpath

import mysql.connector
from mysql.connector import errorcode
from slugify import slugify
from tqdm import tqdm


class MySQL2SQLite:
    def __init__(self, **kwargs):
        if kwargs.get("mysql_user", None) is None:
            print("Please provide a MySQL user!")
            sys.exit(1)

        if kwargs.get("mysql_password", None) is None:
            print("Please provide a MySQL password")
            sys.exit(1)

        self._current_chunk_number = 0
        self._chunk_size = kwargs.get("chunk", None)
        if self._chunk_size:
            self._chunk_size = int(self._chunk_size)

        self._mysql_database = kwargs.get("mysql_database", "transfer")
        self._sqlite_file = kwargs.get("sqlite_file", None)

        self._buffered = kwargs.get("buffered", False)

        self._vacuum = kwargs.get("vacuum", False)

        self._logger = self._setup_logger(log_file=kwargs.get("log_file", None))

        self._sqlite = sqlite3.connect(realpath(self._sqlite_file))
        self._sqlite.row_factory = sqlite3.Row

        self._sqlite_cur = self._sqlite.cursor()

        self._mysql = mysql.connector.connect(
            user=kwargs.get("mysql_user", None),
            password=kwargs.get("mysql_password", None),
            host=kwargs.get("mysql_host", "localhost"),
            port=kwargs.get("mysql_port", "3306"),
        )
        self._mysql_cur = self._mysql.cursor(raw=True, buffered=self._buffered)
        self._mysql_cur_dict = self._mysql.cursor(dictionary=True, buffered=self._buffered)
        try:
            self._mysql.database = self._mysql_database
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_BAD_DB_ERROR:
                self._logger.error("MySQL Database does not exist!")
                sys.exit(1)
            else:
                self._logger.error(err)
                sys.exit(1)

    @classmethod
    def _setup_logger(cls, log_file=None):
        formatter = logging.Formatter(
            fmt="%(asctime)s %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        screen_handler = logging.StreamHandler(stream=sys.stdout)
        screen_handler.setFormatter(formatter)
        logger = logging.getLogger(cls.__name__)
        logger.setLevel(logging.DEBUG)
        logger.addHandler(screen_handler)

        if log_file:
            file_handler = logging.FileHandler(realpath(log_file), mode="w")
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        return logger

    @staticmethod
    def _translate_type_from_mysql_to_sqlite(column_type):
        """
        This method could be optimized even further, however at the time
        of writing it seemed adequate enough.
        """
        match = re.match(r"^[^(]+", column_type.strip())
        if not match:
            raise ValueError("Invalid column_type!")

        column_type = match.group(0).upper()
        if column_type == "TINYINT":
            return "TINYINT"
        elif column_type == "SMALLINT":
            return "SMALLINT"
        elif column_type == "MEDIUMINT":
            return "MEDIUMINT"
        elif column_type in {"INT", "INTEGER"}:
            return "INTEGER"
        elif column_type == "BIGINT":
            return "BIGINT"
        elif column_type == "DOUBLE":
            return "DOUBLE"
        elif column_type == "FLOAT":
            return "FLOAT"
        elif column_type in {"DECIMAL", "YEAR"}:
            return "NUMERIC"
        elif column_type in {"DATETIME", "TIMESTAMP"}:
            return "DATETIME"
        elif column_type == "DATE":
            return "DATE"
        elif column_type == "BLOB":
            return "BLOB"
        else:
            return "TEXT"

    def _build_create_table_sql(self, table_name):
        sql = 'CREATE TABLE IF NOT EXISTS "{}" ('.format(table_name)
        primary = "PRIMARY KEY ("
        indices = ""

        self._mysql_cur_dict.execute("SHOW COLUMNS FROM `{}`".format(table_name))

        for row in self._mysql_cur_dict.fetchall():
            sql += ' "{name}" {type} {notnull}, '.format(
                name=row["Field"],
                type=self._translate_type_from_mysql_to_sqlite(row["Type"]),
                notnull="NULL" if row["Null"] == "YES" else "NOT NULL",
            )
            if row["Key"] in {"PRI", "UNI", "MUL"}:
                if row["Key"] == "PRI":
                    primary += '"{name}", '.format(name=row["Field"])
                else:
                    indices += ' CREATE {unique} INDEX {table_name}_{column_slug_name}_IDX ON "{table_name}" ("{column_name}");'.format(
                        unique="UNIQUE" if row["Key"] == "UNI" else "",
                        table_name=table_name,
                        column_slug_name=slugify(row["Field"], separator="_"),
                        column_name=row["Field"],
                    )
        sql += primary.rstrip(", ")
        sql += "));"
        sql += indices
        return " ".join(sql.split())

    def _create_table(self, table_name):
        try:
            # create the table
            self._sqlite_cur.executescript(self._build_create_table_sql(table_name))
            self._sqlite.commit()
        except mysql.connector.Error as err:
            if err.errno == errorcode.CR_SERVER_LOST:
                self._logger.warning(
                    "Connection to MySQL server lost.\nAttempting to reconnect."
                )
                # attempt a reconnect
                self._mysql.reconnect()
                # create the table again
                self._sqlite_cur.executescript(self._build_create_table_sql(table_name))
                self._sqlite.commit()
            else:
                self._logger.error(
                    "_create_table failed creating table {}: {}".format(table_name, err)
                )
                sys.exit(1)
        except sqlite3.Error as err:
            self._logger.error(
                "_create_table failed creating table {}: {}".format(table_name, err)
            )
            sys.exit(1)

    def _transfer_table_data(self, sql, total_records=0):
        if self._chunk_size is not None and self._chunk_size > 0:
            for chunk in tqdm(
                    range(
                        self._current_chunk_number, ceil(total_records / self._chunk_size)
                    )
            ):
                self._current_chunk_number = chunk
                self._sqlite_cur.executemany(
                    sql,
                    (
                        tuple(col.decode() if col is not None else None for col in row)
                        for row in self._mysql_cur.fetchmany(self._chunk_size)
                    ),
                )
                self._sqlite.commit()
        else:
            self._sqlite_cur.executemany(
                sql,
                (
                    tuple(col.decode() if col is not None else None for col in row)
                    for row in self._mysql_cur.fetchall()
                ),
            )
            self._sqlite.commit()

    def transfer(self):
        self._mysql_cur.execute("SHOW TABLES")

        for row in self._mysql_cur.fetchall():
            # reset the chunk
            self._current_chunk_number = 0

            # create the table
            table_name = row[0].decode()
            self._create_table(table_name)

            # get the size of the data
            self._mysql_cur_dict.execute(
                "SELECT COUNT(*) AS `total_records` FROM `{}`".format(table_name)
            )
            total_records = int(self._mysql_cur_dict.fetchone()["total_records"])

            # populate it
            self._logger.info("Transferring table {}".format(table_name))
            self._mysql_cur.execute("SELECT * FROM `{}`".format(table_name))
            columns = [column[0] for column in self._mysql_cur.description]
            # build the SQL string
            sql = 'INSERT OR IGNORE INTO "{table}" ({fields}) VALUES ({placeholders})'.format(
                table=table_name,
                fields=('"{}", ' * len(columns)).rstrip(" ,").format(*columns),
                placeholders=("?, " * len(columns)).rstrip(" ,"),
            )
            try:
                # transfer the table data
                self._transfer_table_data(sql=sql, total_records=total_records)
            except mysql.connector.Error as err:
                if err.errno == errorcode.CR_SERVER_LOST:
                    self._logger.warning(
                        "Connection to MySQL server lost.\nAttempting to reconnect."
                    )
                    # attempt a reconnect
                    self._mysql.reconnect()
                    # resume the transfer
                    self._transfer_table_data(sql=sql, total_records=total_records)
                else:
                    self._logger.error(
                        "transfer failed inserting data into table {}: {}".format(
                            table_name, err
                        )
                    )
                    sys.exit(1)
            except sqlite3.Error as err:
                self._logger.error(
                    "transfer failed inserting data into table {}: {}".format(
                        table_name, err
                    )
                )
                sys.exit(1)

        if self._vacuum:
            self._logger.info(
                "Vacuuming created SQLite database file.\nThis might take a while."
            )
            self._sqlite_cur.execute("VACUUM")

        self._logger.info("Done!")


if __name__ == "__main__":
    import argparse

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
        converter = MySQL2SQLite(
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
        print("\nProcess interrupted Exiting...")
        sys.exit(1)
