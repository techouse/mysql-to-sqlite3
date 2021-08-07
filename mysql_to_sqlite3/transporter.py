"""Use to transfer a MySQL database to SQLite."""

from __future__ import division

import logging
import re
import sqlite3
from datetime import timedelta
from decimal import Decimal
from math import ceil
from os.path import realpath
from sys import stdout

import mysql.connector
import six
from mysql.connector import errorcode
from tqdm import tqdm, trange

from mysql_to_sqlite3.sqlite_utils import (
    CollatingSequences,
    adapt_decimal,
    adapt_timedelta,
    convert_decimal,
    convert_timedelta,
    encode_data_for_sqlite,
)

if six.PY2:
    from .sixeptions import *  # pylint: disable=W0401


class MySQLtoSQLite:
    """Use this class to transfer a MySQL database to SQLite."""

    COLUMN_PATTERN = re.compile(r"^[^(]+")
    COLUMN_LENGTH_PATTERN = re.compile(r"\(\d+\)$")

    def __init__(self, **kwargs):
        """Constructor."""
        if not kwargs.get("mysql_database"):
            raise ValueError("Please provide a MySQL database")

        if not kwargs.get("mysql_user"):
            raise ValueError("Please provide a MySQL user")

        self._mysql_database = str(kwargs.get("mysql_database"))

        self._mysql_tables = (
            tuple(kwargs.get("mysql_tables"))
            if kwargs.get("mysql_tables") is not None
            else tuple()
        )

        self._limit_rows = int(kwargs.get("limit_rows") or 0)

        if kwargs.get("collation") is not None and kwargs.get("collation").upper() in {
            CollatingSequences.BINARY,
            CollatingSequences.NOCASE,
            CollatingSequences.RTRIM,
        }:
            self._collation = kwargs.get("collation").upper()
        else:
            self._collation = CollatingSequences.BINARY

        self._prefix_indices = kwargs.get("prefix_indices") or False

        self._without_foreign_keys = (
            True
            if len(self._mysql_tables) > 0
            else (kwargs.get("without_foreign_keys") or False)
        )

        self._mysql_user = str(kwargs.get("mysql_user"))

        self._mysql_password = (
            str(kwargs.get("mysql_password")) if kwargs.get("mysql_password") else None
        )

        self._mysql_host = str(kwargs.get("mysql_host") or "localhost")

        self._mysql_port = int(kwargs.get("mysql_port") or 3306)

        self._mysql_ssl_disabled = kwargs.get("mysql_ssl_disabled") or False

        self._current_chunk_number = 0
        self._chunk_size = int(kwargs.get("chunk")) if kwargs.get("chunk") else None

        self._sqlite_file = kwargs.get("sqlite_file") or None

        self._buffered = kwargs.get("buffered") or False

        self._vacuum = kwargs.get("vacuum") or False

        self._quiet = kwargs.get("quiet") or False

        self._logger = self._setup_logger(
            log_file=kwargs.get("log_file") or None, quiet=self._quiet
        )

        sqlite3.register_adapter(Decimal, adapt_decimal)
        sqlite3.register_converter("DECIMAL", convert_decimal)
        sqlite3.register_adapter(timedelta, adapt_timedelta)
        sqlite3.register_converter("TIME", convert_timedelta)

        self._sqlite = sqlite3.connect(
            realpath(self._sqlite_file), detect_types=sqlite3.PARSE_DECLTYPES
        )
        self._sqlite.row_factory = sqlite3.Row

        self._sqlite_cur = self._sqlite.cursor()

        try:
            self._mysql = mysql.connector.connect(
                user=self._mysql_user,
                password=self._mysql_password,
                host=self._mysql_host,
                port=self._mysql_port,
                ssl_disabled=self._mysql_ssl_disabled,
            )
            if not self._mysql.is_connected():
                raise ConnectionError("Unable to connect to MySQL")

            self._mysql_cur = self._mysql.cursor(buffered=self._buffered, raw=True)
            self._mysql_cur_prepared = self._mysql.cursor(prepared=True)
            self._mysql_cur_dict = self._mysql.cursor(
                buffered=self._buffered,
                dictionary=True,
            )
            try:
                self._mysql.database = self._mysql_database
            except (mysql.connector.Error, Exception) as err:
                if hasattr(err, "errno") and err.errno == errorcode.ER_BAD_DB_ERROR:
                    self._logger.error("MySQL Database does not exist!")
                    raise
                self._logger.error(err)
                raise
        except mysql.connector.Error as err:
            self._logger.error(err)
            raise

    @classmethod
    def _setup_logger(cls, log_file=None, quiet=False):
        formatter = logging.Formatter(
            fmt="%(asctime)s %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        logger = logging.getLogger(cls.__name__)
        logger.setLevel(logging.DEBUG)

        if not quiet:
            screen_handler = logging.StreamHandler(stream=stdout)
            screen_handler.setFormatter(formatter)
            logger.addHandler(screen_handler)

        if log_file:
            file_handler = logging.FileHandler(realpath(log_file), mode="w")
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        return logger

    @classmethod
    def _valid_column_type(cls, column_type):
        return cls.COLUMN_PATTERN.match(column_type.strip())

    @classmethod
    def _column_type_length(cls, column_type):
        suffix = cls.COLUMN_LENGTH_PATTERN.search(column_type)
        if suffix:
            return suffix.group(0)
        return ""

    @classmethod
    def _translate_type_from_mysql_to_sqlite(cls, column_type):
        """Handle MySQL 8."""
        try:
            column_type = column_type.decode()
        except (UnicodeDecodeError, AttributeError):
            pass

        # This could be optimized even further, however is seems adequate.
        match = cls._valid_column_type(column_type)
        if not match:
            raise ValueError("Invalid column_type!")

        data_type = match.group(0).upper()

        if data_type.endswith(" UNSIGNED"):
            data_type = data_type.replace(" UNSIGNED", "")

        if data_type in {
            "BIGINT",
            "BLOB",
            "BOOLEAN",
            "DATE",
            "DATETIME",
            "DECIMAL",
            "DOUBLE",
            "FLOAT",
            "INTEGER",
            "MEDIUMINT",
            "NUMERIC",
            "REAL",
            "SMALLINT",
            "TIME",
            "TINYINT",
            "YEAR",
        }:
            return data_type
        if data_type in {
            "BIT",
            "BINARY",
            "LONGBLOB",
            "MEDIUMBLOB",
            "TINYBLOB",
            "VARBINARY",
        }:
            return "BLOB"
        if data_type in {"NCHAR", "NVARCHAR", "VARCHAR"}:
            return data_type + cls._column_type_length(column_type)
        if data_type == "CHAR":
            return "CHARACTER" + cls._column_type_length(column_type)
        if data_type == "INT":
            return "INTEGER"
        if data_type in "TIMESTAMP":
            return "DATETIME"
        return "TEXT"

    @classmethod
    def _translate_default_from_mysql_to_sqlite(
        cls, column_default=None, column_type=None
    ):
        try:
            column_default = column_default.decode()
        except (UnicodeDecodeError, AttributeError):
            pass

        if column_default is None:
            return ""
        if isinstance(column_default, bool):
            if column_type == "BOOLEAN" and sqlite3.sqlite_version >= "3.23.0":
                if column_default:
                    return "DEFAULT(TRUE)"
                return "DEFAULT(FALSE)"
            return "DEFAULT '{}'".format(int(column_default))
        if (
            six.PY2
            and isinstance(
                column_default, unicode  # noqa: ignore=F405 pylint: disable=E0602
            )
        ) or isinstance(column_default, str):
            if column_default.upper() in {
                "CURRENT_TIME",
                "CURRENT_DATE",
                "CURRENT_TIMESTAMP",
            }:
                return "DEFAULT {}".format(column_default.upper())
        return "DEFAULT '{}'".format(column_default)

    @classmethod
    def _data_type_collation_sequence(
        cls, collation=CollatingSequences.BINARY, column_type=None
    ):
        if column_type and collation != CollatingSequences.BINARY:
            if column_type.startswith(
                (
                    "CHARACTER",
                    "NCHAR",
                    "NVARCHAR",
                    "TEXT",
                    "VARCHAR",
                )
            ):
                return "COLLATE {collation}".format(collation=collation)
        return ""

    def _build_create_table_sql(self, table_name):
        sql = 'CREATE TABLE IF NOT EXISTS "{}" ('.format(table_name)
        primary = ""
        indices = ""

        self._mysql_cur_dict.execute("SHOW COLUMNS FROM `{}`".format(table_name))

        for row in self._mysql_cur_dict.fetchall():
            column_type = self._translate_type_from_mysql_to_sqlite(row["Type"])
            sql += '\n\t"{name}" {type} {notnull} {default} {collation},'.format(
                name=row["Field"],
                type=column_type,
                notnull="NULL" if row["Null"] == "YES" else "NOT NULL",
                default=self._translate_default_from_mysql_to_sqlite(
                    row["Default"], column_type
                ),
                collation=self._data_type_collation_sequence(
                    self._collation, column_type
                ),
            )

        self._mysql_cur_dict.execute(
            """
            SELECT INDEX_NAME AS `name`,
                  IF (NON_UNIQUE = 0 AND INDEX_NAME = 'PRIMARY', 1, 0) AS `primary`,
                  IF (NON_UNIQUE = 0 AND INDEX_NAME <> 'PRIMARY', 1, 0) AS `unique`,
                  GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS `columns`
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = %s
            AND TABLE_NAME = %s
            GROUP BY INDEX_NAME, NON_UNIQUE
            """,
            (self._mysql_database, table_name),
        )
        for index in self._mysql_cur_dict.fetchall():
            if int(index["primary"]) == 1:
                primary += "\n\tPRIMARY KEY ({columns})".format(
                    columns=", ".join(
                        '"{}"'.format(column) for column in index["columns"].split(",")
                    )
                )
            else:
                indices += """CREATE {unique} INDEX IF NOT EXISTS "{name}" ON "{table}" ({columns});""".format(
                    unique="UNIQUE" if int(index["unique"]) == 1 else "",
                    name="{table}_{name}".format(table=table_name, name=index["name"])
                    if self._prefix_indices
                    else index["name"],
                    table=table_name,
                    columns=", ".join(
                        '"{}"'.format(column) for column in index["columns"].split(",")
                    ),
                )

        sql += primary
        sql = sql.rstrip(", ")

        if not self._without_foreign_keys:
            server_version = self._mysql.get_server_version()
            self._mysql_cur_dict.execute(
                """
                SELECT k.COLUMN_NAME AS `column`,
                       k.REFERENCED_TABLE_NAME AS `ref_table`,
                       k.REFERENCED_COLUMN_NAME AS `ref_column`,
                       c.UPDATE_RULE AS `on_update`,
                       c.DELETE_RULE AS `on_delete`
                FROM information_schema.TABLE_CONSTRAINTS AS i
                {JOIN} information_schema.KEY_COLUMN_USAGE AS k
                    ON i.CONSTRAINT_NAME = k.CONSTRAINT_NAME
                {JOIN} information_schema.REFERENTIAL_CONSTRAINTS AS c
                    ON c.CONSTRAINT_NAME = i.CONSTRAINT_NAME
                WHERE i.TABLE_SCHEMA = %s
                AND i.TABLE_NAME = %s
                AND i.CONSTRAINT_TYPE = %s
                GROUP BY i.CONSTRAINT_NAME,
                         k.COLUMN_NAME,
                         k.REFERENCED_TABLE_NAME,
                         k.REFERENCED_COLUMN_NAME,
                         c.UPDATE_RULE,
                         c.DELETE_RULE
                """.format(
                    JOIN="JOIN"
                    if (server_version[0] == 8 and server_version[2] > 19)
                    else "LEFT JOIN"
                ),
                (self._mysql_database, table_name, "FOREIGN KEY"),
            )
            for foreign_key in self._mysql_cur_dict.fetchall():
                sql += """,\n\tFOREIGN KEY("{column}") REFERENCES "{ref_table}" ("{ref_column}") ON UPDATE {on_update} ON DELETE {on_delete}""".format(
                    **foreign_key
                )

        sql += "\n);"
        sql += indices
        return sql

    def _create_table(self, table_name, attempting_reconnect=False):
        try:
            if attempting_reconnect:
                self._mysql.reconnect()
            self._sqlite_cur.executescript(self._build_create_table_sql(table_name))
            self._sqlite.commit()
        except mysql.connector.Error as err:
            if err.errno == errorcode.CR_SERVER_LOST:
                if not attempting_reconnect:
                    self._logger.warning(
                        "Connection to MySQL server lost." "\nAttempting to reconnect."
                    )
                    self._create_table(table_name, True)
                else:
                    self._logger.warning(
                        "Connection to MySQL server lost."
                        "\nReconnection attempt aborted."
                    )
                    raise
            self._logger.error(
                "MySQL failed reading table definition from table %s: %s",
                table_name,
                err,
            )
            raise
        except sqlite3.Error as err:
            self._logger.error("SQLite failed creating table %s: %s", table_name, err)
            raise

    def _transfer_table_data(
        self, table_name, sql, total_records=0, attempting_reconnect=False
    ):
        if attempting_reconnect:
            self._mysql.reconnect()
        try:
            if self._chunk_size is not None and self._chunk_size > 0:
                for chunk in trange(
                    self._current_chunk_number,
                    int(ceil(total_records / self._chunk_size)),
                    disable=self._quiet,
                ):
                    self._current_chunk_number = chunk
                    self._sqlite_cur.executemany(
                        sql,
                        (
                            tuple(
                                encode_data_for_sqlite(col) if col is not None else None
                                for col in row
                            )
                            for row in self._mysql_cur.fetchmany(self._chunk_size)
                        ),
                    )
            else:
                self._sqlite_cur.executemany(
                    sql,
                    (
                        tuple(
                            encode_data_for_sqlite(col) if col is not None else None
                            for col in row
                        )
                        for row in tqdm(
                            self._mysql_cur.fetchall(),
                            total=total_records,
                            disable=self._quiet,
                        )
                    ),
                )
            self._sqlite.commit()
        except mysql.connector.Error as err:
            if err.errno == errorcode.CR_SERVER_LOST:
                if not attempting_reconnect:
                    self._logger.warning(
                        "Connection to MySQL server lost." "\nAttempting to reconnect."
                    )
                    self._transfer_table_data(
                        table_name=table_name,
                        sql=sql,
                        total_records=total_records,
                        attempting_reconnect=True,
                    )
                else:
                    self._logger.warning(
                        "Connection to MySQL server lost."
                        "\nReconnection attempt aborted."
                    )
                    raise
            self._logger.error(
                "MySQL transfer failed reading table data from table %s: %s",
                table_name,
                err,
            )
            raise
        except sqlite3.Error as err:
            self._logger.error(
                "SQLite transfer failed inserting data into table %s: %s",
                table_name,
                err,
            )
            raise

    def transfer(self):
        """The primary and only method with which we transfer all the data."""
        if len(self._mysql_tables) > 0:
            # transfer only specific tables

            self._mysql_cur_prepared.execute(
                """
                SELECT TABLE_NAME
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = SCHEMA()
                AND TABLE_NAME IN ({placeholders})
            """.format(
                    placeholders=("%s, " * len(self._mysql_tables)).rstrip(" ,")
                ),
                self._mysql_tables,
            )
            tables = (row[0] for row in self._mysql_cur_prepared.fetchall())
        else:
            # transfer all tables
            self._mysql_cur.execute(
                """
                SELECT TABLE_NAME
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = SCHEMA()
            """
            )
            tables = (row[0].decode() for row in self._mysql_cur.fetchall())

        try:
            # turn off foreign key checking in SQLite while transferring data
            self._sqlite_cur.execute("PRAGMA foreign_keys=OFF")

            for table_name in tables:
                # reset the chunk
                self._current_chunk_number = 0

                # create the table
                self._create_table(table_name)

                # get the size of the data
                if self._limit_rows > 0:
                    # limit to the requested number of rows
                    self._mysql_cur_dict.execute(
                        """
                            SELECT COUNT(*) AS `total_records`
                            FROM (SELECT * FROM `{table_name}` LIMIT {limit}) AS `table`
                        """.format(
                            table_name=table_name, limit=self._limit_rows
                        )
                    )
                else:
                    # get all rows
                    self._mysql_cur_dict.execute(
                        "SELECT COUNT(*) AS `total_records` FROM `{table_name}`".format(
                            table_name=table_name
                        )
                    )
                total_records = int(self._mysql_cur_dict.fetchone()["total_records"])

                # only continue if there is anything to transfer
                if total_records > 0:
                    # populate it
                    self._logger.info("Transferring table %s", table_name)
                    self._mysql_cur.execute(
                        "SELECT * FROM `{table_name}` {limit}".format(
                            table_name=table_name,
                            limit="LIMIT {}".format(self._limit_rows)
                            if self._limit_rows > 0
                            else "",
                        )
                    )
                    columns = [column[0] for column in self._mysql_cur.description]
                    # build the SQL string
                    sql = """
                        INSERT OR IGNORE
                        INTO "{table}" ({fields})
                        VALUES ({placeholders})
                    """.format(
                        table=table_name,
                        fields=('"{}", ' * len(columns)).rstrip(" ,").format(*columns),
                        placeholders=("?, " * len(columns)).rstrip(" ,"),
                    )
                    self._transfer_table_data(
                        table_name=table_name, sql=sql, total_records=total_records
                    )
        except Exception:  # pylint: disable=W0706
            raise
        finally:
            # re-enable foreign key checking once done transferring
            self._sqlite_cur.execute("PRAGMA foreign_keys=ON")

        if self._vacuum:
            self._logger.info(
                "Vacuuming created SQLite database file.\nThis might take a while."
            )
            self._sqlite_cur.execute("VACUUM")

        self._logger.info("Done!")
