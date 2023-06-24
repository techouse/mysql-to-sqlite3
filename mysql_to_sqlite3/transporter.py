"""Use to transfer a MySQL database to SQLite."""

import logging
import os
import re
import sqlite3
import typing as t
from datetime import timedelta
from decimal import Decimal
from math import ceil
from os.path import realpath
from sys import stdout

import mysql.connector
import typing_extensions as tx
from mysql.connector import MySQLConnection, errorcode
from mysql.connector.connection_cext import CMySQLConnection
from mysql.connector.types import ToPythonOutputTypes
from tqdm import tqdm, trange

from mysql_to_sqlite3.mysql_utils import CHARSET_INTRODUCERS
from mysql_to_sqlite3.sqlite_utils import (
    CollatingSequences,
    adapt_decimal,
    adapt_timedelta,
    convert_date,
    convert_decimal,
    convert_timedelta,
    encode_data_for_sqlite,
)
from mysql_to_sqlite3.types import MySQLtoSQLiteAttributes, MySQLtoSQLiteParams


class MySQLtoSQLite(MySQLtoSQLiteAttributes):
    """Use this class to transfer a MySQL database to SQLite."""

    COLUMN_PATTERN: t.Pattern[str] = re.compile(r"^[^(]+")
    COLUMN_LENGTH_PATTERN: t.Pattern[str] = re.compile(r"\(\d+\)$")

    def __init__(self, **kwargs: tx.Unpack[MySQLtoSQLiteParams]) -> None:
        """Constructor."""
        if kwargs.get("mysql_database") is not None:
            self._mysql_database = str(kwargs.get("mysql_database"))
        else:
            raise ValueError("Please provide a MySQL database")

        if kwargs.get("mysql_user") is not None:
            self._mysql_user = str(kwargs.get("mysql_user"))
        else:
            raise ValueError("Please provide a MySQL user")

        if kwargs.get("sqlite_file") is None:
            raise ValueError("Please provide an SQLite file")
        else:
            self._sqlite_file = realpath(str(kwargs.get("sqlite_file")))

        self._mysql_password = str(kwargs.get("mysql_password")) or None

        self._mysql_host = kwargs.get("mysql_host") or "localhost"

        self._mysql_port = kwargs.get("mysql_port") or 3306

        self._mysql_tables = kwargs.get("mysql_tables") or tuple()

        self._exclude_mysql_tables = kwargs.get("exclude_mysql_tables") or tuple()

        if len(self._mysql_tables) > 0 and len(self._exclude_mysql_tables) > 0:
            raise ValueError("mysql_tables and exclude_mysql_tables are mutually exclusive")

        self._limit_rows = kwargs.get("limit_rows") or 0

        if kwargs.get("collation") is not None and str(kwargs.get("collation")).upper() in {
            CollatingSequences.BINARY,
            CollatingSequences.NOCASE,
            CollatingSequences.RTRIM,
        }:
            self._collation = str(kwargs.get("collation")).upper()
        else:
            self._collation = CollatingSequences.BINARY

        self._prefix_indices = kwargs.get("prefix_indices") or False

        if len(self._mysql_tables) > 0 or len(self._exclude_mysql_tables) > 0:
            self._without_foreign_keys = True
        else:
            self._without_foreign_keys = kwargs.get("without_foreign_keys") or False

        self._without_data = kwargs.get("without_data") or False

        self._mysql_ssl_disabled = kwargs.get("mysql_ssl_disabled") or False

        self._current_chunk_number = 0

        self._chunk_size = kwargs.get("chunk") or None

        self._buffered = kwargs.get("buffered") or False

        self._vacuum = kwargs.get("vacuum") or False

        self._quiet = kwargs.get("quiet") or False

        self._logger = self._setup_logger(log_file=kwargs.get("log_file") or None, quiet=self._quiet)

        sqlite3.register_adapter(Decimal, adapt_decimal)
        sqlite3.register_converter("DECIMAL", convert_decimal)
        sqlite3.register_adapter(timedelta, adapt_timedelta)
        sqlite3.register_converter("DATE", convert_date)
        sqlite3.register_converter("TIME", convert_timedelta)

        self._sqlite = sqlite3.connect(realpath(self._sqlite_file), detect_types=sqlite3.PARSE_DECLTYPES)
        self._sqlite.row_factory = sqlite3.Row

        self._sqlite_cur = self._sqlite.cursor()

        self._json_as_text = kwargs.get("json_as_text") or False

        self._sqlite_json1_extension_enabled = not self._json_as_text and self._check_sqlite_json1_extension_enabled()

        try:
            _mysql_connection = mysql.connector.connect(
                user=self._mysql_user,
                password=self._mysql_password,
                host=self._mysql_host,
                port=self._mysql_port,
                ssl_disabled=self._mysql_ssl_disabled,
            )
            if isinstance(_mysql_connection, (MySQLConnection, CMySQLConnection)):
                self._mysql = _mysql_connection
            else:
                raise ConnectionError("Unable to connect to MySQL")
            if not self._mysql.is_connected():
                raise ConnectionError("Unable to connect to MySQL")

            self._mysql_cur = self._mysql.cursor(buffered=self._buffered, raw=True)  # type: ignore[assignment]
            self._mysql_cur_prepared = self._mysql.cursor(prepared=True)  # type: ignore[assignment]
            self._mysql_cur_dict = self._mysql.cursor(  # type: ignore[assignment]
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
    def _setup_logger(
        cls, log_file: t.Optional[t.Union[str, "os.PathLike[t.Any]"]] = None, quiet: bool = False
    ) -> logging.Logger:
        formatter: logging.Formatter = logging.Formatter(
            fmt="%(asctime)s %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        logger: logging.Logger = logging.getLogger(cls.__name__)
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
    def _valid_column_type(cls, column_type: str) -> t.Optional[t.Match[str]]:
        return cls.COLUMN_PATTERN.match(column_type.strip())

    @classmethod
    def _column_type_length(cls, column_type: str) -> str:
        suffix: t.Optional[t.Match[str]] = cls.COLUMN_LENGTH_PATTERN.search(column_type)
        if suffix:
            return suffix.group(0)
        return ""

    @staticmethod
    def _decode_column_type(column_type: t.Union[str, bytes]) -> str:
        if isinstance(column_type, str):
            return column_type
        if isinstance(column_type, bytes):
            try:
                return column_type.decode()
            except (UnicodeDecodeError, AttributeError):
                pass
        return str(column_type)

    @classmethod
    def _translate_type_from_mysql_to_sqlite(
        cls, column_type: t.Union[str, bytes], sqlite_json1_extension_enabled=False
    ) -> str:
        _column_type: str = cls._decode_column_type(column_type)

        # This could be optimized even further, however is seems adequate.
        match: t.Optional[t.Match[str]] = cls._valid_column_type(_column_type)
        if not match:
            raise ValueError("Invalid column_type!")

        data_type: str = match.group(0).upper()

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
            return data_type + cls._column_type_length(_column_type)
        if data_type == "CHAR":
            return "CHARACTER" + cls._column_type_length(_column_type)
        if data_type == "INT":
            return "INTEGER"
        if data_type in "TIMESTAMP":
            return "DATETIME"
        if data_type == "JSON" and sqlite_json1_extension_enabled:
            return "JSON"
        return "TEXT"

    @classmethod
    def _translate_default_from_mysql_to_sqlite(
        cls,
        column_default: ToPythonOutputTypes = None,
        column_type: t.Optional[str] = None,
        column_extra: ToPythonOutputTypes = None,
    ) -> str:
        is_binary: bool
        is_hex: bool
        if isinstance(column_default, bytes):
            if column_type in {
                "BIT",
                "BINARY",
                "BLOB",
                "LONGBLOB",
                "MEDIUMBLOB",
                "TINYBLOB",
                "VARBINARY",
            }:
                if column_extra in {"DEFAULT_GENERATED", "default_generated"}:
                    for charset_introducer in CHARSET_INTRODUCERS:
                        if column_default.startswith(charset_introducer.encode()):
                            is_binary = False
                            is_hex = False
                            for b_prefix in ("B", "b"):
                                if column_default.startswith(rf"{charset_introducer} {b_prefix}\'".encode()):
                                    is_binary = True
                                    break
                            for x_prefix in ("X", "x"):
                                if column_default.startswith(rf"{charset_introducer} {x_prefix}\'".encode()):
                                    is_hex = True
                                    break
                            column_default = (
                                column_default.replace(charset_introducer.encode(), b"")
                                .replace(rb"x\'", b"")
                                .replace(rb"X\'", b"")
                                .replace(rb"b\'", b"")
                                .replace(rb"B\'", b"")
                                .replace(rb"\'", b"")
                                .replace(rb"'", b"")
                                .strip()
                            )
                            if is_binary:
                                return f"DEFAULT '{chr(int(column_default, 2))}'"
                            if is_hex:
                                return f"DEFAULT x'{column_default.decode()}'"
                            break
                return f"DEFAULT x'{column_default.hex()}'"
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
            return f"DEFAULT '{int(column_default)}'"
        if isinstance(column_default, str):
            if column_extra in {"DEFAULT_GENERATED", "default_generated"}:
                if column_default.upper() in {
                    "CURRENT_TIME",
                    "CURRENT_DATE",
                    "CURRENT_TIMESTAMP",
                }:
                    return f"DEFAULT {column_default.upper()}"
                for charset_introducer in CHARSET_INTRODUCERS:
                    if column_default.startswith(charset_introducer):
                        is_binary = False
                        is_hex = False
                        for b_prefix in ("B", "b"):
                            if column_default.startswith(rf"{charset_introducer} {b_prefix}\'"):
                                is_binary = True
                                break
                        for x_prefix in ("X", "x"):
                            if column_default.startswith(rf"{charset_introducer} {x_prefix}\'"):
                                is_hex = True
                                break
                        column_default = (
                            column_default.replace(charset_introducer, "")
                            .replace(r"x\'", "")
                            .replace(r"X\'", "")
                            .replace(r"b\'", "")
                            .replace(r"B\'", "")
                            .replace(r"\'", "")
                            .replace(r"'", "")
                            .strip()
                        )
                        if is_binary:
                            return f"DEFAULT '{chr(int(column_default, 2))}'"
                        if is_hex:
                            return f"DEFAULT x'{column_default}'"
                        return f"DEFAULT '{column_default}'"
            return "DEFAULT '{}'".format(column_default.replace(r"\'", r"''"))
        return "DEFAULT '{}'".format(str(column_default).replace(r"\'", r"''"))

    @classmethod
    def _data_type_collation_sequence(
        cls, collation: str = CollatingSequences.BINARY, column_type: t.Optional[str] = None
    ) -> str:
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
                return f"COLLATE {collation}"
        return ""

    def _check_sqlite_json1_extension_enabled(self) -> bool:
        try:
            self._sqlite_cur.execute("PRAGMA compile_options")
            return "ENABLE_JSON1" in set(row[0] for row in self._sqlite_cur.fetchall())
        except sqlite3.Error:
            return False

    def _build_create_table_sql(self, table_name: str) -> str:
        sql: str = f'CREATE TABLE IF NOT EXISTS "{table_name}" ('
        primary: str = ""
        indices: str = ""

        self._mysql_cur_dict.execute(f"SHOW COLUMNS FROM `{table_name}`")

        for row in self._mysql_cur_dict.fetchall():
            if row is not None:
                column_type = self._translate_type_from_mysql_to_sqlite(
                    column_type=row["Type"],  # type: ignore[arg-type]
                    sqlite_json1_extension_enabled=self._sqlite_json1_extension_enabled,
                )
                sql += '\n\t"{name}" {type} {notnull} {default} {collation},'.format(
                    name=row["Field"].decode() if isinstance(row["Field"], bytes) else row["Field"],
                    type=column_type,
                    notnull="NULL" if row["Null"] == "YES" else "NOT NULL",
                    default=self._translate_default_from_mysql_to_sqlite(row["Default"], column_type, row["Extra"]),
                    collation=self._data_type_collation_sequence(self._collation, column_type),
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
            if index is not None:
                columns: str = ""
                if isinstance(index["columns"], bytes):
                    columns = index["columns"].decode()
                elif isinstance(index["columns"], str):
                    columns = index["columns"]

                if len(columns) > 0:
                    if index["primary"] in {1, "1"}:
                        primary += "\n\tPRIMARY KEY ({})".format(
                            ", ".join(f'"{column}"' for column in columns.split(","))
                        )
                    else:
                        indices += """CREATE {unique} INDEX IF NOT EXISTS "{name}" ON "{table}" ({columns});""".format(
                            unique="UNIQUE" if index["unique"] in {1, "1"} else "",
                            name="{table}_{name}".format(
                                table=table_name,
                                name=index["name"].decode() if isinstance(index["name"], bytes) else index["name"],
                            )
                            if self._prefix_indices
                            else index["name"].decode()
                            if isinstance(index["name"], bytes)
                            else index["name"],
                            table=table_name,
                            columns=", ".join(f'"{column}"' for column in columns.split(",")),
                        )

        sql += primary
        sql = sql.rstrip(", ")

        if not self._without_foreign_keys:
            server_version: t.Tuple[int, ...] = self._mysql.get_server_version()
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
                    JOIN="JOIN" if (server_version[0] == 8 and server_version[2] > 19) else "LEFT JOIN"
                ),
                (self._mysql_database, table_name, "FOREIGN KEY"),
            )
            for foreign_key in self._mysql_cur_dict.fetchall():
                if foreign_key is not None:
                    sql += (
                        ',\n\tFOREIGN KEY("{column}") REFERENCES "{ref_table}" ("{ref_column}") '
                        "ON UPDATE {on_update} "
                        "ON DELETE {on_delete}".format(**foreign_key)  # type: ignore[str-bytes-safe]
                    )

        sql += "\n);"
        sql += indices

        return sql

    def _create_table(self, table_name: str, attempting_reconnect: bool = False) -> None:
        try:
            if attempting_reconnect:
                self._mysql.reconnect()
            self._sqlite_cur.executescript(self._build_create_table_sql(table_name))
            self._sqlite.commit()
        except mysql.connector.Error as err:
            if err.errno == errorcode.CR_SERVER_LOST:
                if not attempting_reconnect:
                    self._logger.warning("Connection to MySQL server lost.\nAttempting to reconnect.")
                    self._create_table(table_name, True)
                else:
                    self._logger.warning("Connection to MySQL server lost.\nReconnection attempt aborted.")
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
        self, table_name: str, sql: str, total_records: int = 0, attempting_reconnect: bool = False
    ) -> None:
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
                            tuple(encode_data_for_sqlite(col) if col is not None else None for col in row)
                            for row in self._mysql_cur.fetchmany(self._chunk_size)
                        ),
                    )
            else:
                self._sqlite_cur.executemany(
                    sql,
                    (
                        tuple(encode_data_for_sqlite(col) if col is not None else None for col in row)
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
                    self._logger.warning("Connection to MySQL server lost.\nAttempting to reconnect.")
                    self._transfer_table_data(
                        table_name=table_name,
                        sql=sql,
                        total_records=total_records,
                        attempting_reconnect=True,
                    )
                else:
                    self._logger.warning("Connection to MySQL server lost.\nReconnection attempt aborted.")
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

    def transfer(self) -> None:
        """The primary and only method with which we transfer all the data."""
        if len(self._mysql_tables) > 0 or len(self._exclude_mysql_tables) > 0:
            # transfer only specific tables
            specific_tables: t.Sequence[str] = (
                self._exclude_mysql_tables if len(self._exclude_mysql_tables) > 0 else self._mysql_tables
            )

            self._mysql_cur_prepared.execute(
                """
                SELECT TABLE_NAME
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = SCHEMA()
                AND TABLE_NAME {exclude} IN ({placeholders})
            """.format(
                    exclude="NOT" if len(self._exclude_mysql_tables) > 0 else "",
                    placeholders=("%s, " * len(specific_tables)).rstrip(" ,"),
                ),
                specific_tables,
            )
            tables: t.Iterable[ToPythonOutputTypes] = (row[0] for row in self._mysql_cur_prepared.fetchall())
        else:
            # transfer all tables
            self._mysql_cur.execute(
                """
                SELECT TABLE_NAME
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = SCHEMA()
            """
            )
            tables = (row[0].decode() for row in self._mysql_cur.fetchall())  # type: ignore[union-attr]

        try:
            # turn off foreign key checking in SQLite while transferring data
            self._sqlite_cur.execute("PRAGMA foreign_keys=OFF")

            for table_name in tables:
                if isinstance(table_name, bytes):
                    table_name = table_name.decode()

                self._logger.info(
                    "%sTransferring table %s",
                    "[WITHOUT DATA] " if self._without_data else "",
                    table_name,
                )

                # reset the chunk
                self._current_chunk_number = 0

                # create the table
                self._create_table(table_name)  # type: ignore[arg-type]

                if not self._without_data:
                    # get the size of the data
                    if self._limit_rows > 0:
                        # limit to the requested number of rows
                        self._mysql_cur_dict.execute(
                            "SELECT COUNT(*) AS `total_records` "
                            f"FROM (SELECT * FROM `{table_name}` LIMIT {self._limit_rows}) AS `table`"
                        )
                    else:
                        # get all rows
                        self._mysql_cur_dict.execute(f"SELECT COUNT(*) AS `total_records` FROM `{table_name}`")

                    total_records: t.Optional[t.Dict[str, ToPythonOutputTypes]] = self._mysql_cur_dict.fetchone()
                    if total_records is not None:
                        total_records_count: int = int(total_records["total_records"])  # type: ignore[arg-type]
                    else:
                        total_records_count = 0

                    # only continue if there is anything to transfer
                    if total_records_count > 0:
                        # populate it
                        self._mysql_cur.execute(
                            "SELECT * FROM `{table_name}` {limit}".format(
                                table_name=table_name,
                                limit=f"LIMIT {self._limit_rows}" if self._limit_rows > 0 else "",
                            )
                        )
                        columns: t.Tuple[str, ...] = tuple(column[0] for column in self._mysql_cur.description)  # type: ignore[union-attr]
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
                            table_name=table_name,  # type: ignore[arg-type]
                            sql=sql,
                            total_records=total_records_count,
                        )
        except Exception:  # pylint: disable=W0706
            raise
        finally:
            # re-enable foreign key checking once done transferring
            self._sqlite_cur.execute("PRAGMA foreign_keys=ON")

        if self._vacuum:
            self._logger.info("Vacuuming created SQLite database file.\nThis might take a while.")
            self._sqlite_cur.execute("VACUUM")

        self._logger.info("Done!")
