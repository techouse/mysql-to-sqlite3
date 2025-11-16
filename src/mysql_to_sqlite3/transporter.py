"""Use to transfer a MySQL database to SQLite."""

from __future__ import annotations

import logging
import os
import re
import sqlite3
import typing as t
from datetime import timedelta
from decimal import Decimal
from os.path import realpath
from sys import stdout

import mysql.connector
from mysql.connector import CharacterSet, errorcode
from mysql.connector.abstracts import MySQLConnectionAbstract
from mysql.connector.types import RowItemType
from sqlglot import Expression, exp, parse_one
from sqlglot.errors import ParseError


# pylint: disable=protected-access  # Legacy helpers intentionally exposed for tests


try:
    # Python 3.11+
    from typing import Unpack  # type: ignore[attr-defined]
except ImportError:
    # Python < 3.11
    from typing_extensions import Unpack  # type: ignore

from mysql_to_sqlite3 import type_translation as _type_helpers
from mysql_to_sqlite3.data_transfer import DataTransferManager
from mysql_to_sqlite3.schema_writer import SchemaWriter
from mysql_to_sqlite3.sqlite_utils import (
    CollatingSequences,
    adapt_decimal,
    adapt_timedelta,
    convert_date,
    convert_decimal,
    convert_timedelta,
)
from mysql_to_sqlite3.types import MySQLtoSQLiteAttributes, MySQLtoSQLiteParams


class MySQLtoSQLite(MySQLtoSQLiteAttributes):
    """Use this class to transfer a MySQL database to SQLite."""

    escape_mysql_backticks = staticmethod(_type_helpers.escape_mysql_backticks)

    def __init__(self, **kwargs: Unpack[MySQLtoSQLiteParams]) -> None:
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

        password: t.Optional[t.Union[str, bool]] = kwargs.get("mysql_password")
        self._mysql_password = password if isinstance(password, str) else None

        self._mysql_host = kwargs.get("mysql_host", "localhost") or "localhost"

        self._mysql_port = kwargs.get("mysql_port", 3306) or 3306

        self._mysql_charset = kwargs.get("mysql_charset", "utf8mb4") or "utf8mb4"

        self._mysql_collation = (
            kwargs.get("mysql_collation") or CharacterSet().get_default_collation(self._mysql_charset.lower())[0]
        )
        if not kwargs.get("mysql_collation") and self._mysql_collation == "utf8mb4_0900_ai_ci":
            self._mysql_collation = "utf8mb4_unicode_ci"

        self._mysql_tables = kwargs.get("mysql_tables") or tuple()

        self._exclude_mysql_tables = kwargs.get("exclude_mysql_tables") or tuple()

        if bool(self._mysql_tables) and bool(self._exclude_mysql_tables):
            raise ValueError("mysql_tables and exclude_mysql_tables are mutually exclusive")

        self._limit_rows = kwargs.get("limit_rows", 0) or 0

        if kwargs.get("collation") is not None and str(kwargs.get("collation")).upper() in {
            CollatingSequences.BINARY,
            CollatingSequences.NOCASE,
            CollatingSequences.RTRIM,
        }:
            self._collation = str(kwargs.get("collation")).upper()
        else:
            self._collation = CollatingSequences.BINARY

        self._prefix_indices = kwargs.get("prefix_indices", False) or False

        if bool(self._mysql_tables) or bool(self._exclude_mysql_tables):
            self._without_foreign_keys = True
        else:
            self._without_foreign_keys = bool(kwargs.get("without_foreign_keys", False))

        self._without_data = bool(kwargs.get("without_data", False))
        self._without_tables = bool(kwargs.get("without_tables", False))

        if self._without_tables and self._without_data:
            raise ValueError("Unable to continue without transferring data or creating tables!")

        self._mysql_ssl_disabled = bool(kwargs.get("mysql_ssl_disabled", False))

        self._current_chunk_number = 0

        self._chunk_size = kwargs.get("chunk") or None

        self._buffered = bool(kwargs.get("buffered", False))

        self._vacuum = bool(kwargs.get("vacuum", False))

        self._quiet = bool(kwargs.get("quiet", False))

        self._views_as_views = bool(kwargs.get("views_as_views", True))

        self._sqlite_strict = bool(kwargs.get("sqlite_strict", False))

        self._logger = self._setup_logger(log_file=kwargs.get("log_file") or None, quiet=self._quiet)

        if self._sqlite_strict and sqlite3.sqlite_version < "3.37.0":
            self._logger.warning(
                "SQLite version %s does not support STRICT tables. Tables will be created without strict mode.",
                sqlite3.sqlite_version,
            )
            self._sqlite_strict = False

        sqlite3.register_adapter(Decimal, adapt_decimal)
        sqlite3.register_converter("DECIMAL", convert_decimal)
        sqlite3.register_adapter(timedelta, adapt_timedelta)
        sqlite3.register_converter("DATE", convert_date)
        sqlite3.register_converter("TIME", convert_timedelta)

        self._sqlite = sqlite3.connect(realpath(self._sqlite_file), detect_types=sqlite3.PARSE_DECLTYPES)
        self._sqlite.row_factory = sqlite3.Row

        self._sqlite_cur = self._sqlite.cursor()

        self._json_as_text = bool(kwargs.get("json_as_text", False))

        self._sqlite_json1_extension_enabled = not self._json_as_text and self._check_sqlite_json1_extension_enabled()

        # Track seen SQLite index names to generate unique names when prefixing is disabled
        self._seen_sqlite_index_names: t.Set[str] = set()
        # Counter for duplicate index names to assign numeric suffixes (name_2, name_3, ...)
        self._sqlite_index_name_counters: t.Dict[str, int] = {}

        try:
            _mysql_connection = mysql.connector.connect(
                user=self._mysql_user,
                password=self._mysql_password,
                host=self._mysql_host,
                port=self._mysql_port,
                ssl_disabled=self._mysql_ssl_disabled,
                charset=self._mysql_charset,
                collation=self._mysql_collation,
            )
            if isinstance(_mysql_connection, MySQLConnectionAbstract):
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

        self._schema_writer = SchemaWriter(self)
        self._data_transfer = DataTransferManager(self)

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
        return _type_helpers._valid_column_type(column_type)

    @classmethod
    def _column_type_length(cls, column_type: str) -> str:
        return _type_helpers._column_type_length(column_type)

    @staticmethod
    def _decode_column_type(column_type: t.Union[str, bytes]) -> str:
        return _type_helpers._decode_column_type(column_type)

    @classmethod
    def _translate_type_from_mysql_to_sqlite(
        cls, column_type: t.Union[str, bytes], sqlite_json1_extension_enabled: bool = False
    ) -> str:
        return _type_helpers.translate_type_from_mysql_to_sqlite(
            column_type=column_type,
            sqlite_json1_extension_enabled=sqlite_json1_extension_enabled,
            decode_column_type=cls._decode_column_type,
            valid_column_type=cls._valid_column_type,
            transpile_mysql_type_to_sqlite=cls._transpile_mysql_type_to_sqlite,
        )

    @classmethod
    def _transpile_mysql_expr_to_sqlite(cls, expr_sql: str) -> t.Optional[str]:
        return _type_helpers._transpile_mysql_expr_to_sqlite(expr_sql, parse_func=parse_one)

    @classmethod
    def _normalize_literal_with_sqlglot(cls, expr_sql: str) -> t.Optional[str]:
        return _type_helpers._normalize_literal_with_sqlglot(expr_sql)

    @staticmethod
    def _quote_sqlite_identifier(name: t.Union[str, bytes, bytearray]) -> str:
        return _type_helpers.quote_sqlite_identifier(name)

    @staticmethod
    def _escape_mysql_backticks(identifier: str) -> str:
        return _type_helpers.escape_mysql_backticks(identifier)

    @classmethod
    def _transpile_mysql_type_to_sqlite(
        cls, column_type: str, sqlite_json1_extension_enabled: bool = False
    ) -> t.Optional[str]:
        return _type_helpers._transpile_mysql_type_to_sqlite(
            column_type,
            sqlite_json1_extension_enabled=sqlite_json1_extension_enabled,
            parse_func=parse_one,
            regex_search=re.search,
        )

    @classmethod
    def _translate_default_from_mysql_to_sqlite(
        cls,
        column_default: RowItemType = None,
        column_type: t.Optional[str] = None,
        column_extra: RowItemType = None,
    ) -> str:
        return _type_helpers.translate_default_from_mysql_to_sqlite(
            column_default=column_default,
            column_type=column_type,
            column_extra=column_extra,
            normalize_literal=cls._normalize_literal_with_sqlglot,
            transpile_expr=cls._transpile_mysql_expr_to_sqlite,
        )

    @classmethod
    def _data_type_collation_sequence(
        cls, collation: str = CollatingSequences.BINARY, column_type: t.Optional[str] = None
    ) -> str:
        return _type_helpers.data_type_collation_sequence(
            collation=collation,
            column_type=column_type,
            transpile_mysql_type_to_sqlite=cls._transpile_mysql_type_to_sqlite,
        )

    def _check_sqlite_json1_extension_enabled(self) -> bool:
        try:
            self._sqlite_cur.execute("PRAGMA compile_options")
            return "ENABLE_JSON1" in set(row[0] for row in self._sqlite_cur.fetchall())
        except sqlite3.Error:
            return False

    def _get_schema_writer(self) -> SchemaWriter:
        writer = getattr(self, "_schema_writer", None)
        if writer is None:
            writer = SchemaWriter(self)
            self._schema_writer = writer
        return writer

    def _get_data_transfer_manager(self) -> DataTransferManager:
        manager = getattr(self, "_data_transfer", None)
        if manager is None:
            manager = DataTransferManager(self)
            self._data_transfer = manager
        return manager

    def _get_unique_index_name(self, base_name: str) -> str:
        if base_name not in self._seen_sqlite_index_names:
            self._seen_sqlite_index_names.add(base_name)
            return base_name
        next_num = self._sqlite_index_name_counters.get(base_name, 2)
        candidate = f"{base_name}_{next_num}"
        while candidate in self._seen_sqlite_index_names:
            next_num += 1
            candidate = f"{base_name}_{next_num}"
        self._seen_sqlite_index_names.add(candidate)
        self._sqlite_index_name_counters[base_name] = next_num + 1
        self._logger.info(
            'Index "%s" renamed to "%s" to ensure uniqueness across the SQLite database.',
            base_name,
            candidate,
        )
        return candidate

    def _build_create_table_sql(self, table_name: str) -> str:
        return self._get_schema_writer()._build_create_table_sql(table_name)

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
                    return
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

    def _mysql_viewdef_to_sqlite(self, view_select_sql: str, view_name: str) -> str:
        cleaned_sql = view_select_sql.strip().rstrip(";")

        try:
            tree: Expression = parse_one(cleaned_sql, read="mysql")
        except (ParseError, ValueError, AttributeError, TypeError):
            stripped_sql = cleaned_sql
            sn: str = re.escape(self._mysql_database)
            for pat in (rf"`{sn}`\.", rf'"{sn}"\.', rf"\b{sn}\."):
                stripped_sql = re.sub(pat, "", stripped_sql, flags=re.IGNORECASE)
            view_ident = self._quote_sqlite_identifier(view_name)
            return f"CREATE VIEW IF NOT EXISTS {view_ident} AS\n{stripped_sql};"

        for tbl in tree.find_all(exp.Table):
            db = tbl.args.get("db")
            if db and db.name.strip('`"').lower() == self._mysql_database.lower():
                tbl.set("db", None)
        for col in tree.find_all(exp.Column):
            db = col.args.get("db")
            if db and db.name.strip('`"').lower() == self._mysql_database.lower():
                col.set("db", None)

        sqlite_select: str = tree.sql(dialect="sqlite")
        view_ident = self._quote_sqlite_identifier(view_name)
        return f"CREATE VIEW IF NOT EXISTS {view_ident} AS\n{sqlite_select};"

    def _build_create_view_sql(self, view_name: str) -> str:
        definition: t.Optional[str] = None
        try:
            self._mysql_cur_dict.execute(
                """
                SELECT VIEW_DEFINITION AS `definition`
                FROM information_schema.VIEWS
                WHERE TABLE_SCHEMA = %s
                  AND TABLE_NAME = %s
                """,
                (self._mysql_database, view_name),
            )
            row: t.Optional[t.Dict[str, RowItemType]] = self._mysql_cur_dict.fetchone()
            if row is not None and row.get("definition") is not None:
                val = row["definition"]
                if isinstance(val, bytes):
                    try:
                        definition = val.decode()
                    except UnicodeDecodeError:
                        definition = str(val)
                else:
                    definition = t.cast(str, val)
        except mysql.connector.Error:
            definition = None

        if not definition:
            try:
                safe_view_name = view_name.replace("`", "``")
                self._mysql_cur.execute(f"SHOW CREATE VIEW `{safe_view_name}`")
                res = self._mysql_cur.fetchone()
                if res and len(res) >= 2:
                    create_stmt = res[1]
                    if isinstance(create_stmt, bytes):
                        try:
                            create_stmt_str = create_stmt.decode()
                        except UnicodeDecodeError:
                            create_stmt_str = str(create_stmt)
                    else:
                        create_stmt_str = t.cast(str, create_stmt)
                    m = re.search(r"\bAS\b\s*(.*)$", create_stmt_str, re.IGNORECASE | re.DOTALL)
                    if m:
                        definition = m.group(1).strip().rstrip(";")
                    else:
                        idx = create_stmt_str.upper().find(" AS ")
                        if idx != -1:
                            definition = create_stmt_str[idx + 4 :].strip().rstrip(";")
            except mysql.connector.Error:
                pass

        if not definition:
            raise sqlite3.Error(f"Unable to fetch definition for MySQL view '{view_name}'")

        return self._mysql_viewdef_to_sqlite(
            view_name=view_name,
            view_select_sql=definition,
        )

    def _create_view(self, view_name: str, attempting_reconnect: bool = False) -> None:
        try:
            if attempting_reconnect:
                self._mysql.reconnect()
            sql = self._build_create_view_sql(view_name)
            self._sqlite_cur.execute(sql)
            self._sqlite.commit()
        except mysql.connector.Error as err:
            if err.errno == errorcode.CR_SERVER_LOST:
                if not attempting_reconnect:
                    self._logger.warning("Connection to MySQL server lost.\nAttempting to reconnect.")
                    self._create_view(view_name, True)
                    return
                self._logger.warning("Connection to MySQL server lost.\nReconnection attempt aborted.")
                raise
            self._logger.error(
                "MySQL failed reading view definition from view %s: %s",
                view_name,
                err,
            )
            raise
        except sqlite3.Error as err:
            self._logger.error("SQLite failed creating view %s: %s", view_name, err)
            raise

    def _transfer_table_data(
        self, table_name: str, sql: str, total_records: int = 0, attempting_reconnect: bool = False
    ) -> None:
        self._get_data_transfer_manager().transfer_table_data(
            table_name=table_name,
            sql=sql,
            total_records=total_records,
            attempting_reconnect=attempting_reconnect,
        )

    def transfer(self) -> None:
        """The primary and only method with which we transfer all the data."""
        if len(self._mysql_tables) > 0 or len(self._exclude_mysql_tables) > 0:
            # transfer only specific tables
            specific_tables: t.Sequence[str] = (
                self._exclude_mysql_tables if len(self._exclude_mysql_tables) > 0 else self._mysql_tables
            )

            self._mysql_cur_prepared.execute(
                """
                SELECT TABLE_NAME, TABLE_TYPE
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = SCHEMA()
                AND TABLE_NAME {exclude} IN ({placeholders})
            """.format(
                    exclude="NOT" if len(self._exclude_mysql_tables) > 0 else "",
                    placeholders=("%s, " * len(specific_tables)).rstrip(" ,"),
                ),
                specific_tables,
            )
            tables: t.Iterable[t.Tuple[str, str]] = (
                (
                    str(row[0].decode() if isinstance(row[0], (bytes, bytearray)) else row[0]),
                    str(row[1].decode() if isinstance(row[1], (bytes, bytearray)) else row[1]),
                )
                for row in self._mysql_cur_prepared.fetchall()
            )
        else:
            # transfer all tables
            self._mysql_cur.execute(
                """
                SELECT TABLE_NAME, TABLE_TYPE
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = SCHEMA()
            """
            )

            def _coerce_row(row: t.Any) -> t.Tuple[str, str]:
                try:
                    # Row like (name, type)
                    name = row[0].decode() if isinstance(row[0], (bytes, bytearray)) else row[0]
                    ttype = (
                        row[1].decode()
                        if (isinstance(row, (list, tuple)) and len(row) > 1 and isinstance(row[1], (bytes, bytearray)))
                        else (row[1] if (isinstance(row, (list, tuple)) and len(row) > 1) else "BASE TABLE")
                    )
                    return str(name), str(ttype)
                except (TypeError, IndexError, UnicodeDecodeError):
                    # Fallback: treat as a single value name when row is not a 2-tuple or decoding fails
                    name = row.decode() if isinstance(row, (bytes, bytearray)) else str(row)
                    return name, "BASE TABLE"

            tables = (_coerce_row(row) for row in self._mysql_cur.fetchall())

        try:
            # turn off foreign key checking in SQLite while transferring data
            self._sqlite_cur.execute("PRAGMA foreign_keys=OFF")

            for table_name, table_type in tables:
                if isinstance(table_name, bytes):
                    table_name = table_name.decode()
                if isinstance(table_type, bytes):
                    table_type = table_type.decode()

                self._logger.info(
                    "%s%sTransferring table %s",
                    "[WITHOUT DATA] " if self._without_data else "",
                    "[ONLY DATA] " if self._without_tables else "",
                    table_name,
                )

                # reset the chunk
                self._current_chunk_number = 0

                if not self._without_tables:
                    # create the table or view
                    if table_type == "VIEW" and self._views_as_views:
                        self._create_view(table_name)  # type: ignore[arg-type]
                    else:
                        self._create_table(table_name)  # type: ignore[arg-type]

                if not self._without_data and not (table_type == "VIEW" and self._views_as_views):
                    # get the size of the data
                    if self._limit_rows > 0:
                        # limit to the requested number of rows
                        safe_table = self._escape_mysql_backticks(table_name)
                        self._mysql_cur_dict.execute(
                            "SELECT COUNT(*) AS `total_records` "
                            f"FROM (SELECT * FROM `{safe_table}` LIMIT {self._limit_rows}) AS `table`"
                        )
                    else:
                        # get all rows
                        safe_table = self._escape_mysql_backticks(table_name)
                        self._mysql_cur_dict.execute(f"SELECT COUNT(*) AS `total_records` FROM `{safe_table}`")

                    total_records: t.Optional[t.Dict[str, RowItemType]] = self._mysql_cur_dict.fetchone()
                    if total_records is not None:
                        total_records_count: int = int(total_records["total_records"])  # type: ignore[arg-type]
                    else:
                        total_records_count = 0

                    # only continue if there is anything to transfer
                    if total_records_count > 0:
                        # populate it
                        safe_table = self._escape_mysql_backticks(table_name)
                        self._mysql_cur.execute(
                            "SELECT * FROM `{table_name}` {limit}".format(
                                table_name=safe_table,
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
                        self._data_transfer.transfer_table_data(
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
