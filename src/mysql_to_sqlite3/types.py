"""Types for mysql-to-sqlite3."""

import os
import typing as t
from logging import Logger
from sqlite3 import Connection, Cursor

import typing_extensions as tx
from mysql.connector.abstracts import MySQLConnectionAbstract
from mysql.connector.cursor import MySQLCursorDict, MySQLCursorPrepared, MySQLCursorRaw


class MySQLtoSQLiteParams(tx.TypedDict):
    """MySQLtoSQLite parameters."""

    buffered: t.Optional[bool]
    chunk: t.Optional[int]
    collation: t.Optional[str]
    exclude_mysql_tables: t.Optional[t.Sequence[str]]
    json_as_text: t.Optional[bool]
    limit_rows: t.Optional[int]
    log_file: t.Optional[t.Union[str, "os.PathLike[t.Any]"]]
    mysql_database: str
    mysql_host: str
    mysql_password: t.Optional[t.Union[str, bool]]
    mysql_port: int
    mysql_charset: t.Optional[str]
    mysql_collation: t.Optional[str]
    mysql_ssl_disabled: t.Optional[bool]
    mysql_tables: t.Optional[t.Sequence[str]]
    mysql_user: str
    prefix_indices: t.Optional[bool]
    quiet: t.Optional[bool]
    sqlite_file: t.Union[str, "os.PathLike[t.Any]"]
    vacuum: t.Optional[bool]
    without_tables: t.Optional[bool]
    without_data: t.Optional[bool]
    without_foreign_keys: t.Optional[bool]


class MySQLtoSQLiteAttributes:
    """MySQLtoSQLite attributes."""

    _buffered: bool
    _chunk_size: t.Optional[int]
    _collation: str
    _current_chunk_number: int
    _exclude_mysql_tables: t.Sequence[str]
    _json_as_text: bool
    _limit_rows: int
    _logger: Logger
    _mysql: MySQLConnectionAbstract
    _mysql_cur: MySQLCursorRaw
    _mysql_cur_dict: MySQLCursorDict
    _mysql_cur_prepared: MySQLCursorPrepared
    _mysql_database: str
    _mysql_host: str
    _mysql_password: t.Optional[str]
    _mysql_port: int
    _mysql_charset: str
    _mysql_collation: str
    _mysql_ssl_disabled: bool
    _mysql_tables: t.Sequence[str]
    _mysql_user: str
    _prefix_indices: bool
    _quiet: bool
    _sqlite: Connection
    _sqlite_cur: Cursor
    _sqlite_file: t.Union[str, "os.PathLike[t.Any]"]
    _without_tables: bool
    _sqlite_json1_extension_enabled: bool
    _vacuum: bool
    _without_data: bool
    _without_foreign_keys: bool
