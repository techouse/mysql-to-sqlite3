import sqlite3
from unittest.mock import MagicMock, patch

import mysql.connector
from mysql.connector import errorcode

from mysql_to_sqlite3.transporter import MySQLtoSQLite


def test_create_table_reconnect_on_server_lost_then_success() -> None:
    with patch.object(MySQLtoSQLite, "__init__", return_value=None):
        inst = MySQLtoSQLite()  # type: ignore[call-arg]

    # Patch dependencies
    inst._mysql = MagicMock()
    inst._sqlite = MagicMock()
    inst._sqlite_cur = MagicMock()
    inst._logger = MagicMock()

    # First call to build SQL raises CR_SERVER_LOST; second returns a valid SQL
    err = mysql.connector.Error(msg="lost", errno=errorcode.CR_SERVER_LOST)

    inst._build_create_table_sql = MagicMock(side_effect=[err, 'CREATE TABLE IF NOT EXISTS "t" ("id" INTEGER);'])

    inst._create_table("t")

    # Reconnect should have been attempted once
    inst._mysql.reconnect.assert_called_once()
    # executescript should have been called once with the returned SQL
    inst._sqlite_cur.executescript.assert_called_once()
    inst._sqlite.commit.assert_called_once()


def test_create_table_sqlite_error_is_logged_and_raised() -> None:
    with patch.object(MySQLtoSQLite, "__init__", return_value=None):
        inst = MySQLtoSQLite()  # type: ignore[call-arg]

    inst._mysql = MagicMock()
    inst._sqlite = MagicMock()
    inst._sqlite_cur = MagicMock()
    inst._logger = MagicMock()

    inst._build_create_table_sql = MagicMock(return_value='CREATE TABLE "t" ("id" INTEGER);')
    inst._sqlite_cur.executescript.side_effect = sqlite3.Error("broken")

    try:
        inst._create_table("t")
    except sqlite3.Error:
        pass
    else:
        raise AssertionError("Expected sqlite3.Error to be raised")

    inst._logger.error.assert_called()


def test_transfer_table_data_reconnect_on_server_lost_then_success() -> None:
    with patch.object(MySQLtoSQLite, "__init__", return_value=None):
        inst = MySQLtoSQLite()  # type: ignore[call-arg]

    inst._mysql = MagicMock()
    inst._mysql_cur = MagicMock()
    inst._sqlite = MagicMock()
    inst._sqlite_cur = MagicMock()
    inst._logger = MagicMock()
    inst._quiet = True
    inst._chunk_size = None

    # First fetchall raises CR_SERVER_LOST; second returns rows
    err = mysql.connector.Error(msg="lost", errno=errorcode.CR_SERVER_LOST)
    inst._mysql_cur.fetchall.side_effect = [err, [(1,), (2,)]]

    inst._sqlite_cur.executemany = MagicMock()

    inst._transfer_table_data(table_name="t", sql="INSERT INTO t VALUES (?)", total_records=2)

    inst._mysql.reconnect.assert_called_once()
    inst._sqlite_cur.executemany.assert_called_once()
    inst._sqlite.commit.assert_called_once()
