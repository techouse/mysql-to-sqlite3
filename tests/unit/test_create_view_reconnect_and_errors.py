import sqlite3
from unittest.mock import MagicMock, patch

import mysql.connector
from mysql.connector import errorcode

from mysql_to_sqlite3.transporter import MySQLtoSQLite


def test_create_view_reconnect_on_server_lost_then_success() -> None:
    with patch.object(MySQLtoSQLite, "__init__", return_value=None):
        inst = MySQLtoSQLite()  # type: ignore[call-arg]

    inst._mysql = MagicMock()
    inst._sqlite = MagicMock()
    inst._sqlite_cur = MagicMock()
    inst._logger = MagicMock()

    # First build fails with CR_SERVER_LOST, second returns valid SQL
    err = mysql.connector.Error(msg="lost", errno=errorcode.CR_SERVER_LOST)
    inst._build_create_view_sql = MagicMock(side_effect=[err, 'CREATE VIEW "v" AS SELECT 1;'])

    inst._create_view("v")

    inst._mysql.reconnect.assert_called_once()
    inst._sqlite_cur.execute.assert_called_once()
    inst._sqlite.commit.assert_called_once()


def test_create_view_sqlite_error_is_logged_and_raised() -> None:
    with patch.object(MySQLtoSQLite, "__init__", return_value=None):
        inst = MySQLtoSQLite()  # type: ignore[call-arg]

    inst._mysql = MagicMock()
    inst._sqlite = MagicMock()
    inst._sqlite_cur = MagicMock()
    inst._logger = MagicMock()

    inst._build_create_view_sql = MagicMock(return_value='CREATE VIEW "v" AS SELECT 1;')
    inst._sqlite_cur.execute.side_effect = sqlite3.Error("broken")

    try:
        inst._create_view("v")
    except sqlite3.Error:
        pass
    else:
        raise AssertionError("Expected sqlite3.Error to be raised")

    inst._logger.error.assert_called()
