import sqlite3
import typing as t
from unittest.mock import MagicMock, patch

import mysql.connector
import pytest
from mysql.connector import errorcode

from mysql_to_sqlite3.transporter import MySQLtoSQLite


class TestCreateView:
    def _minimal_instance(self) -> MySQLtoSQLite:
        with patch.object(MySQLtoSQLite, "__init__", return_value=None):
            inst = MySQLtoSQLite()
        # Minimal attributes used by the tested methods
        inst._mysql_cur_dict = MagicMock()
        inst._mysql_cur = MagicMock()
        inst._mysql = MagicMock()
        inst._sqlite_cur = MagicMock()
        inst._sqlite = MagicMock()
        inst._logger = MagicMock()
        inst._mysql_database = "db"
        return inst

    def test_build_create_view_sql_from_information_schema(self) -> None:
        inst = self._minimal_instance()
        # information_schema.VIEWS returns a bytes definition
        inst._mysql_cur_dict.fetchone.return_value = {"definition": b"SELECT 1"}

        sql = inst._build_create_view_sql("v1")

        assert sql.startswith('CREATE VIEW IF NOT EXISTS "v1" AS')
        assert "SELECT 1" in sql
        # Ensure only one trailing semicolon
        assert sql.strip().endswith(";")

    def test_build_create_view_sql_fallback_show_create_and_strip_schema(self) -> None:
        inst = self._minimal_instance()
        # Force information_schema path to fail to provide definition
        inst._mysql_cur_dict.fetchone.return_value = None

        # SHOW CREATE VIEW returns a full statement; ensure SELECT retains and schema is stripped
        create_stmt = (
            "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER "
            "VIEW `v_users` AS SELECT `u`.`id`, `u`.`name` FROM `db`.`users` AS `u` WHERE `u`.`id` > 1"
        )
        inst._mysql_cur.fetchone.return_value = ("v_users", create_stmt)

        sql = inst._build_create_view_sql("v_users")
        assert sql.startswith('CREATE VIEW IF NOT EXISTS "v_users" AS')
        # Schema qualifiers to current DB should be stripped
        assert "`db`." not in sql
        assert '"db".' not in sql
        assert 'FROM "users"' in sql
        assert sql.strip().endswith(";")

    def test_create_view_success_executes_sql_and_commits(self) -> None:
        inst = self._minimal_instance()
        inst._build_create_view_sql = MagicMock(return_value='CREATE VIEW IF NOT EXISTS "v" AS SELECT 1;')

        inst._create_view("v")

        inst._sqlite_cur.execute.assert_called_once_with('CREATE VIEW IF NOT EXISTS "v" AS SELECT 1;')
        inst._sqlite.commit.assert_called_once()

    def test_create_view_sqlite_error_propagates(self) -> None:
        inst = self._minimal_instance()
        inst._build_create_view_sql = MagicMock(return_value='CREATE VIEW IF NOT EXISTS "v" AS SELECT 1;')
        inst._sqlite_cur.execute.side_effect = sqlite3.Error("boom")

        with patch.object(inst._logger, "error") as log_err:
            with pytest.raises(sqlite3.Error):
                inst._create_view("v")
            assert log_err.called
