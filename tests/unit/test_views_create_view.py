import typing as t
from unittest.mock import MagicMock, patch

import mysql.connector
from mysql.connector import errorcode

from mysql_to_sqlite3.transporter import MySQLtoSQLite


def test_show_create_view_fallback_handles_newline_and_backticks(monkeypatch: "t.Any") -> None:
    """
    Force the SHOW CREATE VIEW fallback path and verify:
    - The executed SQL escapes backticks in the view name.
    - The regex extracts the SELECT when it follows "AS\n" (across newline).
    - The extracted SELECT (without trailing semicolon) is passed to _mysql_viewdef_to_sqlite.
    """
    with patch.object(MySQLtoSQLite, "__init__", return_value=None):
        instance = MySQLtoSQLite()  # type: ignore[call-arg]

    # Make information_schema path return None so fallback is used
    instance._mysql_cur_dict = MagicMock()
    instance._mysql_cur_dict.execute.return_value = None
    instance._mysql_cur_dict.fetchone.return_value = None

    # Prepare SHOW CREATE VIEW return value with AS followed by newline
    create_stmt = (
        "CREATE ALGORITHM=UNDEFINED DEFINER=`user`@`%` SQL SECURITY DEFINER " "VIEW `we``ird` AS\nSELECT 1 AS `x`;"
    )
    executed_sql: t.List[str] = []

    def capture_execute(sql: str) -> None:
        executed_sql.append(sql)

    instance._mysql_cur = MagicMock()
    instance._mysql_cur.execute.side_effect = capture_execute
    instance._mysql_cur.fetchone.return_value = ("we`ird", create_stmt)

    # Capture the definition passed to _mysql_viewdef_to_sqlite and return a dummy SQL
    captured: t.Dict[str, str] = {}

    def fake_mysql_viewdef_to_sqlite(*, view_select_sql: str, view_name: str) -> str:
        captured["select"] = view_select_sql
        captured["view_name"] = view_name
        return 'CREATE VIEW IF NOT EXISTS "dummy" AS SELECT 1;'

    monkeypatch.setattr(MySQLtoSQLite, "_mysql_viewdef_to_sqlite", staticmethod(fake_mysql_viewdef_to_sqlite))

    instance._mysql_database = "db"

    # Build the SQL (triggers fallback path)
    sql = instance._build_create_view_sql("we`ird")

    # Assert backticks in the view name were escaped in the SHOW CREATE VIEW statement
    assert executed_sql and executed_sql[0] == "SHOW CREATE VIEW `we``ird`"

    # The resulting SQL is our fake output
    assert sql.startswith('CREATE VIEW IF NOT EXISTS "dummy" AS')

    # Ensure the extracted SELECT excludes the trailing semicolon and spans newlines
    assert captured["select"] == "SELECT 1 AS `x`"
    # Check view_name was threaded unchanged to the converter
    assert captured["view_name"] == "we`ird"
