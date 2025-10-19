import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from mysql_to_sqlite3.transporter import MySQLtoSQLite


def _inst_with_mysql_dict():
    with patch.object(MySQLtoSQLite, "__init__", return_value=None):
        inst = MySQLtoSQLite()  # type: ignore[call-arg]
    inst._mysql_cur_dict = MagicMock()
    inst._mysql_cur = MagicMock()
    inst._mysql_database = "db"
    return inst


def test_build_create_view_sql_information_schema_bytes_decode_failure_falls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inst = _inst_with_mysql_dict()

    # information_schema returns bytes that fail to decode in UTF-8
    bad_bytes = b"\xff\xfe\xfa"
    inst._mysql_cur_dict.fetchone.return_value = {"definition": bad_bytes}

    captured = {}

    def fake_converter(*, view_select_sql: str, view_name: str) -> str:
        captured["view_select_sql"] = view_select_sql
        captured["view_name"] = view_name
        return f'CREATE VIEW IF NOT EXISTS "{view_name}" AS SELECT 1;'

    monkeypatch.setattr(MySQLtoSQLite, "_mysql_viewdef_to_sqlite", staticmethod(fake_converter))

    sql = inst._build_create_view_sql("v_strange")

    # Converter was invoked with the string representation of the undecodable bytes
    assert captured["view_name"] == "v_strange"
    assert isinstance(captured["view_select_sql"], str)
    # And a CREATE VIEW statement was produced
    assert sql.startswith('CREATE VIEW IF NOT EXISTS "v_strange" AS')
    assert sql.strip().endswith(";")


def test_build_create_view_sql_raises_when_no_definition_available() -> None:
    inst = _inst_with_mysql_dict()

    # information_schema path -> None
    inst._mysql_cur_dict.fetchone.return_value = None
    # SHOW CREATE VIEW returns None
    inst._mysql_cur.fetchone.return_value = None

    with pytest.raises(sqlite3.Error):
        inst._build_create_view_sql("missing_view")
