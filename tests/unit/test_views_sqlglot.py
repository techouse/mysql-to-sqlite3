import re

import pytest

from mysql_to_sqlite3.transporter import MySQLtoSQLite


class TestViewsSqlglot:
    def test_mysql_viewdef_to_sqlite_strips_schema_and_transpiles(self) -> None:
        mysql_select = "SELECT `u`.`id`, `u`.`name` FROM `db`.`users` AS `u` WHERE `u`.`id` > 1"
        sql = MySQLtoSQLite._mysql_viewdef_to_sqlite(
            view_select_sql=mysql_select,
            view_name="v_users",
            schema_name="db",
        )
        assert sql.startswith('CREATE VIEW IF NOT EXISTS "v_users" AS')
        # Ensure schema qualifier was removed
        assert '"db".' not in sql
        assert "`db`." not in sql
        # Ensure it targets sqlite dialect (identifiers quoted with ")
        assert 'FROM "users"' in sql
        # Ends with single semicolon
        assert re.search(r";\s*$", sql) is not None

    def test_mysql_viewdef_to_sqlite_parse_fallback(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Force parse_one to raise so we hit the fallback path
        from sqlglot.errors import ParseError

        def boom(*args, **kwargs):
            raise ParseError("boom")

        monkeypatch.setattr("mysql_to_sqlite3.transporter.parse_one", boom)

        sql_in = "SELECT 1"
        out = MySQLtoSQLite._mysql_viewdef_to_sqlite(
            view_select_sql=sql_in,
            view_name="v1",
            schema_name="db",
        )
        assert out.startswith('CREATE VIEW IF NOT EXISTS "v1" AS')
        assert "SELECT 1" in out
        assert out.strip().endswith(";")
