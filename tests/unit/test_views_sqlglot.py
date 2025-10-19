import re
from unittest.mock import patch

import pytest

from mysql_to_sqlite3.transporter import MySQLtoSQLite


class TestViewsSqlglot:
    def test_mysql_viewdef_to_sqlite_strips_schema_and_transpiles(self) -> None:
        mysql_select = "SELECT `u`.`id`, `u`.`name` FROM `db`.`users` AS `u` WHERE `u`.`id` > 1"
        # Use an instance to ensure access to _mysql_database for stripping
        with patch.object(MySQLtoSQLite, "__init__", return_value=None):
            inst = MySQLtoSQLite()  # type: ignore[call-arg]
        inst._mysql_database = "db"  # type: ignore[attr-defined]
        sql = inst._mysql_viewdef_to_sqlite(
            view_select_sql=mysql_select,
            view_name="v_users",
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

        def boom(*_, **__):
            raise ParseError("boom")

        monkeypatch.setattr("mysql_to_sqlite3.transporter.parse_one", boom)

        sql_in = "SELECT 1"
        with patch.object(MySQLtoSQLite, "__init__", return_value=None):
            inst = MySQLtoSQLite()  # type: ignore[call-arg]
        inst._mysql_database = "db"  # type: ignore[attr-defined]
        out = inst._mysql_viewdef_to_sqlite(
            view_select_sql=sql_in,
            view_name="v1",
        )
        assert out.startswith('CREATE VIEW IF NOT EXISTS "v1" AS')
        assert "SELECT 1" in out
        assert out.strip().endswith(";")

    def test_mysql_viewdef_to_sqlite_parse_fallback_strips_schema(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Force parse_one to raise so we exercise the fallback path with schema qualifiers
        from sqlglot.errors import ParseError

        def boom(*_, **__):
            raise ParseError("boom")

        monkeypatch.setattr("mysql_to_sqlite3.transporter.parse_one", boom)

        mysql_select = "SELECT `u`.`id` FROM `db`.`users` AS `u` WHERE `u`.`id` > 1"
        with patch.object(MySQLtoSQLite, "__init__", return_value=None):
            inst = MySQLtoSQLite()  # type: ignore[call-arg]
        inst._mysql_database = "db"  # type: ignore[attr-defined]
        out = inst._mysql_viewdef_to_sqlite(
            view_select_sql=mysql_select,
            view_name="v_users",
        )
        # Should not contain schema qualifier anymore
        assert "`db`." not in out and '"db".' not in out and " db." not in out
        # Should still reference the table name
        assert "FROM `users`" in out or 'FROM "users"' in out or "FROM users" in out
        assert out.strip().endswith(";")

    def test_mysql_viewdef_to_sqlite_strips_schema_from_qualified_columns_nested(self) -> None:
        # Based on the user-reported example with nested subquery and fully-qualified columns
        mysql_sql = (
            "select `p`.`instrument_id` AS `instrument_id`,`p`.`price_date` AS `price_date`,`p`.`close` AS `close` "
            "from (`example`.`prices` `p` join (select `example`.`prices`.`instrument_id` AS `instrument_id`,"
            "max(`example`.`prices`.`price_date`) AS `max_date` from `example`.`prices` group by "
            "`example`.`prices`.`instrument_id`) `t` on(((`t`.`instrument_id` = `p`.`instrument_id`) and "
            "(`t`.`max_date` = `p`.`price_date`))))"
        )
        with patch.object(MySQLtoSQLite, "__init__", return_value=None):
            inst = MySQLtoSQLite()  # type: ignore[call-arg]
        inst._mysql_database = "example"  # type: ignore[attr-defined]
        out = inst._mysql_viewdef_to_sqlite(view_select_sql=mysql_sql, view_name="v_prices")
        # Ensure all schema qualifiers are removed, including on qualified columns inside subqueries
        assert '"example".' not in out and "`example`." not in out and " example." not in out
        # Still references the base table name
        assert 'FROM "prices"' in out or 'FROM ("prices"' in out or "FROM prices" in out
        assert out.strip().endswith(";")

    def test_mysql_viewdef_to_sqlite_strips_matching_schema_qualifiers(self) -> None:
        mysql_select = "SELECT `u`.`id` FROM `db`.`users` AS `u`"
        # Use instance for consistent attribute access
        with patch.object(MySQLtoSQLite, "__init__", return_value=None):
            inst = MySQLtoSQLite()  # type: ignore[call-arg]
        inst._mysql_database = "db"  # type: ignore[attr-defined]
        # Since keep_schema behavior is no longer parameterized, ensure that if schema matches current db, it is stripped
        sql = inst._mysql_viewdef_to_sqlite(
            view_select_sql=mysql_select,
            view_name="v_users",
        )
        assert "`db`." not in sql and '"db".' not in sql
        assert sql.strip().endswith(";")
