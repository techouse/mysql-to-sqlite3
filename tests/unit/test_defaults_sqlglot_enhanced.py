import pytest

from mysql_to_sqlite3.transporter import MySQLtoSQLite


class TestDefaultsSqlglotEnhanced:
    @pytest.mark.parametrize(
        "expr,expected",
        [
            ("CURRENT_TIME", "DEFAULT CURRENT_TIME"),
            ("CURRENT_DATE", "DEFAULT CURRENT_DATE"),
            ("CURRENT_TIMESTAMP", "DEFAULT CURRENT_TIMESTAMP"),
        ],
    )
    def test_current_tokens_passthrough(self, expr: str, expected: str) -> None:
        assert MySQLtoSQLite._translate_default_from_mysql_to_sqlite(expr, column_extra="DEFAULT_GENERATED") == expected

    def test_null_literal_generated(self) -> None:
        assert (
            MySQLtoSQLite._translate_default_from_mysql_to_sqlite("NULL", column_extra="DEFAULT_GENERATED")
            == "DEFAULT NULL"
        )

    @pytest.mark.parametrize(
        "expr,boolean_type,expected",
        [
            ("true", "BOOLEAN", {"DEFAULT(TRUE)", "DEFAULT '1'"}),
            ("false", "BOOLEAN", {"DEFAULT(FALSE)", "DEFAULT '0'"}),
            ("true", "INTEGER", {"DEFAULT '1'"}),
            ("false", "INTEGER", {"DEFAULT '0'"}),
        ],
    )
    def test_boolean_tokens_generated(self, expr: str, boolean_type: str, expected: set) -> None:
        out = MySQLtoSQLite._translate_default_from_mysql_to_sqlite(
            expr, column_type=boolean_type, column_extra="DEFAULT_GENERATED"
        )
        assert out in expected

    def test_parenthesized_string_literal_generated(self) -> None:
        out = MySQLtoSQLite._translate_default_from_mysql_to_sqlite("('abc')", column_extra="DEFAULT_GENERATED")
        # Either DEFAULT 'abc' or DEFAULT ('abc') depending on normalization
        assert out in {"DEFAULT 'abc'", "DEFAULT ('abc')"}

    def test_parenthesized_numeric_literal_generated(self) -> None:
        out = MySQLtoSQLite._translate_default_from_mysql_to_sqlite("(42)", column_extra="DEFAULT_GENERATED")
        assert out in {"DEFAULT 42", "DEFAULT (42)"}

    def test_constant_arithmetic_expression_generated(self) -> None:
        out = MySQLtoSQLite._translate_default_from_mysql_to_sqlite("1+2*3", column_extra="DEFAULT_GENERATED")
        # sqlglot formats with spaces for sqlite dialect
        assert out in {"DEFAULT 1 + 2 * 3", "DEFAULT (1 + 2 * 3)"}

    def test_hex_blob_literal_generated(self) -> None:
        out = MySQLtoSQLite._translate_default_from_mysql_to_sqlite("x'41'", column_extra="DEFAULT_GENERATED")
        # Should recognize as blob literal and keep as-is
        assert out.upper() == "DEFAULT X'41'"

    def test_plain_string_escaping_single_quote(self) -> None:
        out = MySQLtoSQLite._translate_default_from_mysql_to_sqlite("O'Reilly")
        assert out == "DEFAULT 'O''Reilly'"
