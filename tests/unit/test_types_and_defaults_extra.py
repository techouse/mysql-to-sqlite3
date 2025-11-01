import sqlite3
import typing as t

import pytest

from mysql_to_sqlite3.sqlite_utils import CollatingSequences
from mysql_to_sqlite3.transporter import MySQLtoSQLite


class TestTypesAndDefaultsExtra:
    def test_valid_column_type_and_length(self) -> None:
        # COLUMN_PATTERN should match the type name without length
        m = MySQLtoSQLite._valid_column_type("varchar(255)")
        assert m is not None
        assert m.group(0).lower() == "varchar"
        # No parenthesis -> no length suffix
        assert MySQLtoSQLite._column_type_length("int") == ""
        # With parenthesis -> returns the (N)
        assert MySQLtoSQLite._column_type_length("nvarchar(42)") == "(42)"

    def test_data_type_collation_sequence(self) -> None:
        # Collation applies to textual affinity types only
        assert (
            MySQLtoSQLite._data_type_collation_sequence(collation=CollatingSequences.NOCASE, column_type="VARCHAR(10)")
            == f"COLLATE {CollatingSequences.NOCASE}"
        )
        assert (
            MySQLtoSQLite._data_type_collation_sequence(collation=CollatingSequences.NOCASE, column_type="INTEGER")
            == ""
        )

    @pytest.mark.parametrize(
        "default,expected",
        [
            ("curtime()", "DEFAULT CURRENT_TIME"),
            ("curdate()", "DEFAULT CURRENT_DATE"),
            ("now()", "DEFAULT CURRENT_TIMESTAMP"),
        ],
    )
    def test_translate_default_common_keywords(self, default: str, expected: str) -> None:
        assert MySQLtoSQLite._translate_default_from_mysql_to_sqlite(default) == expected

    def test_translate_default_current_timestamp_precision_transpiled(self) -> None:
        # MySQL allows fractional seconds: CURRENT_TIMESTAMP(6). Ensure it's normalized to SQLite token.
        out = MySQLtoSQLite._translate_default_from_mysql_to_sqlite(
            "CURRENT_TIMESTAMP(6)", column_extra="DEFAULT_GENERATED"
        )
        assert out == "DEFAULT CURRENT_TIMESTAMP"

    def test_translate_default_generated_expr_fallback_quotes(self) -> None:
        # Unknown expressions should fall back to quoted string default for safety
        out = MySQLtoSQLite._translate_default_from_mysql_to_sqlite("uuid()", column_extra="DEFAULT_GENERATED")
        assert out == "DEFAULT 'uuid()'"

    def test_translate_default_charset_introducer_str_hex_and_bin(self) -> None:
        # DEFAULT_GENERATED with charset introducer and hex (escaped as in MySQL)
        s = "_utf8mb4 X\\'41\\'"  # hex for 'A'
        out = MySQLtoSQLite._translate_default_from_mysql_to_sqlite(
            s, column_type="BLOB", column_extra="DEFAULT_GENERATED"
        )
        assert out == "DEFAULT x'41'"
        # DEFAULT_GENERATED with charset introducer and binary literal (escaped)
        s2 = "_utf8mb4 b\\'01000001\\'"  # binary for 'A'
        out2 = MySQLtoSQLite._translate_default_from_mysql_to_sqlite(
            s2, column_type="BLOB", column_extra="DEFAULT_GENERATED"
        )
        assert out2 == "DEFAULT 'A'"

    def test_translate_default_charset_introducer_bytes(self) -> None:
        # Escaped form in bytes
        s = b"_utf8mb4 x\\'41\\'"
        out = MySQLtoSQLite._translate_default_from_mysql_to_sqlite(
            s, column_type="BLOB", column_extra="DEFAULT_GENERATED"
        )
        assert out == "DEFAULT x'41'"

    def test_translate_default_bool_non_boolean_type(self) -> None:
        # When column_type is not BOOLEAN, booleans become '1'/'0'
        assert MySQLtoSQLite._translate_default_from_mysql_to_sqlite(True, column_type="INTEGER") == "DEFAULT '1'"
        assert MySQLtoSQLite._translate_default_from_mysql_to_sqlite(False, column_type="INTEGER") == "DEFAULT '0'"

    def test_translate_default_bool_boolean_type(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Ensure SQLite version satisfies condition
        monkeypatch.setattr(sqlite3, "sqlite_version", "3.40.0")
        assert MySQLtoSQLite._translate_default_from_mysql_to_sqlite(True, column_type="BOOLEAN") == "DEFAULT(TRUE)"
        assert MySQLtoSQLite._translate_default_from_mysql_to_sqlite(False, column_type="BOOLEAN") == "DEFAULT(FALSE)"

    def test_translate_default_prequoted_string_literal(self) -> None:
        # MariaDB can report TEXT defaults already wrapped in single quotes; ensure they're normalized
        assert MySQLtoSQLite._translate_default_from_mysql_to_sqlite("'[]'") == "DEFAULT '[]'"
        assert MySQLtoSQLite._translate_default_from_mysql_to_sqlite("'It''s'") == "DEFAULT 'It''s'"
        assert MySQLtoSQLite._translate_default_from_mysql_to_sqlite("'a\\'b'") == "DEFAULT 'a''b'"
