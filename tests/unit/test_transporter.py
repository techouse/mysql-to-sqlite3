import builtins
import importlib.util
import re
import sqlite3
import sys
import types as pytypes
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import mysql.connector
import pytest
from mysql.connector import errorcode
from pytest_mock import MockerFixture
from typing_extensions import Unpack as ExtensionsUnpack

from mysql_to_sqlite3.sqlite_utils import CollatingSequences
from mysql_to_sqlite3.transporter import MySQLtoSQLite
from tests.conftest import MySQLCredentials


class TestMySQLtoSQLiteTransporter:
    def test_transporter_uses_typing_extensions_unpack_when_missing(self) -> None:
        """Reload transporter without typing.Unpack to exercise fallback branch."""
        import mysql_to_sqlite3.transporter as transporter_module

        module_path = transporter_module.__file__
        assert module_path is not None

        real_typing = sys.modules["typing"]
        fake_typing = pytypes.ModuleType("typing")
        fake_typing.__dict__.update({k: v for k, v in real_typing.__dict__.items() if k != "Unpack"})
        sys.modules["typing"] = fake_typing

        try:
            spec = importlib.util.spec_from_file_location("mysql_to_sqlite3.transporter_fallback", module_path)
            assert spec and spec.loader
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        finally:
            sys.modules["typing"] = real_typing
            sys.modules.pop("mysql_to_sqlite3.transporter_fallback", None)

        assert module.Unpack is ExtensionsUnpack

    def test_constructor_normalizes_default_utf8mb4_collation(
        self,
        sqlite_database: "os.PathLike[t.Any]",
        mysql_credentials: MySQLCredentials,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Ensure utf8mb4_0900_ai_ci defaults downgrade to utf8mb4_unicode_ci."""
        from mysql_to_sqlite3 import transporter as transporter_module

        class FakeMySQLConnection:
            def __init__(self) -> None:
                self.database = None

            def is_connected(self) -> bool:
                return True

            def cursor(self, *args, **kwargs) -> MagicMock:
                return MagicMock()

            def get_server_version(self):
                return (8, 0, 21)

            def reconnect(self) -> None:
                return None

        fake_conn = FakeMySQLConnection()

        fake_charset = SimpleNamespace(
            get_default_collation=lambda charset: ("utf8mb4_0900_ai_ci", None),
            get_supported=lambda: ("utf8mb4",),
        )

        monkeypatch.setattr("mysql_to_sqlite3.transporter.CharacterSet", lambda: fake_charset)
        monkeypatch.setattr(
            "mysql_to_sqlite3.transporter.mysql.connector.connect",
            lambda **kwargs: fake_conn,
        )
        monkeypatch.setattr(MySQLtoSQLite, "_setup_logger", MagicMock(return_value=MagicMock()))

        original_isinstance = builtins.isinstance

        def fake_isinstance(obj: object, classinfo: object) -> bool:
            if obj is fake_conn and classinfo is transporter_module.MySQLConnectionAbstract:
                return True
            return original_isinstance(obj, classinfo)

        monkeypatch.setattr("mysql_to_sqlite3.transporter.isinstance", fake_isinstance, raising=False)

        instance = MySQLtoSQLite(
            sqlite_file=sqlite_database,
            mysql_user=mysql_credentials.user,
            mysql_password=mysql_credentials.password,
            mysql_host=mysql_credentials.host,
            mysql_port=mysql_credentials.port,
            mysql_database=mysql_credentials.database,
        )

        assert instance._mysql_collation == "utf8mb4_unicode_ci"

    def test_constructor_raises_when_mysql_not_connected(
        self,
        sqlite_database: "os.PathLike[t.Any]",
        mysql_credentials: MySQLCredentials,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Raise ConnectionError when mysql.connector.connect returns disconnected handle."""
        from mysql_to_sqlite3 import transporter as transporter_module

        class FakeMySQLConnection:
            def __init__(self) -> None:
                self.database = None

            def is_connected(self) -> bool:
                return False

            def cursor(self, *args, **kwargs) -> MagicMock:
                return MagicMock()

        fake_conn = FakeMySQLConnection()

        fake_charset = SimpleNamespace(
            get_default_collation=lambda charset: ("utf8mb4_0900_ai_ci", None),
            get_supported=lambda: ("utf8mb4",),
        )

        monkeypatch.setattr("mysql_to_sqlite3.transporter.CharacterSet", lambda: fake_charset)
        monkeypatch.setattr(
            "mysql_to_sqlite3.transporter.mysql.connector.connect",
            lambda **kwargs: fake_conn,
        )
        monkeypatch.setattr(MySQLtoSQLite, "_setup_logger", MagicMock(return_value=MagicMock()))

        original_isinstance = builtins.isinstance

        def fake_isinstance(obj: object, classinfo: object) -> bool:
            if obj is fake_conn and classinfo is transporter_module.MySQLConnectionAbstract:
                return True
            return original_isinstance(obj, classinfo)

        monkeypatch.setattr("mysql_to_sqlite3.transporter.isinstance", fake_isinstance, raising=False)

        with pytest.raises(ConnectionError, match="Unable to connect to MySQL"):
            MySQLtoSQLite(
                sqlite_file=sqlite_database,
                mysql_user=mysql_credentials.user,
                mysql_password=mysql_credentials.password,
                mysql_host=mysql_credentials.host,
                mysql_port=mysql_credentials.port,
                mysql_database=mysql_credentials.database,
            )

    def test_transpile_mysql_expr_to_sqlite_parse_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Gracefully handle sqlglot parse errors when evaluating expressions."""

        def explode(*args, **kwargs):
            raise ValueError("boom")

        monkeypatch.setattr("mysql_to_sqlite3.transporter.parse_one", explode)
        assert MySQLtoSQLite._transpile_mysql_expr_to_sqlite("invalid SQL") is None

    def test_transpile_mysql_type_to_sqlite_handles_length_and_synonyms(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Ensure sqlglot-assisted mapper preserves length suffixes and synonyms."""

        def fake_parse_one(expr_sql: str, read: str):
            class FakeExpression:
                def __init__(self, text: str) -> None:
                    self._text = text

                def sql(self, dialect: str) -> str:
                    return self._text

            return FakeExpression(expr_sql)

        real_search = re.search

        def fake_search(pattern: str, string: str, flags: int = 0):
            if string.startswith("CAST(NULL AS"):
                extracted = string[len("CAST(NULL AS ") : -1]

                class FakeMatch:
                    def __init__(self, value: str) -> None:
                        self._value = value

                    def group(self, index: int) -> str:
                        return self._value

                return FakeMatch(extracted)
            return real_search(pattern, string, flags)

        monkeypatch.setattr("mysql_to_sqlite3.transporter.parse_one", fake_parse_one)
        monkeypatch.setattr("mysql_to_sqlite3.transporter.re.search", fake_search)

        assert MySQLtoSQLite._transpile_mysql_type_to_sqlite("VARCHAR(42)") == "VARCHAR(42)"
        assert MySQLtoSQLite._transpile_mysql_type_to_sqlite("CHAR(5)") == "CHARACTER(5)"
        assert MySQLtoSQLite._transpile_mysql_type_to_sqlite("DECIMAL(10,2)") == "DECIMAL"
        assert MySQLtoSQLite._transpile_mysql_type_to_sqlite("VARBINARY(8)") == "BLOB"

    def test_quote_sqlite_identifier_handles_non_utf8_bytes(self) -> None:
        """Bytes that are not UTF-8 decodable should still be quoted safely."""
        quoted = MySQLtoSQLite._quote_sqlite_identifier(b"\xff")
        assert quoted.startswith('"')
        assert "xff" in quoted

    def test_translate_default_bytes_binary_literal(self) -> None:
        """Binary defaults encoded with charset introducers should be converted."""
        column_default = b"_utf8mb4 b\\'1000001\\'"
        result = MySQLtoSQLite._translate_default_from_mysql_to_sqlite(column_default, "VARBINARY", "DEFAULT_GENERATED")
        assert result == "DEFAULT 'A'"

    def test_translate_default_bytes_hex_literal(self) -> None:
        """Hex defaults encoded with charset introducers should be preserved."""
        column_default = b"_utf8mb4 x\\'41\\'"
        result = MySQLtoSQLite._translate_default_from_mysql_to_sqlite(column_default, "VARBINARY", "DEFAULT_GENERATED")
        assert result == "DEFAULT x'41'"

    def test_translate_default_bytes_decode_error(self) -> None:
        """Un-decodable bytes should fall back to their repr-safe form."""
        column_default = b"\xff"
        result = MySQLtoSQLite._translate_default_from_mysql_to_sqlite(column_default, "TEXT")
        assert result == "DEFAULT 'b''\\xff'''"

    def test_translate_default_bytes_without_literal_prefix(self) -> None:
        """Charset introducer without hex/bin prefix should fall back to hex literal."""
        column_default = b"_utf8mb4'abc'"
        result = MySQLtoSQLite._translate_default_from_mysql_to_sqlite(
            column_default,
            "BLOB",
            "DEFAULT_GENERATED",
        )
        assert result == "DEFAULT x'616263'"

    def test_translate_default_generated_expression_variants(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Generated defaults should handle arithmetic inside parentheses and plain expressions."""
        monkeypatch.setattr(
            MySQLtoSQLite,
            "_transpile_mysql_expr_to_sqlite",
            lambda expr: "(1+2)",
        )
        assert (
            MySQLtoSQLite._translate_default_from_mysql_to_sqlite("expr", column_extra="DEFAULT_GENERATED")
            == "DEFAULT (1+2)"
        )

        monkeypatch.setattr(
            MySQLtoSQLite,
            "_transpile_mysql_expr_to_sqlite",
            lambda expr: "123",
        )
        assert (
            MySQLtoSQLite._translate_default_from_mysql_to_sqlite("expr", column_extra="DEFAULT_GENERATED")
            == "DEFAULT 123"
        )

        monkeypatch.setattr(
            MySQLtoSQLite,
            "_transpile_mysql_expr_to_sqlite",
            lambda expr: "1 + 3",
        )
        assert (
            MySQLtoSQLite._translate_default_from_mysql_to_sqlite("expr", column_extra="DEFAULT_GENERATED")
            == "DEFAULT 1 + 3"
        )

    def test_data_type_collation_sequence_uses_transpiled_mapping(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Apply collation when sqlglot-based mapping yields textual type."""

        def fake_transpile(cls, column_type: str, sqlite_json1_extension_enabled: bool = False) -> str:
            return "varchar(10)"

        monkeypatch.setattr(
            MySQLtoSQLite,
            "_transpile_mysql_type_to_sqlite",
            classmethod(fake_transpile),
        )

        result = MySQLtoSQLite._data_type_collation_sequence(
            collation=CollatingSequences.NOCASE,
            column_type="custom type",
        )
        assert result == f"COLLATE {CollatingSequences.NOCASE}"

    def test_get_unique_index_name_handles_existing_suffixes(self) -> None:
        """Ensure duplicate index names increment suffix until free slot is found."""
        with patch.object(MySQLtoSQLite, "__init__", return_value=None):
            instance = MySQLtoSQLite()

        instance._seen_sqlite_index_names = {"idx_name", "idx_name_2"}
        instance._sqlite_index_name_counters = {"idx_name": 2}
        instance._prefix_indices = False
        instance._logger = MagicMock()

        result = instance._get_unique_index_name("idx_name")
        assert result == "idx_name_3"
        instance._logger.info.assert_called_once()

    def test_build_create_table_sql_warns_on_non_integer_auto_increment(self) -> None:
        """Auto increment primary keys with non-integer types should trigger warning and index decoding branches."""
        with patch.object(MySQLtoSQLite, "__init__", return_value=None):
            instance = MySQLtoSQLite()

        instance._sqlite_strict = False
        instance._sqlite_json1_extension_enabled = False
        instance._mysql_cur_dict = MagicMock()
        instance._mysql_cur = MagicMock()
        instance._mysql = MagicMock()
        instance._sqlite = MagicMock()
        instance._mysql_database = "demo"
        instance._collation = CollatingSequences.NOCASE
        instance._prefix_indices = False
        instance._without_tables = False
        instance._without_foreign_keys = True
        instance._logger = MagicMock()

        columns_rows = [
            {
                "Field": "id",
                "Type": "TEXT",
                "Null": "NO",
                "Default": None,
                "Key": "PRI",
                "Extra": "auto_increment",
            }
        ]
        index_rows = [
            {
                "name": b"idx_primary",
                "primary": 0,
                "unique": 0,
                "auto_increment": 0,
                "columns": b"name",
                "types": b"VARCHAR(10)",
            },
            {
                "name": 123,
                "primary": 0,
                "unique": 1,
                "auto_increment": 0,
                "columns": "email",
                "types": "INT",
            },
        ]

        instance._mysql_cur_dict.fetchall.side_effect = [columns_rows, index_rows]
        instance._mysql_cur_dict.fetchone.side_effect = [{"count": 1}, {"count": 0}]
        instance._get_unique_index_name = MagicMock(side_effect=lambda name: f"{name}_unique")

        sql = instance._build_create_table_sql("users")
        assert "CREATE TABLE" in sql
        instance._logger.warning.assert_called_once()
        assert instance._mysql_cur_dict.fetchone.call_count == 2

    def test_build_create_view_sql_fallbacks_to_show_create(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When information_schema lookup fails, SHOW CREATE VIEW fallback should decode bytes safely."""
        with patch.object(MySQLtoSQLite, "__init__", return_value=None):
            instance = MySQLtoSQLite()

        instance._mysql_database = "demo"
        instance._mysql_cur_dict = MagicMock()
        instance._mysql_cur = MagicMock()
        instance._logger = MagicMock()

        error = mysql.connector.Error(msg="boom")
        instance._mysql_cur_dict.execute.side_effect = error

        create_stmt = b"CREATE VIEW \xff AS SELECT 1"
        instance._mysql_cur.fetchone.return_value = ("demo", create_stmt)

        real_search = re.search

        def fake_search(pattern: str, string: str, flags: int = 0):
            if pattern == r"\bAS\b\s*(.*)$":
                return None
            return real_search(pattern, string, flags)

        monkeypatch.setattr("mysql_to_sqlite3.transporter.re.search", fake_search)

        instance._mysql_viewdef_to_sqlite = MagicMock(return_value="CREATE VIEW demo AS SELECT 1")

        result = instance._build_create_view_sql("demo_view")

        assert result == "CREATE VIEW demo AS SELECT 1"
        instance._mysql_viewdef_to_sqlite.assert_called_once()
        view_sql = instance._mysql_viewdef_to_sqlite.call_args.kwargs["view_select_sql"]
        assert "SELECT" in view_sql

    def test_create_view_reconnect_aborts_after_retry(self) -> None:
        """Lost connections during retry should warn and propagate the mysql error."""
        with patch.object(MySQLtoSQLite, "__init__", return_value=None):
            instance = MySQLtoSQLite()

        class LostError(mysql.connector.Error):
            def __init__(self, msg: str = "lost") -> None:
                super().__init__(msg)
                self.errno = errorcode.CR_SERVER_LOST

        instance._mysql = MagicMock()
        instance._mysql_cur = MagicMock()
        instance._sqlite_cur = MagicMock()
        instance._sqlite = MagicMock()
        instance._logger = MagicMock()
        instance._build_create_view_sql = MagicMock(side_effect=LostError())

        with pytest.raises(LostError):
            instance._create_view("demo_view", attempting_reconnect=True)

        instance._mysql.reconnect.assert_called_once()
        instance._logger.warning.assert_called_with("Connection to MySQL server lost.\nReconnection attempt aborted.")

    def test_transfer_creates_view_when_flag_enabled(self) -> None:
        """When views_as_views is True, encountering a MySQL VIEW should create a SQLite VIEW and skip data transfer."""
        with patch.object(MySQLtoSQLite, "__init__", return_value=None):
            instance = MySQLtoSQLite()
        # Configure minimal attributes used by transfer()
        instance._mysql_tables = []
        instance._exclude_mysql_tables = []
        instance._mysql_cur = MagicMock()
        # All-tables branch returns one VIEW
        instance._mysql_cur.fetchall.return_value = [(b"my_view", b"VIEW")]
        instance._sqlite_cur = MagicMock()
        instance._without_data = False
        instance._without_tables = False
        instance._views_as_views = True
        instance._vacuum = False
        instance._logger = MagicMock()

        # Spy on methods to ensure correct calls
        instance._create_view = MagicMock()
        instance._create_table = MagicMock()
        instance._transfer_table_data = MagicMock()

        instance.transfer()

        instance._create_view.assert_called_once_with("my_view")
        instance._create_table.assert_not_called()
        instance._transfer_table_data.assert_not_called()

    def test_decode_column_type_with_string(self) -> None:
        """Test _decode_column_type with string input."""
        assert MySQLtoSQLite._decode_column_type("VARCHAR") == "VARCHAR"
        assert MySQLtoSQLite._decode_column_type("INTEGER") == "INTEGER"
        assert MySQLtoSQLite._decode_column_type("TEXT") == "TEXT"

    def test_decode_column_type_with_bytes(self) -> None:
        """Test _decode_column_type with bytes input."""
        assert MySQLtoSQLite._decode_column_type(b"VARCHAR") == "VARCHAR"
        assert MySQLtoSQLite._decode_column_type(b"INTEGER") == "INTEGER"
        assert MySQLtoSQLite._decode_column_type(b"TEXT") == "TEXT"

    def test_decode_column_type_with_bytes_decode_error(self) -> None:
        """Test _decode_column_type with bytes that fail to decode."""
        # Create a mock bytes object that raises UnicodeDecodeError when decode is called
        mock_bytes = MagicMock(spec=bytes)
        mock_bytes.decode.side_effect = UnicodeDecodeError("utf-8", b"", 0, 1, "invalid")

        # Patch the isinstance function to return True for our mock_bytes
        with patch(
            "mysql_to_sqlite3.transporter.isinstance",
            lambda obj, cls: True if obj is mock_bytes and cls is bytes else isinstance(obj, cls),
        ):
            result = MySQLtoSQLite._decode_column_type(mock_bytes)
            assert isinstance(result, str)
            # The string representation of the mock should be in the result
            assert str(mock_bytes) in result

    def test_decode_column_type_with_non_string_non_bytes(self) -> None:
        """Test _decode_column_type with input that is neither string nor bytes."""
        assert MySQLtoSQLite._decode_column_type(123) == "123"
        assert MySQLtoSQLite._decode_column_type(None) == "None"
        assert MySQLtoSQLite._decode_column_type(True) == "True"

    def test_get_unique_index_name_suffixing_sequence(self) -> None:
        with patch.object(MySQLtoSQLite, "__init__", return_value=None):
            instance = MySQLtoSQLite()
            # minimal attributes required by the helper
            instance._seen_sqlite_index_names = set()
            instance._sqlite_index_name_counters = {}
            instance._prefix_indices = False
            instance._logger = MagicMock()

            # First occurrence: no suffix
            assert instance._get_unique_index_name("idx_page_id") == "idx_page_id"
            # Second occurrence: _2
            assert instance._get_unique_index_name("idx_page_id") == "idx_page_id_2"
            # Third occurrence: _3
            assert instance._get_unique_index_name("idx_page_id") == "idx_page_id_3"

            # A different base name should start without suffix
            assert instance._get_unique_index_name("idx_user_id") == "idx_user_id"
            # And then suffix from 2
            assert instance._get_unique_index_name("idx_user_id") == "idx_user_id_2"

    @patch("sqlite3.connect")
    def test_check_sqlite_json1_extension_enabled_success(self, mock_connect: MagicMock) -> None:
        """Test _check_sqlite_json1_extension_enabled when JSON1 is enabled."""
        # Setup mock cursor
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [("ENABLE_JSON1",), ("ENABLE_FTS5",)]

        # Setup mock connection
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        # Create a minimal instance with just what we need for the test
        with patch.object(MySQLtoSQLite, "__init__", return_value=None):
            instance = MySQLtoSQLite()
            instance._sqlite_cur = mock_cursor

            # Test the method
            result = instance._check_sqlite_json1_extension_enabled()
            assert result is True
            mock_cursor.execute.assert_called_with("PRAGMA compile_options")

    @patch("sqlite3.connect")
    def test_check_sqlite_json1_extension_disabled(self, mock_connect: MagicMock) -> None:
        """Test _check_sqlite_json1_extension_enabled when JSON1 is not enabled."""
        # Setup mock cursor
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [("ENABLE_FTS5",), ("ENABLE_RTREE",)]

        # Setup mock connection
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        # Create a minimal instance with just what we need for the test
        with patch.object(MySQLtoSQLite, "__init__", return_value=None):
            instance = MySQLtoSQLite()
            instance._sqlite_cur = mock_cursor

            # Test the method
            result = instance._check_sqlite_json1_extension_enabled()
            assert result is False
            mock_cursor.execute.assert_called_with("PRAGMA compile_options")

    @patch("sqlite3.connect")
    def test_check_sqlite_json1_extension_error(self, mock_connect: MagicMock) -> None:
        """Test _check_sqlite_json1_extension_enabled when an error occurs."""
        # Setup mock cursor
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = sqlite3.Error("Test error")

        # Setup mock connection
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        # Create a minimal instance with just what we need for the test
        with patch.object(MySQLtoSQLite, "__init__", return_value=None):
            instance = MySQLtoSQLite()
            instance._sqlite_cur = mock_cursor

            # Test the method
            result = instance._check_sqlite_json1_extension_enabled()
            assert result is False
            mock_cursor.execute.assert_called_with("PRAGMA compile_options")

    @patch("mysql.connector.connect")
    @patch("sqlite3.connect")
    def test_transfer_exception_handling(self, mock_sqlite_connect: MagicMock, mock_mysql_connect: MagicMock) -> None:
        """Test transfer method exception handling."""
        # Setup mock SQLite cursor
        mock_sqlite_cursor = MagicMock()

        # Setup mock SQLite connection
        mock_sqlite_connection = MagicMock()
        mock_sqlite_connection.cursor.return_value = mock_sqlite_cursor
        mock_sqlite_connect.return_value = mock_sqlite_connection

        # Setup mock MySQL cursor
        mock_mysql_cursor = MagicMock()
        mock_mysql_cursor.fetchall.return_value = [(b"table1",)]

        # Setup mock MySQL connection
        mock_mysql_connection = MagicMock()
        mock_mysql_connection.cursor.return_value = mock_mysql_cursor
        mock_mysql_connect.return_value = mock_mysql_connection

        # Create a minimal instance with just what we need for the test
        with patch.object(MySQLtoSQLite, "__init__", return_value=None):
            instance = MySQLtoSQLite()
            instance._mysql_tables = []
            instance._exclude_mysql_tables = []
            instance._mysql_cur = mock_mysql_cursor
            instance._sqlite_cur = mock_sqlite_cursor
            instance._without_data = False
            instance._without_tables = False
            instance._vacuum = False
            instance._logger = MagicMock()

            # Mock the _create_table method to raise an exception
            instance._create_table = MagicMock(side_effect=Exception("Test exception"))

            # Test that the exception is properly propagated
            with pytest.raises(Exception) as excinfo:
                instance.transfer()

            assert "Test exception" in str(excinfo.value)

            # Verify that foreign keys are re-enabled in the finally block
            mock_sqlite_cursor.execute.assert_called_with("PRAGMA foreign_keys=ON")

    @patch("mysql_to_sqlite3.transporter.sqlite3.connect")
    @patch("mysql_to_sqlite3.transporter.mysql.connector.connect")
    def test_sqlite_strict_supported_keeps_flag(
        self,
        mock_mysql_connect: MagicMock,
        mock_sqlite_connect: MagicMock,
        sqlite_database: "os.PathLike[t.Any]",
        mysql_credentials: MySQLCredentials,
        mocker: MockerFixture,
    ) -> None:
        """Ensure STRICT mode remains enabled when SQLite supports it."""

        class FakeMySQLConnection:
            def __init__(self) -> None:
                self.database = None

            def is_connected(self) -> bool:
                return True

            def cursor(self, *args, **kwargs) -> MagicMock:
                return MagicMock()

        mock_logger = MagicMock()
        mocker.patch.object(MySQLtoSQLite, "_setup_logger", return_value=mock_logger)
        mocker.patch("mysql_to_sqlite3.transporter.sqlite3.sqlite_version", "3.38.0")
        mock_mysql_connect.return_value = FakeMySQLConnection()

        mock_sqlite_cursor = MagicMock()
        mock_sqlite_connection = MagicMock()
        mock_sqlite_connection.cursor.return_value = mock_sqlite_cursor
        mock_sqlite_connect.return_value = mock_sqlite_connection

        from mysql_to_sqlite3 import transporter as transporter_module

        original_isinstance = builtins.isinstance

        def fake_isinstance(obj: object, classinfo: object) -> bool:
            if classinfo is transporter_module.MySQLConnectionAbstract:
                return True
            return original_isinstance(obj, classinfo)

        mocker.patch("mysql_to_sqlite3.transporter.isinstance", side_effect=fake_isinstance)

        instance = MySQLtoSQLite(
            sqlite_file=sqlite_database,
            mysql_user=mysql_credentials.user,
            mysql_password=mysql_credentials.password,
            mysql_host=mysql_credentials.host,
            mysql_port=mysql_credentials.port,
            mysql_database=mysql_credentials.database,
            sqlite_strict=True,
        )

        assert instance._sqlite_strict is True
        mock_logger.warning.assert_not_called()

    @patch("mysql_to_sqlite3.transporter.sqlite3.connect")
    @patch("mysql_to_sqlite3.transporter.mysql.connector.connect")
    def test_sqlite_strict_unsupported_disables_flag(
        self,
        mock_mysql_connect: MagicMock,
        mock_sqlite_connect: MagicMock,
        sqlite_database: "os.PathLike[t.Any]",
        mysql_credentials: MySQLCredentials,
        mocker: MockerFixture,
    ) -> None:
        """Ensure STRICT mode is disabled with a warning on old SQLite versions."""

        class FakeMySQLConnection:
            def __init__(self) -> None:
                self.database = None

            def is_connected(self) -> bool:
                return True

            def cursor(self, *args, **kwargs) -> MagicMock:
                return MagicMock()

        mock_logger = MagicMock()
        mocker.patch.object(MySQLtoSQLite, "_setup_logger", return_value=mock_logger)
        mocker.patch("mysql_to_sqlite3.transporter.sqlite3.sqlite_version", "3.36.0")
        mock_mysql_connect.return_value = FakeMySQLConnection()

        mock_sqlite_cursor = MagicMock()
        mock_sqlite_connection = MagicMock()
        mock_sqlite_connection.cursor.return_value = mock_sqlite_cursor
        mock_sqlite_connect.return_value = mock_sqlite_connection

        from mysql_to_sqlite3 import transporter as transporter_module

        original_isinstance = builtins.isinstance

        def fake_isinstance(obj: object, classinfo: object) -> bool:
            if classinfo is transporter_module.MySQLConnectionAbstract:
                return True
            return original_isinstance(obj, classinfo)

        mocker.patch("mysql_to_sqlite3.transporter.isinstance", side_effect=fake_isinstance)

        instance = MySQLtoSQLite(
            sqlite_file=sqlite_database,
            mysql_user=mysql_credentials.user,
            mysql_password=mysql_credentials.password,
            mysql_host=mysql_credentials.host,
            mysql_port=mysql_credentials.port,
            mysql_database=mysql_credentials.database,
            sqlite_strict=True,
        )

        assert instance._sqlite_strict is False
        mock_logger.warning.assert_called_once()

    def test_build_create_table_sql_appends_strict(self) -> None:
        """Ensure STRICT is appended to CREATE TABLE statements when enabled."""
        with patch.object(MySQLtoSQLite, "__init__", return_value=None):
            instance = MySQLtoSQLite()

        instance._sqlite_strict = True
        instance._sqlite_json1_extension_enabled = False
        instance._mysql_cur_dict = MagicMock()
        instance._mysql_cur_dict.fetchall.side_effect = [
            [
                {
                    "Field": "id",
                    "Type": "INTEGER",
                    "Null": "NO",
                    "Default": None,
                    "Key": "PRI",
                    "Extra": "auto_increment",
                },
                {
                    "Field": "name",
                    "Type": "TEXT",
                    "Null": "NO",
                    "Default": None,
                    "Key": "",
                    "Extra": "",
                },
            ],
            [],
        ]
        instance._mysql_cur_dict.fetchone.return_value = {"count": 0}
        instance._mysql_database = "db"
        instance._collation = CollatingSequences.BINARY
        instance._prefix_indices = False
        instance._without_tables = False
        instance._without_foreign_keys = True
        instance._logger = MagicMock()

        sql = instance._build_create_table_sql("products")

        assert "STRICT;" in sql

    def test_constructor_missing_mysql_database(
        self,
        sqlite_database: "os.PathLike[t.Any]",
        mysql_credentials: MySQLCredentials,
    ) -> None:
        """Test constructor raises ValueError if mysql_database is missing."""
        from mysql_to_sqlite3.transporter import MySQLtoSQLite

        with pytest.raises(ValueError, match="Please provide a MySQL database"):
            MySQLtoSQLite(
                sqlite_file=sqlite_database,
                mysql_user=mysql_credentials.user,
            )

    def test_constructor_missing_mysql_user(
        self,
        sqlite_database: "os.PathLike[t.Any]",
        mysql_credentials: MySQLCredentials,
    ) -> None:
        """Test constructor raises ValueError if mysql_user is missing."""
        from mysql_to_sqlite3.transporter import MySQLtoSQLite

        with pytest.raises(ValueError, match="Please provide a MySQL user"):
            MySQLtoSQLite(
                mysql_database=mysql_credentials.database,
                sqlite_file=sqlite_database,
            )

    def test_constructor_missing_sqlite_file(
        self,
        sqlite_database: "os.PathLike[t.Any]",
        mysql_credentials: MySQLCredentials,
    ) -> None:
        """Test constructor raises ValueError if sqlite_file is missing."""
        from mysql_to_sqlite3.transporter import MySQLtoSQLite

        with pytest.raises(ValueError, match="Please provide an SQLite file"):
            MySQLtoSQLite(
                mysql_database=mysql_credentials.database,
                mysql_user=mysql_credentials.user,
            )

    def test_constructor_mutually_exclusive_tables(
        self,
        sqlite_database: "os.PathLike[t.Any]",
        mysql_credentials: MySQLCredentials,
    ) -> None:
        """Test constructor raises ValueError if both mysql_tables and exclude_mysql_tables are provided."""
        from mysql_to_sqlite3.transporter import MySQLtoSQLite

        with pytest.raises(ValueError, match="mutually exclusive"):
            MySQLtoSQLite(
                sqlite_file=sqlite_database,
                mysql_user=mysql_credentials.user,
                mysql_password=mysql_credentials.password,
                mysql_host=mysql_credentials.host,
                mysql_port=mysql_credentials.port,
                mysql_database=mysql_credentials.database,
                mysql_tables=["a"],
                exclude_mysql_tables=["b"],
            )

    def test_constructor_without_tables_and_data(
        self,
        sqlite_database: "os.PathLike[t.Any]",
        mysql_credentials: MySQLCredentials,
    ) -> None:
        """Test constructor raises ValueError if both without_tables and without_data are True."""
        from mysql_to_sqlite3.transporter import MySQLtoSQLite

        with pytest.raises(ValueError, match="Unable to continue without transferring data or creating tables!"):
            MySQLtoSQLite(
                sqlite_file=sqlite_database,
                mysql_user=mysql_credentials.user,
                mysql_password=mysql_credentials.password,
                mysql_host=mysql_credentials.host,
                mysql_port=mysql_credentials.port,
                mysql_database=mysql_credentials.database,
                without_tables=True,
                without_data=True,
            )

    def test_translate_default_from_mysql_to_sqlite_none(self) -> None:
        """Test _translate_default_from_mysql_to_sqlite with None default."""
        assert MySQLtoSQLite._translate_default_from_mysql_to_sqlite(None) == ""

    def test_translate_default_from_mysql_to_sqlite_bool(self) -> None:
        """Test _translate_default_from_mysql_to_sqlite with boolean default."""
        assert MySQLtoSQLite._translate_default_from_mysql_to_sqlite(True, column_type="BOOLEAN") in (
            "DEFAULT(TRUE)",
            "DEFAULT '1'",
        )
        assert MySQLtoSQLite._translate_default_from_mysql_to_sqlite(False, column_type="BOOLEAN") in (
            "DEFAULT(FALSE)",
            "DEFAULT '0'",
        )

    def test_translate_default_from_mysql_to_sqlite_str(self) -> None:
        """Test _translate_default_from_mysql_to_sqlite with string default."""
        assert MySQLtoSQLite._translate_default_from_mysql_to_sqlite("test") == "DEFAULT 'test'"

    def test_translate_default_from_mysql_to_sqlite_current_timestamp(self) -> None:
        """Test _translate_default_from_mysql_to_sqlite with CURRENT_TIMESTAMP."""
        assert (
            MySQLtoSQLite._translate_default_from_mysql_to_sqlite("CURRENT_TIMESTAMP", column_extra="DEFAULT_GENERATED")
            == "DEFAULT CURRENT_TIMESTAMP"
        )

    def test_translate_default_from_mysql_to_sqlite_bytes(self) -> None:
        """Test _translate_default_from_mysql_to_sqlite with bytes default."""
        result = MySQLtoSQLite._translate_default_from_mysql_to_sqlite(b"abc", column_type="BLOB")
        assert result.startswith("DEFAULT x'")


def test_transfer_coerce_row_fallback_non_subscriptable() -> None:
    """Ensure transfer() handles rows that are not indexable by using fallback path in _coerce_row."""
    with patch.object(MySQLtoSQLite, "__init__", return_value=None):
        instance = MySQLtoSQLite()
    # Configure minimal attributes used by transfer()
    instance._mysql_tables = []
    instance._exclude_mysql_tables = []
    instance._mysql_cur = MagicMock()
    # Return a non-subscriptable row (int) to trigger the except fallback branch
    instance._mysql_cur.fetchall.return_value = [123]
    instance._sqlite_cur = MagicMock()
    # Skip creating tables/data to keep the test isolated
    instance._without_data = True
    instance._without_tables = True
    instance._views_as_views = True
    instance._vacuum = False
    instance._logger = MagicMock()

    instance.transfer()

    # Confirm the logger received a transfer message with the coerced table name "123"
    # The info call is like: ("%s%sTransferring table %s", prefix1, prefix2, table_name)
    called_with_123 = any(call.args and call.args[-1] == "123" for call in instance._logger.info.call_args_list)
    assert called_with_123
