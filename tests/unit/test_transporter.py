import builtins
import sqlite3
from unittest.mock import MagicMock, patch

import pytest
from pytest_mock import MockerFixture

from mysql_to_sqlite3.sqlite_utils import CollatingSequences
from mysql_to_sqlite3.transporter import MySQLtoSQLite


class TestMySQLtoSQLiteTransporter:
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
            sqlite_file="file.db",
            mysql_user="user",
            mysql_password=None,
            mysql_database="db",
            mysql_host="localhost",
            mysql_port=3306,
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
            sqlite_file="file.db",
            mysql_user="user",
            mysql_password=None,
            mysql_database="db",
            mysql_host="localhost",
            mysql_port=3306,
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

    def test_constructor_missing_mysql_database(self) -> None:
        """Test constructor raises ValueError if mysql_database is missing."""
        from mysql_to_sqlite3.transporter import MySQLtoSQLite

        with pytest.raises(ValueError, match="Please provide a MySQL database"):
            MySQLtoSQLite(mysql_user="user", sqlite_file="file.db")

    def test_constructor_missing_mysql_user(self) -> None:
        """Test constructor raises ValueError if mysql_user is missing."""
        from mysql_to_sqlite3.transporter import MySQLtoSQLite

        with pytest.raises(ValueError, match="Please provide a MySQL user"):
            MySQLtoSQLite(mysql_database="db", sqlite_file="file.db")

    def test_constructor_missing_sqlite_file(self) -> None:
        """Test constructor raises ValueError if sqlite_file is missing."""
        from mysql_to_sqlite3.transporter import MySQLtoSQLite

        with pytest.raises(ValueError, match="Please provide an SQLite file"):
            MySQLtoSQLite(mysql_database="db", mysql_user="user")

    def test_constructor_mutually_exclusive_tables(self) -> None:
        """Test constructor raises ValueError if both mysql_tables and exclude_mysql_tables are provided."""
        from mysql_to_sqlite3.transporter import MySQLtoSQLite

        with pytest.raises(ValueError, match="mutually exclusive"):
            MySQLtoSQLite(
                mysql_database="db",
                mysql_user="user",
                sqlite_file="file.db",
                mysql_tables=["a"],
                exclude_mysql_tables=["b"],
            )

    def test_constructor_without_tables_and_data(self) -> None:
        """Test constructor raises ValueError if both without_tables and without_data are True."""
        from mysql_to_sqlite3.transporter import MySQLtoSQLite

        with pytest.raises(ValueError, match="Unable to continue without transferring data or creating tables!"):
            MySQLtoSQLite(
                mysql_database="db", mysql_user="user", sqlite_file="file.db", without_tables=True, without_data=True
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
