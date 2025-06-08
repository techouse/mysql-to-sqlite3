import sqlite3
import typing as t
from unittest.mock import MagicMock, patch

import pytest

from mysql_to_sqlite3.transporter import MySQLtoSQLite


class TestMySQLtoSQLiteTransporter:
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
            mock_sqlite_cursor.execute.assert_any_call("PRAGMA foreign_keys=ON")

            # Verify that foreign key check is performed
            mock_sqlite_cursor.execute.assert_called_with("PRAGMA foreign_key_check")

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

    @patch("mysql.connector.connect")
    @patch("sqlite3.connect")
    @patch("mysql_to_sqlite3.transporter.compute_creation_order")
    def test_transfer_table_ordering(
        self, mock_compute_creation_order: MagicMock, mock_sqlite_connect: MagicMock, mock_mysql_connect: MagicMock
    ) -> None:
        """Test that tables are transferred in the correct order respecting foreign key constraints."""
        # Setup mock SQLite cursor
        mock_sqlite_cursor: MagicMock = MagicMock()

        # Setup mock SQLite connection
        mock_sqlite_connection: MagicMock = MagicMock()
        mock_sqlite_connection.cursor.return_value = mock_sqlite_cursor
        mock_sqlite_connect.return_value = mock_sqlite_connection

        # Setup mock MySQL cursor
        mock_mysql_cursor: MagicMock = MagicMock()
        mock_mysql_cursor.fetchall.return_value = [(b"table1",), (b"table2",), (b"table3",)]

        # Setup mock MySQL connection
        mock_mysql_connection: MagicMock = MagicMock()
        mock_mysql_connection.cursor.return_value = mock_mysql_cursor
        mock_mysql_connect.return_value = mock_mysql_connection

        # Mock compute_creation_order to return a specific order
        ordered_tables: t.List[str] = ["table2", "table1", "table3"]  # Specific order for testing
        cyclic_edges: t.List[t.Tuple[str, str]] = []  # No cycles for this test
        mock_compute_creation_order.return_value = (ordered_tables, cyclic_edges)

        # Create a minimal instance with just what we need for the test
        with patch.object(MySQLtoSQLite, "__init__", return_value=None):
            instance = MySQLtoSQLite()
            instance._mysql = mock_mysql_connection
            instance._mysql_tables = []
            instance._exclude_mysql_tables = []
            instance._mysql_cur = mock_mysql_cursor
            instance._mysql_cur_dict = MagicMock()
            instance._mysql_cur_prepared = MagicMock()
            instance._sqlite_cur = mock_sqlite_cursor
            instance._without_data = True  # Skip data transfer for simplicity
            instance._without_tables = False
            instance._without_foreign_keys = False
            instance._vacuum = False
            instance._logger = MagicMock()
            instance._create_table = MagicMock()  # Mock table creation

            # Call the transfer method
            instance.transfer()

            # Verify compute_creation_order was called
            mock_compute_creation_order.assert_called_once_with(mock_mysql_connection)

            # Verify tables were created in the correct order
            creation_calls: list[t.Any] = [call[0][0] for call in instance._create_table.call_args_list]
            assert creation_calls == ordered_tables

            # Verify foreign keys were disabled at start and enabled at end
            mock_sqlite_cursor.execute.assert_any_call("PRAGMA foreign_keys=OFF")
            mock_sqlite_cursor.execute.assert_any_call("PRAGMA foreign_keys=ON")

            # Verify foreign key check was performed
            mock_sqlite_cursor.execute.assert_any_call("PRAGMA foreign_key_check")

    @patch("mysql.connector.connect")
    @patch("sqlite3.connect")
    @patch("mysql_to_sqlite3.transporter.compute_creation_order")
    def test_transfer_with_circular_dependencies(
        self, mock_compute_creation_order: MagicMock, mock_sqlite_connect: MagicMock, mock_mysql_connect: MagicMock
    ) -> None:
        """Test transfer with circular foreign key dependencies."""
        # Setup mock SQLite cursor
        mock_sqlite_cursor: MagicMock = MagicMock()

        # Setup mock SQLite connection
        mock_sqlite_connection: MagicMock = MagicMock()
        mock_sqlite_connection.cursor.return_value = mock_sqlite_cursor
        mock_sqlite_connect.return_value = mock_sqlite_connection

        # Setup mock MySQL cursor
        mock_mysql_cursor: MagicMock = MagicMock()
        mock_mysql_cursor.fetchall.return_value = [(b"table1",), (b"table2",), (b"table3",)]

        # Setup mock MySQL connection
        mock_mysql_connection: MagicMock = MagicMock()
        mock_mysql_connection.cursor.return_value = mock_mysql_cursor
        mock_mysql_connect.return_value = mock_mysql_connection

        # Mock compute_creation_order to return circular dependencies
        ordered_tables: t.List[str] = ["table2", "table1", "table3"]
        cyclic_edges: t.List[t.Tuple[str, str]] = [("table1", "table3"), ("table3", "table1")]  # Circular dependency
        mock_compute_creation_order.return_value = (ordered_tables, cyclic_edges)

        # Create a minimal instance with just what we need for the test
        with patch.object(MySQLtoSQLite, "__init__", return_value=None):
            instance = MySQLtoSQLite()
            instance._mysql = mock_mysql_connection
            instance._mysql_tables = []
            instance._exclude_mysql_tables = []
            instance._mysql_cur = mock_mysql_cursor
            instance._mysql_cur_dict = MagicMock()
            instance._mysql_cur_prepared = MagicMock()
            instance._sqlite_cur = mock_sqlite_cursor
            instance._without_data = True  # Skip data transfer for simplicity
            instance._without_tables = False
            instance._without_foreign_keys = False
            instance._vacuum = False
            instance._logger = MagicMock()
            instance._create_table = MagicMock()  # Mock table creation

            # Call the transfer method
            instance.transfer()

            # Verify compute_creation_order was called
            mock_compute_creation_order.assert_called_once_with(mock_mysql_connection)

            # Verify warning was logged about circular dependencies
            instance._logger.warning.assert_any_call(
                "Circular foreign key dependencies detected: %s", "table1 -> table3, table3 -> table1"
            )

            # Verify tables were still created in the computed order
            creation_calls: t.List[t.Any] = [call[0][0] for call in instance._create_table.call_args_list]
            assert creation_calls == ordered_tables

            # Verify foreign keys were disabled at start and enabled at end
            mock_sqlite_cursor.execute.assert_any_call("PRAGMA foreign_keys=OFF")
            mock_sqlite_cursor.execute.assert_any_call("PRAGMA foreign_keys=ON")

            # Verify foreign key check was performed
            mock_sqlite_cursor.execute.assert_any_call("PRAGMA foreign_key_check")

    @patch("mysql.connector.connect")
    @patch("sqlite3.connect")
    @patch("mysql_to_sqlite3.transporter.compute_creation_order")
    def test_transfer_fallback_on_error(
        self, mock_compute_creation_order: MagicMock, mock_sqlite_connect: MagicMock, mock_mysql_connect: MagicMock
    ) -> None:
        """Test transfer falls back to original table list if compute_creation_order fails."""
        # Setup mock SQLite cursor
        mock_sqlite_cursor: MagicMock = MagicMock()

        # Setup mock SQLite connection
        mock_sqlite_connection: MagicMock = MagicMock()
        mock_sqlite_connection.cursor.return_value = mock_sqlite_cursor
        mock_sqlite_connect.return_value = mock_sqlite_connection

        # Setup mock MySQL cursor
        mock_mysql_cursor: MagicMock = MagicMock()
        mock_mysql_cursor.fetchall.return_value = [(b"table1",), (b"table2",), (b"table3",)]

        # Setup mock MySQL connection
        mock_mysql_connection: MagicMock = MagicMock()
        mock_mysql_connection.cursor.return_value = mock_mysql_cursor
        mock_mysql_connect.return_value = mock_mysql_connection

        # Mock compute_creation_order to raise an exception
        mock_compute_creation_order.side_effect = Exception("Test error in compute_creation_order")

        # Create a minimal instance with just what we need for the test
        with patch.object(MySQLtoSQLite, "__init__", return_value=None):
            instance = MySQLtoSQLite()
            instance._mysql = mock_mysql_connection
            instance._mysql_tables = []
            instance._exclude_mysql_tables = []
            instance._mysql_cur = mock_mysql_cursor
            instance._mysql_cur_dict = MagicMock()
            instance._mysql_cur_prepared = MagicMock()
            instance._sqlite_cur = mock_sqlite_cursor
            instance._without_data = True  # Skip data transfer for simplicity
            instance._without_tables = False
            instance._without_foreign_keys = False
            instance._vacuum = False
            instance._logger = MagicMock()
            instance._create_table = MagicMock()  # Mock table creation

            # Call the transfer method
            instance.transfer()

            # Verify compute_creation_order was called
            mock_compute_creation_order.assert_called_once_with(mock_mysql_connection)

            # Verify warning was logged about the error
            instance._logger.warning.assert_any_call(
                "Failed to compute table creation order: %s", "Test error in compute_creation_order"
            )

            # Verify tables were still created (using the fallback order)
            assert instance._create_table.call_count == 3

            # Verify foreign keys were disabled at start and enabled at end
            mock_sqlite_cursor.execute.assert_any_call("PRAGMA foreign_keys=OFF")
            mock_sqlite_cursor.execute.assert_any_call("PRAGMA foreign_keys=ON")

            # Verify foreign key check was performed
            mock_sqlite_cursor.execute.assert_any_call("PRAGMA foreign_key_check")
