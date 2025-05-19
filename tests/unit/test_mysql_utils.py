"""Unit tests for the mysql_utils module."""

import typing as t
from unittest import mock

import pytest
from mysql.connector import CharacterSet

from mysql_to_sqlite3.mysql_utils import (
    CHARSET_INTRODUCERS,
    CharSet,
    mysql_supported_character_sets,
)


class TestMySQLUtils:
    """Unit tests for the mysql_utils module."""

    def test_charset_introducers(self) -> None:
        """Test that CHARSET_INTRODUCERS contains the expected values."""
        assert isinstance(CHARSET_INTRODUCERS, tuple)
        assert len(CHARSET_INTRODUCERS) > 0
        assert all(isinstance(intro, str) for intro in CHARSET_INTRODUCERS)
        assert all(intro.startswith("_") for intro in CHARSET_INTRODUCERS)

    def test_charset_named_tuple(self) -> None:
        """Test the CharSet named tuple."""
        charset = CharSet(id=1, charset="utf8", collation="utf8_general_ci")
        assert charset.id == 1
        assert charset.charset == "utf8"
        assert charset.collation == "utf8_general_ci"

    def test_mysql_supported_character_sets_with_charset(self) -> None:
        """Test mysql_supported_character_sets with a specific charset."""
        test_charset = "utf8mb4"
        results = list(mysql_supported_character_sets(test_charset))
        assert len(results) > 0
        for result in results:
            assert result.charset == test_charset
            assert isinstance(result.id, int)
            assert isinstance(result.collation, str)

    @mock.patch("mysql_to_sqlite3.mysql_utils.MYSQL_CHARACTER_SETS", [(None, None), ("utf8", "utf8_general_ci", True)])
    def test_mysql_supported_character_sets_with_charset_keyerror(self) -> None:
        """Test handling KeyError in mysql_supported_character_sets with charset."""
        # Override the MYSQL_CHARACTER_SETS behavior to raise KeyError
        with mock.patch("mysql_to_sqlite3.mysql_utils.MYSQL_CHARACTER_SETS") as mock_charset_sets:
            mock_charset_sets.__getitem__.side_effect = KeyError("Test KeyError")

            # This should not raise any exceptions
            results = list(mysql_supported_character_sets("utf8"))
            assert len(results) == 0

    @mock.patch("mysql_to_sqlite3.mysql_utils.CharacterSet")
    def test_mysql_supported_character_sets_no_charset(self, mock_charset_class) -> None:
        """Test mysql_supported_character_sets with no charset."""
        # Create a custom mock class for CharacterSet that has get_supported method
        mock_instance = mock.MagicMock()
        mock_instance.get_supported.return_value = ["utf8"]
        mock_charset_class.return_value = mock_instance

        with mock.patch(
            "mysql_to_sqlite3.mysql_utils.MYSQL_CHARACTER_SETS",
            [
                None,  # This will be skipped due to None check
                ("utf8", "utf8_general_ci", True),  # This will be processed
            ],
        ):
            results = list(mysql_supported_character_sets())

            mock_instance.get_supported.assert_called_once()

            assert len(results) > 0
            charset_results = [r.charset for r in results]
            assert "utf8" in charset_results

    @mock.patch("mysql_to_sqlite3.mysql_utils.CharacterSet")
    def test_mysql_supported_character_sets_no_charset_keyerror(self, mock_charset_class) -> None:
        """Test handling KeyError in mysql_supported_character_sets without charset."""
        # Setup mock to return specific values
        mock_instance = mock.MagicMock()
        mock_instance.get_supported.return_value = ["utf8"]
        mock_charset_class.return_value = mock_instance

        # Create a mock object to return either valid info or raise KeyError
        mock_char_sets = mock.MagicMock()
        mock_char_sets.__len__.return_value = 2

        # Make the first item None and the second one raise KeyError when accessed
        def getitem_side_effect(idx):
            if idx == 0:
                return (None, None, None)
            else:
                raise KeyError("Test KeyError")

        mock_char_sets.__getitem__.side_effect = getitem_side_effect

        # Patch with the mock object
        with mock.patch("mysql_to_sqlite3.mysql_utils.MYSQL_CHARACTER_SETS", mock_char_sets):
            # Should continue without raising exceptions despite KeyError
            results = list(mysql_supported_character_sets())
            assert len(results) == 0

            # Verify the get_supported method was called
            mock_instance.get_supported.assert_called_once()

    def test_mysql_supported_character_sets_complete_coverage(self) -> None:
        """Test mysql_supported_character_sets to target specific edge cases for full coverage."""
        # Test with a charset that doesn't match any entries
        with mock.patch(
            "mysql_to_sqlite3.mysql_utils.MYSQL_CHARACTER_SETS",
            [("utf8", "utf8_general_ci", True), None, ("latin1", "latin1_swedish_ci", True)],  # Test None handling
        ):
            # Should return empty when charset doesn't match any entries
            results = list(mysql_supported_character_sets("non_existent_charset"))
            assert len(results) == 0

            # Should process all valid charsets when no specific charset is requested
            with mock.patch("mysql_to_sqlite3.mysql_utils.CharacterSet") as mock_charset_class:
                mock_instance = mock.MagicMock()
                mock_instance.get_supported.return_value = ["utf8", "latin1", "invalid_charset"]
                mock_charset_class.return_value = mock_instance

                # Test when no charset is specified - should process all entries
                results = list(mysql_supported_character_sets())
                # Should have entries for utf8 and latin1
                assert len([r for r in results if r.charset in ["utf8", "latin1"]]) > 0

    def test_mysql_supported_character_sets_with_specific_keyerror(self) -> None:
        """Test mysql_supported_character_sets with specific KeyError scenarios."""
        # Test the specific KeyError scenario in the first branch (with charset specified)
        with mock.patch(
            "mysql_to_sqlite3.mysql_utils.MYSQL_CHARACTER_SETS",
            [
                None,
                mock.MagicMock(side_effect=KeyError("Key error in info[0]")),  # Trigger KeyError on info[0]
                ("latin1", "latin1_swedish_ci", True),
            ],
        ):
            # This should not raise exceptions despite the KeyError
            results = list(mysql_supported_character_sets("utf8"))
            assert len(results) == 0

    @mock.patch("mysql_to_sqlite3.mysql_utils.CharacterSet")
    def test_mysql_supported_character_sets_with_specific_info_keyerror(self, mock_charset_class) -> None:
        """Test mysql_supported_character_sets with KeyError on info[1] access."""
        mock_instance = mock.MagicMock()
        mock_instance.get_supported.return_value = ["utf8"]
        mock_charset_class.return_value = mock_instance

        # Create a special mock that will raise KeyError on accessing index 1
        class InfoMock:
            def __getitem__(self, key):
                if key == 0:
                    return "utf8"
                elif key == 1:
                    raise KeyError("Test KeyError on info[1]")
                return None

        # Set up MYSQL_CHARACTER_SETS with our special mock
        with mock.patch(
            "mysql_to_sqlite3.mysql_utils.MYSQL_CHARACTER_SETS",
            [
                None,
                InfoMock(),  # Will raise KeyError on info[1]
            ],
        ):
            # This should not raise exceptions despite the KeyError
            results = list(mysql_supported_character_sets())
            assert len(results) == 0

            # Now test with a specific charset to cover both branches
            results = list(mysql_supported_character_sets("utf8"))
            assert len(results) == 0
