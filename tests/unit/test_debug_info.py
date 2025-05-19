import sys
import typing as t
from unittest.mock import MagicMock, patch

import pytest
from pytest_mock import MockFixture

from mysql_to_sqlite3.debug_info import _implementation, _mysql_version, info


class TestDebugInfo:
    def test_implementation_cpython(self, mocker: MockFixture) -> None:
        """Test _implementation function with CPython."""
        mocker.patch("platform.python_implementation", return_value="CPython")
        mocker.patch("platform.python_version", return_value="3.8.10")

        result = _implementation()
        assert result == "CPython 3.8.10"

    def test_implementation_pypy(self, mocker: MockFixture) -> None:
        """Test _implementation function with PyPy."""
        mocker.patch("platform.python_implementation", return_value="PyPy")

        # Create a mock for pypy_version_info
        mock_version_info = MagicMock()
        mock_version_info.major = 3
        mock_version_info.minor = 7
        mock_version_info.micro = 4
        mock_version_info.releaselevel = "final"

        # Need to use patch instead of mocker.patch for sys module attributes
        with patch.object(sys, "pypy_version_info", mock_version_info, create=True):
            result = _implementation()
            assert result == "PyPy 3.7.4"

    def test_implementation_pypy_non_final(self, mocker: MockFixture) -> None:
        """Test _implementation function with PyPy non-final release."""
        mocker.patch("platform.python_implementation", return_value="PyPy")

        # Create a mock for pypy_version_info
        mock_version_info = MagicMock()
        mock_version_info.major = 3
        mock_version_info.minor = 7
        mock_version_info.micro = 4
        mock_version_info.releaselevel = "beta"

        # Need to use patch instead of mocker.patch for sys module attributes
        with patch.object(sys, "pypy_version_info", mock_version_info, create=True):
            result = _implementation()
            assert result == "PyPy 3.7.4beta"

    def test_implementation_jython(self, mocker: MockFixture) -> None:
        """Test _implementation function with Jython."""
        mocker.patch("platform.python_implementation", return_value="Jython")
        mocker.patch("platform.python_version", return_value="2.7.2")

        result = _implementation()
        assert result == "Jython 2.7.2"

    def test_implementation_ironpython(self, mocker: MockFixture) -> None:
        """Test _implementation function with IronPython."""
        mocker.patch("platform.python_implementation", return_value="IronPython")
        mocker.patch("platform.python_version", return_value="2.7.9")

        result = _implementation()
        assert result == "IronPython 2.7.9"

    def test_implementation_unknown(self, mocker: MockFixture) -> None:
        """Test _implementation function with unknown implementation."""
        mocker.patch("platform.python_implementation", return_value="UnknownPython")

        result = _implementation()
        assert result == "UnknownPython Unknown"

    def test_mysql_version_success(self, mocker: MockFixture) -> None:
        """Test _mysql_version function when mysql client is available."""
        mocker.patch("mysql_to_sqlite3.debug_info.which", return_value="/usr/bin/mysql")
        mocker.patch(
            "mysql_to_sqlite3.debug_info.check_output",
            return_value=b"mysql  Ver 8.0.26 for Linux on x86_64",
        )

        result = _mysql_version()
        assert result == "mysql  Ver 8.0.26 for Linux on x86_64"

    def test_mysql_version_bytes_decode_error(self, mocker: MockFixture) -> None:
        """Test _mysql_version function when bytes decoding fails."""
        mocker.patch("mysql_to_sqlite3.debug_info.which", return_value="/usr/bin/mysql")
        mock_output = MagicMock()
        mock_output.decode.side_effect = UnicodeDecodeError("utf-8", b"", 0, 1, "invalid")
        mocker.patch(
            "mysql_to_sqlite3.debug_info.check_output",
            return_value=mock_output,
        )

        result = _mysql_version()
        assert isinstance(result, str)

    def test_mysql_version_exception(self, mocker: MockFixture) -> None:
        """Test _mysql_version function when an exception occurs."""
        mocker.patch("mysql_to_sqlite3.debug_info.which", return_value="/usr/bin/mysql")
        mocker.patch(
            "mysql_to_sqlite3.debug_info.check_output",
            side_effect=Exception("Command failed"),
        )

        result = _mysql_version()
        assert result == "MySQL client not found on the system"

    def test_mysql_version_not_found(self, mocker: MockFixture) -> None:
        """Test _mysql_version function when mysql client is not found."""
        mocker.patch("mysql_to_sqlite3.debug_info.which", return_value=None)

        result = _mysql_version()
        assert result == "MySQL client not found on the system"

    def test_info_success(self, mocker: MockFixture) -> None:
        """Test info function."""
        mocker.patch("platform.system", return_value="Linux")
        mocker.patch("platform.release", return_value="5.4.0-80-generic")
        mocker.patch("mysql_to_sqlite3.debug_info._implementation", return_value="CPython 3.8.10")
        mocker.patch("mysql_to_sqlite3.debug_info._mysql_version", return_value="mysql  Ver 8.0.26 for Linux on x86_64")

        result = info()
        assert isinstance(result, list)
        assert len(result) > 0
        assert result[2] == ["Operating System", "Linux 5.4.0-80-generic"]
        assert result[3] == ["Python", "CPython 3.8.10"]
        assert result[4] == ["MySQL", "mysql  Ver 8.0.26 for Linux on x86_64"]

    def test_info_platform_error(self, mocker: MockFixture) -> None:
        """Test info function when platform.system raises IOError."""
        mocker.patch("platform.system", side_effect=IOError("Platform error"))

        result = info()
        assert isinstance(result, list)
        assert len(result) > 0
        assert result[2] == ["Operating System", "Unknown"]
