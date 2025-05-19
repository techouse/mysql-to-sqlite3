from datetime import date, timedelta
from decimal import Decimal

import pytest

from mysql_to_sqlite3.sqlite_utils import (
    CollatingSequences,
    Integer_Types,
    adapt_decimal,
    adapt_timedelta,
    convert_date,
    convert_decimal,
    convert_timedelta,
    encode_data_for_sqlite,
)


class TestSQLiteUtils:
    def test_adapt_decimal(self) -> None:
        """Test adapt_decimal function."""
        assert adapt_decimal(Decimal("123.45")) == "123.45"
        assert adapt_decimal(Decimal("0")) == "0"
        assert adapt_decimal(Decimal("-123.45")) == "-123.45"
        assert adapt_decimal(Decimal("123456789.123456789")) == "123456789.123456789"

    def test_convert_decimal(self) -> None:
        """Test convert_decimal function."""
        assert convert_decimal("123.45") == Decimal("123.45")
        assert convert_decimal("0") == Decimal("0")
        assert convert_decimal("-123.45") == Decimal("-123.45")
        assert convert_decimal("123456789.123456789") == Decimal("123456789.123456789")

    def test_adapt_timedelta(self) -> None:
        """Test adapt_timedelta function."""
        assert adapt_timedelta(timedelta(hours=1, minutes=30, seconds=45)) == "01:30:45"
        assert adapt_timedelta(timedelta(hours=0, minutes=0, seconds=0)) == "00:00:00"
        assert adapt_timedelta(timedelta(hours=100, minutes=0, seconds=0)) == "100:00:00"
        assert adapt_timedelta(timedelta(hours=0, minutes=90, seconds=0)) == "01:30:00"
        assert adapt_timedelta(timedelta(hours=0, minutes=0, seconds=90)) == "00:01:30"

    def test_convert_timedelta(self) -> None:
        """Test convert_timedelta function."""
        assert convert_timedelta("01:30:45") == timedelta(hours=1, minutes=30, seconds=45)
        assert convert_timedelta("00:00:00") == timedelta(hours=0, minutes=0, seconds=0)
        assert convert_timedelta("100:00:00") == timedelta(hours=100, minutes=0, seconds=0)
        assert convert_timedelta("01:30:00") == timedelta(hours=1, minutes=30, seconds=0)
        assert convert_timedelta("00:01:30") == timedelta(hours=0, minutes=1, seconds=30)

    def test_encode_data_for_sqlite_string(self) -> None:
        """Test encode_data_for_sqlite with string."""
        assert encode_data_for_sqlite("test") == "test"

    def test_encode_data_for_sqlite_bytes_success(self) -> None:
        """Test encode_data_for_sqlite with bytes that can be decoded."""
        assert encode_data_for_sqlite(b"test") == "test"

    def test_encode_data_for_sqlite_bytes_failure(self) -> None:
        """Test encode_data_for_sqlite with bytes that cannot be decoded."""
        # Create invalid UTF-8 bytes
        invalid_bytes = b"\xff\xfe\xfd"
        result = encode_data_for_sqlite(invalid_bytes)
        # Should return a sqlite3.Binary object or something that behaves similarly
        # Check if it's either a Binary object or at least contains the original bytes
        if hasattr(result, "adapt"):
            assert result.adapt() == invalid_bytes
        else:
            # If it's a memoryview or other type, verify it contains our original bytes
            assert bytes(result) == invalid_bytes

    def test_encode_data_for_sqlite_non_bytes(self) -> None:
        """Test encode_data_for_sqlite with non-bytes object that has no decode method."""
        result = encode_data_for_sqlite(123)
        # In our implementation, the function should either:
        # 1. Return a sqlite3.Binary object, or
        # 2. Return the original value if it can't be converted to Binary
        # Either way, the result should work with SQLite
        if hasattr(result, "adapt"):
            assert result.adapt() == 123
        else:
            assert result == 123

    def test_convert_date_valid_string(self) -> None:
        """Test convert_date with valid string."""
        assert convert_date("2021-01-01") == date(2021, 1, 1)
        assert convert_date("2021/01/01") == date(2021, 1, 1)
        assert convert_date("Jan 1, 2021") == date(2021, 1, 1)

    def test_convert_date_valid_bytes(self) -> None:
        """Test convert_date with valid bytes."""
        assert convert_date(b"2021-01-01") == date(2021, 1, 1)
        assert convert_date(b"2021/01/01") == date(2021, 1, 1)
        assert convert_date(b"Jan 1, 2021") == date(2021, 1, 1)

    def test_convert_date_invalid(self) -> None:
        """Test convert_date with invalid date string."""
        with pytest.raises(ValueError) as excinfo:
            convert_date("not a date")
        assert "DATE field contains" in str(excinfo.value)

    def test_collating_sequences(self) -> None:
        """Test CollatingSequences class."""
        assert CollatingSequences.BINARY == "BINARY"
        assert CollatingSequences.NOCASE == "NOCASE"
        assert CollatingSequences.RTRIM == "RTRIM"

    def test_integer_types(self) -> None:
        """Test Integer_Types set."""
        assert "INTEGER" in Integer_Types
        assert "INT" in Integer_Types
        assert "BIGINT" in Integer_Types
        assert "SMALLINT" in Integer_Types
        assert "TINYINT" in Integer_Types
        assert "MEDIUMINT" in Integer_Types
        assert "NUMERIC" in Integer_Types
        # Check that unsigned variants are included
        assert "INTEGER UNSIGNED" in Integer_Types
        assert "INT UNSIGNED" in Integer_Types
        assert "BIGINT UNSIGNED" in Integer_Types
        assert "SMALLINT UNSIGNED" in Integer_Types
        assert "TINYINT UNSIGNED" in Integer_Types
        assert "MEDIUMINT UNSIGNED" in Integer_Types
