"""SQLite adapters and converters for unsupported data types."""

import sqlite3
import typing as t
from datetime import date, timedelta
from decimal import Decimal

from dateutil.parser import ParserError
from dateutil.parser import parse as dateutil_parse
from pytimeparse2 import parse


def adapt_decimal(value: t.Any) -> str:
    """Convert decimal.Decimal to string."""
    return str(value)


def convert_decimal(value: t.Any) -> Decimal:
    """Convert string to decimal.Decimal."""
    return Decimal(value)


def adapt_timedelta(value: t.Any) -> str:
    """Convert datetime.timedelta to %H:%M:%S string."""
    hours, remainder = divmod(value.total_seconds(), 3600)
    minutes, seconds = divmod(remainder, 60)
    return "{:02}:{:02}:{:02}".format(int(hours), int(minutes), int(seconds))


def convert_timedelta(value: t.Any) -> timedelta:
    """Convert %H:%M:%S string to datetime.timedelta."""
    return timedelta(seconds=parse(value))


def encode_data_for_sqlite(value: t.Any) -> t.Any:
    """Fix encoding bytes."""
    try:
        return value.decode()
    except (UnicodeDecodeError, AttributeError):
        return sqlite3.Binary(value)


class CollatingSequences:
    """Taken from https://www.sqlite.org/datatype3.html#collating_sequences."""

    BINARY: str = "BINARY"
    NOCASE: str = "NOCASE"
    RTRIM: str = "RTRIM"


def convert_date(value: t.Union[str, bytes]) -> date:
    """Handle SQLite date conversion."""
    try:
        return dateutil_parse(value.decode() if isinstance(value, bytes) else value).date()
    except ParserError as err:
        raise ValueError(f"DATE field contains {err}")  # pylint: disable=W0707


Integer_Types: t.Set[str] = {
    "INTEGER",
    "INTEGER UNSIGNED",
    "INT",
    "INT UNSIGNED",
    "BIGINT",
    "BIGINT UNSIGNED",
    "MEDIUMINT",
    "MEDIUMINT UNSIGNED",
    "SMALLINT",
    "SMALLINT UNSIGNED",
    "TINYINT",
    "TINYINT UNSIGNED",
    "NUMERIC",
}
