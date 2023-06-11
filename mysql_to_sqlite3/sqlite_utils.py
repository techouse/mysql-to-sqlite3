"""SQLite adapters and converters for unsupported data types."""

import sqlite3
from datetime import date, timedelta
from decimal import Decimal

from pytimeparse2 import parse


def adapt_decimal(value):
    """Convert decimal.Decimal to string."""
    return str(value)


def convert_decimal(value):
    """Convert string to decimalDecimal."""
    return Decimal(value)


def adapt_timedelta(value):
    """Convert datetime.timedelta to %H:%M:%S string."""
    hours, remainder = divmod(value.total_seconds(), 3600)
    minutes, seconds = divmod(remainder, 60)
    return "{:02}:{:02}:{:02}".format(int(hours), int(minutes), int(seconds))


def convert_timedelta(value):
    """Convert %H:%M:%S string to datetime.timedelta."""
    return timedelta(seconds=parse(value))


def encode_data_for_sqlite(value):
    """Fix encoding bytes."""
    try:
        return value.decode()
    except (UnicodeDecodeError, AttributeError):
        return sqlite3.Binary(value)


class CollatingSequences:
    """Taken from https://www.sqlite.org/datatype3.html#collating_sequences."""

    BINARY = "BINARY"
    NOCASE = "NOCASE"
    RTRIM = "RTRIM"


def convert_date(value):
    """Handle SQLite date conversion."""
    try:
        return date.fromisoformat(value.decode())
    except ValueError as err:
        raise ValueError("DATE field contains {}".format(err))  # pylint: disable=W0707
