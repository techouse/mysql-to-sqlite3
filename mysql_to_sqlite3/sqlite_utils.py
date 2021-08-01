"""SQLite adapters and converters for unsupported data types."""

from __future__ import division

import sqlite3
from datetime import timedelta
from decimal import Decimal

from pytimeparse.timeparse import timeparse


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
    return timedelta(seconds=timeparse(value))


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
