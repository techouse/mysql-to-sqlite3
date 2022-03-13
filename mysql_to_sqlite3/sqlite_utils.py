"""SQLite adapters and converters for unsupported data types."""

from __future__ import division

import sqlite3
from datetime import date, datetime, timedelta
from decimal import Decimal
from sys import version_info

import six
from pytimeparse.timeparse import timeparse

if version_info.major == 3 and 4 <= version_info.minor <= 6:
    from backports.datetime_fromisoformat import MonkeyPatch  # pylint: disable=E0401

    MonkeyPatch.patch_fromisoformat()


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


def convert_date(value):
    """Handle SQLite date conversion."""
    if six.PY3:
        try:
            return date.fromisoformat(value.decode())
        except ValueError as err:
            raise ValueError(  # pylint: disable=W0707
                "DATE field contains {}".format(err)
            )
    try:
        return datetime.strptime(value.decode(), "%Y-%m-%d").date()
    except ValueError as err:
        raise ValueError(  # pylint: disable=W0707
            "DATE field contains Invalid isoformat string: {}".format(err)
        )
