"""Miscellaneous MySQL utilities."""

import typing as t

from mysql.connector.charsets import MYSQL_CHARACTER_SETS


CHARSET_INTRODUCERS: t.Tuple[str, ...] = tuple(
    f"_{charset[0]}" for charset in MYSQL_CHARACTER_SETS if charset is not None
)
