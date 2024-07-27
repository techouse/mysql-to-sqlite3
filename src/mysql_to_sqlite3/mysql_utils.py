"""Miscellaneous MySQL utilities."""

import typing as t

from mysql.connector import CharacterSet
from mysql.connector.charsets import MYSQL_CHARACTER_SETS


CHARSET_INTRODUCERS: t.Tuple[str, ...] = tuple(
    f"_{charset[0]}" for charset in MYSQL_CHARACTER_SETS if charset is not None
)


class CharSet(t.NamedTuple):
    """MySQL character set as a named tuple."""

    id: int
    charset: str
    collation: str


def mysql_supported_character_sets(charset: t.Optional[str] = None) -> t.Iterator[CharSet]:
    """Get supported MySQL character sets."""
    index: int
    info: t.Optional[t.Tuple[str, str, bool]]
    if charset is not None:
        for index, info in enumerate(MYSQL_CHARACTER_SETS):
            if info is not None:
                try:
                    if info[0] == charset:
                        yield CharSet(index, charset, info[1])
                except KeyError:
                    continue
    else:
        for charset in CharacterSet().get_supported():
            for index, info in enumerate(MYSQL_CHARACTER_SETS):
                if info is not None:
                    try:
                        yield CharSet(index, charset, info[1])
                    except KeyError:
                        continue
