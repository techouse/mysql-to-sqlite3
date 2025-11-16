"""Utilities for translating MySQL schema definitions to SQLite constructs."""

from __future__ import annotations

import logging
import re
import sqlite3
import typing as t

from mysql.connector.types import RowItemType
from sqlglot import Expression, exp, parse_one
from sqlglot.errors import ParseError

from mysql_to_sqlite3.mysql_utils import CHARSET_INTRODUCERS
from mysql_to_sqlite3.sqlite_utils import CollatingSequences


LOGGER = logging.getLogger(__name__)

COLUMN_PATTERN: t.Pattern[str] = re.compile(r"^[^(]+")
COLUMN_LENGTH_PATTERN: t.Pattern[str] = re.compile(r"\(\d+\)$")

ParseCallable = t.Callable[..., Expression]
RegexSearchCallable = t.Callable[..., t.Optional[t.Match[str]]]


def _valid_column_type(column_type: str) -> t.Optional[t.Match[str]]:
    return COLUMN_PATTERN.match(column_type.strip())


def _column_type_length(column_type: str) -> str:
    suffix: t.Optional[t.Match[str]] = COLUMN_LENGTH_PATTERN.search(column_type)
    if suffix:
        return suffix.group(0)
    return ""


def _decode_column_type(column_type: t.Union[str, bytes]) -> str:
    if isinstance(column_type, str):
        return column_type
    if isinstance(column_type, bytes):
        try:
            return column_type.decode()
        except (UnicodeDecodeError, AttributeError):
            pass
    return str(column_type)


def translate_type_from_mysql_to_sqlite(
    column_type: t.Union[str, bytes],
    sqlite_json1_extension_enabled: bool = False,
    *,
    decode_column_type: t.Optional[t.Callable[[t.Union[str, bytes]], str]] = None,
    valid_column_type: t.Optional[t.Callable[[str], t.Optional[t.Match[str]]]] = None,
    transpile_mysql_type_to_sqlite: t.Optional[t.Callable[[str, bool], t.Optional[str]]] = None,
) -> str:
    """Return a SQLite column definition for a MySQL column type."""
    decoder = decode_column_type or _decode_column_type
    validator = valid_column_type or _valid_column_type
    transpiler = transpile_mysql_type_to_sqlite or _transpile_mysql_type_to_sqlite
    _column_type: str = decoder(column_type)

    match: t.Optional[t.Match[str]] = validator(_column_type)
    if not match:
        raise ValueError(f'"{_column_type}" is not a valid column_type!')

    data_type: str = match.group(0).upper()

    if data_type.endswith(" UNSIGNED"):
        data_type = data_type.replace(" UNSIGNED", "")

    if data_type in {
        "BIGINT",
        "BLOB",
        "BOOLEAN",
        "DATE",
        "DATETIME",
        "DECIMAL",
        "DOUBLE",
        "FLOAT",
        "INTEGER",
        "MEDIUMINT",
        "NUMERIC",
        "REAL",
        "SMALLINT",
        "TIME",
        "TINYINT",
        "YEAR",
    }:
        return data_type
    if data_type == "DOUBLE PRECISION":
        return "DOUBLE"
    if data_type == "FIXED":
        return "DECIMAL"
    if data_type in {"CHARACTER VARYING", "CHAR VARYING"}:
        return "VARCHAR" + _column_type_length(_column_type)
    if data_type in {"NATIONAL CHARACTER VARYING", "NATIONAL CHAR VARYING", "NATIONAL VARCHAR"}:
        return "NVARCHAR" + _column_type_length(_column_type)
    if data_type == "NATIONAL CHARACTER":
        return "NCHAR" + _column_type_length(_column_type)
    if data_type in {
        "BIT",
        "BINARY",
        "LONGBLOB",
        "MEDIUMBLOB",
        "TINYBLOB",
        "VARBINARY",
    }:
        return "BLOB"
    if data_type in {"NCHAR", "NVARCHAR", "VARCHAR"}:
        return data_type + _column_type_length(_column_type)
    if data_type == "CHAR":
        return "CHARACTER" + _column_type_length(_column_type)
    if data_type == "INT":
        return "INTEGER"
    if data_type in "TIMESTAMP":
        return "DATETIME"
    if data_type == "JSON" and sqlite_json1_extension_enabled:
        return "JSON"
    sqlglot_type: t.Optional[str] = transpiler(_column_type, sqlite_json1_extension_enabled)
    if sqlglot_type:
        return sqlglot_type
    return "TEXT"


def _transpile_mysql_expr_to_sqlite(expr_sql: str, parse_func: t.Optional[ParseCallable] = None) -> t.Optional[str]:
    """Transpile a MySQL scalar expression to SQLite using sqlglot."""
    cleaned: str = expr_sql.strip().rstrip(";")
    parser = parse_func or parse_one
    try:
        tree: Expression = parser(cleaned, read="mysql")
        return tree.sql(dialect="sqlite")
    except (ParseError, ValueError):
        return None
    except (AttributeError, TypeError):  # pragma: no cover - unexpected sqlglot failure
        LOGGER.debug("sqlglot failed to transpile expr: %r", expr_sql)
        return None


def _normalize_literal_with_sqlglot(expr_sql: str) -> t.Optional[str]:
    """Normalize a MySQL literal using sqlglot, returning SQLite SQL if literal-like."""
    cleaned: str = expr_sql.strip().rstrip(";")
    try:
        node: Expression = parse_one(cleaned, read="mysql")
    except (ParseError, ValueError):
        return None
    if isinstance(node, exp.Literal):
        return node.sql(dialect="sqlite")
    if isinstance(node, exp.Paren) and isinstance(node.this, exp.Literal):
        return node.this.sql(dialect="sqlite")
    return None


def quote_sqlite_identifier(name: t.Union[str, bytes, bytearray]) -> str:
    """Safely quote an identifier for SQLite using sqlglot."""
    if isinstance(name, (bytes, bytearray)):
        try:
            s: str = name.decode()
        except (UnicodeDecodeError, AttributeError):
            s = str(name)
    else:
        s = str(name)
    try:
        normalized: str = exp.to_identifier(s).name
    except (AttributeError, ValueError, TypeError):  # pragma: no cover - extremely unlikely
        normalized = s
    replaced: str = normalized.replace('"', '""')
    return f'"{replaced}"'


def escape_mysql_backticks(identifier: str) -> str:
    """Escape backticks in a MySQL identifier for safe backtick quoting."""
    return identifier.replace("`", "``")


def _transpile_mysql_type_to_sqlite(
    column_type: str,
    sqlite_json1_extension_enabled: bool = False,
    *,
    parse_func: t.Optional[ParseCallable] = None,
    regex_search: t.Optional[RegexSearchCallable] = None,
) -> t.Optional[str]:
    """Attempt to derive a suitable SQLite column type using sqlglot."""
    expr_sql: str = f"CAST(NULL AS {column_type.strip()})"
    parser = parse_func or parse_one
    search = regex_search or re.search
    try:
        tree: Expression = parser(expr_sql, read="mysql")
        rendered: str = tree.sql(dialect="sqlite")
    except (ParseError, ValueError, AttributeError, TypeError):
        return None

    m: t.Optional[t.Match[str]] = search(r"CAST\(NULL AS\s+([^)]+)\)", rendered, re.IGNORECASE)
    if not m:
        return None
    extracted: str = m.group(1).strip()
    upper: str = extracted.upper()

    if "JSON" in upper:
        return "JSON" if sqlite_json1_extension_enabled else "TEXT"

    base: str = upper
    length_suffix: str = ""
    paren: t.Optional[t.Match[str]] = re.match(r"^([A-Z ]+)(\(.*\))$", upper)
    if paren:
        base = paren.group(1).strip()
        length_suffix = paren.group(2)

    synonyms: t.Dict[str, str] = {
        "DOUBLE PRECISION": "DOUBLE",
        "FIXED": "DECIMAL",
        "CHAR VARYING": "VARCHAR",
        "CHARACTER VARYING": "VARCHAR",
        "NATIONAL VARCHAR": "NVARCHAR",
        "NATIONAL CHARACTER VARYING": "NVARCHAR",
        "NATIONAL CHAR VARYING": "NVARCHAR",
        "NATIONAL CHARACTER": "NCHAR",
    }
    base = synonyms.get(base, base)

    if base in {"NCHAR", "NVARCHAR", "VARCHAR"} and length_suffix:
        return f"{base}{length_suffix}"
    if base in {"CHAR", "CHARACTER"}:
        return f"CHARACTER{length_suffix}"
    if base in {"DECIMAL", "NUMERIC"}:
        return base
    if base in {
        "DOUBLE",
        "REAL",
        "FLOAT",
        "INTEGER",
        "BIGINT",
        "SMALLINT",
        "MEDIUMINT",
        "TINYINT",
        "BLOB",
        "DATE",
        "DATETIME",
        "TIME",
        "YEAR",
        "BOOLEAN",
    }:
        return base
    if base in {"VARBINARY", "BINARY", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB"}:
        return "BLOB"
    if base in {"TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT", "CLOB"}:
        return "TEXT"
    if base in {"ENUM", "SET"}:
        return "TEXT"
    return None


def translate_default_from_mysql_to_sqlite(
    column_default: RowItemType = None,
    column_type: t.Optional[str] = None,
    column_extra: RowItemType = None,
    *,
    normalize_literal: t.Optional[t.Callable[[str], t.Optional[str]]] = None,
    transpile_expr: t.Optional[t.Callable[[str], t.Optional[str]]] = None,
) -> str:
    """Render a DEFAULT clause suitable for SQLite."""
    normalizer = normalize_literal or _normalize_literal_with_sqlglot
    expr_transpiler = transpile_expr or _transpile_mysql_expr_to_sqlite
    is_binary: bool
    is_hex: bool
    if isinstance(column_default, bytes):
        if column_type in {
            "BIT",
            "BINARY",
            "BLOB",
            "LONGBLOB",
            "MEDIUMBLOB",
            "TINYBLOB",
            "VARBINARY",
        }:
            if column_extra in {"DEFAULT_GENERATED", "default_generated"}:
                for charset_introducer in CHARSET_INTRODUCERS:
                    if column_default.startswith(charset_introducer.encode()):
                        is_binary = False
                        is_hex = False
                        for b_prefix in ("B", "b"):
                            if column_default.startswith(rf"{charset_introducer} {b_prefix}\'".encode()):
                                is_binary = True
                                break
                        for x_prefix in ("X", "x"):
                            if column_default.startswith(rf"{charset_introducer} {x_prefix}\'".encode()):
                                is_hex = True
                                break
                        column_default = (
                            column_default.replace(charset_introducer.encode(), b"")
                            .replace(rb"x\'", b"")
                            .replace(rb"X\'", b"")
                            .replace(rb"b\'", b"")
                            .replace(rb"B\'", b"")
                            .replace(rb"\'", b"")
                            .replace(rb"'", b"")
                            .strip()
                        )
                        if is_binary:
                            return f"DEFAULT '{chr(int(column_default, 2))}'"
                        if is_hex:
                            return f"DEFAULT x'{column_default.decode()}'"
                        break
            return f"DEFAULT x'{column_default.hex()}'"
        try:
            column_default = column_default.decode()
        except (UnicodeDecodeError, AttributeError):
            pass
    if column_default is None:
        return ""
    if isinstance(column_default, bool):
        if column_type == "BOOLEAN" and sqlite3.sqlite_version >= "3.23.0":
            if column_default:
                return "DEFAULT(TRUE)"
            return "DEFAULT(FALSE)"
        return f"DEFAULT '{int(column_default)}'"
    if isinstance(column_default, str):
        if column_default.lower() == "curtime()":
            return "DEFAULT CURRENT_TIME"
        if column_default.lower() == "curdate()":
            return "DEFAULT CURRENT_DATE"
        if column_default.lower() in {"current_timestamp()", "now()"}:
            return "DEFAULT CURRENT_TIMESTAMP"
        if column_extra in {"DEFAULT_GENERATED", "default_generated"}:
            if column_default.upper() in {
                "CURRENT_TIME",
                "CURRENT_DATE",
                "CURRENT_TIMESTAMP",
            }:
                return f"DEFAULT {column_default.upper()}"
            for charset_introducer in CHARSET_INTRODUCERS:
                if column_default.startswith(charset_introducer):
                    is_binary = False
                    is_hex = False
                    for b_prefix in ("B", "b"):
                        if column_default.startswith(rf"{charset_introducer} {b_prefix}\'"):
                            is_binary = True
                            break
                    for x_prefix in ("X", "x"):
                        if column_default.startswith(rf"{charset_introducer} {x_prefix}\'"):
                            is_hex = True
                            break
                    column_default = (
                        column_default.replace(charset_introducer, "")
                        .replace(r"x\'", "")
                        .replace(r"X\'", "")
                        .replace(r"b\'", "")
                        .replace(r"B\'", "")
                        .replace(r"\'", "")
                        .replace(r"'", "")
                        .strip()
                    )
                    if is_binary:
                        return f"DEFAULT '{chr(int(column_default, 2))}'"
                    if is_hex:
                        return f"DEFAULT x'{column_default}'"
                    return f"DEFAULT '{column_default}'"
            transpiled: t.Optional[str] = expr_transpiler(column_default)
            if transpiled:
                norm: str = transpiled.strip().rstrip(";")
                upper: str = norm.upper()
                if upper in {"CURRENT_TIME", "CURRENT_DATE", "CURRENT_TIMESTAMP"}:
                    return f"DEFAULT {upper}"
                if upper == "NULL":
                    return "DEFAULT NULL"
                if re.match(r"^[Xx]'[0-9A-Fa-f]+'$", norm):
                    return f"DEFAULT {norm}"
                if upper in {"TRUE", "FALSE"}:
                    if column_type == "BOOLEAN" and sqlite3.sqlite_version >= "3.23.0":
                        return f"DEFAULT({upper})"
                    return f"DEFAULT '{1 if upper == 'TRUE' else 0}'"
                if norm.startswith("(") and norm.endswith(")"):
                    inner = norm[1:-1].strip()
                    if (inner.startswith("'") and inner.endswith("'")) or re.match(r"^-?\d+(?:\.\d+)?$", inner):
                        return f"DEFAULT {inner}"
                    if re.match(r"^[\d\.\s\+\-\*/\(\)]+$", norm) and any(ch.isdigit() for ch in norm):
                        return f"DEFAULT {norm}"
                if (norm.startswith("'") and norm.endswith("'")) or re.match(r"^-?\d+(?:\.\d+)?$", norm):
                    return f"DEFAULT {norm}"
                if re.match(r"^[\d\.\s\+\-\*/\(\)]+$", norm) and any(ch.isdigit() for ch in norm):
                    return f"DEFAULT {norm}"
        stripped_default = column_default.strip()
        if stripped_default.startswith("'") or (stripped_default.startswith("(") and stripped_default.endswith(")")):
            normalized_literal: t.Optional[str] = normalizer(column_default)
            if normalized_literal is not None:
                return f"DEFAULT {normalized_literal}"

        _escaped = column_default.replace("\\'", "'")
        _escaped = _escaped.replace("'", "''")
        return f"DEFAULT '{_escaped}'"
    s = str(column_default)
    s = s.replace("\\'", "'")
    s = s.replace("'", "''")
    return f"DEFAULT '{s}'"


def data_type_collation_sequence(
    collation: str = CollatingSequences.BINARY,
    column_type: t.Optional[str] = None,
    *,
    transpile_mysql_type_to_sqlite: t.Optional[t.Callable[[str, bool], t.Optional[str]]] = None,
) -> str:
    """Return a SQLite COLLATE clause for textual affinity types."""
    if not column_type or collation == CollatingSequences.BINARY:
        return ""

    ct: str = column_type.strip()
    upper: str = ct.upper()

    if upper.startswith(("CHARACTER", "NCHAR", "NVARCHAR", "TEXT", "VARCHAR")):
        return f"COLLATE {collation}"

    if "JSON" in upper or "BLOB" in upper:
        return ""

    if any(tok in upper for tok in ("VARCHAR", "NVARCHAR", "NCHAR", "CHAR", "TEXT", "CLOB", "CHARACTER")):
        return f"COLLATE {collation}"

    transpiler = transpile_mysql_type_to_sqlite or _transpile_mysql_type_to_sqlite
    mapped: t.Optional[str] = transpiler(ct, False)
    if mapped:
        mu = mapped.upper()
        if (
            "CHAR" in mu or "VARCHAR" in mu or "NCHAR" in mu or "NVARCHAR" in mu or "TEXT" in mu or "CLOB" in mu
        ) and not ("JSON" in mu or "BLOB" in mu):
            return f"COLLATE {collation}"

    return ""


__all__ = [
    "data_type_collation_sequence",
    "escape_mysql_backticks",
    "quote_sqlite_identifier",
    "translate_default_from_mysql_to_sqlite",
    "translate_type_from_mysql_to_sqlite",
]
