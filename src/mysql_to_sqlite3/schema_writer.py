"""Schema creation helpers for the transporter."""

from __future__ import annotations

import typing as t

from mysql.connector.types import RowItemType

from mysql_to_sqlite3.sqlite_utils import Integer_Types


# pylint: disable=protected-access  # Helper intentionally uses transporter internals


if t.TYPE_CHECKING:
    from mysql_to_sqlite3.transporter import MySQLtoSQLite


class SchemaWriter:
    """Builds SQLite schemas (tables and views) from MySQL metadata."""

    def __init__(self, ctx: "MySQLtoSQLite") -> None:
        """Hold a reference to the transporter orchestrator."""
        self._ctx = ctx

    def _build_create_table_sql(self, table_name: str) -> str:
        ctx = self._ctx
        table_ident = ctx._quote_sqlite_identifier(table_name)
        sql: str = f"CREATE TABLE IF NOT EXISTS {table_ident} ("
        primary: str = ""
        indices: str = ""

        safe_table = ctx._escape_mysql_backticks(table_name)
        ctx._mysql_cur_dict.execute(f"SHOW COLUMNS FROM `{safe_table}`")
        rows: t.Sequence[t.Optional[t.Dict[str, RowItemType]]] = ctx._mysql_cur_dict.fetchall()

        primary_keys: int = sum(1 for row in rows if row is not None and row["Key"] == "PRI")

        for row in rows:
            if row is None:
                continue
            column_type = ctx._translate_type_from_mysql_to_sqlite(
                column_type=row["Type"],  # type: ignore[arg-type]
                sqlite_json1_extension_enabled=ctx._sqlite_json1_extension_enabled,
            )
            if row["Key"] == "PRI" and row["Extra"] == "auto_increment" and primary_keys == 1:
                if column_type in Integer_Types:
                    sql += "\n\t{name} INTEGER PRIMARY KEY AUTOINCREMENT,".format(
                        name=ctx._quote_sqlite_identifier(
                            str(row["Field"].decode() if isinstance(row["Field"], (bytes, bytearray)) else row["Field"])
                        ),
                    )
                else:
                    ctx._logger.warning(
                        'Primary key "%s" in table "%s" is not an INTEGER type! Skipping.',
                        row["Field"],
                        table_name,
                    )
            else:
                sql += "\n\t{name} {type} {notnull} {default} {collation},".format(
                    name=ctx._quote_sqlite_identifier(
                        str(row["Field"].decode() if isinstance(row["Field"], (bytes, bytearray)) else row["Field"])
                    ),
                    type=column_type,
                    notnull="NULL" if row["Null"] == "YES" else "NOT NULL",
                    default=ctx._translate_default_from_mysql_to_sqlite(row["Default"], column_type, row["Extra"]),
                    collation=ctx._data_type_collation_sequence(ctx._collation, column_type),
                )

        ctx._mysql_cur_dict.execute(
            """
            SELECT s.INDEX_NAME AS `name`,
                IF (NON_UNIQUE = 0 AND s.INDEX_NAME = 'PRIMARY', 1, 0) AS `primary`,
                IF (NON_UNIQUE = 0 AND s.INDEX_NAME <> 'PRIMARY', 1, 0) AS `unique`,
                {auto_increment}
                GROUP_CONCAT(s.COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS `columns`,
                GROUP_CONCAT(c.COLUMN_TYPE ORDER BY SEQ_IN_INDEX) AS `types`
            FROM information_schema.STATISTICS AS s
            JOIN information_schema.COLUMNS AS c
                ON s.TABLE_SCHEMA = c.TABLE_SCHEMA
                AND s.TABLE_NAME = c.TABLE_NAME
                AND s.COLUMN_NAME = c.COLUMN_NAME
            WHERE s.TABLE_SCHEMA = %s
            AND s.TABLE_NAME = %s
            GROUP BY s.INDEX_NAME, s.NON_UNIQUE {group_by_extra}
            """.format(
                auto_increment=(
                    "IF (c.EXTRA = 'auto_increment', 1, 0) AS `auto_increment`,"
                    if primary_keys == 1
                    else "0 as `auto_increment`,"
                ),
                group_by_extra=" ,c.EXTRA" if primary_keys == 1 else "",
            ),
            (ctx._mysql_database, table_name),
        )
        mysql_indices: t.Sequence[t.Optional[t.Dict[str, RowItemType]]] = ctx._mysql_cur_dict.fetchall()
        for index in mysql_indices:
            if index is None:
                continue
            if isinstance(index["name"], bytes):
                index_name = index["name"].decode()
            elif isinstance(index["name"], str):
                index_name = index["name"]
            else:
                index_name = str(index["name"])

            ctx._mysql_cur_dict.execute(
                """
                SELECT COUNT(*) AS `count`
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = %s
                AND TABLE_NAME = %s
                """,
                (ctx._mysql_database, index_name),
            )
            collision: t.Optional[t.Dict[str, RowItemType]] = ctx._mysql_cur_dict.fetchone()
            table_collisions: int = 0
            if collision is not None:
                table_collisions = int(collision["count"])  # type: ignore[arg-type]

            columns: str = (
                index["columns"].decode() if isinstance(index["columns"], (bytes, bytearray)) else str(index["columns"])
            )

            types: str = ""
            if isinstance(index["types"], bytes):
                types = index["types"].decode()
            elif isinstance(index["types"], str):
                types = index["types"]

            if len(columns) > 0:
                if index["primary"] in {1, "1"}:
                    if (index["auto_increment"] not in {1, "1"}) or any(
                        ctx._translate_type_from_mysql_to_sqlite(
                            column_type=_type,
                            sqlite_json1_extension_enabled=ctx._sqlite_json1_extension_enabled,
                        )
                        not in Integer_Types
                        for _type in types.split(",")
                    ):
                        primary += "\n\tPRIMARY KEY ({columns})".format(
                            columns=", ".join(
                                ctx._quote_sqlite_identifier(column.strip()) for column in columns.split(",")
                            )
                        )
                else:
                    proposed_index_name = (
                        f"{table_name}_{index_name}" if (table_collisions > 0 or ctx._prefix_indices) else index_name
                    )
                    if not ctx._prefix_indices:
                        unique_index_name = ctx._get_unique_index_name(proposed_index_name)
                    else:
                        unique_index_name = proposed_index_name
                    unique_kw = "UNIQUE " if index["unique"] in {1, "1"} else ""
                    indices += """CREATE {unique}INDEX IF NOT EXISTS {name} ON {table} ({columns});""".format(
                        unique=unique_kw,
                        name=ctx._quote_sqlite_identifier(unique_index_name),
                        table=ctx._quote_sqlite_identifier(table_name),
                        columns=", ".join(
                            ctx._quote_sqlite_identifier(column.strip()) for column in columns.split(",")
                        ),
                    )

        sql += primary
        sql = sql.rstrip(", ")

        if not ctx._without_tables and not ctx._without_foreign_keys:
            server_version: t.Optional[t.Tuple[int, ...]] = ctx._mysql.get_server_version()
            ctx._mysql_cur_dict.execute(
                """
                SELECT k.COLUMN_NAME AS `column`,
                       k.REFERENCED_TABLE_NAME AS `ref_table`,
                       k.REFERENCED_COLUMN_NAME AS `ref_column`,
                       c.UPDATE_RULE AS `on_update`,
                       c.DELETE_RULE AS `on_delete`
                FROM information_schema.TABLE_CONSTRAINTS AS i
                {JOIN} information_schema.KEY_COLUMN_USAGE AS k
                    ON i.CONSTRAINT_NAME = k.CONSTRAINT_NAME
                    AND i.TABLE_NAME = k.TABLE_NAME
                {JOIN} information_schema.REFERENTIAL_CONSTRAINTS AS c
                    ON c.CONSTRAINT_NAME = i.CONSTRAINT_NAME
                    AND c.TABLE_NAME = i.TABLE_NAME
                WHERE i.TABLE_SCHEMA = %s
                AND i.TABLE_NAME = %s
                AND i.CONSTRAINT_TYPE = %s
                GROUP BY i.CONSTRAINT_NAME,
                         k.COLUMN_NAME,
                         k.REFERENCED_TABLE_NAME,
                         k.REFERENCED_COLUMN_NAME,
                         c.UPDATE_RULE,
                         c.DELETE_RULE
                """.format(
                    JOIN=(
                        "JOIN"
                        if (server_version is not None and server_version[0] == 8 and server_version[2] > 19)
                        else "LEFT JOIN"
                    )
                ),
                (ctx._mysql_database, table_name, "FOREIGN KEY"),
            )
            for foreign_key in ctx._mysql_cur_dict.fetchall():
                if foreign_key is None:
                    continue
                col = ctx._quote_sqlite_identifier(
                    foreign_key["column"].decode()
                    if isinstance(foreign_key["column"], (bytes, bytearray))
                    else str(foreign_key["column"])  # type: ignore[index]
                )
                ref_table = ctx._quote_sqlite_identifier(
                    foreign_key["ref_table"].decode()
                    if isinstance(foreign_key["ref_table"], (bytes, bytearray))
                    else str(foreign_key["ref_table"])  # type: ignore[index]
                )
                ref_col = ctx._quote_sqlite_identifier(
                    foreign_key["ref_column"].decode()
                    if isinstance(foreign_key["ref_column"], (bytes, bytearray))
                    else str(foreign_key["ref_column"])  # type: ignore[index]
                )
                on_update = str(foreign_key["on_update"] or "NO ACTION").upper()  # type: ignore[index]
                on_delete = str(foreign_key["on_delete"] or "NO ACTION").upper()  # type: ignore[index]
                sql += (
                    f",\n\tFOREIGN KEY({col}) REFERENCES {ref_table} ({ref_col}) "
                    f"ON UPDATE {on_update} "
                    f"ON DELETE {on_delete}"
                )

        sql += "\n)"
        if ctx._sqlite_strict:
            sql += " STRICT"
        sql += ";\n"
        sql += indices

        return sql
