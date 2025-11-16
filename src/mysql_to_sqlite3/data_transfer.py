"""Chunked data transfer helpers."""

from __future__ import annotations

import sqlite3
import typing as t
from math import ceil

import mysql.connector
from mysql.connector import errorcode
from tqdm import tqdm, trange

from mysql_to_sqlite3.sqlite_utils import encode_data_for_sqlite


# pylint: disable=protected-access  # Access transporter internals for efficiency


if t.TYPE_CHECKING:
    from mysql_to_sqlite3.transporter import MySQLtoSQLite


class DataTransferManager:
    """Handles moving table data from MySQL to SQLite."""

    def __init__(self, ctx: "MySQLtoSQLite") -> None:
        """Store transporter context for DB access, logging, and chunk options."""
        self._ctx = ctx

    def transfer_table_data(
        self, table_name: str, sql: str, total_records: int = 0, attempting_reconnect: bool = False
    ) -> None:
        """Stream rows from MySQL and batch insert into SQLite, handling reconnects."""
        ctx = self._ctx
        if attempting_reconnect:
            ctx._mysql.reconnect()
        try:
            if ctx._chunk_size is not None and ctx._chunk_size > 0:
                for chunk in trange(
                    ctx._current_chunk_number,
                    int(ceil(total_records / ctx._chunk_size)),
                    disable=ctx._quiet,
                ):
                    ctx._current_chunk_number = chunk
                    ctx._sqlite_cur.executemany(
                        sql,
                        (
                            tuple(encode_data_for_sqlite(col) if col is not None else None for col in row)
                            for row in ctx._mysql_cur.fetchmany(ctx._chunk_size)
                        ),
                    )
            else:
                ctx._sqlite_cur.executemany(
                    sql,
                    (
                        tuple(encode_data_for_sqlite(col) if col is not None else None for col in row)
                        for row in tqdm(
                            ctx._mysql_cur.fetchall(),
                            total=total_records,
                            disable=ctx._quiet,
                        )
                    ),
                )
            ctx._sqlite.commit()
        except mysql.connector.Error as err:
            if err.errno == errorcode.CR_SERVER_LOST:
                if not attempting_reconnect:
                    ctx._logger.warning("Connection to MySQL server lost.\nAttempting to reconnect.")
                    self.transfer_table_data(
                        table_name=table_name,
                        sql=sql,
                        total_records=total_records,
                        attempting_reconnect=True,
                    )
                    return
                ctx._logger.warning("Connection to MySQL server lost.\nReconnection attempt aborted.")
                raise
            ctx._logger.error(
                "MySQL transfer failed reading table data from table %s: %s",
                table_name,
                err,
            )
            raise
        except sqlite3.Error as err:
            ctx._logger.error(
                "SQLite transfer failed inserting data into table %s: %s",
                table_name,
                err,
            )
            raise
