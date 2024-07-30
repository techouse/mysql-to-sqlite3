import logging
import os
import re
import typing as t
from collections import namedtuple
from decimal import Decimal
from pathlib import Path
from random import choice, sample

import mysql.connector
import pytest
import simplejson as json
from _pytest._py.path import LocalPath
from _pytest.logging import LogCaptureFixture
from faker import Faker
from mysql.connector import MySQLConnection, errorcode
from mysql.connector.connection_cext import CMySQLConnection
from mysql.connector.cursor import MySQLCursor
from mysql.connector.pooling import PooledMySQLConnection
from pytest_mock import MockFixture
from sqlalchemy import (
    Connection,
    CursorResult,
    Engine,
    Inspector,
    MetaData,
    Row,
    Select,
    Table,
    TextClause,
    create_engine,
    inspect,
    select,
    text,
)
from sqlalchemy.engine.interfaces import ReflectedIndex

from mysql_to_sqlite3 import MySQLtoSQLite
from tests.conftest import Helpers, MySQLCredentials
from tests.database import Database


@pytest.mark.usefixtures("mysql_instance")
class TestMySQLtoSQLite:
    @pytest.mark.init
    @pytest.mark.parametrize(
        "quiet",
        [
            pytest.param(False, id="verbose"),
            pytest.param(True, id="quiet"),
        ],
    )
    def test_missing_mysql_user_raises_exception(self, mysql_credentials: MySQLCredentials, quiet: bool) -> None:
        with pytest.raises(ValueError) as excinfo:
            MySQLtoSQLite(mysql_database=mysql_credentials.database, quiet=quiet)  # type: ignore[call-arg]
        assert "Please provide a MySQL user" in str(excinfo.value)

    @pytest.mark.init
    @pytest.mark.parametrize(
        "quiet",
        [
            pytest.param(False, id="verbose"),
            pytest.param(True, id="quiet"),
        ],
    )
    def test_missing_mysql_database_raises_exception(self, faker: Faker, quiet: bool) -> None:
        with pytest.raises(ValueError) as excinfo:
            MySQLtoSQLite(mysql_user=faker.first_name().lower(), quiet=quiet)  # type: ignore[call-arg]
        assert "Please provide a MySQL database" in str(excinfo.value)

    @pytest.mark.init
    @pytest.mark.xfail
    @pytest.mark.parametrize(
        "quiet",
        [
            pytest.param(False, id="verbose"),
            pytest.param(True, id="quiet"),
        ],
    )
    def test_invalid_mysql_credentials_raises_access_denied_exception(
        self,
        sqlite_database: "os.PathLike[t.Any]",
        mysql_database: Database,
        mysql_credentials: MySQLCredentials,
        faker: Faker,
        quiet: bool,
    ) -> None:
        with pytest.raises(mysql.connector.Error) as excinfo:
            MySQLtoSQLite(  # type: ignore[call-arg]
                sqlite_file=sqlite_database,
                mysql_user=faker.first_name().lower(),
                mysql_password=faker.password(length=16),
                mysql_database=mysql_credentials.database,
                mysql_host=mysql_credentials.host,
                mysql_port=mysql_credentials.port,
                quiet=quiet,
            )
        assert "Access denied for user" in str(excinfo.value)

    @pytest.mark.init
    @pytest.mark.parametrize(
        "quiet",
        [
            pytest.param(False, id="verbose"),
            pytest.param(True, id="quiet"),
        ],
    )
    def test_bad_mysql_connection(
        self,
        sqlite_database: "os.PathLike[t.Any]",
        mysql_credentials: MySQLCredentials,
        mocker: MockFixture,
        quiet: bool,
    ) -> None:
        FakeConnector = namedtuple("FakeConnector", ["is_connected"])
        mocker.patch.object(
            mysql.connector,
            "connect",
            return_value=FakeConnector(is_connected=lambda: False),
        )
        with pytest.raises((ConnectionError, IOError)) as excinfo:
            MySQLtoSQLite(  # type: ignore[call-arg]
                sqlite_file=sqlite_database,
                mysql_user=mysql_credentials.user,
                mysql_password=mysql_credentials.password,
                mysql_host=mysql_credentials.host,
                mysql_port=mysql_credentials.port,
                mysql_database=mysql_credentials.database,
                chunk=1000,
                quiet=quiet,
            )
        assert "Unable to connect to MySQL" in str(excinfo.value)

    @pytest.mark.init
    @pytest.mark.parametrize(
        "exception, quiet",
        [
            pytest.param(
                mysql.connector.Error(msg="Unknown database 'test_db'", errno=errorcode.ER_BAD_DB_ERROR),
                False,
                id="mysql.connector.Error verbose",
            ),
            pytest.param(
                mysql.connector.Error(msg="Unknown database 'test_db'", errno=errorcode.ER_BAD_DB_ERROR),
                True,
                id="mysql.connector.Error quiet",
            ),
            pytest.param(Exception("Unknown database 'test_db'"), False, id="Exception verbose"),
            pytest.param(Exception("Unknown database 'test_db'"), True, id="Exception quiet"),
        ],
    )
    def test_non_existing_mysql_database_raises_exception(
        self,
        sqlite_database: "os.PathLike[t.Any]",
        mysql_database: Database,
        mysql_credentials: MySQLCredentials,
        faker: Faker,
        mocker: MockFixture,
        caplog: LogCaptureFixture,
        exception: Exception,
        quiet: bool,
    ) -> None:
        class FakeMySQLConnection(MySQLConnection):
            @property
            def database(self) -> str:
                return self._database

            @database.setter
            def database(self, value) -> None:
                self._database = value
                # raise a fake exception
                raise exception

            def is_connected(self) -> bool:
                return True

            def cursor(
                self,
                buffered: t.Optional[bool] = None,
                raw: t.Optional[bool] = None,
                prepared: t.Optional[bool] = None,
                cursor_class: t.Optional[t.Type[MySQLCursor]] = None,
                dictionary: t.Optional[bool] = None,
                named_tuple: t.Optional[bool] = None,
            ) -> t.Union[t.Any, MySQLCursor]:
                return True

        caplog.set_level(logging.DEBUG)
        mocker.patch.object(mysql.connector, "connect", return_value=FakeMySQLConnection())
        with pytest.raises((mysql.connector.Error, Exception)) as excinfo:
            MySQLtoSQLite(  # type: ignore[call-arg]
                sqlite_file=sqlite_database,
                mysql_user=mysql_credentials.user,
                mysql_password=mysql_credentials.password,
                mysql_database=mysql_credentials.database,
                mysql_host=mysql_credentials.host,
                mysql_port=mysql_credentials.port,
                quiet=quiet,
            )
            assert any("MySQL Database does not exist!" in message for message in caplog.messages)
        assert "Unknown database" in str(excinfo.value)

    @pytest.mark.init
    def test_without_tables_and_without_data(
        self,
        sqlite_database: "os.PathLike[t.Any]",
        mysql_database: Database,
        mysql_credentials: MySQLCredentials,
        caplog: LogCaptureFixture,
        tmpdir: LocalPath,
        faker: Faker,
    ) -> None:
        with pytest.raises(ValueError) as excinfo:
            MySQLtoSQLite(  # type: ignore[call-arg]
                sqlite_file=sqlite_database,
                mysql_user=mysql_credentials.user,
                mysql_password=mysql_credentials.password,
                mysql_database=mysql_credentials.database,
                mysql_host=mysql_credentials.host,
                mysql_port=mysql_credentials.port,
                without_tables=True,
                without_data=True,
            )
        assert "Unable to continue without transferring data or creating tables!" in str(excinfo.value)

    @pytest.mark.xfail
    @pytest.mark.init
    @pytest.mark.parametrize(
        "quiet",
        [
            pytest.param(False, id="verbose"),
            pytest.param(True, id="quiet"),
        ],
    )
    def test_log_to_file(
        self,
        sqlite_database: "os.PathLike[t.Any]",
        mysql_database: Database,
        mysql_credentials: MySQLCredentials,
        caplog: LogCaptureFixture,
        tmpdir: LocalPath,
        faker: Faker,
        quiet: bool,
    ) -> None:
        log_file: LocalPath = tmpdir.join(Path("db.log"))
        caplog.set_level(logging.DEBUG)
        with pytest.raises(mysql.connector.Error):
            MySQLtoSQLite(  # type: ignore[call-arg]
                sqlite_file=sqlite_database,
                mysql_user=faker.first_name().lower(),
                mysql_password=faker.password(length=16),
                mysql_database=mysql_credentials.database,
                mysql_host=mysql_credentials.host,
                mysql_port=mysql_credentials.port,
                log_file=str(log_file),
                quiet=quiet,
            )
        assert any("Access denied for user" in message for message in caplog.messages)
        with log_file.open("r") as log_fh:
            log = log_fh.read()
            if len(caplog.messages) > 1:
                assert caplog.messages[1] in log
            else:
                assert caplog.messages[0] in log
            assert re.match(r"^\d{4,}-\d{2,}-\d{2,}\s+\d{2,}:\d{2,}:\d{2,}\s+\w+\s+", log) is not None

    @pytest.mark.transfer
    @pytest.mark.parametrize(
        "chunk, vacuum, buffered, prefix_indices",
        [
            # 0000
            pytest.param(
                None,
                False,
                False,
                False,
                id="no chunk, no vacuum, no buffered cursor, no prefix indices",
            ),
            # 0001
            pytest.param(
                None,
                False,
                False,
                True,
                id="no chunk, no vacuum, no buffered cursor, prefix indices",
            ),
            # 1110
            pytest.param(
                10,
                True,
                True,
                False,
                id="chunk, vacuum, buffered cursor, no prefix indices",
            ),
            # 1111
            pytest.param(
                10,
                True,
                True,
                True,
                id="chunk, vacuum, buffered cursor, prefix indices",
            ),
            # 1100
            pytest.param(
                10,
                True,
                False,
                False,
                id="chunk, vacuum, no buffered cursor, no prefix indices",
            ),
            # 1101
            pytest.param(
                10,
                True,
                False,
                True,
                id="chunk, vacuum, no buffered cursor, prefix indices",
            ),
            # 0110
            pytest.param(
                None,
                True,
                True,
                False,
                id="no chunk, vacuum, buffered cursor, no prefix indices",
            ),
            # 0111
            pytest.param(
                None,
                True,
                True,
                True,
                id="no chunk, vacuum, buffered cursor, prefix indices",
            ),
            # 0100
            pytest.param(
                None,
                True,
                False,
                False,
                id="no chunk, vacuum, no buffered cursor, no prefix indices",
            ),
            # 0101
            pytest.param(
                None,
                True,
                False,
                True,
                id="no chunk, vacuum, no buffered cursor, prefix indices",
            ),
            # 1000
            pytest.param(
                10,
                False,
                False,
                False,
                id="chunk, no vacuum, no buffered cursor, no prefix indices",
            ),
            # 1001
            pytest.param(
                10,
                False,
                False,
                True,
                id="chunk, no vacuum, no buffered cursor, prefix indices",
            ),
            # 0010
            pytest.param(
                None,
                False,
                True,
                False,
                id="no chunk, no vacuum, buffered cursor, no prefix indices",
            ),
            # 0011
            pytest.param(
                None,
                False,
                True,
                True,
                id="no chunk, no vacuum, buffered cursor, prefix indices",
            ),
            # 1010
            pytest.param(
                10,
                False,
                True,
                False,
                id="chunk, no vacuum, buffered cursor, no prefix indices",
            ),
            # 1011
            pytest.param(
                10,
                False,
                True,
                True,
                id="chunk, no vacuum, buffered cursor, prefix indices",
            ),
        ],
    )
    def test_transfer_transfers_all_tables_from_mysql_to_sqlite(
        self,
        sqlite_database: "os.PathLike[t.Any]",
        mysql_database: Database,
        mysql_credentials: MySQLCredentials,
        helpers: Helpers,
        caplog: LogCaptureFixture,
        chunk: t.Optional[int],
        vacuum: bool,
        buffered: bool,
        prefix_indices: bool,
    ) -> None:
        proc: MySQLtoSQLite = MySQLtoSQLite(  # type: ignore[call-arg]
            sqlite_file=sqlite_database,
            mysql_user=mysql_credentials.user,
            mysql_password=mysql_credentials.password,
            mysql_database=mysql_credentials.database,
            mysql_host=mysql_credentials.host,
            mysql_port=mysql_credentials.port,
            chunk=chunk,
            vacuum=vacuum,
            buffered=buffered,
            prefix_indices=prefix_indices,
        )
        caplog.set_level(logging.DEBUG)
        proc.transfer()
        assert all(
            message in [record.message for record in caplog.records]
            for message in {
                "Transferring table article_authors",
                "Transferring table article_images",
                "Transferring table article_tags",
                "Transferring table articles",
                "Transferring table authors",
                "Transferring table images",
                "Transferring table tags",
                "Transferring table misc",
                "Done!",
            }
        )
        assert all(record.levelname == "INFO" for record in caplog.records)
        assert not any(record.levelname == "ERROR" for record in caplog.records)

        sqlite_engine: Engine = create_engine(
            f"sqlite:///{sqlite_database}",
            json_serializer=json.dumps,
            json_deserializer=json.loads,
        )

        sqlite_cnx: Connection = sqlite_engine.connect()
        sqlite_inspect: Inspector = inspect(sqlite_engine)
        sqlite_tables: t.List[str] = sqlite_inspect.get_table_names()
        mysql_engine: Engine = create_engine(
            f"mysql+mysqldb://{mysql_credentials.user}:{mysql_credentials.password}@{mysql_credentials.host}:{mysql_credentials.port}/{mysql_credentials.database}"
        )
        mysql_cnx: Connection = mysql_engine.connect()
        mysql_inspect: Inspector = inspect(mysql_engine)
        mysql_tables: t.List[str] = mysql_inspect.get_table_names()

        mysql_connector_connection: t.Union[PooledMySQLConnection, MySQLConnection, CMySQLConnection] = (
            mysql.connector.connect(
                user=mysql_credentials.user,
                password=mysql_credentials.password,
                host=mysql_credentials.host,
                port=mysql_credentials.port,
                database=mysql_credentials.database,
                charset="utf8mb4",
                collation="utf8mb4_unicode_ci",
            )
        )
        server_version: t.Tuple[int, ...] = mysql_connector_connection.get_server_version()

        """ Test if both databases have the same table names """
        assert sqlite_tables == mysql_tables

        """ Test if all the tables have the same column names """
        for table_name in sqlite_tables:
            assert [column["name"] for column in sqlite_inspect.get_columns(table_name)] == [
                column["name"] for column in mysql_inspect.get_columns(table_name)
            ]

        """ Test if all the tables have the same indices """
        index_keys: t.Tuple[str, ...] = ("name", "column_names", "unique")
        mysql_indices: t.List[ReflectedIndex] = []
        for table_name in mysql_tables:
            for index in mysql_inspect.get_indexes(table_name):
                mysql_index: t.Dict[str, t.Any] = {}
                for key in index_keys:
                    if key == "name" and prefix_indices:
                        mysql_index[key] = f"{table_name}_{index[key]}"  # type: ignore[literal-required]
                    else:
                        mysql_index[key] = index[key]  # type: ignore[literal-required]
                mysql_indices.append(t.cast(ReflectedIndex, mysql_index))

        for table_name in sqlite_tables:
            for sqlite_index in sqlite_inspect.get_indexes(table_name):
                sqlite_index["unique"] = bool(sqlite_index["unique"])
                if "dialect_options" in sqlite_index:
                    sqlite_index.pop("dialect_options", None)
                assert sqlite_index in mysql_indices

        """ Test if all the tables have the same foreign keys """
        for table_name in mysql_tables:
            mysql_fk_stmt: TextClause = text(
                """
                SELECT k.COLUMN_NAME AS `from`,
                       k.REFERENCED_TABLE_NAME AS `table`,
                       k.REFERENCED_COLUMN_NAME AS `to`,
                       c.UPDATE_RULE AS `on_update`,
                       c.DELETE_RULE AS `on_delete`
                FROM information_schema.TABLE_CONSTRAINTS AS i
                {JOIN} information_schema.KEY_COLUMN_USAGE AS k ON i.CONSTRAINT_NAME = k.CONSTRAINT_NAME
                {JOIN} information_schema.REFERENTIAL_CONSTRAINTS c ON c.CONSTRAINT_NAME = i.CONSTRAINT_NAME
                WHERE i.TABLE_SCHEMA = :table_schema
                AND i.TABLE_NAME = :table_name
                AND i.CONSTRAINT_TYPE = :constraint_type
            """.format(
                    # MySQL 8.0.19 still works with "LEFT JOIN" everything above requires "JOIN"
                    JOIN="JOIN" if (server_version[0] == 8 and server_version[2] > 19) else "LEFT JOIN"
                )
            ).bindparams(
                table_schema=mysql_credentials.database,
                table_name=table_name,
                constraint_type="FOREIGN KEY",
            )
            mysql_fk_result: CursorResult = mysql_cnx.execute(mysql_fk_stmt)
            mysql_foreign_keys: t.List[t.Dict[str, t.Any]] = [dict(row) for row in mysql_fk_result.mappings()]

            sqlite_fk_stmt: TextClause = text(f'PRAGMA foreign_key_list("{table_name}")')
            sqlite_fk_result: CursorResult = sqlite_cnx.execute(sqlite_fk_stmt)
            if sqlite_fk_result.returns_rows:
                for row in sqlite_fk_result.mappings():
                    fk: t.Dict[str, t.Any] = dict(row)
                    assert {
                        "table": fk["table"],
                        "from": fk["from"],
                        "to": fk["to"],
                        "on_update": fk["on_update"],
                        "on_delete": fk["on_delete"],
                    } in mysql_foreign_keys

        """ Check if all the data was transferred correctly """
        sqlite_results: t.List[t.Tuple[t.Tuple[t.Any, ...], ...]] = []
        mysql_results: t.List[t.Tuple[t.Tuple[t.Any, ...], ...]] = []

        meta: MetaData = MetaData()
        for table_name in sqlite_tables:
            sqlite_table: Table = Table(table_name, meta, autoload_with=sqlite_engine)
            sqlite_stmt: Select = select(sqlite_table)
            sqlite_result: t.List[Row[t.Any]] = list(sqlite_cnx.execute(sqlite_stmt).fetchall())
            sqlite_result.sort()
            sqlite_result_adapted: t.Tuple[t.Tuple[t.Any, ...], ...] = tuple(
                tuple(float(data) if isinstance(data, Decimal) else data for data in row) for row in sqlite_result
            )
            sqlite_results.append(sqlite_result_adapted)

        for table_name in mysql_tables:
            mysql_table: Table = Table(table_name, meta, autoload_with=mysql_engine)
            mysql_stmt: Select = select(mysql_table)
            mysql_result: t.List[Row[t.Any]] = list(mysql_cnx.execute(mysql_stmt).fetchall())
            mysql_result.sort()
            mysql_result_adapted: t.Tuple[t.Tuple[t.Any, ...], ...] = tuple(
                tuple(float(data) if isinstance(data, Decimal) else data for data in row) for row in mysql_result
            )
            mysql_results.append(mysql_result_adapted)

        assert sqlite_results == mysql_results

        mysql_cnx.close()
        sqlite_cnx.close()
        mysql_engine.dispose()
        sqlite_engine.dispose()

    @pytest.mark.transfer
    def test_specific_tables_include_and_exclude_are_mutually_exclusive_options(
        self,
        sqlite_database: "os.PathLike[t.Any]",
        mysql_credentials: MySQLCredentials,
        caplog: LogCaptureFixture,
        faker: Faker,
    ) -> None:
        with pytest.raises(ValueError) as excinfo:
            MySQLtoSQLite(  # type: ignore[call-arg]
                sqlite_file=sqlite_database,
                mysql_user=mysql_credentials.user,
                mysql_password=mysql_credentials.password,
                mysql_database=mysql_credentials.database,
                mysql_tables=faker.words(nb=3),
                exclude_mysql_tables=faker.words(nb=3),
                mysql_host=mysql_credentials.host,
                mysql_port=mysql_credentials.port,
            )
        assert "mysql_tables and exclude_mysql_tables are mutually exclusive" in str(excinfo.value)

    @pytest.mark.transfer
    @pytest.mark.parametrize(
        "chunk, vacuum, buffered, prefix_indices, exclude_tables",
        [
            # 00000
            pytest.param(
                None,
                False,
                False,
                False,
                False,
                id="no chunk, no vacuum, no buffered cursor, no prefix indices, include tables",
            ),
            # 00001
            pytest.param(
                None,
                False,
                False,
                False,
                True,
                id="no chunk, no vacuum, no buffered cursor, no prefix indices, exclude tables",
            ),
            # 00010
            pytest.param(
                None,
                False,
                False,
                True,
                False,
                id="no chunk, no vacuum, no buffered cursor, prefix indices, include tables",
            ),
            # 00011
            pytest.param(
                None,
                False,
                False,
                True,
                True,
                id="no chunk, no vacuum, no buffered cursor, prefix indices, exclude tables",
            ),
            # 11100
            pytest.param(
                10,
                True,
                True,
                False,
                False,
                id="chunk, vacuum, buffered cursor, no prefix indices, include tables",
            ),
            # 11101
            pytest.param(
                10,
                True,
                True,
                False,
                True,
                id="chunk, vacuum, buffered cursor, no prefix indices, exclude tables",
            ),
            # 11110
            pytest.param(
                10,
                True,
                True,
                True,
                False,
                id="chunk, vacuum, buffered cursor, prefix indices, include tables",
            ),
            # 11111
            pytest.param(
                10,
                True,
                True,
                True,
                True,
                id="chunk, vacuum, buffered cursor, prefix indices, exclude tables",
            ),
            # 11000
            pytest.param(
                10,
                True,
                False,
                False,
                False,
                id="chunk, vacuum, no buffered cursor, no prefix indices, include tables",
            ),
            # 11001
            pytest.param(
                10,
                True,
                False,
                False,
                True,
                id="chunk, vacuum, no buffered cursor, no prefix indices, exclude tables",
            ),
            # 11010
            pytest.param(
                10,
                True,
                False,
                True,
                False,
                id="chunk, vacuum, no buffered cursor, prefix indices, include tables",
            ),
            # 11011
            pytest.param(
                10,
                True,
                False,
                True,
                True,
                id="chunk, vacuum, no buffered cursor, prefix indices, exclude tables",
            ),
            # 01100
            pytest.param(
                None,
                True,
                True,
                False,
                False,
                id="no chunk, vacuum, buffered cursor, no prefix indices, include tables",
            ),
            # 01101
            pytest.param(
                None,
                True,
                True,
                False,
                True,
                id="no chunk, vacuum, buffered cursor, no prefix indices, exclude tables",
            ),
            # 01110
            pytest.param(
                None,
                True,
                True,
                True,
                False,
                id="no chunk, vacuum, buffered cursor, prefix indices, include tables",
            ),
            # 01111
            pytest.param(
                None,
                True,
                True,
                True,
                True,
                id="no chunk, vacuum, buffered cursor, prefix indices, exclude tables",
            ),
            # 01000
            pytest.param(
                None,
                True,
                False,
                False,
                False,
                id="no chunk, vacuum, no buffered cursor, no prefix indices, include tables",
            ),
            # 01001
            pytest.param(
                None,
                True,
                False,
                False,
                True,
                id="no chunk, vacuum, no buffered cursor, no prefix indices, exclude tables",
            ),
            # 01010
            pytest.param(
                None,
                True,
                False,
                True,
                False,
                id="no chunk, vacuum, no buffered cursor, prefix indices, include tables",
            ),
            # 01011
            pytest.param(
                None,
                True,
                False,
                True,
                True,
                id="no chunk, vacuum, no buffered cursor, prefix indices, exclude tables",
            ),
            # 10000
            pytest.param(
                10,
                False,
                False,
                False,
                False,
                id="chunk, no vacuum, no buffered cursor, no prefix indices, include tables",
            ),
            # 10001
            pytest.param(
                10,
                False,
                False,
                False,
                True,
                id="chunk, no vacuum, no buffered cursor, no prefix indices, exclude tables",
            ),
            # 10010
            pytest.param(
                10,
                False,
                False,
                True,
                False,
                id="chunk, no vacuum, no buffered cursor, prefix indices, include tables",
            ),
            # 10011
            pytest.param(
                10,
                False,
                False,
                True,
                True,
                id="chunk, no vacuum, no buffered cursor, prefix indices, exclude tables",
            ),
            # 00100
            pytest.param(
                None,
                False,
                True,
                False,
                False,
                id="no chunk, no vacuum, buffered cursor, no prefix indices, include tables",
            ),
            # 00101
            pytest.param(
                None,
                False,
                True,
                False,
                True,
                id="no chunk, no vacuum, buffered cursor, no prefix indices, exclude tables",
            ),
            # 00110
            pytest.param(
                None,
                False,
                True,
                True,
                False,
                id="no chunk, no vacuum, buffered cursor, prefix indices, include tables",
            ),
            # 00111
            pytest.param(
                None,
                False,
                True,
                True,
                True,
                id="no chunk, no vacuum, buffered cursor, prefix indices, exclude tables",
            ),
            # 10100
            pytest.param(
                10,
                False,
                True,
                False,
                False,
                id="chunk, no vacuum, buffered cursor, no prefix indices, include tables",
            ),
            # 10101
            pytest.param(
                10,
                False,
                True,
                False,
                True,
                id="chunk, no vacuum, buffered cursor, no prefix indices, exclude tables",
            ),
            # 10110
            pytest.param(
                10,
                False,
                True,
                True,
                False,
                id="chunk, no vacuum, buffered cursor, prefix indices, include tables",
            ),
            # 10111
            pytest.param(
                10,
                False,
                True,
                True,
                True,
                id="chunk, no vacuum, buffered cursor, prefix indices, exclude tables",
            ),
        ],
    )
    def test_transfer_specific_tables_transfers_only_specified_tables_from_mysql_to_sqlite(
        self,
        sqlite_database: "os.PathLike[t.Any]",
        mysql_database: Database,
        mysql_credentials: MySQLCredentials,
        helpers: Helpers,
        caplog: LogCaptureFixture,
        chunk: t.Optional[int],
        vacuum: bool,
        buffered: bool,
        prefix_indices: bool,
        exclude_tables: bool,
    ) -> None:
        mysql_engine: Engine = create_engine(
            f"mysql+mysqldb://{mysql_credentials.user}:{mysql_credentials.password}@{mysql_credentials.host}:{mysql_credentials.port}/{mysql_credentials.database}"
        )
        mysql_cnx: Connection = mysql_engine.connect()
        mysql_inspect: Inspector = inspect(mysql_engine)
        mysql_tables: t.List[str] = mysql_inspect.get_table_names()

        table_number: int = choice(range(1, len(mysql_tables)))

        random_mysql_tables: t.List[str] = sample(mysql_tables, table_number)
        random_mysql_tables.sort()

        remaining_tables: t.List[str] = list(set(mysql_tables) - set(random_mysql_tables))
        remaining_tables.sort()

        proc: MySQLtoSQLite = MySQLtoSQLite(  # type: ignore[call-arg]
            sqlite_file=sqlite_database,
            mysql_user=mysql_credentials.user,
            mysql_password=mysql_credentials.password,
            mysql_database=mysql_credentials.database,
            mysql_tables=None if exclude_tables else random_mysql_tables,
            exclude_mysql_tables=random_mysql_tables if exclude_tables else None,
            mysql_host=mysql_credentials.host,
            mysql_port=mysql_credentials.port,
            prefix_indices=prefix_indices,
        )
        caplog.set_level(logging.DEBUG)
        proc.transfer()
        assert all(
            message in [record.message for record in caplog.records]
            for message in set(
                [
                    f"Transferring table {table}"
                    for table in (remaining_tables if exclude_tables else random_mysql_tables)
                ]
                + ["Done!"]
            )
        )
        assert all(record.levelname == "INFO" for record in caplog.records)
        assert not any(record.levelname == "ERROR" for record in caplog.records)

        sqlite_engine: Engine = create_engine(
            f"sqlite:///{sqlite_database}",
            json_serializer=json.dumps,
            json_deserializer=json.loads,
        )

        sqlite_cnx: Connection = sqlite_engine.connect()
        sqlite_inspect: Inspector = inspect(sqlite_engine)
        sqlite_tables: t.List[str] = sqlite_inspect.get_table_names()

        """ Test if both databases have the same table names """
        if exclude_tables:
            assert set(sqlite_tables) == set(remaining_tables)
        else:
            assert set(sqlite_tables) == set(random_mysql_tables)

        """ Test if all the tables have the same column names """
        for table_name in sqlite_tables:
            assert [column["name"] for column in sqlite_inspect.get_columns(table_name)] == [
                column["name"] for column in mysql_inspect.get_columns(table_name)
            ]

        """ Test if all the tables have the same indices """
        index_keys: t.Tuple[str, ...] = ("name", "column_names", "unique")
        mysql_indices: t.List[ReflectedIndex] = []
        for table_name in remaining_tables if exclude_tables else random_mysql_tables:
            for index in mysql_inspect.get_indexes(table_name):
                mysql_index: t.Dict[str, t.Any] = {}
                for key in index_keys:
                    if key == "name" and prefix_indices:
                        mysql_index[key] = f"{table_name}_{index[key]}"  # type: ignore[literal-required]
                    else:
                        mysql_index[key] = index[key]  # type: ignore[literal-required]
                mysql_indices.append(t.cast(ReflectedIndex, mysql_index))

        for table_name in sqlite_tables:
            for sqlite_index in sqlite_inspect.get_indexes(table_name):
                sqlite_index["unique"] = bool(sqlite_index["unique"])
                if "dialect_options" in sqlite_index:
                    sqlite_index.pop("dialect_options", None)
                assert sqlite_index in mysql_indices

        """ Check if all the data was transferred correctly """
        sqlite_results: t.List[t.Tuple[t.Tuple[t.Any, ...], ...]] = []
        mysql_results: t.List[t.Tuple[t.Tuple[t.Any, ...], ...]] = []

        meta: MetaData = MetaData()
        for table_name in sqlite_tables:
            sqlite_table: Table = Table(table_name, meta, autoload_with=sqlite_engine)
            sqlite_stmt: Select = select(sqlite_table)
            sqlite_result: t.List[Row[t.Any]] = list(sqlite_cnx.execute(sqlite_stmt).fetchall())
            sqlite_result.sort()
            sqlite_result_adapted = tuple(
                tuple(float(data) if isinstance(data, Decimal) else data for data in row) for row in sqlite_result
            )
            sqlite_results.append(sqlite_result_adapted)

        for table_name in remaining_tables if exclude_tables else random_mysql_tables:
            mysql_table: Table = Table(table_name, meta, autoload_with=mysql_engine)
            mysql_stmt: Select = select(mysql_table)
            mysql_result: t.List[Row[t.Any]] = list(mysql_cnx.execute(mysql_stmt).fetchall())
            mysql_result.sort()
            mysql_result_adapted = tuple(
                tuple(float(data) if isinstance(data, Decimal) else data for data in row) for row in mysql_result
            )
            mysql_results.append(mysql_result_adapted)

        assert sqlite_results == mysql_results

        mysql_cnx.close()
        sqlite_cnx.close()
        mysql_engine.dispose()
        sqlite_engine.dispose()

    @pytest.mark.transfer
    @pytest.mark.parametrize(
        "chunk, vacuum, buffered, prefix_indices",
        [
            # 0000
            pytest.param(
                None,
                False,
                False,
                False,
                id="no chunk, no vacuum, no buffered cursor, no prefix indices",
            ),
            # 0001
            pytest.param(
                None,
                False,
                False,
                True,
                id="no chunk, no vacuum, no buffered cursor, prefix indices",
            ),
            # 1110
            pytest.param(
                10,
                True,
                True,
                False,
                id="chunk, vacuum, buffered cursor, no prefix indices",
            ),
            # 1111
            pytest.param(
                10,
                True,
                True,
                True,
                id="chunk, vacuum, buffered cursor, prefix indices",
            ),
            # 1100
            pytest.param(
                10,
                True,
                False,
                False,
                id="chunk, vacuum, no buffered cursor, no prefix indices",
            ),
            # 1101
            pytest.param(
                10,
                True,
                False,
                True,
                id="chunk, vacuum, no buffered cursor, prefix indices",
            ),
            # 0110
            pytest.param(
                None,
                True,
                True,
                False,
                id="no chunk, vacuum, buffered cursor, no prefix indices",
            ),
            # 0111
            pytest.param(
                None,
                True,
                True,
                True,
                id="no chunk, vacuum, buffered cursor, prefix indices",
            ),
            # 0100
            pytest.param(
                None,
                True,
                False,
                False,
                id="no chunk, vacuum, no buffered cursor, no prefix indices",
            ),
            # 0101
            pytest.param(
                None,
                True,
                False,
                True,
                id="no chunk, vacuum, no buffered cursor, prefix indices",
            ),
            # 1000
            pytest.param(
                10,
                False,
                False,
                False,
                id="chunk, no vacuum, no buffered cursor, no prefix indices",
            ),
            # 1001
            pytest.param(
                10,
                False,
                False,
                True,
                id="chunk, no vacuum, no buffered cursor, prefix indices",
            ),
            # 0010
            pytest.param(
                None,
                False,
                True,
                False,
                id="no chunk, no vacuum, buffered cursor, no prefix indices",
            ),
            # 0011
            pytest.param(
                None,
                False,
                True,
                True,
                id="no chunk, no vacuum, buffered cursor, prefix indices",
            ),
            # 1010
            pytest.param(
                10,
                False,
                True,
                False,
                id="chunk, no vacuum, buffered cursor, no prefix indices",
            ),
            # 1011
            pytest.param(
                10,
                False,
                True,
                True,
                id="chunk, no vacuum, buffered cursor, prefix indices",
            ),
        ],
    )
    def test_transfer_limited_rows_from_mysql_to_sqlite(
        self,
        sqlite_database: "os.PathLike[t.Any]",
        mysql_database: Database,
        mysql_credentials: MySQLCredentials,
        helpers: Helpers,
        caplog: LogCaptureFixture,
        chunk: t.Optional[int],
        vacuum: bool,
        buffered: bool,
        prefix_indices: bool,
    ) -> None:
        limit_rows: int = choice(range(1, 10))

        proc: MySQLtoSQLite = MySQLtoSQLite(  # type: ignore[call-arg]
            sqlite_file=sqlite_database,
            mysql_user=mysql_credentials.user,
            mysql_password=mysql_credentials.password,
            mysql_database=mysql_credentials.database,
            limit_rows=limit_rows,
            mysql_host=mysql_credentials.host,
            mysql_port=mysql_credentials.port,
            prefix_indices=prefix_indices,
        )
        caplog.set_level(logging.DEBUG)
        proc.transfer()
        assert all(
            message in [record.message for record in caplog.records]
            for message in {
                "Transferring table article_authors",
                "Transferring table article_images",
                "Transferring table article_tags",
                "Transferring table articles",
                "Transferring table authors",
                "Transferring table images",
                "Transferring table tags",
                "Transferring table misc",
                "Done!",
            }
        )
        assert all(record.levelname == "INFO" for record in caplog.records)
        assert not any(record.levelname == "ERROR" for record in caplog.records)

        sqlite_engine: Engine = create_engine(
            f"sqlite:///{sqlite_database}",
            json_serializer=json.dumps,
            json_deserializer=json.loads,
        )

        sqlite_cnx: Connection = sqlite_engine.connect()
        sqlite_inspect: Inspector = inspect(sqlite_engine)
        sqlite_tables: t.List[str] = sqlite_inspect.get_table_names()
        mysql_engine: Engine = create_engine(
            f"mysql+mysqldb://{mysql_credentials.user}:{mysql_credentials.password}@{mysql_credentials.host}:{mysql_credentials.port}/{mysql_credentials.database}"
        )
        mysql_cnx: Connection = mysql_engine.connect()
        mysql_inspect: Inspector = inspect(mysql_engine)
        mysql_tables: t.List[str] = mysql_inspect.get_table_names()

        mysql_connector_connection: t.Union[PooledMySQLConnection, MySQLConnection, CMySQLConnection] = (
            mysql.connector.connect(
                user=mysql_credentials.user,
                password=mysql_credentials.password,
                host=mysql_credentials.host,
                port=mysql_credentials.port,
                database=mysql_credentials.database,
                charset="utf8mb4",
                collation="utf8mb4_unicode_ci",
            )
        )
        server_version: t.Tuple[int, ...] = mysql_connector_connection.get_server_version()

        """ Test if both databases have the same table names """
        assert sqlite_tables == mysql_tables

        """ Test if all the tables have the same column names """
        for table_name in sqlite_tables:
            assert [column["name"] for column in sqlite_inspect.get_columns(table_name)] == [
                column["name"] for column in mysql_inspect.get_columns(table_name)
            ]

        """ Test if all the tables have the same indices """
        index_keys: t.Tuple[str, ...] = ("name", "column_names", "unique")
        mysql_indices: t.List[ReflectedIndex] = []
        for table_name in mysql_tables:
            for index in mysql_inspect.get_indexes(table_name):
                mysql_index: t.Dict[str, t.Any] = {}
                for key in index_keys:
                    if key == "name" and prefix_indices:
                        mysql_index[key] = f"{table_name}_{index[key]}"  # type: ignore[literal-required]
                    else:
                        mysql_index[key] = index[key]  # type: ignore[literal-required]
                mysql_indices.append(t.cast(ReflectedIndex, mysql_index))

        for table_name in sqlite_tables:
            for sqlite_index in sqlite_inspect.get_indexes(table_name):
                sqlite_index["unique"] = bool(sqlite_index["unique"])
                if "dialect_options" in sqlite_index:
                    sqlite_index.pop("dialect_options", None)
                assert sqlite_index in mysql_indices

        """ Test if all the tables have the same foreign keys """
        for table_name in mysql_tables:
            mysql_fk_stmt: TextClause = text(
                """
                SELECT k.COLUMN_NAME AS `from`,
                       k.REFERENCED_TABLE_NAME AS `table`,
                       k.REFERENCED_COLUMN_NAME AS `to`,
                       c.UPDATE_RULE AS `on_update`,
                       c.DELETE_RULE AS `on_delete`
                FROM information_schema.TABLE_CONSTRAINTS AS i
                {JOIN} information_schema.KEY_COLUMN_USAGE AS k ON i.CONSTRAINT_NAME = k.CONSTRAINT_NAME
                {JOIN} information_schema.REFERENTIAL_CONSTRAINTS c ON c.CONSTRAINT_NAME = i.CONSTRAINT_NAME
                WHERE i.TABLE_SCHEMA = :table_schema
                AND i.TABLE_NAME = :table_name
                AND i.CONSTRAINT_TYPE = :constraint_type
            """.format(
                    # MySQL 8.0.19 still works with "LEFT JOIN" everything above requires "JOIN"
                    JOIN="JOIN" if (server_version[0] == 8 and server_version[2] > 19) else "LEFT JOIN"
                )
            ).bindparams(
                table_schema=mysql_credentials.database,
                table_name=table_name,
                constraint_type="FOREIGN KEY",
            )
            mysql_fk_result: CursorResult = mysql_cnx.execute(mysql_fk_stmt)
            mysql_foreign_keys: t.List[t.Dict[str, t.Any]] = [dict(row) for row in mysql_fk_result.mappings()]

            sqlite_fk_stmt: TextClause = text(f'PRAGMA foreign_key_list("{table_name}")')
            sqlite_fk_result: CursorResult = sqlite_cnx.execute(sqlite_fk_stmt)
            if sqlite_fk_result.returns_rows:
                for row in sqlite_fk_result.mappings():
                    fk: t.Dict[str, t.Any] = dict(row)
                    assert {
                        "table": fk["table"],
                        "from": fk["from"],
                        "to": fk["to"],
                        "on_update": fk["on_update"],
                        "on_delete": fk["on_delete"],
                    } in mysql_foreign_keys

        """ Check if all the data was transferred correctly """
        sqlite_results: t.List[t.Tuple[t.Tuple[t.Any, ...], ...]] = []
        mysql_results: t.List[t.Tuple[t.Tuple[t.Any, ...], ...]] = []

        meta: MetaData = MetaData()
        for table_name in sqlite_tables:
            sqlite_table: Table = Table(table_name, meta, autoload_with=sqlite_engine)
            sqlite_stmt: Select = select(sqlite_table)
            sqlite_result: t.List[Row[t.Any]] = list(sqlite_cnx.execute(sqlite_stmt).fetchall())
            sqlite_result.sort()
            sqlite_result_adapted = tuple(
                tuple(float(data) if isinstance(data, Decimal) else data for data in row) for row in sqlite_result
            )
            sqlite_results.append(sqlite_result_adapted)

        for table_name in mysql_tables:
            mysql_table: Table = Table(table_name, meta, autoload_with=mysql_engine)
            mysql_stmt: Select = select(mysql_table).limit(limit_rows)
            mysql_result: t.List[Row[t.Any]] = list(mysql_cnx.execute(mysql_stmt).fetchall())
            mysql_result.sort()
            sqlite_result_adapted = tuple(
                tuple(float(data) if isinstance(data, Decimal) else data for data in row) for row in mysql_result
            )
            mysql_results.append(sqlite_result_adapted)

        assert sqlite_results == mysql_results

        mysql_cnx.close()
        sqlite_cnx.close()
        mysql_engine.dispose()
        sqlite_engine.dispose()
