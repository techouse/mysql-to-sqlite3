import logging
import sqlite3
from random import choice

import mysql.connector
import pytest
from mysql.connector import errorcode
from sqlalchemy import inspect
from sqlalchemy.dialects.mysql import __all__ as mysql_column_types

from mysql_to_sqlite3 import MySQLtoSQLite


class TestMySQLtoSQLiteClassmethods:
    def test_translate_type_from_mysql_to_sqlite_invalid_column_type(self, mocker):
        with pytest.raises(ValueError) as excinfo:
            mocker.patch.object(MySQLtoSQLite, "_valid_column_type", return_value=False)
            MySQLtoSQLite._translate_type_from_mysql_to_sqlite("text")
        assert "Invalid column_type!" in str(excinfo.value)

    def test_translate_type_from_mysql_to_sqlite_all_valid_columns(self):
        for column_type in mysql_column_types + (
            "BIGINT UNSIGNED",
            "INTEGER UNSIGNED",
            "INT",
            "INT UNSIGNED",
            "SMALLINT UNSIGNED",
            "TINYINT UNSIGNED",
            "MEDIUMINT UNSIGNED",
            "CHAR(2)",
            "NCHAR(7)",
            "NVARCHAR(17)",
            "VARCHAR(123)",
        ):
            if column_type in {"dialect", "insert", "Insert"}:
                continue
            elif column_type == "INT":
                assert (
                    MySQLtoSQLite._translate_type_from_mysql_to_sqlite(column_type)
                    == "INTEGER"
                )
            elif column_type == "DECIMAL":
                assert (
                    MySQLtoSQLite._translate_type_from_mysql_to_sqlite(column_type)
                    == "DECIMAL"
                )
            elif column_type == "YEAR":
                assert (
                    MySQLtoSQLite._translate_type_from_mysql_to_sqlite(column_type)
                    == "YEAR"
                )
            elif column_type == "TIME":
                assert (
                    MySQLtoSQLite._translate_type_from_mysql_to_sqlite(column_type)
                    == "TIME"
                )
            elif column_type == "TIMESTAMP":
                assert (
                    MySQLtoSQLite._translate_type_from_mysql_to_sqlite(column_type)
                    == "DATETIME"
                )
            elif column_type in {
                "BINARY",
                "BIT",
                "LONGBLOB",
                "MEDIUMBLOB",
                "TINYBLOB",
                "VARBINARY",
            }:
                assert (
                    MySQLtoSQLite._translate_type_from_mysql_to_sqlite(column_type)
                    == "BLOB"
                )
            elif column_type == "CHAR":
                assert (
                    MySQLtoSQLite._translate_type_from_mysql_to_sqlite(column_type)
                    == "CHARACTER"
                )
            elif column_type == "CHAR(2)":
                assert (
                    MySQLtoSQLite._translate_type_from_mysql_to_sqlite(column_type)
                    == "CHARACTER(2)"
                )
            elif column_type == "NCHAR(7)":
                assert (
                    MySQLtoSQLite._translate_type_from_mysql_to_sqlite(column_type)
                    == "NCHAR(7)"
                )
            elif column_type == "NVARCHAR(17)":
                assert (
                    MySQLtoSQLite._translate_type_from_mysql_to_sqlite(column_type)
                    == "NVARCHAR(17)"
                )
            elif column_type == "VARCHAR(123)":
                assert (
                    MySQLtoSQLite._translate_type_from_mysql_to_sqlite(column_type)
                    == "VARCHAR(123)"
                )
            elif column_type in {
                "ENUM",
                "JSON",
                "LONGTEXT",
                "MEDIUMTEXT",
                "SET",
                "TINYTEXT",
            }:
                assert (
                    MySQLtoSQLite._translate_type_from_mysql_to_sqlite(column_type)
                    == "TEXT"
                )
            elif column_type.endswith(" UNSIGNED"):
                if column_type.startswith("INT "):
                    assert (
                        MySQLtoSQLite._translate_type_from_mysql_to_sqlite(column_type)
                        == "INTEGER"
                    )
                else:
                    assert MySQLtoSQLite._translate_type_from_mysql_to_sqlite(
                        column_type
                    ) == column_type.replace(" UNSIGNED", "")
            else:
                assert (
                    MySQLtoSQLite._translate_type_from_mysql_to_sqlite(column_type)
                    == column_type
                )


@pytest.mark.exceptions
@pytest.mark.usefixtures("mysql_instance")
class TestMySQLtoSQLiteSQLExceptions:
    @pytest.mark.parametrize(
        "quiet",
        [
            pytest.param(False, id="verbose"),
            pytest.param(True, id="quiet"),
        ],
    )
    def test_create_table_server_lost_connection_error(
        self, sqlite_database, mysql_database, mysql_credentials, mocker, caplog, quiet
    ):
        proc = MySQLtoSQLite(
            sqlite_file=sqlite_database,
            mysql_user=mysql_credentials.user,
            mysql_password=mysql_credentials.password,
            mysql_database=mysql_credentials.database,
            mysql_host=mysql_credentials.host,
            mysql_port=mysql_credentials.port,
            quiet=quiet,
        )

        class FakeSQLiteCursor:
            def executescript(self, *args, **kwargs):
                raise mysql.connector.Error(
                    msg="Error Code: 2013. Lost connection to MySQL server during query",
                    errno=errorcode.CR_SERVER_LOST,
                )

        class FakeSQLiteConnector:
            def commit(self, *args, **kwargs):
                return True

        mysql_inspect = inspect(mysql_database.engine)
        mysql_tables = mysql_inspect.get_table_names()

        mocker.patch.object(proc, "_sqlite_cur", FakeSQLiteCursor())
        mocker.patch.object(proc._mysql, "reconnect", return_value=True)
        mocker.patch.object(proc, "_sqlite", FakeSQLiteConnector())
        caplog.set_level(logging.DEBUG)
        with pytest.raises(mysql.connector.Error):
            proc._create_table(choice(mysql_tables))

    @pytest.mark.parametrize(
        "quiet",
        [
            pytest.param(False, id="verbose"),
            pytest.param(True, id="quiet"),
        ],
    )
    def test_create_table_unknown_mysql_connector_error(
        self, sqlite_database, mysql_database, mysql_credentials, mocker, caplog, quiet
    ):
        proc = MySQLtoSQLite(
            sqlite_file=sqlite_database,
            mysql_user=mysql_credentials.user,
            mysql_password=mysql_credentials.password,
            mysql_database=mysql_credentials.database,
            mysql_host=mysql_credentials.host,
            mysql_port=mysql_credentials.port,
            quiet=quiet,
        )

        class FakeSQLiteCursor:
            def executescript(self, statement):
                raise mysql.connector.Error(
                    msg="Error Code: 2000. Unknown MySQL error",
                    errno=errorcode.CR_UNKNOWN_ERROR,
                )

        mysql_inspect = inspect(mysql_database.engine)
        mysql_tables = mysql_inspect.get_table_names()
        mocker.patch.object(proc, "_sqlite_cur", FakeSQLiteCursor())
        caplog.set_level(logging.DEBUG)
        with pytest.raises(mysql.connector.Error):
            proc._create_table(choice(mysql_tables))

    @pytest.mark.parametrize(
        "quiet",
        [
            pytest.param(False, id="verbose"),
            pytest.param(True, id="quiet"),
        ],
    )
    def test_create_table_sqlite3_error(
        self, sqlite_database, mysql_database, mysql_credentials, mocker, caplog, quiet
    ):
        proc = MySQLtoSQLite(
            sqlite_file=sqlite_database,
            mysql_user=mysql_credentials.user,
            mysql_password=mysql_credentials.password,
            mysql_database=mysql_credentials.database,
            mysql_host=mysql_credentials.host,
            mysql_port=mysql_credentials.port,
            quiet=quiet,
        )

        class FakeSQLiteCursor:
            def executescript(self, *args, **kwargs):
                raise sqlite3.Error("Unknown SQLite error")

        mysql_inspect = inspect(mysql_database.engine)
        mysql_tables = mysql_inspect.get_table_names()
        mocker.patch.object(proc, "_sqlite_cur", FakeSQLiteCursor())
        caplog.set_level(logging.DEBUG)
        with pytest.raises(sqlite3.Error):
            proc._create_table(choice(mysql_tables))

    @pytest.mark.parametrize(
        "exception, quiet",
        [
            pytest.param(
                mysql.connector.Error(
                    msg="Error Code: 2013. Lost connection to MySQL server during query",
                    errno=errorcode.CR_SERVER_LOST,
                ),
                False,
                id="errorcode.CR_SERVER_LOST verbose",
            ),
            pytest.param(
                mysql.connector.Error(
                    msg="Error Code: 2013. Lost connection to MySQL server during query",
                    errno=errorcode.CR_SERVER_LOST,
                ),
                True,
                id="errorcode.CR_SERVER_LOST quiet",
            ),
            pytest.param(
                mysql.connector.Error(
                    msg="Error Code: 2000. Unknown MySQL error",
                    errno=errorcode.CR_UNKNOWN_ERROR,
                ),
                False,
                id="errorcode.CR_UNKNOWN_ERROR verbose",
            ),
            pytest.param(
                mysql.connector.Error(
                    msg="Error Code: 2000. Unknown MySQL error",
                    errno=errorcode.CR_UNKNOWN_ERROR,
                ),
                True,
                id="errorcode.CR_UNKNOWN_ERROR quiet",
            ),
            pytest.param(
                sqlite3.Error("Unknown SQLite error"), False, id="sqlite3.Error verbose"
            ),
            pytest.param(
                sqlite3.Error("Unknown SQLite error"), True, id="sqlite3.Error quiet"
            ),
        ],
    )
    def test_transfer_table_data_exceptions(
        self,
        sqlite_database,
        mysql_database,
        mysql_credentials,
        mocker,
        caplog,
        exception,
        quiet,
    ):
        proc = MySQLtoSQLite(
            sqlite_file=sqlite_database,
            mysql_user=mysql_credentials.user,
            mysql_password=mysql_credentials.password,
            mysql_database=mysql_credentials.database,
            mysql_host=mysql_credentials.host,
            mysql_port=mysql_credentials.port,
            quiet=quiet,
        )

        class FakeMySQLCursor:
            def fetchall(self):
                raise exception

            def fetchmany(self, size=1):
                raise exception

        mysql_inspect = inspect(mysql_database.engine)
        mysql_tables = mysql_inspect.get_table_names()

        table_name = choice(mysql_tables)
        columns = [column["name"] for column in mysql_inspect.get_columns(table_name)]

        sql = (
            'INSERT OR IGNORE INTO "{table}" ({fields}) VALUES ({placeholders})'.format(
                table=table_name,
                fields=('"{}", ' * len(columns)).rstrip(" ,").format(*columns),
                placeholders=("?, " * len(columns)).rstrip(" ,"),
            )
        )

        mocker.patch.object(proc, "_mysql_cur", FakeMySQLCursor())

        with pytest.raises((mysql.connector.Error, sqlite3.Error)):
            proc._transfer_table_data(table_name, sql)
