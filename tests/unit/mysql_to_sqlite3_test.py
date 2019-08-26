import logging
import sqlite3
from random import choice

import mysql.connector
import pytest
from mysql.connector import errorcode
from sqlalchemy import inspect
from sqlalchemy.dialects.mysql import __all__ as mysql_column_types

from src.mysql_to_sqlite3 import MySQLtoSQLite


class TestMySQLtoSQLiteClassmethods:
    def test_translate_type_from_mysql_to_sqlite_invalid_column_type(self, mocker):
        with pytest.raises(ValueError) as excinfo:
            mocker.patch.object(MySQLtoSQLite, "_valid_column_type", return_value=False)
            MySQLtoSQLite._translate_type_from_mysql_to_sqlite("text")
        assert "Invalid column_type!" in str(excinfo.value)

    def test_translate_type_from_mysql_to_sqlite_all_valid_columns(self):
        for column_type in mysql_column_types + (
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
            elif column_type in {"DECIMAL", "YEAR", "TIME"}:
                assert (
                    MySQLtoSQLite._translate_type_from_mysql_to_sqlite(column_type)
                    == "NUMERIC"
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
            else:
                assert (
                    MySQLtoSQLite._translate_type_from_mysql_to_sqlite(column_type)
                    == column_type
                )


@pytest.mark.exceptions
@pytest.mark.usefixtures("mysql_instance")
class TestMySQLtoSQLiteSQLExceptions:
    def test_create_table_server_lost_connection_error(
        self, sqlite_database, mysql_database, mysql_credentials, mocker, caplog
    ):
        proc = MySQLtoSQLite(
            sqlite_file=sqlite_database,
            mysql_user=mysql_credentials.user,
            mysql_password=mysql_credentials.password,
            mysql_database=mysql_credentials.database,
            mysql_host=mysql_credentials.host,
            mysql_port=mysql_credentials.port,
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

    def test_create_table_unknown_mysql_connector_error(
        self, sqlite_database, mysql_database, mysql_credentials, mocker, caplog
    ):
        proc = MySQLtoSQLite(
            sqlite_file=sqlite_database,
            mysql_user=mysql_credentials.user,
            mysql_password=mysql_credentials.password,
            mysql_database=mysql_credentials.database,
            mysql_host=mysql_credentials.host,
            mysql_port=mysql_credentials.port,
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

    def test_create_table_sqlite3_error(
        self, sqlite_database, mysql_database, mysql_credentials, mocker, caplog
    ):
        proc = MySQLtoSQLite(
            sqlite_file=sqlite_database,
            mysql_user=mysql_credentials.user,
            mysql_password=mysql_credentials.password,
            mysql_database=mysql_credentials.database,
            mysql_host=mysql_credentials.host,
            mysql_port=mysql_credentials.port,
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
        "exception",
        [
            pytest.param(
                mysql.connector.Error(
                    msg="Error Code: 2013. Lost connection to MySQL server during query",
                    errno=errorcode.CR_SERVER_LOST,
                ),
                id="errorcode.CR_SERVER_LOST",
            ),
            pytest.param(
                mysql.connector.Error(
                    msg="Error Code: 2000. Unknown MySQL error",
                    errno=errorcode.CR_UNKNOWN_ERROR,
                ),
                id="errorcode.CR_UNKNOWN_ERROR",
            ),
            pytest.param(sqlite3.Error("Unknown SQLite error"), id="sqlite3.Error"),
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
    ):
        proc = MySQLtoSQLite(
            sqlite_file=sqlite_database,
            mysql_user=mysql_credentials.user,
            mysql_password=mysql_credentials.password,
            mysql_database=mysql_credentials.database,
            mysql_host=mysql_credentials.host,
            mysql_port=mysql_credentials.port,
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

        sql = 'INSERT OR IGNORE INTO "{table}" ({fields}) VALUES ({placeholders})'.format(
            table=table_name,
            fields=('"{}", ' * len(columns)).rstrip(" ,").format(*columns),
            placeholders=("?, " * len(columns)).rstrip(" ,"),
        )

        mocker.patch.object(proc, "_mysql_cur", FakeMySQLCursor())

        with pytest.raises((mysql.connector.Error, sqlite3.Error)):
            proc._transfer_table_data(table_name, sql)
