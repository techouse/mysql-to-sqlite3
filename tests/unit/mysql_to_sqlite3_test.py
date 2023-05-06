import logging
import sqlite3
from random import choice

import mysql.connector
import pytest
from mysql.connector import errorcode
from sqlalchemy import inspect
from sqlalchemy.dialects.mysql import __all__ as mysql_column_types

from mysql_to_sqlite3 import MySQLtoSQLite
from mysql_to_sqlite3.sqlite_utils import CollatingSequences


class TestMySQLtoSQLiteClassmethods:
    def test_translate_type_from_mysql_to_sqlite_invalid_column_type(self, mocker):
        with pytest.raises(ValueError) as excinfo:
            mocker.patch.object(MySQLtoSQLite, "_valid_column_type", return_value=False)
            MySQLtoSQLite._translate_type_from_mysql_to_sqlite(column_type="text")
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
            if any(c for c in column_type if c.islower()):
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
                "LONGTEXT",
                "MEDIUMTEXT",
                "SET",
                "TINYTEXT",
            }:
                assert (
                    MySQLtoSQLite._translate_type_from_mysql_to_sqlite(column_type)
                    == "TEXT"
                )
            elif column_type == "JSON":
                assert (
                    MySQLtoSQLite._translate_type_from_mysql_to_sqlite(column_type)
                    == "TEXT"
                )
                assert (
                    MySQLtoSQLite._translate_type_from_mysql_to_sqlite(
                        column_type, sqlite_json1_extension_enabled=True
                    )
                    == "JSON"
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

    @pytest.mark.parametrize(
        "column_default, sqlite_default_translation",
        [
            pytest.param(None, "", id="None"),
            pytest.param("", "DEFAULT ''", id='""'),
            pytest.param("lorem", "DEFAULT 'lorem'", id='"lorem"'),
            pytest.param(
                "lorem ipsum dolor",
                "DEFAULT 'lorem ipsum dolor'",
                id='"lorem ipsum dolor"',
            ),
            pytest.param("CURRENT_TIME", "DEFAULT CURRENT_TIME", id='"CURRENT_TIME"'),
            pytest.param("current_time", "DEFAULT CURRENT_TIME", id='"current_time"'),
            pytest.param("CURRENT_DATE", "DEFAULT CURRENT_DATE", id='"CURRENT_DATE"'),
            pytest.param("current_date", "DEFAULT CURRENT_DATE", id='"current_date"'),
            pytest.param(
                "CURRENT_TIMESTAMP",
                "DEFAULT CURRENT_TIMESTAMP",
                id='"CURRENT_TIMESTAMP"',
            ),
            pytest.param(
                "current_timestamp",
                "DEFAULT CURRENT_TIMESTAMP",
                id='"current_timestamp"',
            ),
        ],
    )
    def test_translate_default_from_mysql_to_sqlite(
        self, column_default, sqlite_default_translation
    ):
        assert (
            MySQLtoSQLite._translate_default_from_mysql_to_sqlite(column_default)
            == sqlite_default_translation
        )

    @pytest.mark.parametrize(
        "column_default, sqlite_default_translation, sqlite_version",
        [
            pytest.param(False, "DEFAULT(FALSE)", "3.23.0", id="False (NEW)"),
            pytest.param(True, "DEFAULT(TRUE)", "3.23.0", id="True (NEW)"),
            pytest.param(False, "DEFAULT '0'", "3.22.0", id="False (OLD)"),
            pytest.param(True, "DEFAULT '1'", "3.22.0", id="True (OLD)"),
        ],
    )
    def test_translate_default_booleans_from_mysql_to_sqlite(
        self, mocker, column_default, sqlite_default_translation, sqlite_version
    ):
        mocker.patch.object(sqlite3, "sqlite_version", sqlite_version)
        assert (
            MySQLtoSQLite._translate_default_from_mysql_to_sqlite(
                column_default, "BOOLEAN"
            )
            == sqlite_default_translation
        )

    @pytest.mark.parametrize(
        "column_default, sqlite_default_translation, column_type",
        [
            pytest.param("0", "DEFAULT '0'", "NUMERIC", id='"0" (NUMERIC)'),
            pytest.param("1", "DEFAULT '1'", "NUMERIC", id='"1" (NUMERIC)'),
            pytest.param("0", "DEFAULT '0'", "TEXT", id='"0" (TEXT)'),
            pytest.param("1", "DEFAULT '1'", "TEXT", id='"1" (TEXT)'),
            pytest.param(0, "DEFAULT '0'", "NUMERIC", id="0 (NUMERIC)"),
            pytest.param(1, "DEFAULT '1'", "NUMERIC", id="1 (NUMERIC)"),
            pytest.param(0, "DEFAULT '0'", "TEXT", id="0 (TEXT)"),
            pytest.param(1, "DEFAULT '1'", "TEXT", id="1 (TEXT)"),
            pytest.param(
                123456789, "DEFAULT '123456789'", "NUMERIC", id="123456789 (NUMERIC)"
            ),
            pytest.param(
                1234.56789, "DEFAULT '1234.56789'", "NUMERIC", id="1234.56789 (NUMERIC)"
            ),
            pytest.param(
                123456789, "DEFAULT '123456789'", "TEXT", id="123456789 (TEXT)"
            ),
            pytest.param(
                1234.56789, "DEFAULT '1234.56789'", "TEXT", id="1234.56789 (TEXT)"
            ),
        ],
    )
    def test_translate_default_numbers_from_mysql_to_sqlite(
        self, column_default, sqlite_default_translation, column_type
    ):
        assert (
            MySQLtoSQLite._translate_default_from_mysql_to_sqlite(
                column_default, column_type
            )
            == sqlite_default_translation
        )

    @pytest.mark.parametrize(
        "column_default, sqlite_default_translation",
        [
            pytest.param(b"", "DEFAULT x''", id="b''"),
            pytest.param(b"-1", "DEFAULT x'2d31'", id="b'-1'"),
            pytest.param(b"0", "DEFAULT x'30'", id="b'0'"),
            pytest.param(b"1", "DEFAULT x'31'", id="b'1'"),
            pytest.param(
                b"-1234567890", "DEFAULT x'2d31323334353637383930'", id="b'-1234567890'"
            ),
            pytest.param(
                b"1234567890", "DEFAULT x'31323334353637383930'", id="b'1234567890'"
            ),
            pytest.param(b"SQLite", "DEFAULT x'53514c697465'", id="b'SQLite'"),
            pytest.param(
                b"Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nam pretium, purus vitae sollicitudin varius, nisi lectus vehicula dui, ut dignissim felis dolor blandit justo. Donec eleifend lectus ut feugiat rhoncus. Donec erat nibh, dapibus nec diam id, lacinia lacinia nisl. Mauris sagittis efficitur nisl. Ut tincidunt elementum rhoncus. Cras suscipit dolor sed est ultricies, quis dapibus neque suscipit. Etiam ac enim eu ligula bibendum blandit quis sit amet felis. Praesent mi nisi, luctus sit amet nunc ut, fermentum tempus purus. Suspendisse vel purus a nibh aliquam hendrerit. Aliquam sit amet tristique lorem. Sed elementum congue ante id mollis. Donec vitae pretium neque.",
                "DEFAULT x'4c6f72656d20697073756d20646f6c6f722073697420616d65742c20636f6e73656374657475722061646970697363696e6720656c69742e204e616d207072657469756d2c20707572757320766974616520736f6c6c696369747564696e207661726975732c206e697369206c6563747573207665686963756c61206475692c207574206469676e697373696d2066656c697320646f6c6f7220626c616e646974206a7573746f2e20446f6e656320656c656966656e64206c656374757320757420666575676961742072686f6e6375732e20446f6e65632065726174206e6962682c2064617069627573206e6563206469616d2069642c206c6163696e6961206c6163696e6961206e69736c2e204d617572697320736167697474697320656666696369747572206e69736c2e2055742074696e636964756e7420656c656d656e74756d2072686f6e6375732e204372617320737573636970697420646f6c6f72207365642065737420756c747269636965732c20717569732064617069627573206e657175652073757363697069742e20457469616d20616320656e696d206575206c6967756c6120626962656e64756d20626c616e64697420717569732073697420616d65742066656c69732e205072616573656e74206d69206e6973692c206c75637475732073697420616d6574206e756e632075742c206665726d656e74756d2074656d7075732070757275732e2053757370656e64697373652076656c2070757275732061206e69626820616c697175616d2068656e6472657269742e20416c697175616d2073697420616d657420747269737469717565206c6f72656d2e2053656420656c656d656e74756d20636f6e67756520616e7465206964206d6f6c6c69732e20446f6e6563207669746165207072657469756d206e657175652e'",
                id="b'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nam pretium, purus vitae sollicitudin varius, nisi lectus vehicula dui, ut dignissim felis dolor blandit justo. Donec eleifend lectus ut feugiat rhoncus. Donec erat nibh, dapibus nec diam id, lacinia lacinia nisl. Mauris sagittis efficitur nisl. Ut tincidunt elementum rhoncus. Cras suscipit dolor sed est ultricies, quis dapibus neque suscipit. Etiam ac enim eu ligula bibendum blandit quis sit amet felis. Praesent mi nisi, luctus sit amet nunc ut, fermentum tempus purus. Suspendisse vel purus a nibh aliquam hendrerit. Aliquam sit amet tristique lorem. Sed elementum congue ante id mollis. Donec vitae pretium neque.'",
            ),
        ],
    )
    def test_translate_default_blob_bytes_from_mysql_to_sqlite(
        self, column_default, sqlite_default_translation
    ):
        assert (
            MySQLtoSQLite._translate_default_from_mysql_to_sqlite(
                column_default, "BLOB"
            )
            == sqlite_default_translation
        )

    @pytest.mark.parametrize(
        "collation, resulting_column_collation, column_type",
        [
            pytest.param(
                CollatingSequences.BINARY,
                "",
                "CHARACTER",
                id="{} (CHARACTER)".format(CollatingSequences.BINARY),
            ),
            pytest.param(
                CollatingSequences.NOCASE,
                "COLLATE {}".format(CollatingSequences.NOCASE),
                "CHARACTER",
                id="{} (CHARACTER)".format(CollatingSequences.NOCASE),
            ),
            pytest.param(
                CollatingSequences.RTRIM,
                "COLLATE {}".format(CollatingSequences.RTRIM),
                "CHARACTER",
                id="{} (CHARACTER)".format(CollatingSequences.RTRIM),
            ),
            pytest.param(
                CollatingSequences.BINARY,
                "",
                "NCHAR",
                id="{} (NCHAR)".format(CollatingSequences.BINARY),
            ),
            pytest.param(
                CollatingSequences.NOCASE,
                "COLLATE {}".format(CollatingSequences.NOCASE),
                "NCHAR",
                id="{} (NCHAR)".format(CollatingSequences.NOCASE),
            ),
            pytest.param(
                CollatingSequences.RTRIM,
                "COLLATE {}".format(CollatingSequences.RTRIM),
                "NCHAR",
                id="{} (NCHAR)".format(CollatingSequences.RTRIM),
            ),
            pytest.param(
                CollatingSequences.BINARY,
                "",
                "NVARCHAR",
                id="{} (NVARCHAR)".format(CollatingSequences.BINARY),
            ),
            pytest.param(
                CollatingSequences.NOCASE,
                "COLLATE {}".format(CollatingSequences.NOCASE),
                "NVARCHAR",
                id="{} (NVARCHAR)".format(CollatingSequences.NOCASE),
            ),
            pytest.param(
                CollatingSequences.RTRIM,
                "COLLATE {}".format(CollatingSequences.RTRIM),
                "NVARCHAR",
                id="{} (NVARCHAR)".format(CollatingSequences.RTRIM),
            ),
            pytest.param(
                CollatingSequences.BINARY,
                "",
                "TEXT",
                id="{} (TEXT)".format(CollatingSequences.BINARY),
            ),
            pytest.param(
                CollatingSequences.NOCASE,
                "COLLATE {}".format(CollatingSequences.NOCASE),
                "TEXT",
                id="{} (TEXT)".format(CollatingSequences.NOCASE),
            ),
            pytest.param(
                CollatingSequences.RTRIM,
                "COLLATE {}".format(CollatingSequences.RTRIM),
                "TEXT",
                id="{} (TEXT)".format(CollatingSequences.RTRIM),
            ),
            pytest.param(
                CollatingSequences.BINARY,
                "",
                "VARCHAR",
                id="{} (VARCHAR)".format(CollatingSequences.BINARY),
            ),
            pytest.param(
                CollatingSequences.NOCASE,
                "COLLATE {}".format(CollatingSequences.NOCASE),
                "VARCHAR",
                id="{} (VARCHAR)".format(CollatingSequences.NOCASE),
            ),
            pytest.param(
                CollatingSequences.RTRIM,
                "COLLATE {}".format(CollatingSequences.RTRIM),
                "VARCHAR",
                id="{} (VARCHAR)".format(CollatingSequences.RTRIM),
            ),
        ],
    )
    def test_data_type_collation_sequence_is_applied_on_textual_data_types(
        self,
        collation,
        resulting_column_collation,
        column_type,
    ):
        assert (
            MySQLtoSQLite._data_type_collation_sequence(collation, column_type)
            == resulting_column_collation
        )

    def test_data_type_collation_sequence_is_not_applied_on_non_textual_data_types(
        self,
    ):
        for column_type in (
            "BIGINT",
            "BINARY",
            "BIT",
            "BLOB",
            "BOOLEAN",
            "DATE",
            "DATETIME",
            "DATETIME",
            "DECIMAL",
            "DOUBLE",
            "FLOAT",
            "INTEGER",
            "INTEGER",
            "LONGBLOB",
            "MEDIUMBLOB",
            "MEDIUMINT",
            "NUMERIC",
            "REAL",
            "SMALLINT",
            "TIME",
            "TINYBLOB",
            "TINYINT",
            "VARBINARY",
            "YEAR",
        ):
            for collation in (
                CollatingSequences.BINARY,
                CollatingSequences.NOCASE,
                CollatingSequences.RTRIM,
            ):
                assert (
                    MySQLtoSQLite._data_type_collation_sequence(collation, column_type)
                    == ""
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
