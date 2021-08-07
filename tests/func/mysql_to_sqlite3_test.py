import logging
import re
from collections import namedtuple
from decimal import Decimal
from random import choice, sample

import mysql.connector
import pytest
import simplejson as json
import six
from mysql.connector import errorcode, MySQLConnection
from sqlalchemy import MetaData, Table, select, create_engine, inspect, text

from mysql_to_sqlite3 import MySQLtoSQLite

if six.PY2:
    from ..sixeptions import *


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
    def test_missing_mysql_user_raises_exception(self, mysql_credentials, quiet):
        with pytest.raises(ValueError) as excinfo:
            MySQLtoSQLite(mysql_database=mysql_credentials.database, quiet=quiet)
        assert "Please provide a MySQL user" in str(excinfo.value)

    @pytest.mark.init
    @pytest.mark.parametrize(
        "quiet",
        [
            pytest.param(False, id="verbose"),
            pytest.param(True, id="quiet"),
        ],
    )
    def test_missing_mysql_database_raises_exception(self, faker, quiet):
        with pytest.raises(ValueError) as excinfo:
            MySQLtoSQLite(mysql_user=faker.first_name().lower(), quiet=quiet)
        assert "Please provide a MySQL database" in str(excinfo.value)

    @pytest.mark.init
    @pytest.mark.parametrize(
        "quiet",
        [
            pytest.param(False, id="verbose"),
            pytest.param(True, id="quiet"),
        ],
    )
    def test_invalid_mysql_credentials_raises_access_denied_exception(
        self, sqlite_database, mysql_database, mysql_credentials, faker, quiet
    ):
        with pytest.raises(mysql.connector.Error) as excinfo:
            MySQLtoSQLite(
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
        self, sqlite_database, mysql_credentials, mocker, quiet
    ):
        FakeConnector = namedtuple("FakeConnector", ["is_connected"])
        mocker.patch.object(
            mysql.connector,
            "connect",
            return_value=FakeConnector(is_connected=lambda: False),
        )
        with pytest.raises((ConnectionError, IOError)) as excinfo:
            MySQLtoSQLite(
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
                mysql.connector.Error(
                    msg="Unknown database 'test_db'", errno=errorcode.ER_BAD_DB_ERROR
                ),
                False,
                id="mysql.connector.Error verbose",
            ),
            pytest.param(
                mysql.connector.Error(
                    msg="Unknown database 'test_db'", errno=errorcode.ER_BAD_DB_ERROR
                ),
                True,
                id="mysql.connector.Error quiet",
            ),
            pytest.param(
                Exception("Unknown database 'test_db'"), False, id="Exception verbose"
            ),
            pytest.param(
                Exception("Unknown database 'test_db'"), True, id="Exception quiet"
            ),
        ],
    )
    def test_non_existing_mysql_database_raises_exception(
        self,
        sqlite_database,
        mysql_database,
        mysql_credentials,
        faker,
        mocker,
        caplog,
        exception,
        quiet,
    ):
        class FakeMySQLConnection(MySQLConnection):
            @property
            def database(self):
                return self._database

            @database.setter
            def database(self, value):
                self._database = value
                # raise a fake exception
                raise exception

            def is_connected(self):
                return True

            def cursor(
                self,
                buffered=None,
                raw=None,
                prepared=None,
                cursor_class=None,
                dictionary=None,
                named_tuple=None,
            ):
                return True

        caplog.set_level(logging.DEBUG)
        mocker.patch.object(
            mysql.connector, "connect", return_value=FakeMySQLConnection()
        )
        with pytest.raises((mysql.connector.Error, Exception)) as excinfo:
            MySQLtoSQLite(
                sqlite_file=sqlite_database,
                mysql_user=mysql_credentials.user,
                mysql_password=mysql_credentials.password,
                mysql_database=mysql_credentials.database,
                mysql_host=mysql_credentials.host,
                mysql_port=mysql_credentials.port,
                quiet=quiet,
            )
            assert any(
                "MySQL Database does not exist!" in message
                for message in caplog.messages
            )
        assert "Unknown database" in str(excinfo.value)

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
        sqlite_database,
        mysql_database,
        mysql_credentials,
        caplog,
        tmpdir,
        faker,
        quiet,
    ):
        log_file = tmpdir.join("db.log")
        caplog.set_level(logging.DEBUG)
        with pytest.raises(mysql.connector.Error):
            MySQLtoSQLite(
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
            assert (
                re.match(r"^\d{4,}-\d{2,}-\d{2,}\s+\d{2,}:\d{2,}:\d{2,}\s+\w+\s+", log)
                is not None
            )

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
        sqlite_database,
        mysql_database,
        mysql_credentials,
        helpers,
        caplog,
        chunk,
        vacuum,
        buffered,
        prefix_indices,
    ):
        proc = MySQLtoSQLite(
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

        sqlite_engine = create_engine(
            "sqlite:///{database}".format(
                database=sqlite_database,
                json_serializer=json.dumps,
                json_deserializer=json.loads,
            )
        )
        sqlite_cnx = sqlite_engine.connect()
        sqlite_inspect = inspect(sqlite_engine)
        sqlite_tables = sqlite_inspect.get_table_names()
        mysql_engine = create_engine(
            "mysql+mysqldb://{user}:{password}@{host}:{port}/{database}".format(
                user=mysql_credentials.user,
                password=mysql_credentials.password,
                host=mysql_credentials.host,
                port=mysql_credentials.port,
                database=mysql_credentials.database,
            )
        )
        mysql_cnx = mysql_engine.connect()
        mysql_inspect = inspect(mysql_engine)
        mysql_tables = mysql_inspect.get_table_names()

        mysql_connector_connection = mysql.connector.connect(
            user=mysql_credentials.user,
            password=mysql_credentials.password,
            host=mysql_credentials.host,
            port=mysql_credentials.port,
            database=mysql_credentials.database,
        )
        server_version = mysql_connector_connection.get_server_version()

        """ Test if both databases have the same table names """
        assert sqlite_tables == mysql_tables

        """ Test if all the tables have the same column names """
        for table_name in sqlite_tables:
            assert [
                column["name"] for column in sqlite_inspect.get_columns(table_name)
            ] == [column["name"] for column in mysql_inspect.get_columns(table_name)]

        """ Test if all the tables have the same indices """
        index_keys = {"name", "column_names", "unique"}
        mysql_indices = []
        for table_name in mysql_tables:
            for index in mysql_inspect.get_indexes(table_name):
                mysql_index = {}
                for key in index_keys:
                    if key == "name" and prefix_indices:
                        mysql_index[key] = "{table}_{name}".format(
                            table=table_name, name=index[key]
                        )
                    else:
                        mysql_index[key] = index[key]
                mysql_indices.append(mysql_index)

        for table_name in sqlite_tables:
            for sqlite_index in sqlite_inspect.get_indexes(table_name):
                sqlite_index["unique"] = bool(sqlite_index["unique"])
                assert sqlite_index in mysql_indices

        """ Test if all the tables have the same foreign keys """
        for table_name in mysql_tables:
            mysql_fk_stmt = text(
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
                    JOIN="JOIN"
                    if (server_version[0] == 8 and server_version[2] > 19)
                    else "LEFT JOIN"
                )
            ).bindparams(
                table_schema=mysql_credentials.database,
                table_name=table_name,
                constraint_type="FOREIGN KEY",
            )
            mysql_fk_result = mysql_cnx.execute(mysql_fk_stmt)
            mysql_foreign_keys = [dict(row) for row in mysql_fk_result]

            sqlite_fk_stmt = 'PRAGMA foreign_key_list("{table}")'.format(
                table=table_name
            )
            sqlite_fk_result = sqlite_cnx.execute(sqlite_fk_stmt)
            if sqlite_fk_result.returns_rows:
                for row in sqlite_fk_result:
                    fk = dict(row)
                    assert {
                        "table": fk["table"],
                        "from": fk["from"],
                        "to": fk["to"],
                        "on_update": fk["on_update"],
                        "on_delete": fk["on_delete"],
                    } in mysql_foreign_keys

        """ Check if all the data was transferred correctly """
        sqlite_results = []
        mysql_results = []

        meta = MetaData(bind=None)
        for table_name in sqlite_tables:
            sqlite_table = Table(
                table_name, meta, autoload=True, autoload_with=sqlite_engine
            )
            sqlite_stmt = select([sqlite_table])
            sqlite_result = sqlite_cnx.execute(sqlite_stmt).fetchall()
            sqlite_result.sort()
            sqlite_result = [
                [float(data) if isinstance(data, Decimal) else data for data in row]
                for row in sqlite_result
            ]
            sqlite_results.append(sqlite_result)

        for table_name in mysql_tables:
            mysql_table = Table(
                table_name, meta, autoload=True, autoload_with=mysql_engine
            )
            mysql_stmt = select([mysql_table])
            mysql_result = mysql_cnx.execute(mysql_stmt).fetchall()
            mysql_result.sort()
            mysql_result = [
                [float(data) if isinstance(data, Decimal) else data for data in row]
                for row in mysql_result
            ]
            mysql_results.append(mysql_result)

        assert sqlite_results == mysql_results

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
    def test_transfer_specific_tables_transfers_only_specified_tables_from_mysql_to_sqlite(
        self,
        sqlite_database,
        mysql_database,
        mysql_credentials,
        helpers,
        caplog,
        chunk,
        vacuum,
        buffered,
        prefix_indices,
    ):
        mysql_engine = create_engine(
            "mysql+mysqldb://{user}:{password}@{host}:{port}/{database}".format(
                user=mysql_credentials.user,
                password=mysql_credentials.password,
                host=mysql_credentials.host,
                port=mysql_credentials.port,
                database=mysql_credentials.database,
            )
        )
        mysql_cnx = mysql_engine.connect()
        mysql_inspect = inspect(mysql_engine)
        mysql_tables = mysql_inspect.get_table_names()

        if six.PY2:
            table_number = choice(xrange(1, len(mysql_tables)))
        else:
            table_number = choice(range(1, len(mysql_tables)))

        random_mysql_tables = sample(mysql_tables, table_number)
        random_mysql_tables.sort()

        proc = MySQLtoSQLite(
            sqlite_file=sqlite_database,
            mysql_user=mysql_credentials.user,
            mysql_password=mysql_credentials.password,
            mysql_database=mysql_credentials.database,
            mysql_tables=random_mysql_tables,
            mysql_host=mysql_credentials.host,
            mysql_port=mysql_credentials.port,
            prefix_indices=prefix_indices,
        )
        caplog.set_level(logging.DEBUG)
        proc.transfer()
        assert all(
            message in [record.message for record in caplog.records]
            for message in set(
                ["Transferring table {}".format(table) for table in random_mysql_tables]
                + ["Done!"]
            )
        )
        assert all(record.levelname == "INFO" for record in caplog.records)
        assert not any(record.levelname == "ERROR" for record in caplog.records)

        sqlite_engine = create_engine(
            "sqlite:///{database}".format(
                database=sqlite_database,
                json_serializer=json.dumps,
                json_deserializer=json.loads,
            )
        )
        sqlite_cnx = sqlite_engine.connect()
        sqlite_inspect = inspect(sqlite_engine)
        sqlite_tables = sqlite_inspect.get_table_names()

        """ Test if both databases have the same table names """
        assert sqlite_tables == random_mysql_tables

        """ Test if all the tables have the same column names """
        for table_name in sqlite_tables:
            assert [
                column["name"] for column in sqlite_inspect.get_columns(table_name)
            ] == [column["name"] for column in mysql_inspect.get_columns(table_name)]

        """ Test if all the tables have the same indices """
        index_keys = {"name", "column_names", "unique"}
        mysql_indices = []
        for table_name in random_mysql_tables:
            for index in mysql_inspect.get_indexes(table_name):
                mysql_index = {}
                for key in index_keys:
                    if key == "name" and prefix_indices:
                        mysql_index[key] = "{table}_{name}".format(
                            table=table_name, name=index[key]
                        )
                    else:
                        mysql_index[key] = index[key]
                mysql_indices.append(mysql_index)

        for table_name in sqlite_tables:
            for sqlite_index in sqlite_inspect.get_indexes(table_name):
                sqlite_index["unique"] = bool(sqlite_index["unique"])
                assert sqlite_index in mysql_indices

        """ Check if all the data was transferred correctly """
        sqlite_results = []
        mysql_results = []

        meta = MetaData(bind=None)
        for table_name in sqlite_tables:
            sqlite_table = Table(
                table_name, meta, autoload=True, autoload_with=sqlite_engine
            )
            sqlite_stmt = select([sqlite_table])
            sqlite_result = sqlite_cnx.execute(sqlite_stmt).fetchall()
            sqlite_result.sort()
            sqlite_result = [
                [float(data) if isinstance(data, Decimal) else data for data in row]
                for row in sqlite_result
            ]
            sqlite_results.append(sqlite_result)

        for table_name in random_mysql_tables:
            mysql_table = Table(
                table_name, meta, autoload=True, autoload_with=mysql_engine
            )
            mysql_stmt = select([mysql_table])
            mysql_result = mysql_cnx.execute(mysql_stmt).fetchall()
            mysql_result.sort()
            mysql_result = [
                [float(data) if isinstance(data, Decimal) else data for data in row]
                for row in mysql_result
            ]
            mysql_results.append(mysql_result)

        assert sqlite_results == mysql_results

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
        sqlite_database,
        mysql_database,
        mysql_credentials,
        helpers,
        caplog,
        chunk,
        vacuum,
        buffered,
        prefix_indices,
    ):
        if six.PY2:
            limit_rows = choice(xrange(1, 10))
        else:
            limit_rows = choice(range(1, 10))

        proc = MySQLtoSQLite(
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

        sqlite_engine = create_engine(
            "sqlite:///{database}".format(
                database=sqlite_database,
                json_serializer=json.dumps,
                json_deserializer=json.loads,
            )
        )
        sqlite_cnx = sqlite_engine.connect()
        sqlite_inspect = inspect(sqlite_engine)
        sqlite_tables = sqlite_inspect.get_table_names()
        mysql_engine = create_engine(
            "mysql+mysqldb://{user}:{password}@{host}:{port}/{database}".format(
                user=mysql_credentials.user,
                password=mysql_credentials.password,
                host=mysql_credentials.host,
                port=mysql_credentials.port,
                database=mysql_credentials.database,
            )
        )
        mysql_cnx = mysql_engine.connect()
        mysql_inspect = inspect(mysql_engine)
        mysql_tables = mysql_inspect.get_table_names()

        mysql_connector_connection = mysql.connector.connect(
            user=mysql_credentials.user,
            password=mysql_credentials.password,
            host=mysql_credentials.host,
            port=mysql_credentials.port,
            database=mysql_credentials.database,
        )
        server_version = mysql_connector_connection.get_server_version()

        """ Test if both databases have the same table names """
        assert sqlite_tables == mysql_tables

        """ Test if all the tables have the same column names """
        for table_name in sqlite_tables:
            assert [
                column["name"] for column in sqlite_inspect.get_columns(table_name)
            ] == [column["name"] for column in mysql_inspect.get_columns(table_name)]

        """ Test if all the tables have the same indices """
        index_keys = {"name", "column_names", "unique"}
        mysql_indices = []
        for table_name in mysql_tables:
            for index in mysql_inspect.get_indexes(table_name):
                mysql_index = {}
                for key in index_keys:
                    if key == "name" and prefix_indices:
                        mysql_index[key] = "{table}_{name}".format(
                            table=table_name, name=index[key]
                        )
                    else:
                        mysql_index[key] = index[key]
                mysql_indices.append(mysql_index)

        for table_name in sqlite_tables:
            for sqlite_index in sqlite_inspect.get_indexes(table_name):
                sqlite_index["unique"] = bool(sqlite_index["unique"])
                assert sqlite_index in mysql_indices

        """ Test if all the tables have the same foreign keys """
        for table_name in mysql_tables:
            mysql_fk_stmt = text(
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
                    JOIN="JOIN"
                    if (server_version[0] == 8 and server_version[2] > 19)
                    else "LEFT JOIN"
                )
            ).bindparams(
                table_schema=mysql_credentials.database,
                table_name=table_name,
                constraint_type="FOREIGN KEY",
            )
            mysql_fk_result = mysql_cnx.execute(mysql_fk_stmt)
            mysql_foreign_keys = [dict(row) for row in mysql_fk_result]

            sqlite_fk_stmt = 'PRAGMA foreign_key_list("{table}")'.format(
                table=table_name
            )
            sqlite_fk_result = sqlite_cnx.execute(sqlite_fk_stmt)
            if sqlite_fk_result.returns_rows:
                for row in sqlite_fk_result:
                    fk = dict(row)
                    assert {
                        "table": fk["table"],
                        "from": fk["from"],
                        "to": fk["to"],
                        "on_update": fk["on_update"],
                        "on_delete": fk["on_delete"],
                    } in mysql_foreign_keys

        """ Check if all the data was transferred correctly """
        sqlite_results = []
        mysql_results = []

        meta = MetaData(bind=None)
        for table_name in sqlite_tables:
            sqlite_table = Table(
                table_name, meta, autoload=True, autoload_with=sqlite_engine
            )
            sqlite_stmt = select([sqlite_table])
            sqlite_result = sqlite_cnx.execute(sqlite_stmt).fetchall()
            sqlite_result.sort()
            sqlite_result = [
                [float(data) if isinstance(data, Decimal) else data for data in row]
                for row in sqlite_result
            ]
            sqlite_results.append(sqlite_result)

        for table_name in mysql_tables:
            mysql_table = Table(
                table_name, meta, autoload=True, autoload_with=mysql_engine
            )
            mysql_stmt = select([mysql_table]).limit(limit_rows)
            mysql_result = mysql_cnx.execute(mysql_stmt).fetchall()
            mysql_result.sort()
            mysql_result = [
                [float(data) if isinstance(data, Decimal) else data for data in row]
                for row in mysql_result
            ]
            mysql_results.append(mysql_result)

        assert sqlite_results == mysql_results
