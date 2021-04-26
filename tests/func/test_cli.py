from random import choice, sample

import pytest
import six
from sqlalchemy import create_engine, inspect

from mysql_to_sqlite3 import MySQLtoSQLite
from mysql_to_sqlite3.cli import cli as mysql2sqlite


@pytest.mark.cli
@pytest.mark.usefixtures("mysql_instance")
class TestMySQLtoSQLite:
    def test_no_arguments(self, cli_runner):
        result = cli_runner.invoke(mysql2sqlite)
        assert result.exit_code > 0
        assert any(
            message in result.output
            for message in {
                'Error: Missing option "-f" / "--sqlite-file"',
                "Error: Missing option '-f' / '--sqlite-file'",
            }
        )

    def test_non_existing_sqlite_file(self, cli_runner, faker):
        result = cli_runner.invoke(
            mysql2sqlite, ["-f", faker.file_path(depth=1, extension=".sqlite3")]
        )
        assert result.exit_code > 0
        assert any(
            message in result.output
            for message in {
                'Error: Missing option "-d" / "--mysql-database"',
                "Error: Missing option '-d' / '--mysql-database'",
            }
        )

    def test_no_database_name(self, cli_runner, sqlite_database):
        result = cli_runner.invoke(mysql2sqlite, ["-f", sqlite_database])
        assert result.exit_code > 0
        assert any(
            message in result.output
            for message in {
                'Error: Missing option "-d" / "--mysql-database"',
                "Error: Missing option '-d' / '--mysql-database'",
            }
        )

    def test_no_database_user(self, cli_runner, sqlite_database, mysql_credentials):
        result = cli_runner.invoke(
            mysql2sqlite, ["-f", sqlite_database, "-d", mysql_credentials.database]
        )
        assert result.exit_code > 0
        assert any(
            message in result.output
            for message in {
                'Error: Missing option "-u" / "--mysql-user"',
                "Error: Missing option '-u' / '--mysql-user'",
            }
        )

    def test_invalid_database_name(
        self, cli_runner, sqlite_database, mysql_database, mysql_credentials, faker
    ):
        result = cli_runner.invoke(
            mysql2sqlite,
            [
                "-f",
                sqlite_database,
                "-d",
                "_".join(faker.words(nb=3)),
                "-u",
                faker.first_name().lower(),
            ],
        )
        assert result.exit_code > 0
        assert "1045 (28000): Access denied" in result.output

    def test_invalid_database_user(
        self, cli_runner, sqlite_database, mysql_database, mysql_credentials, faker
    ):
        result = cli_runner.invoke(
            mysql2sqlite,
            [
                "-f",
                sqlite_database,
                "-d",
                mysql_credentials.database,
                "-u",
                faker.first_name().lower(),
            ],
        )
        assert result.exit_code > 0
        assert "1045 (28000): Access denied" in result.output

    def test_invalid_database_password(
        self, cli_runner, sqlite_database, mysql_database, mysql_credentials, faker
    ):
        result = cli_runner.invoke(
            mysql2sqlite,
            [
                "-f",
                sqlite_database,
                "-d",
                mysql_credentials.database,
                "-u",
                mysql_credentials.user,
                "--mysql-password",
                faker.password(length=16),
            ],
        )
        assert result.exit_code > 0
        assert "1045 (28000): Access denied" in result.output

    def test_database_password_prompt(
        self,
        cli_runner,
        sqlite_database,
        mysql_credentials,
        mysql_database,
    ):
        result = cli_runner.invoke(
            mysql2sqlite,
            args=[
                "-f",
                sqlite_database,
                "-d",
                mysql_credentials.database,
                "-u",
                mysql_credentials.user,
                "-p",
            ],
            input=mysql_credentials.password,
        )
        assert result.exit_code == 0

    def test_invalid_database_password_prompt(
        self,
        cli_runner,
        sqlite_database,
        mysql_credentials,
        mysql_database,
        faker,
    ):
        result = cli_runner.invoke(
            mysql2sqlite,
            args=[
                "-f",
                sqlite_database,
                "-d",
                mysql_credentials.database,
                "-u",
                mysql_credentials.user,
                "-p",
            ],
            input=faker.password(length=16),
        )
        assert result.exit_code > 0
        assert "1045 (28000): Access denied" in result.output

    def test_invalid_database_port(
        self, cli_runner, sqlite_database, mysql_database, mysql_credentials, faker
    ):
        if six.PY2:
            port = choice(xrange(2, 2 ** 16 - 1))
        else:
            port = choice(range(2, 2 ** 16 - 1))
        if port == mysql_credentials.port:
            port -= 1
        result = cli_runner.invoke(
            mysql2sqlite,
            [
                "-f",
                sqlite_database,
                "-d",
                mysql_credentials.database,
                "-u",
                mysql_credentials.user,
                "--mysql-password",
                mysql_credentials.password,
                "-h",
                mysql_credentials.host,
                "-P",
                port,
            ],
        )
        assert result.exit_code > 0
        assert any(
            message in result.output
            for message in {
                "2003 (HY000): Can't connect to MySQL server on",
                "2003: Can't connect to MySQL server",
            }
        )

    @pytest.mark.parametrize(
        "chunk, vacuum, use_buffered_cursors, quiet",
        [
            # 0000
            pytest.param(
                None,
                False,
                False,
                False,
                id="no chunk, no vacuum, no buffered cursor, verbose",
            ),
            # 1110
            pytest.param(
                10, True, True, False, id="chunk, vacuum, buffered cursor, verbose"
            ),
            # 1100
            pytest.param(
                10, True, False, False, id="chunk, vacuum, no buffered cursor, verbose"
            ),
            # 0110
            pytest.param(
                None, True, True, False, id="no chunk, vacuum, buffered cursor, verbose"
            ),
            # 0100
            pytest.param(
                None,
                True,
                False,
                False,
                id="no chunk, vacuum, no buffered cursor, verbose",
            ),
            # 1000
            pytest.param(
                10,
                False,
                False,
                False,
                id="chunk, no vacuum, no buffered cursor, verbose",
            ),
            # 0010
            pytest.param(
                None,
                False,
                True,
                False,
                id="no chunk, no vacuum, buffered cursor, verbose",
            ),
            # 1010
            pytest.param(
                10, False, True, False, id="chunk, no vacuum, buffered cursor, verbose"
            ),
            # 0001
            pytest.param(
                None,
                False,
                False,
                True,
                id="no chunk, no vacuum, no buffered cursor, quiet",
            ),
            # 1111
            pytest.param(
                10, True, True, True, id="chunk, vacuum, buffered cursor, quiet"
            ),
            # 1101
            pytest.param(
                10, True, False, True, id="chunk, vacuum, no buffered cursor, quiet"
            ),
            # 0111
            pytest.param(
                None, True, True, True, id="no chunk, vacuum, buffered cursor, quiet"
            ),
            # 0101
            pytest.param(
                None,
                True,
                False,
                True,
                id="no chunk, vacuum, no buffered cursor, quiet",
            ),
            # 1001
            pytest.param(
                10, False, False, True, id="chunk, no vacuum, no buffered cursor, quiet"
            ),
            # 0011
            pytest.param(
                None,
                False,
                True,
                True,
                id="no chunk, no vacuum, buffered cursor, quiet",
            ),
            # 1011
            pytest.param(
                10, False, True, True, id="chunk, no vacuum, buffered cursor, quiet"
            ),
        ],
    )
    def test_minimum_valid_parameters(
        self,
        cli_runner,
        sqlite_database,
        mysql_database,
        mysql_credentials,
        chunk,
        vacuum,
        use_buffered_cursors,
        quiet,
    ):
        arguments = [
            "-f",
            sqlite_database,
            "-d",
            mysql_credentials.database,
            "-u",
            mysql_credentials.user,
            "--mysql-password",
            mysql_credentials.password,
            "-h",
            mysql_credentials.host,
            "-P",
            mysql_credentials.port,
            "-c",
            chunk,
        ]
        if vacuum:
            arguments.append("-V")
        if use_buffered_cursors:
            arguments.append("--use-buffered-cursors")
        if quiet:
            arguments.append("-q")
        result = cli_runner.invoke(mysql2sqlite, arguments)
        assert result.exit_code == 0
        if quiet:
            assert result.output == ""
        else:
            assert result.output != ""

    def test_keyboard_interrupt(
        self, cli_runner, sqlite_database, mysql_credentials, mysql_database, mocker
    ):
        mocker.patch.object(MySQLtoSQLite, "transfer", side_effect=KeyboardInterrupt())
        result = cli_runner.invoke(
            mysql2sqlite,
            [
                "-f",
                sqlite_database,
                "-d",
                mysql_credentials.database,
                "-u",
                mysql_credentials.user,
                "--mysql-password",
                mysql_credentials.password,
                "-h",
                mysql_credentials.host,
                "-P",
                mysql_credentials.port,
            ],
        )
        assert result.exit_code > 0
        assert "Process interrupted" in result.output

    def test_transfer_specific_tables_only(
        self, cli_runner, sqlite_database, mysql_credentials, mysql_database
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
        mysql_inspect = inspect(mysql_engine)
        mysql_tables = mysql_inspect.get_table_names()

        if six.PY2:
            table_number = choice(xrange(1, len(mysql_tables)))
        else:
            table_number = choice(range(1, len(mysql_tables)))

        result = cli_runner.invoke(
            mysql2sqlite,
            [
                "-f",
                sqlite_database,
                "-d",
                mysql_credentials.database,
                "-t",
                " ".join(sample(mysql_tables, table_number)),
                "-u",
                mysql_credentials.user,
                "--mysql-password",
                mysql_credentials.password,
                "-h",
                mysql_credentials.host,
                "-P",
                mysql_credentials.port,
            ],
        )
        assert result.exit_code == 0

    def test_version(self, cli_runner):
        result = cli_runner.invoke(mysql2sqlite, ["--version"])
        assert result.exit_code == 0
        assert all(
            message in result.output
            for message in {
                "mysql-to-sqlite3",
                "Operating",
                "System",
                "Python",
                "MySQL",
                "SQLite",
                "click",
                "mysql-connector-python",
                "python-slugify",
                "pytimeparse",
                "simplejson",
                "six",
                "tabulate",
                "tqdm",
            }
        )
