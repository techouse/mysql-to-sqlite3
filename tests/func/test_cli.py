import os
import typing as t
from datetime import datetime
from random import choice, sample

import pytest
from click.testing import CliRunner, Result
from faker import Faker
from pytest_mock import MockFixture
from sqlalchemy import Connection, Engine, Inspector, create_engine, inspect

from mysql_to_sqlite3 import MySQLtoSQLite
from mysql_to_sqlite3 import __version__ as package_version
from mysql_to_sqlite3.cli import cli as mysql2sqlite
from tests.conftest import MySQLCredentials
from tests.database import Database


@pytest.mark.cli
@pytest.mark.usefixtures("mysql_instance")
class TestMySQLtoSQLite:
    def test_no_arguments(self, cli_runner: CliRunner) -> None:
        result: Result = cli_runner.invoke(mysql2sqlite)
        assert result.exit_code == 0
        assert all(
            message in result.output
            for message in {
                f"Usage: {mysql2sqlite.name} [OPTIONS]",
                f"{mysql2sqlite.name} version {package_version} Copyright (c) 2019-{datetime.now().year} Klemen Tusar",
            }
        )

    def test_non_existing_sqlite_file(self, cli_runner: CliRunner, faker: Faker) -> None:
        result: Result = cli_runner.invoke(mysql2sqlite, ["-f", faker.file_path(depth=1, extension=".sqlite3")])
        assert result.exit_code > 0
        assert any(
            message in result.output
            for message in {
                'Error: Missing option "-d" / "--mysql-database"',
                "Error: Missing option '-d' / '--mysql-database'",
            }
        )

    def test_no_database_name(self, cli_runner: CliRunner, sqlite_database: "os.PathLike[t.Any]") -> None:
        result: Result = cli_runner.invoke(mysql2sqlite, ["-f", str(sqlite_database)])
        assert result.exit_code > 0
        assert any(
            message in result.output
            for message in {
                'Error: Missing option "-d" / "--mysql-database"',
                "Error: Missing option '-d' / '--mysql-database'",
            }
        )

    def test_no_database_user(
        self,
        cli_runner: CliRunner,
        sqlite_database: "os.PathLike[t.Any]",
        mysql_credentials: MySQLCredentials,
    ) -> None:
        result = cli_runner.invoke(mysql2sqlite, ["-f", str(sqlite_database), "-d", mysql_credentials.database])
        assert result.exit_code > 0
        assert any(
            message in result.output
            for message in {
                'Error: Missing option "-u" / "--mysql-user"',
                "Error: Missing option '-u' / '--mysql-user'",
            }
        )

    @pytest.mark.xfail
    def test_invalid_database_name(
        self,
        cli_runner: CliRunner,
        sqlite_database: "os.PathLike[t.Any]",
        mysql_database: Database,
        mysql_credentials: MySQLCredentials,
        faker: Faker,
    ) -> None:
        result: Result = cli_runner.invoke(
            mysql2sqlite,
            [
                "-f",
                str(sqlite_database),
                "-d",
                "_".join(faker.words(nb=3)),
                "-u",
                faker.first_name().lower(),
                "-h",
                mysql_credentials.host,
                "-P",
                str(mysql_credentials.port),
            ],
        )
        assert result.exit_code > 0
        assert "1045 (28000): Access denied" in result.output

    @pytest.mark.xfail
    def test_invalid_database_user(
        self,
        cli_runner: CliRunner,
        sqlite_database: "os.PathLike[t.Any]",
        mysql_database: Database,
        mysql_credentials: MySQLCredentials,
        faker: Faker,
    ) -> None:
        result: Result = cli_runner.invoke(
            mysql2sqlite,
            [
                "-f",
                str(sqlite_database),
                "-d",
                mysql_credentials.database,
                "-u",
                faker.first_name().lower(),
                "-h",
                mysql_credentials.host,
                "-P",
                str(mysql_credentials.port),
            ],
        )
        assert result.exit_code > 0
        assert "1045 (28000): Access denied" in result.output

    @pytest.mark.xfail
    def test_invalid_database_password(
        self,
        cli_runner: CliRunner,
        sqlite_database: "os.PathLike[t.Any]",
        mysql_database: Database,
        mysql_credentials: MySQLCredentials,
        faker: Faker,
    ) -> None:
        result: Result = cli_runner.invoke(
            mysql2sqlite,
            [
                "-f",
                str(sqlite_database),
                "-d",
                mysql_credentials.database,
                "-u",
                mysql_credentials.user,
                "--mysql-password",
                faker.password(length=16),
                "-h",
                mysql_credentials.host,
                "-P",
                str(mysql_credentials.port),
            ],
        )
        assert result.exit_code > 0
        assert "1045 (28000): Access denied" in result.output

    def test_database_password_prompt(
        self,
        cli_runner: CliRunner,
        sqlite_database: "os.PathLike[t.Any]",
        mysql_credentials: MySQLCredentials,
        mysql_database: Database,
    ) -> None:
        result: Result = cli_runner.invoke(
            mysql2sqlite,
            args=[
                "-f",
                str(sqlite_database),
                "-d",
                mysql_credentials.database,
                "-u",
                mysql_credentials.user,
                "-p",
                "-h",
                mysql_credentials.host,
                "-P",
                str(mysql_credentials.port),
            ],
            input=mysql_credentials.password,
        )
        assert result.exit_code == 0

    @pytest.mark.xfail
    def test_invalid_database_password_prompt(
        self,
        cli_runner: CliRunner,
        sqlite_database: "os.PathLike[t.Any]",
        mysql_credentials: MySQLCredentials,
        mysql_database: Database,
        faker: Faker,
    ) -> None:
        result: Result = cli_runner.invoke(
            mysql2sqlite,
            args=[
                "-f",
                str(sqlite_database),
                "-d",
                mysql_credentials.database,
                "-u",
                mysql_credentials.user,
                "-p",
                "-h",
                mysql_credentials.host,
                "-P",
                str(mysql_credentials.port),
            ],
            input=faker.password(length=16),
        )
        assert result.exit_code > 0
        assert "1045 (28000): Access denied" in result.output

    @pytest.mark.xfail
    def test_invalid_database_port(
        self,
        cli_runner: CliRunner,
        sqlite_database: "os.PathLike[t.Any]",
        mysql_database: Database,
        mysql_credentials: MySQLCredentials,
        faker: Faker,
    ) -> None:
        port: int = choice(range(2, 2**16 - 1))
        if port == mysql_credentials.port:
            port -= 1
        result: Result = cli_runner.invoke(
            mysql2sqlite,
            [
                "-f",
                str(sqlite_database),
                "-d",
                mysql_credentials.database,
                "-u",
                mysql_credentials.user,
                "--mysql-password",
                mysql_credentials.password,
                "-h",
                mysql_credentials.host,
                "-P",
                str(port),
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

    def test_without_data(
        self,
        cli_runner: CliRunner,
        sqlite_database: "os.PathLike[t.Any]",
        mysql_database: Database,
        mysql_credentials: MySQLCredentials,
    ) -> None:
        result: Result = cli_runner.invoke(
            mysql2sqlite,
            [
                "-f",
                str(sqlite_database),
                "-d",
                mysql_credentials.database,
                "-u",
                mysql_credentials.user,
                "--mysql-password",
                mysql_credentials.password,
                "-h",
                mysql_credentials.host,
                "-P",
                str(mysql_credentials.port),
                "-W",
            ],
        )
        assert result.exit_code == 0

    def test_without_tables(
        self,
        cli_runner: CliRunner,
        sqlite_database: "os.PathLike[t.Any]",
        mysql_database: Database,
        mysql_credentials: MySQLCredentials,
    ) -> None:
        # First we need to create the tables in the SQLite database
        result1: Result = cli_runner.invoke(
            mysql2sqlite,
            [
                "-f",
                str(sqlite_database),
                "-d",
                mysql_credentials.database,
                "-u",
                mysql_credentials.user,
                "--mysql-password",
                mysql_credentials.password,
                "-h",
                mysql_credentials.host,
                "-P",
                str(mysql_credentials.port),
                "-W",
            ],
        )
        assert result1.exit_code == 0

        result2: Result = cli_runner.invoke(
            mysql2sqlite,
            [
                "-f",
                str(sqlite_database),
                "-d",
                mysql_credentials.database,
                "-u",
                mysql_credentials.user,
                "--mysql-password",
                mysql_credentials.password,
                "-h",
                mysql_credentials.host,
                "-P",
                str(mysql_credentials.port),
                "-Z",
            ],
        )
        assert result2.exit_code == 0

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
            pytest.param(10, True, True, False, id="chunk, vacuum, buffered cursor, verbose"),
            # 1100
            pytest.param(10, True, False, False, id="chunk, vacuum, no buffered cursor, verbose"),
            # 0110
            pytest.param(None, True, True, False, id="no chunk, vacuum, buffered cursor, verbose"),
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
            pytest.param(10, False, True, False, id="chunk, no vacuum, buffered cursor, verbose"),
            # 0001
            pytest.param(
                None,
                False,
                False,
                True,
                id="no chunk, no vacuum, no buffered cursor, quiet",
            ),
            # 1111
            pytest.param(10, True, True, True, id="chunk, vacuum, buffered cursor, quiet"),
            # 1101
            pytest.param(10, True, False, True, id="chunk, vacuum, no buffered cursor, quiet"),
            # 0111
            pytest.param(None, True, True, True, id="no chunk, vacuum, buffered cursor, quiet"),
            # 0101
            pytest.param(
                None,
                True,
                False,
                True,
                id="no chunk, vacuum, no buffered cursor, quiet",
            ),
            # 1001
            pytest.param(10, False, False, True, id="chunk, no vacuum, no buffered cursor, quiet"),
            # 0011
            pytest.param(
                None,
                False,
                True,
                True,
                id="no chunk, no vacuum, buffered cursor, quiet",
            ),
            # 1011
            pytest.param(10, False, True, True, id="chunk, no vacuum, buffered cursor, quiet"),
        ],
    )
    def test_minimum_valid_parameters(
        self,
        cli_runner: CliRunner,
        sqlite_database: "os.PathLike[t.Any]",
        mysql_database: Database,
        mysql_credentials: MySQLCredentials,
        chunk: t.Optional[int],
        vacuum: bool,
        use_buffered_cursors: bool,
        quiet: bool,
    ) -> None:
        arguments: t.List[str] = [
            "-f",
            str(sqlite_database),
            "-d",
            mysql_credentials.database,
            "-u",
            mysql_credentials.user,
            "--mysql-password",
            mysql_credentials.password,
            "-h",
            mysql_credentials.host,
            "-P",
            str(mysql_credentials.port),
        ]
        if chunk:
            arguments.append("-c")
            arguments.append(str(chunk))
        if vacuum:
            arguments.append("-V")
        if use_buffered_cursors:
            arguments.append("--use-buffered-cursors")
        if quiet:
            arguments.append("-q")
        result: Result = cli_runner.invoke(mysql2sqlite, arguments)
        assert result.exit_code == 0
        copyright_header = (
            f"{mysql2sqlite.name} version {package_version} Copyright (c) 2019-{datetime.now().year} Klemen Tusar\n"
        )
        assert copyright_header in result.output
        if quiet:
            assert result.output.replace(copyright_header, "") == ""
        else:
            assert result.output.replace(copyright_header, "") != ""

    def test_keyboard_interrupt(
        self,
        cli_runner: CliRunner,
        sqlite_database: "os.PathLike[t.Any]",
        mysql_credentials: MySQLCredentials,
        mysql_database: Database,
        mocker: MockFixture,
    ) -> None:
        mocker.patch.object(MySQLtoSQLite, "transfer", side_effect=KeyboardInterrupt())
        result: Result = cli_runner.invoke(
            mysql2sqlite,
            [
                "-f",
                str(sqlite_database),
                "-d",
                mysql_credentials.database,
                "-u",
                mysql_credentials.user,
                "--mysql-password",
                mysql_credentials.password,
                "-h",
                mysql_credentials.host,
                "-P",
                str(mysql_credentials.port),
            ],
        )
        assert result.exit_code > 0
        assert "Process interrupted" in result.output

    def test_specific_tables_include_and_exclude_are_mutually_exclusive_options(
        self,
        cli_runner: CliRunner,
        sqlite_database: "os.PathLike[t.Any]",
        mysql_credentials: MySQLCredentials,
        mysql_database: Database,
    ) -> None:
        mysql_engine: Engine = create_engine(
            f"mysql+mysqldb://{mysql_credentials.user}:{mysql_credentials.password}@{mysql_credentials.host}:{mysql_credentials.port}/{mysql_credentials.database}"
        )
        mysql_cnx: Connection = mysql_engine.connect()
        mysql_inspect: Inspector = inspect(mysql_engine)
        mysql_tables: t.List[str] = mysql_inspect.get_table_names()

        table_number: int = choice(range(1, len(mysql_tables) // 2))

        include_mysql_tables: t.List[str] = sample(mysql_tables, table_number)
        include_mysql_tables.sort()
        exclude_mysql_tables = list(set(sample(mysql_tables, table_number)) - set(include_mysql_tables))
        exclude_mysql_tables.sort()

        result: Result = cli_runner.invoke(
            mysql2sqlite,
            [
                "-f",
                str(sqlite_database),
                "-d",
                mysql_credentials.database,
                "-t",
                " ".join(include_mysql_tables),
                "-e",
                " ".join(exclude_mysql_tables),
                "-u",
                mysql_credentials.user,
                "--mysql-password",
                mysql_credentials.password,
                "-h",
                mysql_credentials.host,
                "-P",
                str(mysql_credentials.port),
            ],
        )
        assert result.exit_code > 0
        assert "Illegal usage: --mysql-tables and --exclude-mysql-tables are mutually exclusive!" in result.output

        mysql_cnx.close()
        mysql_engine.dispose()

    def test_transfer_specific_tables_only(
        self,
        cli_runner: CliRunner,
        sqlite_database: "os.PathLike[t.Any]",
        mysql_credentials: MySQLCredentials,
        mysql_database: Database,
    ) -> None:
        mysql_engine: Engine = create_engine(
            f"mysql+mysqldb://{mysql_credentials.user}:{mysql_credentials.password}@{mysql_credentials.host}:{mysql_credentials.port}/{mysql_credentials.database}"
        )
        mysql_inspect: Inspector = inspect(mysql_engine)
        mysql_tables: t.List[str] = mysql_inspect.get_table_names()

        table_number: int = choice(range(1, len(mysql_tables)))

        result: Result = cli_runner.invoke(
            mysql2sqlite,
            [
                "-f",
                str(sqlite_database),
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
                str(mysql_credentials.port),
            ],
        )
        assert result.exit_code == 0

    @pytest.mark.xfail
    def test_version(self, cli_runner: CliRunner) -> None:
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
                "pytimeparse2",
                "simplejson",
                "tabulate",
                "tqdm",
            }
        )
