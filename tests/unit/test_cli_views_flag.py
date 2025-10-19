import typing as t

import pytest
from click.testing import CliRunner

from mysql_to_sqlite3.cli import cli as mysql2sqlite


class TestCLIViewsFlag:
    def test_mysql_views_as_tables_flag_is_threaded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Ensure --mysql-views-as-tables reaches MySQLtoSQLite as views_as_views=False (legacy materialization)."""
        received_kwargs: t.Dict[str, t.Any] = {}

        class FakeConverter:
            def __init__(self, **kwargs: t.Any) -> None:
                received_kwargs.update(kwargs)

            def transfer(self) -> None:  # pragma: no cover - nothing to do
                return None

        # Patch the converter used by the CLI
        monkeypatch.setattr("mysql_to_sqlite3.cli.MySQLtoSQLite", FakeConverter)

        runner = CliRunner()
        result = runner.invoke(
            mysql2sqlite,
            [
                "-f",
                "out.sqlite3",
                "-d",
                "db",
                "-u",
                "user",
                "--mysql-views-as-tables",
            ],
        )
        assert result.exit_code == 0
        assert received_kwargs.get("views_as_views") is False

    def test_mysql_views_as_tables_short_flag_is_threaded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Ensure -T (short for --mysql-views-as-tables) reaches MySQLtoSQLite as views_as_views=False."""
        received_kwargs: t.Dict[str, t.Any] = {}

        class FakeConverter:
            def __init__(self, **kwargs: t.Any) -> None:
                received_kwargs.update(kwargs)

            def transfer(self) -> None:  # pragma: no cover - nothing to do
                return None

        # Patch the converter used by the CLI
        monkeypatch.setattr("mysql_to_sqlite3.cli.MySQLtoSQLite", FakeConverter)

        runner = CliRunner()
        result = runner.invoke(
            mysql2sqlite,
            [
                "-f",
                "out.sqlite3",
                "-d",
                "db",
                "-u",
                "user",
                "-T",
            ],
        )
        assert result.exit_code == 0
        assert received_kwargs.get("views_as_views") is False
