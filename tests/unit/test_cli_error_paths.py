from types import SimpleNamespace

import pytest
from click.testing import CliRunner

from mysql_to_sqlite3.cli import cli as mysql2sqlite


class _FakeConverter:
    def __init__(self, *args, **kwargs):
        pass

    def transfer(self):
        raise RuntimeError("should not run")


def _fake_supported_charsets(charset=None):
    """Produce deterministic charset/collation pairs for tests."""
    # When called without a charset, emulate the public API generator used by click.Choice.
    if charset is None:
        return iter(
            [
                SimpleNamespace(id=0, charset="utf8mb4", collation="utf8mb4_general_ci"),
                SimpleNamespace(id=1, charset="latin1", collation="latin1_swedish_ci"),
            ]
        )
    # When scoped to a particular charset, expose only the primary collation.
    return iter([SimpleNamespace(id=0, charset=charset, collation=f"{charset}_general_ci")])


class TestCliErrorPaths:
    def test_mysql_collation_must_match_charset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Invalid charset/collation combinations should be rejected before transfer starts."""
        monkeypatch.setattr("mysql_to_sqlite3.cli.mysql_supported_character_sets", _fake_supported_charsets)
        monkeypatch.setattr("mysql_to_sqlite3.cli.MySQLtoSQLite", _FakeConverter)

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
                "--mysql-charset",
                "utf8mb4",
                "--mysql-collation",
                "latin1_swedish_ci",
            ],
        )
        assert result.exit_code == 1
        assert "Invalid value for '--collation'" in result.output

    def test_debug_reraises_keyboard_interrupt(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Debug mode should bubble up KeyboardInterrupt for easier debugging."""

        class KeyboardInterruptConverter:
            def __init__(self, *args, **kwargs):
                pass

            def transfer(self):
                raise KeyboardInterrupt()

        monkeypatch.setattr("mysql_to_sqlite3.cli.MySQLtoSQLite", KeyboardInterruptConverter)

        kwargs = {
            "sqlite_file": "out.sqlite3",
            "mysql_user": "user",
            "prompt_mysql_password": False,
            "mysql_password": None,
            "mysql_database": "db",
            "mysql_tables": None,
            "exclude_mysql_tables": None,
            "mysql_views_as_tables": False,
            "limit_rows": 0,
            "collation": "BINARY",
            "prefix_indices": False,
            "without_foreign_keys": False,
            "without_tables": False,
            "without_data": False,
            "strict": False,
            "mysql_host": "localhost",
            "mysql_port": 3306,
            "mysql_charset": "utf8mb4",
            "mysql_collation": None,
            "skip_ssl": False,
            "chunk": 200000,
            "log_file": None,
            "json_as_text": False,
            "vacuum": False,
            "use_buffered_cursors": False,
            "quiet": False,
            "debug": True,
        }
        with pytest.raises(KeyboardInterrupt):
            mysql2sqlite.callback(**kwargs)

    def test_debug_reraises_generic_exception(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Debug mode should bubble up unexpected exceptions."""

        class ExplodingConverter:
            def __init__(self, *args, **kwargs):
                pass

            def transfer(self):
                raise RuntimeError("boom")

        monkeypatch.setattr("mysql_to_sqlite3.cli.MySQLtoSQLite", ExplodingConverter)

        kwargs = {
            "sqlite_file": "out.sqlite3",
            "mysql_user": "user",
            "prompt_mysql_password": False,
            "mysql_password": None,
            "mysql_database": "db",
            "mysql_tables": None,
            "exclude_mysql_tables": None,
            "mysql_views_as_tables": False,
            "limit_rows": 0,
            "collation": "BINARY",
            "prefix_indices": False,
            "without_foreign_keys": False,
            "without_tables": False,
            "without_data": False,
            "strict": False,
            "mysql_host": "localhost",
            "mysql_port": 3306,
            "mysql_charset": "utf8mb4",
            "mysql_collation": None,
            "skip_ssl": False,
            "chunk": 200000,
            "log_file": None,
            "json_as_text": False,
            "vacuum": False,
            "use_buffered_cursors": False,
            "quiet": False,
            "debug": True,
        }
        with pytest.raises(RuntimeError):
            mysql2sqlite.callback(**kwargs)
