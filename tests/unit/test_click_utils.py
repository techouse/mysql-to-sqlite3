import typing as t

import click
import pytest
from click.testing import CliRunner
from pytest_mock import MockFixture

from mysql_to_sqlite3.click_utils import OptionEatAll, prompt_password, validate_positive_integer


class TestOptionEatAll:
    def test_init_with_invalid_nargs(self) -> None:
        """Test OptionEatAll initialization with invalid nargs."""
        with pytest.raises(ValueError) as excinfo:
            OptionEatAll("--test", nargs=1)
        assert "nargs, if set, must be -1 not 1" in str(excinfo.value)

    def test_init_with_valid_nargs(self) -> None:
        """Test OptionEatAll initialization with valid nargs and behavior."""

        @click.command()
        @click.option("--test", cls=OptionEatAll, nargs=-1, help="Test option")
        def cli(test: t.Optional[t.Tuple[str, ...]] = None) -> None:
            # This just verifies that the option works when nargs=-1
            assert test is not None
            click.echo(f"Success: {len(test)} values")

        runner = CliRunner()
        result = runner.invoke(cli, ["--test", "value1", "value2", "value3"])
        assert result.exit_code == 0
        assert "Success:" in result.output
        assert "values" in result.output

    def test_add_to_parser(self) -> None:
        """Test add_to_parser method."""

        @click.command()
        @click.option("--test", cls=OptionEatAll, help="Test option")
        def cli(test: t.Optional[t.Tuple[str, ...]] = None) -> None:
            click.echo(f"Test: {test}")

        runner = CliRunner()
        result = runner.invoke(cli, ["--test", "value1", "value2", "value3"])
        assert result.exit_code == 0
        assert "Test: ('value1', 'value2', 'value3')" in result.output

    def test_add_to_parser_with_other_options(self) -> None:
        """Test add_to_parser method with other options."""

        @click.command()
        @click.option("--test", cls=OptionEatAll, help="Test option")
        @click.option("--other", help="Other option")
        def cli(test: t.Optional[t.Tuple[str, ...]] = None, other: t.Optional[str] = None) -> None:
            click.echo(f"Test: {test}, Other: {other}")

        runner = CliRunner()
        result = runner.invoke(cli, ["--test", "value1", "value2", "--other", "value3"])
        assert result.exit_code == 0
        assert "Test: ('value1', 'value2'), Other: value3" in result.output

    def test_add_to_parser_without_save_other_options(self) -> None:
        """Test add_to_parser method without saving other options."""

        @click.command()
        @click.option("--test", cls=OptionEatAll, save_other_options=False, help="Test option")
        @click.option("--other", help="Other option")
        def cli(test: t.Optional[t.Tuple[str, ...]] = None, other: t.Optional[str] = None) -> None:
            click.echo(f"Test: {test}, Other: {other}")

        runner = CliRunner()
        result = runner.invoke(cli, ["--test", "value1", "value2", "--other", "value3"])
        assert result.exit_code == 0
        # All remaining args should be consumed by --test
        assert "Test: ('value1', 'value2', '--other', 'value3'), Other: None" in result.output


class TestPromptPassword:
    def test_prompt_password_with_password(self) -> None:
        """Test prompt_password with password already provided."""
        ctx = click.Context(click.Command("test"))
        ctx.params = {"mysql_password": "test_password"}

        result = prompt_password(ctx, None, True)
        assert result == "test_password"

    def test_prompt_password_without_password(self, mocker: MockFixture) -> None:
        """Test prompt_password without password provided."""
        ctx = click.Context(click.Command("test"))
        ctx.params = {"mysql_password": None}

        mocker.patch("click.prompt", return_value="prompted_password")

        result = prompt_password(ctx, None, True)
        assert result == "prompted_password"

    def test_prompt_password_use_password_false(self) -> None:
        """Test prompt_password with use_password=False."""
        ctx = click.Context(click.Command("test"))
        ctx.params = {"mysql_password": "test_password"}

        result = prompt_password(ctx, None, False)
        assert result is None


class TestValidatePositiveInteger:
    def test_validate_positive_integer_valid(self) -> None:
        """Test validate_positive_integer with valid values."""
        ctx = click.Context(click.Command("test"))

        assert validate_positive_integer(ctx, None, 0) == 0
        assert validate_positive_integer(ctx, None, 1) == 1
        assert validate_positive_integer(ctx, None, 100) == 100

    def test_validate_positive_integer_invalid(self) -> None:
        """Test validate_positive_integer with invalid values."""
        ctx = click.Context(click.Command("test"))

        with pytest.raises(click.BadParameter) as excinfo:
            validate_positive_integer(ctx, None, -1)
        assert "Should be a positive integer or 0." in str(excinfo.value)

        with pytest.raises(click.BadParameter) as excinfo:
            validate_positive_integer(ctx, None, -100)
        assert "Should be a positive integer or 0." in str(excinfo.value)
