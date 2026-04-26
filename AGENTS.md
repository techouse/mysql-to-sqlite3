# Repository Guidelines

## Project Structure & Module Organization

The core package lives in `src/mysql_to_sqlite3/` with CLI entry points (`cli.py`), transport logic, and utility
modules. Types and typing stubs ship via `types.py` and `py.typed`. Tests are split between `tests/unit/` for isolated
logic and `tests/func/` for database-backed flows; shared factories and fixtures sit in `tests/factories.py` and
`tests/conftest.py`. Sphinx docs reside in `docs/`, while built artifacts land in `dist/` and coverage data in
`htmlcov/` and `coverage.xml`. The optional user-facing agent skill lives in `skills/mysql-to-sqlite3/SKILL.md`; keep it
focused on migration help rather than contributor workflow.

## Build, Test, and Development Commands

Use `python -m venv env` and `pip install -e .` plus `pip install -r requirements_dev.txt` for a dev environment. Run
`pytest -v --cov=src/mysql_to_sqlite3` for targeted testing, or `tox` to execute the full matrix across Python versions.
`tox -e linters` runs formatters and static analysis (`black`, `isort`, `flake8`, `mypy`, `bandit`). Build distributions
with `python -m build` or `hatch build`.

## Coding Style & Naming Conventions

Format code with `black` (120-char lines) and keep imports sorted via `isort --profile black`. Prefer descriptive
snake_case for functions and variables, and CamelCase for classes. For Click command callbacks, follow the repository's
existing convention: reuse the established top-level `cli` entrypoint or a single consistent descriptive name across
commands. Maintain full type hints; the package is shipped as typed and mypy checks run with `python_version=3.9`. Avoid
introducing new modules without tests.

## Testing Guidelines

Write `pytest` unit tests for the feature under `tests/unit/` and integration tests under `tests/func/`.
Name test files `test_<feature>.py`. Mark long-running scenarios with existing pytest markers such as
`@pytest.mark.transfer`. Ensure the functional suite can target the dockerised MySQL instance configured in
`tests/db_credentials.json`; set `LEGACY_DB` when validating backward compatibility. Keep coverage trending upward;
update `coverage.xml` only through the tooling.

## Commit & Pull Request Guidelines

Follow the repository’s Gitmoji-inspired history (`:safety_vest:`, `:test_tube:`) for quick context, but prioritise
clear subject lines under 72 characters. Larger changes should include a short body detailing impact and migrations.
Reference related issues with `Fixes #123` and consolidate breaking work into focused commits. Pull requests must
describe the change, call out manual test results, and attach CLI output or screenshots when UX is affected.

## Database & Security Notes

Never commit real credentials. Use ephemeral docker containers for MySQL/MariaDB testing and add new connection flags
through the central `cli` options to keep behaviour consistent. Update `README.md`, `docs/README.rst`, and
`docs/index.rst` whenever user-facing flags or caveats change, and note security-related fixes in `SECURITY.md`.
