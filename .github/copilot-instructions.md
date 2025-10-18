# AI Assistant Project Instructions

Concise, project-specific guidance for code-generation agents working in this repository.

## 1. Purpose & High-Level Architecture

This package provides a robust one-shot transfer of a MySQL/MariaDB database schema + data into a single SQLite file via
a CLI (`mysql2sqlite`). Core orchestration lives in `transporter.py` (`MySQLtoSQLite` class) and is invoked by the Click
CLI defined in `cli.py`. The transfer pipeline:

1. Parse/validate CLI options (mutually exclusive flags, charset/collation validation).
2. Introspect MySQL schema via `information_schema` (columns, indices, foreign keys).
3. Generate SQLite DDL (type & default translation, index naming, optional foreign keys, JSON1 detection).
4. Stream or chunk table data rows from MySQL → SQLite with optional chunk size and progress bars (`tqdm`).
5. Post-process (optional `VACUUM`).

## 2. Key Modules & Responsibilities

- `src/mysql_to_sqlite3/cli.py`: Click command. Central place to add new user-facing options (update docs + README +
  `docs/index.rst` if changed).
- `src/mysql_to_sqlite3/transporter.py`: Core transfer logic, schema introspection, type/default translation, batching,
  logging, reconnection handling.
- `sqlite_utils.py` / `mysql_utils.py`: Helpers for charset/collation sets, adapting/encoding values and SQLite
  extension capability detection.
- `types.py`: Typed parameter & attribute Protocols / TypedDict-like structures for constructor kwargs (mypy relies on
  this; keep hints exhaustive).
- `debug_info.py`: Version table printed with `--version` (tabulated output).

## 3. Patterns & Conventions

- Strict option conflict handling: e.g. `--mysql-tables` vs `--exclude-mysql-tables`; `--without-tables` +
  `--without-data` is invalid. Mirror same validation in new features.
- AUTO_INCREMENT single primary key columns converted to `INTEGER PRIMARY KEY AUTOINCREMENT` only when underlying
  translated type is an integer (see `Integer_Types`). Log a warning otherwise.
- Index naming collision avoidance: when an index name equals a table name or `--prefix-indices` set, prefix with
  `<table>_` to ensure uniqueness.
- Default value translation centralised in `_translate_default_from_mysql_to_sqlite`; extend here for new MySQL
  constructs instead of sprinkling conditionals elsewhere.
- JSON handling: if `JSON` column and SQLite JSON1 compiled (`PRAGMA compile_options`), map to `JSON`; else fallback to
  `TEXT` unless `--json-as-text` provided. Preserve logic when adding new types.
- Foreign keys generation skipped if any table subset restriction applied (ensures referential integrity isn’t partially
  generated).
- Logging: use `_setup_logger` — do not instantiate new loggers ad hoc.

## 4. Testing Approach

- Use `pytest` framework; tests in `tests/` mirror `src/mysql_to_sqlite3/` structure.
- Unit tests in `tests/unit/` focus on isolated helpers (e.g. type/default translation, JSON1 detection, constructor
  validation). When adding logic, create a targeted test file `test_<feature>.py`.
- Functional / transfer tests in `tests/func/` (may require live MySQL). Respect existing pytest markers: add
  `@pytest.mark.transfer` for heavy DB flows, `@pytest.mark.cli` for CLI option parsing.
- Keep mypy passing (configured for Python 3.9 baseline). Avoid untyped dynamic attributes; update `types.py` if
  constructor kwargs change.

## 5. Developer Workflows

- Dev install: `pip install -e .` then `pip install -r requirements_dev.txt`.
- Run full test + coverage: `pytest -v --cov=src/mysql_to_sqlite3` (tox orchestrates matrix via `tox`).
- Lint suite: `tox -e linters` (runs black, isort, flake8, pylint, bandit, mypy). Ensure new files adhere to 120-char
  lines (black config) and import ordering (isort profile=black).
- Build distribution: `python -m build` or `hatch build`.

## 6. Performance & Reliability Considerations

- Large tables: prefer `--chunk` to bound memory; logic uses `fetchmany(self._chunk_size)` with `executemany` inserts
  for efficiency.
- Reconnection: On `CR_SERVER_LOST` errors during schema creation or data transfer, a single reconnect attempt is made;
  preserve pattern if extending.
- Foreign keys disabled (`PRAGMA foreign_keys=OFF`) during bulk load, then re-enabled in `finally`; ensure any
  early-return path still re-enables.
- Use `INSERT OR IGNORE` to gracefully handle potential duplicates.

## 7. Adding New CLI Flags

1. Add `@click.option` in `cli.py` (keep mutually exclusive logic consistent).
2. Thread parameter through `MySQLtoSQLite` constructor (update typing + tests).
3. Update docs: `README.md` + `docs/index.rst` + optionally changelog.
4. Add at least one unit test exercising the new behavior / validation.

## 8. Error Handling Philosophy

- Fail fast on invalid configuration (raise `click.ClickException` or `UsageError` in CLI, `ValueError` in constructor).
- Swallow and print only when `--debug` not set; with `--debug` re-raise for stack inspection.

## 9. Security & Credentials

- Never log raw passwords. Current design only accepts password via argument or prompt; continue that pattern.
- MySQL SSL can be disabled via `--skip-ssl`; default is encrypted where possible—don’t silently change.

## 10. Common Extension Points

- Type mapping additions: edit `_translate_type_from_mysql_to_sqlite` and corresponding tests.
- Default expression support: `_translate_default_from_mysql_to_sqlite` + tests.
- Progress/UI changes: centralize around `tqdm` usage; respect `--quiet` flag.

## 11. Examples

- Chunked transfer: `mysql2sqlite -f out.db -d db -u user -p -c 50000` (efficient large table copy).
- Subset tables (no FKs): `mysql2sqlite -f out.db -d db -u user -t users orders`.

## 12. PR Expectations

- Include before/after CLI snippet when adding flags.
- Keep coverage steady; add or adapt tests for new branches.
- Update `SECURITY.md` only if security-relevant surface changes (e.g., new credential flag).

---
Questions or unclear areas? Ask which section needs refinement or provide the diff you’re planning; guidance here should
remain minimal but precise.
