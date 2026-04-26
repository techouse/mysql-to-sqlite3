---
name: mysql-to-sqlite3
description: Use this skill whenever a user wants to transfer, migrate, convert, sample, troubleshoot, or generate commands for moving MySQL or MariaDB schema and data into SQLite using mysql2sqlite. This skill helps gather the required connection details, choose a local or Docker workflow, produce safe copy-pasteable commands, and explain mysql2sqlite caveats.
---

# mysql2sqlite Transfer Assistant

Help users plan and run `mysql2sqlite` transfers from MySQL or MariaDB into a SQLite 3 database file. Focus on user migration outcomes, not on project development.

## Start With Inputs

Before giving a final command, collect any missing details that materially affect the command:

- Destination SQLite file path for `-f` / `--sqlite-file`.
- Source database name for `-d` / `--mysql-database`.
- MySQL/MariaDB user for `-u` / `--mysql-user`.
- Host and port when the server is not local; default to `localhost` and `3306` only when that matches the user's setup.
- Runtime preference: installed CLI, PyPI install, Homebrew install, or Docker image.
- Whether they need a full transfer, schema only, data into an existing schema, selected tables, excluded tables, row-limited sample, SSL, views as tables, STRICT tables, or JSON-as-text behavior.

Do not ask users to paste database passwords. Prefer `-p` / `--prompt-mysql-password` for interactive commands. Use `--mysql-password` only for automation examples, and tell users to provide it through their secret-management mechanism rather than hard-coding it.

## Command Defaults

Use this full-transfer command as the base local pattern:

```bash
mysql2sqlite \
    --sqlite-file ./app.sqlite3 \
    --mysql-database app_db \
    --mysql-user app_user \
    --prompt-mysql-password \
    --mysql-host 127.0.0.1 \
    --mysql-port 3306
```

Use short flags when the user asks for a compact command:

```bash
mysql2sqlite -f ./app.sqlite3 -d app_db -u app_user -p -h 127.0.0.1 -P 3306
```

For Docker, mount the working directory and use `host.docker.internal` when MySQL or MariaDB runs on the host machine.
On Linux Docker Engine, include `--add-host=host.docker.internal:host-gateway` before the image name when the user is on
Linux or says `host.docker.internal` does not resolve:

```bash
docker run -it \
    --rm \
    --workdir "$PWD" \
    --volume "$PWD:$PWD" \
    ghcr.io/techouse/mysql-to-sqlite3:latest \
    -f ./app.sqlite3 \
    -d app_db \
    -u app_user \
    -p \
    -h host.docker.internal
```

If the CLI is not installed, give the installation command that matches the user's platform:

```bash
pip install mysql-to-sqlite3
```

```bash
brew install mysql-to-sqlite3
```

## Recipes

Use these options to adapt the base command:

- Schema only: add `--without-data`.
- Data only into an existing SQLite schema: add `--without-tables`; tell the user the target tables must already exist.
- Selected tables: add `--mysql-tables table_a table_b`; note that foreign keys are not transferred for table subsets.
- Excluded tables: add `--exclude-mysql-tables audit_log temp_imports`; note that foreign keys are not transferred for table subsets.
- Row-limited sample: add `--limit-rows 100`; `0` means no row limit.
- SSL CA verification: add `--mysql-ssl-ca /path/to/ca.pem`.
- Client certificate authentication: add `--mysql-ssl-cert /path/to/client-cert.pem --mysql-ssl-key /path/to/client-key.pem`, usually with `--mysql-ssl-ca`.
- Large transfers: tune `--chunk 50000` when needed and add `--vacuum` if the user wants SQLite repacked after transfer.
- MySQL views: by default, MySQL views become SQLite views; add `--mysql-views-as-tables` only when the user wants materialized tables.
- JSON: add `--json-as-text` when the user wants MySQL/MariaDB JSON columns forced to SQLite `TEXT`.
- SQLite STRICT tables: add `--strict` only when the user's local SQLite is 3.37 or newer.

## Combinations To Check

Warn before producing commands with these invalid or risky combinations:

- `--mysql-tables` and `--exclude-mysql-tables` are mutually exclusive.
- `--mysql-tables` or `--exclude-mysql-tables` disables foreign key transfer.
- `--without-tables` and `--without-data` cannot be used together because there would be nothing to do.
- `--without-tables` alone requires existing target SQLite tables.
- `--skip-ssl` cannot be combined with `--mysql-ssl-ca`, `--mysql-ssl-cert`, or `--mysql-ssl-key`.
- `--mysql-ssl-cert` and `--mysql-ssl-key` must be provided together.
- `--mysql-collation` must belong to the selected `--mysql-charset`.
- `--limit-rows` must be `0` or a positive integer; negative values are invalid.
- `--strict` only creates SQLite STRICT tables on SQLite 3.37 or newer.

## MySQL, MariaDB, And SQLite Notes

Use these notes when users ask about compatibility or results:

- Use the GitHub Actions CI matrix as the source of truth for currently tested MySQL and MariaDB versions.
- MySQL and MariaDB have drifted; defaults, JSON behavior, authentication plugins, and `information_schema` metadata can differ by version.
- Older servers may not support newer native types such as `JSON`.
- MySQL/MariaDB `JSON` maps to SQLite `JSON` only when SQLite JSON1 is available; otherwise it maps to `TEXT`. `--json-as-text` forces `TEXT`.
- `ENUM`, `SET`, unsupported spatial/network-style types, and unknown types fall back to `TEXT`.
- MySQL `TIMESTAMP` columns are represented as SQLite `DATETIME`.
- Users should verify important defaults, collations, JSON columns, views, and foreign keys after transfer.

## Response Shape

For command-generation requests, answer with:

1. A short statement of assumptions, especially host, port, runtime, destination file, and whether `-p` will prompt for the password.
2. One copy-pasteable command.
3. A brief caveats section only for options used in that command.
4. A verification suggestion such as opening the SQLite file with `sqlite3 ./app.sqlite3 ".tables"` or running application-specific checks.

Keep commands concrete. Use placeholders only when the user has not provided a required value, and label them clearly, such as `app_db`, `app_user`, or `/path/to/ca.pem`.
