[![PyPI](https://img.shields.io/pypi/v/mysql-to-sqlite3?logo=pypi)](https://pypi.org/project/mysql-to-sqlite3/)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/mysql-to-sqlite3?logo=pypi&label=PyPI%20downloads)](https://pypistats.org/packages/mysql-to-sqlite3)
[![Homebrew Formula Downloads](https://img.shields.io/homebrew/installs/dm/mysql-to-sqlite3?logo=homebrew&label=Homebrew%20downloads)](https://formulae.brew.sh/formula/mysql-to-sqlite3)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/mysql-to-sqlite3?logo=python)](https://pypi.org/project/mysql-to-sqlite3/)
[![MySQL Support](https://img.shields.io/static/v1?logo=mysql&label=MySQL&message=5.5%7C5.6%7C5.7%7C8.0%7C8.4&color=2b5d80)](https://github.com/techouse/mysql-to-sqlite3/actions/workflows/test.yml)
[![MariaDB Support](https://img.shields.io/static/v1?logo=mariadb&label=MariaDB&message=5.5%7C10.0%7C10.6%7C10.11%7C11.4%7C11.8&color=C0765A)](https://github.com/techouse/mysql-to-sqlite3/actions/workflows/test.yml)
[![GitHub license](https://img.shields.io/github/license/techouse/mysql-to-sqlite3)](https://github.com/techouse/mysql-to-sqlite3/blob/master/LICENSE)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg?logo=contributorcovenant)](CODE-OF-CONDUCT.md)
[![PyPI - Format](https://img.shields.io/pypi/format/mysql-to-sqlite3?logo=python)](https://pypi.org/project/mysql-to-sqlite3/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg?logo=python)](https://github.com/ambv/black)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/64aae8e9599746d58d277852b35cc2bd)](https://www.codacy.com/manual/techouse/mysql-to-sqlite3?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=techouse/mysql-to-sqlite3&amp;utm_campaign=Badge_Grade)
[![Test Status](https://github.com/techouse/mysql-to-sqlite3/actions/workflows/test.yml/badge.svg)](https://github.com/techouse/mysql-to-sqlite3/actions/workflows/test.yml)
[![CodeQL Status](https://github.com/techouse/mysql-to-sqlite3/actions/workflows/github-code-scanning/codeql/badge.svg)](https://github.com/techouse/mysql-to-sqlite3/actions/workflows/github-code-scanning/codeql)
[![Publish PyPI Package Status](https://github.com/techouse/mysql-to-sqlite3/actions/workflows/publish.yml/badge.svg)](https://github.com/techouse/mysql-to-sqlite3/actions/workflows/publish.yml)
[![codecov](https://codecov.io/gh/techouse/mysql-to-sqlite3/branch/master/graph/badge.svg)](https://codecov.io/gh/techouse/mysql-to-sqlite3)
[![GitHub Sponsors](https://img.shields.io/github/sponsors/techouse?logo=github)](https://github.com/sponsors/techouse)
[![GitHub stars](https://img.shields.io/github/stars/techouse/mysql-to-sqlite3.svg?style=social&label=Star&maxAge=2592000)](https://github.com/techouse/mysql-to-sqlite3/stargazers)

# MySQL to SQLite3

A Python CLI for transferring MySQL or MariaDB schema and data to a SQLite 3 database file.

`mysql2sqlite` reads the source schema from MySQL/MariaDB, creates equivalent SQLite tables, indexes, views, and
foreign keys where possible, then transfers table data into the SQLite file.

## Prerequisites

- Python 3.9 or newer, unless you use the Docker image.
- A reachable MySQL or MariaDB server.
- A MySQL user that can read the source database and its metadata in `information_schema`.
- A writable destination path for the SQLite database file.

See the
[GitHub Actions CI matrix](https://github.com/techouse/mysql-to-sqlite3/blob/master/.github/workflows/test.yml) for
the current MySQL and MariaDB versions tested by the project. Very old server versions are more likely to differ in
type, default-value, authentication, or metadata behavior.

## Installation

Install from PyPI:

```bash
pip install mysql-to-sqlite3
mysql2sqlite --help
```

On macOS, you can also install with Homebrew:

```bash
brew install mysql-to-sqlite3
mysql2sqlite --help
```

Or run the published Docker image:

```bash
docker run --rm ghcr.io/techouse/mysql-to-sqlite3:latest --help
```

## Agent skill

This repo includes an optional agent skill at
[`skills/mysql-to-sqlite3/`](https://github.com/techouse/mysql-to-sqlite3/tree/master/skills/mysql-to-sqlite3) for users
who want Codex or another compatible agent to help prepare a safe `mysql2sqlite` transfer command. The skill is
user-facing: it focuses on migration planning, CLI recipes, password-safe defaults, and MySQL/MariaDB caveats.

## Quick start

Use `-p` / `--prompt-mysql-password` for interactive password entry. This avoids putting the password in shell history
or process listings.

```bash
mysql2sqlite \
    --sqlite-file ./app.sqlite3 \
    --mysql-database app_db \
    --mysql-user app_user \
    --prompt-mysql-password \
    --mysql-host 127.0.0.1 \
    --mysql-port 3306
```

Short options are equivalent:

```bash
mysql2sqlite -f ./app.sqlite3 -d app_db -u app_user -p -h 127.0.0.1 -P 3306
```

For automation, `--mysql-password` is available, but prefer a secret manager or environment-expanded value rather than
typing the password directly into your shell history.

## Common recipes

### Run with Docker

Use `host.docker.internal` when the MySQL server is running on the host machine and the Docker container needs to reach
it. On Linux Docker Engine, add `--add-host=host.docker.internal:host-gateway` before the image name if
`host.docker.internal` is not resolvable.

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

Files written inside the mounted working directory are written back to the host directory.

### Transfer schema only

Create the SQLite tables, indexes, views, and foreign keys without transferring table rows.

```bash
mysql2sqlite -f ./schema.sqlite3 -d app_db -u app_user -p --without-data
```

### Transfer data into an existing SQLite schema

`--without-tables` skips DDL creation and only inserts data. The SQLite tables must already exist.

```bash
mysql2sqlite -f ./app.sqlite3 -d app_db -u app_user -p --without-tables
```

A common two-step flow is:

```bash
mysql2sqlite -f ./app.sqlite3 -d app_db -u app_user -p --without-data
mysql2sqlite -f ./app.sqlite3 -d app_db -u app_user -p --without-tables
```

### Transfer only some tables

Table names are space-separated and are consumed until the next CLI option.

```bash
mysql2sqlite -f ./subset.sqlite3 -d app_db -u app_user -p --mysql-tables users orders invoices
```

Transfer everything except selected tables:

```bash
mysql2sqlite -f ./subset.sqlite3 -d app_db -u app_user -p --exclude-mysql-tables audit_log temp_imports
```

Selecting or excluding tables disables foreign key transfer because the referenced tables may not be present.

### Sample rows from every table

Transfer at most 100 rows from each table:

```bash
mysql2sqlite -f ./sample.sqlite3 -d app_db -u app_user -p --limit-rows 100
```

### Tune large transfers

The CLI fetches and writes rows in batches by default. Use `--chunk` to tune the batch size. `--vacuum` repacks the
SQLite file after the transfer finishes.

```bash
mysql2sqlite -f ./app.sqlite3 -d app_db -u app_user -p --chunk 50000 --vacuum
```

### Use SSL certificates

Verify the server certificate with a CA file:

```bash
mysql2sqlite -f ./app.sqlite3 -d app_db -u app_user -p --mysql-ssl-ca /path/to/ca.pem
```

Use a client certificate and key:

```bash
mysql2sqlite \
    -f ./app.sqlite3 \
    -d app_db \
    -u app_user \
    -p \
    --mysql-ssl-ca /path/to/ca.pem \
    --mysql-ssl-cert /path/to/client-cert.pem \
    --mysql-ssl-key /path/to/client-key.pem
```

Use `--skip-ssl` only when you explicitly need to disable MySQL connection encryption.

## Options at a glance

| Option | Purpose |
| --- | --- |
| `-f`, `--sqlite-file PATH` | Destination SQLite database file. Required. |
| `-d`, `--mysql-database TEXT` | Source MySQL/MariaDB database name. Required. |
| `-u`, `--mysql-user TEXT` | MySQL/MariaDB user. Required. |
| `-p`, `--prompt-mysql-password` | Prompt for the MySQL password. Preferred for interactive use. |
| `--mysql-password TEXT` | Provide the MySQL password directly. Useful for automation, but handle carefully. |
| `-h`, `--mysql-host TEXT` | MySQL host. Defaults to `localhost`. |
| `-P`, `--mysql-port INTEGER` | MySQL port. Defaults to `3306`. |
| `-t`, `--mysql-tables TUPLE` | Transfer only the listed tables. Implies no foreign key transfer. |
| `-e`, `--exclude-mysql-tables TUPLE` | Transfer every table except the listed tables. Implies no foreign key transfer. |
| `-T`, `--mysql-views-as-tables` | Materialize MySQL views as SQLite tables instead of creating SQLite views. |
| `-L`, `--limit-rows INTEGER` | Transfer at most this many rows from each table. `0` means no limit. |
| `-C`, `--collation [BINARY\|NOCASE\|RTRIM]` | Add a SQLite collation to text-affinity columns. Defaults to `BINARY`. |
| `-K`, `--prefix-indices` | Prefix SQLite index names with their table names. |
| `-X`, `--without-foreign-keys` | Do not create foreign keys in the SQLite schema. |
| `-Z`, `--without-tables` | Skip table/view creation and transfer data only. |
| `-W`, `--without-data` | Create schema only and skip table data. |
| `-M`, `--strict` | Create SQLite STRICT tables when the local SQLite version supports them. |
| `--mysql-charset TEXT` | MySQL database and table character set. Defaults to `utf8mb4`. |
| `--mysql-collation TEXT` | MySQL database and table collation. Must belong to the selected charset. |
| `--mysql-ssl-ca PATH` | Path to an SSL CA certificate file. |
| `--mysql-ssl-cert PATH` | Path to an SSL client certificate file. Must be paired with `--mysql-ssl-key`. |
| `--mysql-ssl-key PATH` | Path to an SSL client key file. Must be paired with `--mysql-ssl-cert`. |
| `-S`, `--skip-ssl` | Disable MySQL connection encryption. Cannot be used with SSL certificate options. |
| `-c`, `--chunk INTEGER` | Read and write SQL records in batches. Defaults to `200000`. |
| `-l`, `--log-file PATH` | Write logs to a file. |
| `--json-as-text` | Force MySQL/MariaDB JSON columns to SQLite `TEXT`. |
| `-V`, `--vacuum` | Run SQLite `VACUUM` after transfer. |
| `--use-buffered-cursors` | Use buffered MySQL cursors. |
| `-q`, `--quiet` | Show only errors after the initial command banner. |
| `--debug` | Re-raise exceptions for debugging instead of printing friendly errors. |
| `--version` | Show environment and dependency versions. |
| `--help` | Show CLI help. |

## Combinations and caveats

- `--mysql-tables` and `--exclude-mysql-tables` are mutually exclusive.
- `--mysql-tables` or `--exclude-mysql-tables` automatically disables foreign key transfer.
- `--without-tables` and `--without-data` cannot be used together because there would be nothing to do.
- `--without-tables` requires the destination SQLite schema to already exist.
- `--skip-ssl` cannot be combined with `--mysql-ssl-ca`, `--mysql-ssl-cert`, or `--mysql-ssl-key`.
- `--mysql-ssl-cert` and `--mysql-ssl-key` must be provided together.
- `--mysql-collation` must be valid for the selected `--mysql-charset`.
- `--limit-rows` must be `0` or a positive integer. `0` means no limit.
- `--strict` requires SQLite 3.37 or newer. If SQLite rejects a STRICT schema, rerun without `--strict`.
- MySQL views become SQLite views by default. Use `--mysql-views-as-tables` for the older materialized-table behavior.

## MySQL, MariaDB, and SQLite notes

- MySQL and MariaDB are similar but not identical. Default expressions, generated defaults, authentication plugins, JSON
  behavior, and metadata returned from `information_schema` can differ by server family and version.
- Older legacy servers may not support newer column types such as native `JSON`.
- MySQL/MariaDB `JSON` columns map to SQLite `JSON` only when this tool detects SQLite JSON1 support. Otherwise they
  map to `TEXT`. Use `--json-as-text` to force `TEXT`.
- `ENUM`, `SET`, unsupported spatial/network-style types, and unknown types fall back to `TEXT`.
- MySQL `TIMESTAMP` columns are represented as SQLite `DATETIME`.
- Unsigned integer types are converted to their signed SQLite-compatible type names.
- Table names, column names, and index names are quoted for SQLite. Duplicate SQLite index names are made unique, and
  `--prefix-indices` can make this behavior explicit.
- After transfer, verify schema details that are important to your application, especially defaults, collations, JSON
  columns, views, and foreign keys.
