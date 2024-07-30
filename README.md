[![PyPI](https://img.shields.io/pypi/v/mysql-to-sqlite3)](https://pypi.org/project/mysql-to-sqlite3/)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/mysql-to-sqlite3)](https://pypistats.org/packages/mysql-to-sqlite3)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/mysql-to-sqlite3)](https://pypi.org/project/mysql-to-sqlite3/)
[![MySQL Support](https://img.shields.io/static/v1?label=MySQL&message=5.5+|+5.6+|+5.7+|+8.0+|+8.4&color=2b5d80)](https://img.shields.io/static/v1?label=MySQL&message=5.5+|+5.6+|+5.7+|+8.0+|+8.4&color=2b5d80)
[![MariaDB Support](https://img.shields.io/static/v1?label=MariaDB&message=5.5+|+10.0+|+10.1+|+10.2+|+10.3+|+10.4+|+10.5+|+10.6|+10.11+|+11.4&color=C0765A)](https://img.shields.io/static/v1?label=MariaDB&message=5.5|+10.0+|+10.1+|+10.2+|+10.3+|+10.4+|+10.5|+11.4&color=C0765A)
[![GitHub license](https://img.shields.io/github/license/techouse/mysql-to-sqlite3)](https://github.com/techouse/mysql-to-sqlite3/blob/master/LICENSE)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg)](CODE-OF-CONDUCT.md)
[![PyPI - Format](https://img.shields.io/pypi/format/mysql-to-sqlite3)](https://pypi.org/project/sqlite3-to-mysql/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/64aae8e9599746d58d277852b35cc2bd)](https://www.codacy.com/manual/techouse/mysql-to-sqlite3?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=techouse/mysql-to-sqlite3&amp;utm_campaign=Badge_Grade)
[![Test Status](https://github.com/techouse/mysql-to-sqlite3/actions/workflows/test.yml/badge.svg)](https://github.com/techouse/mysql-to-sqlite3/actions/workflows/test.yml)
[![CodeQL Status](https://github.com/techouse/mysql-to-sqlite3/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/techouse/mysql-to-sqlite3/actions/workflows/codeql-analysis.yml)
[![Publish PyPI Package Status](https://github.com/techouse/mysql-to-sqlite3/actions/workflows/publish.yml/badge.svg)](https://github.com/techouse/mysql-to-sqlite3/actions/workflows/publish.yml)
[![codecov](https://codecov.io/gh/techouse/mysql-to-sqlite3/branch/master/graph/badge.svg)](https://codecov.io/gh/techouse/mysql-to-sqlite3)
[![GitHub Sponsors](https://img.shields.io/github/sponsors/techouse)](https://github.com/sponsors/techouse)
[![GitHub stars](https://img.shields.io/github/stars/techouse/mysql-to-sqlite3.svg?style=social&label=Star&maxAge=2592000)](https://github.com/techouse/mysql-to-sqlite3/stargazers)

# MySQL to SQLite3

#### A simple Python tool to transfer data from MySQL to SQLite 3.

### How to run

```bash
pip install mysql-to-sqlite3
mysql2sqlite --help
```

### Usage

```
Usage: mysql2sqlite [OPTIONS]

Options:
  -f, --sqlite-file PATH          SQLite3 database file  [required]
  -d, --mysql-database TEXT       MySQL database name  [required]
  -u, --mysql-user TEXT           MySQL user  [required]
  -p, --prompt-mysql-password     Prompt for MySQL password
  --mysql-password TEXT           MySQL password
  -t, --mysql-tables TUPLE        Transfer only these specific tables (space
                                  separated table names). Implies --without-
                                  foreign-keys which inhibits the transfer of
                                  foreign keys. Can not be used together with
                                  --exclude-mysql-tables.
  -e, --exclude-mysql-tables TUPLE
                                  Transfer all tables except these specific
                                  tables (space separated table names).
                                  Implies --without-foreign-keys which
                                  inhibits the transfer of foreign keys. Can
                                  not be used together with --mysql-tables.
  -L, --limit-rows INTEGER        Transfer only a limited number of rows from
                                  each table.
  -C, --collation [BINARY|NOCASE|RTRIM]
                                  Create datatypes of TEXT affinity using a
                                  specified collation sequence.  [default:
                                  BINARY]
  -K, --prefix-indices            Prefix indices with their corresponding
                                  tables. This ensures that their names remain
                                  unique across the SQLite database.
  -X, --without-foreign-keys      Do not transfer foreign keys.
  -Z, --without-tables            Do not transfer tables, data only.
  -W, --without-data              Do not transfer table data, DDL only.
  -h, --mysql-host TEXT           MySQL host. Defaults to localhost.
  -P, --mysql-port INTEGER        MySQL port. Defaults to 3306.
  --mysql-charset TEXT            MySQL database and table character set
                                  [default: utf8mb4]
  --mysql-collation TEXT          MySQL database and table collation
  -S, --skip-ssl                  Disable MySQL connection encryption.
  -c, --chunk INTEGER             Chunk reading/writing SQL records
  -l, --log-file PATH             Log file
  --json-as-text                  Transfer JSON columns as TEXT.
  -V, --vacuum                    Use the VACUUM command to rebuild the SQLite
                                  database file, repacking it into a minimal
                                  amount of disk space
  --use-buffered-cursors          Use MySQLCursorBuffered for reading the
                                  MySQL database. This can be useful in
                                  situations where multiple queries, with
                                  small result sets, need to be combined or
                                  computed with each other.
  -q, --quiet                     Quiet. Display only errors.
  --debug                         Debug mode. Will throw exceptions.
  --version                       Show the version and exit.
  --help                          Show this message and exit.
```

#### Docker

If you don't want to install the tool on your system, you can use the Docker image instead.

```bash
docker run -it \
    --workdir $(pwd) \
    --volume $(pwd):$(pwd) \
    --rm ghcr.io/techouse/mysql-to-sqlite3:latest \
    --sqlite-file baz.db \
    --mysql-user foo \
    --mysql-password bar \
    --mysql-database baz \
    --mysql-host host.docker.internal
```

This will mount your host current working directory (pwd) inside the Docker container as the current working directory.
Any files Docker would write to the current working directory are written to the host directory where you did docker
run. Note that you have to also use a
[special hostname](https://docs.docker.com/desktop/networking/#use-cases-and-workarounds-for-all-platforms) `host.docker.internal`
to access your host machine from inside the Docker container.

#### Homebrew

If you're on macOS, you can install the tool using [Homebrew](https://brew.sh/).

```bash
brew tap techouse/mysql-to-sqlite3
brew install mysql-to-sqlite3
mysql2sqlite --help
```
