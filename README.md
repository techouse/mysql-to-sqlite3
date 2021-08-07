[![PyPI](https://img.shields.io/pypi/v/mysql-to-sqlite3)](https://pypi.org/project/mysql-to-sqlite3/)
[![Downloads](https://pepy.tech/badge/mysql-to-sqlite3)](https://pepy.tech/project/mysql-to-sqlite3)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/mysql-to-sqlite3)](https://pypi.org/project/mysql-to-sqlite3/)
[![MySQL Support](https://img.shields.io/static/v1?label=MySQL&message=5.5+|+5.6+|+5.7+|+8.0&color=2b5d80)](https://img.shields.io/static/v1?label=MySQL&message=5.6+|+5.7+|+8.0&color=2b5d80)
[![MariaDB Support](https://img.shields.io/static/v1?label=MariaDB&message=5.5+|+10.0+|+10.1+|+10.2+|+10.3+|+10.4+|+10.5+|+10.6&color=C0765A)](https://img.shields.io/static/v1?label=MariaDB&message=10.0+|+10.1+|+10.2+|+10.3+|+10.4+|+10.5&color=C0765A)
[![GitHub license](https://img.shields.io/github/license/techouse/mysql-to-sqlite3)](https://github.com/techouse/mysql-to-sqlite3/blob/master/LICENSE)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.0-4baaaa.svg)](CODE-OF-CONDUCT.md)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/64aae8e9599746d58d277852b35cc2bd)](https://www.codacy.com/manual/techouse/mysql-to-sqlite3?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=techouse/mysql-to-sqlite3&amp;utm_campaign=Badge_Grade)
[![Build Status](https://github.com/techouse/mysql-to-sqlite3/workflows/Test/badge.svg)](https://github.com/techouse/mysql-to-sqlite3/actions?query=workflow%3ATest)
[![codecov](https://codecov.io/gh/techouse/mysql-to-sqlite3/branch/master/graph/badge.svg)](https://codecov.io/gh/techouse/mysql-to-sqlite3)
[![GitHub stars](https://img.shields.io/github/stars/techouse/mysql-to-sqlite3.svg?style=social&label=Star&maxAge=2592000)](https://github.com/techouse/mysql-to-sqlite3/stargazers)


# MySQL to SQLite3

#### A simple Python tool to transfer data from MySQL to SQLite 3.

This is the long overdue complimentary tool to my [SQLite3 to MySQL](https://github.com/techouse/sqlite3-to-mysql). It 
transfers all data from a MySQL database to a SQLite3 database.

### How to run

```bash
pip install mysql-to-sqlite3
mysql2sqlite --help
```

### Usage
```
Usage: mysql2sqlite [OPTIONS]

  Transfer MySQL to SQLite using the provided CLI options.

Options:
  -f, --sqlite-file PATH          SQLite3 database file  [required]
  -d, --mysql-database TEXT       MySQL database name  [required]
  -u, --mysql-user TEXT           MySQL user  [required]
  -p, --prompt-mysql-password     Prompt for MySQL password
  --mysql-password TEXT           MySQL password
  -t, --mysql-tables TEXT         Transfer only these specific tables (space
                                  separated table names). Implies --without-
                                  foreign-keys which inhibits the transfer of
                                  foreign keys.

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
  -h, --mysql-host TEXT           MySQL host. Defaults to localhost.
  -P, --mysql-port INTEGER        MySQL port. Defaults to 3306.
  -S, --skip-ssl                  Disable MySQL connection encryption.
  -c, --chunk INTEGER             Chunk reading/writing SQL records
  -l, --log-file PATH             Log file
  -V, --vacuum                    Use the VACUUM command to rebuild the SQLite
                                  database file, repacking it into a minimal
                                  amount of disk space

  --use-buffered-cursors          Use MySQLCursorBuffered for reading the
                                  MySQL database. This can be useful in
                                  situations where multiple queries, with
                                  small result sets, need to be combined or
                                  computed with each other.

  -q, --quiet                     Quiet. Display only errors.
  --version                       Show the version and exit.
  --help                          Show this message and exit.
```
