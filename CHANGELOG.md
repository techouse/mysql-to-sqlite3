# 2.3.0

* [FEAT] add MySQL 8.4 and MariaDB 11.4 support

# 2.2.2

* [FIX] use `dateutil.parse` to parse SQLite dates

# 2.2.1

* [FIX] fix transferring composite primary keys when AUTOINCREMENT present

# 2.2.0

* [FEAT] add --without-tables option

# 2.1.12

* [CHORE] update MySQL Connector/Python to 8.4.0
* [CHORE] add Sphinx documentation

# 2.1.11

* [CHORE] migrate package from flat layout to src layout

# 2.1.10

* [FEAT] add support for AUTOINCREMENT

# 2.1.9

* [FIX] pin MySQL Connector/Python to 8.3.0

# 2.1.8

* [FIX] ensure index names do not collide with table names

# 2.1.7

* [FIX] use more precise foreign key constraints

# 2.1.6

* [FEAT] build both linux/amd64 and linux/arm64 Docker images

# 2.1.5

* [CHORE] fix Docker package publishing from Github Workflow

# 2.1.4

* [FIX] fix invalid column_type error message

# 2.1.3

* [CHORE] maintenance release to publish first containerized release

# 2.1.2

* [FIX] throw more comprehensive error messages when translating column types

# 2.1.1

* [CHORE] add support for Python 3.12
* [CHORE] bump minimum version of MySQL Connector/Python to 8.2.0

# 2.1.0

* [CHORE] drop support for Python 3.7

# 2.0.3

* [FIX] import MySQLConnectionAbstract instead of concrete implementations

# 2.0.2

* [FIX] properly import CMySQLConnection

# 2.0.1

* [FEAT] add support for MySQL character set introducers in DEFAULT clause

# 2.0.0

* [CHORE] drop support for Python 2.7, 3.5 and 3.6
* [CHORE] migrate pytest.ini configuration into pyproject.toml
* [CHORE] migrate from setuptools to hatch / hatchling
* [CHORE] update dependencies
* [CHORE] add types
* [CHORE] add types to tests
* [CHORE] update dependencies
* [CHORE] use f-strings where appropriate

# 1.4.18

* [CHORE] update dependencies
* [CHORE] use [black](https://github.com/psf/black) and [isort](https://github.com/PyCQA/isort) in tox linters

# 1.4.17

* [CHORE] migrate from setup.py to pyproject.toml
* [CHORE] update the publishing workflow

# 1.4.16

* [CHORE] add MariaDB 10.11 CI tests
* [CHORE] add Python 3.11 support

# 1.4.15

* [FIX] fix BLOB default value
* [CHORE] remove CI tests for Python 3.5, 3.6, add tests for Python 3.11

# 1.4.14

* [FIX] pin mysql-connector-python to <8.0.30
* [CHORE] update CI actions/checkout to v3
* [CHORE] update CI actions/setup-python to v4
* [CHORE] update CI actions/cache to v3
* [CHORE] update CI github/codeql-action/init to v2
* [CHORE] update CI github/codeql-action/analyze to v2

# 1.4.13

* [FEAT] add option to exclude specific MySQL tables
* [CHORE] update CI codecov/codecov-action to v2

# 1.4.12

* [FIX] fix SQLite convert_date converter
* [CHORE] update tests

# 1.4.11

* [FIX] pin python-slugify to <6.0.0

# 1.4.10

* [FEAT] add feature to transfer tables without any data (DDL only)

# 1.4.9

* [CHORE] add Python 3.10 support
* [CHORE] add Python 3.10 tests

# 1.4.8

* [FEAT] transfer JSON columns as JSON

# 1.4.7

* [CHORE] add experimental tests for Python 3.10-dev
* [CHORE] add tests for MariaDB 10.6

# 1.4.6

* [FIX] pin Click to <8.0

# 1.4.5

* [FEAT] add -K, --prefix-indices CLI option to prefix indices with table names. This used to be the default behavior
  until now. To keep the old behavior simply use this CLI option.

# 1.4.4

* [FEAT] add --limit-rows CLI option
* [FEAT] add --collation CLI option to specify SQLite collation sequence

# 1.4.3

* [FIX] pin python-tabulate to <0.8.6 for Python 3.4 or less
* [FIX] pin python-slugify to <5.0.0 for Python 3.5 or less
* [FIX] pin Click to 7.x for Python 3.5 or less

# 1.4.2

* [FIX] fix default column value not getting converted

# 1.4.1

* [FIX] get table list error when Click package is 8.0+

# 1.4.0

* [FEAT] add password prompt. This changes the default behavior of -p
* [FEAT] add option to disable MySQL connection encryption
* [FEAT] add non-chunked progress bar
* [FIX] pin mysql-connector-python to <8.0.24 for Python 3.5 or lower
* [FIX] require sqlalchemy <1.4.0 to make compatible with sqlalchemy-utils

# 1.3.8

* [FIX] some MySQL integer column definitions result in TEXT fields in sqlite3
* [FIX] fix CI tests

# 1.3.7

* [CHORE] transition from Travis CI to GitHub Actions

# 1.3.6

* [FIX] Fix Python 3.9 tests

# 1.3.5

* [FIX] add IF NOT EXISTS to the CREATE INDEX SQL command
* [CHORE] add Python 3.9 CI tests

# 1.3.4

* [FEAT] add --quiet option

# 1.3.3

* [FIX] test for mysql client more gracefully

# 1.3.2

* [FEAT] simpler access to the debug version info using the --version switch
* [FEAT] add debug_info module to be used in bug reports
* [CHORE] remove PyPy and PyPy3 CI tests
* [CHORE] add tabulate to development dependencies
* [CHORE] use pytest fixture fom Faker 4.1.0 in Python 3 tests
* [CHORE] omit debug_info.py in coverage reports

# 1.3.1

* [FIX] fix information_schema issue introduced with MySQL 8.0.21
* [FIX] fix MySQL 8 bug where column types would sometimes be returned as bytes instead of strings
* [FIX] sqlalchemy-utils dropped Python 2.7 support in v0.36.7
* [CHORE] use MySQL Client instead of PyMySQL in tests
* [CHORE] add MySQL version output to CI tests
* [CHORE] add Python 3.9 to the CI tests
* [CHORE] add MariaDB 10.5 to the CI tests
* [CHORE] remove Python 2.7 from allowed CI test failures
* [CHORE] use Ubuntu Bionic instead of Ubuntu Xenial in CI tests
* [CHORE] use Ubuntu Xenial only for MariaDB 10.4 CI tests
* [CHORE] test legacy databases in CI tests

# 1.3.0

* [FEAT] add option to transfer only specific tables using -t
* [CHORE] add tests for transferring only certain tables

# 1.2.11

* [FIX] duplicate foreign keys

# 1.2.10

* [FIX] properly escape SQLite index names
* [FIX] fix SQLite global index name scoping
* [CHORE] test the successful transfer of an unorthodox table name
* [CHORE] test the successful transfer of indices with same names

# 1.2.9

* [FIX] differentiate better between MySQL and SQLite errors
* [CHORE] add Python 3.8 and 3.8-dev test build

# 1.2.8

* [CHORE] add support for Python 3.8
* [CHORE] update mysql-connector-python to a minimum version of 8.0.18 to support Python 3.8
* [CHORE] update development dependencies
* [CHORE] add [bandit](https://github.com/PyCQA/bandit) tests

# 1.2.7

* [FEAT] transfer unique indices
* [FIX] improve index transport
* [CHORE] test transfer of indices

# 1.2.6

* [CHORE] include tests in the PyPI package

# 1.2.5

* [FEAT] transfer foreign keys
* [CHORE] removed duplicate import in test database models

# 1.2.4

* [CHORE] reformat MySQLtoSQLite constructor
* [CHORE] reformat translator function
* [CHORE] add more tests

# 1.2.3

* [CHORE] add more tests

# 1.2.2

* [CHORE] refactor package
* [CHORE] fix CI tests
* [CHORE] add linter rules

# 1.2.1

* [FEAT] add Python 2.7 support

# 1.2.0

* [CHORE] add CI tests
* [CHORE] achieve 100% test coverage

# 1.1.2

* [FIX] fix error of transferring tables without primary keys
* [FIX] fix error of transferring empty tables

# 1.1.1

* [FEAT] add option to use MySQLCursorBuffered cursors
* [FEAT] add MySQL port
* [FEAT] update --help hints
* [FIX] fix slugify import
* [FIX] cursor error

# 1.1.0

* [FEAT] add VACUUM option

# 1.0.0

Initial commit
