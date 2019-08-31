[![GitHub license](https://img.shields.io/github/license/techouse/mysql-to-sqlite3)](https://github.com/techouse/mysql-to-sqlite3/blob/master/LICENSE)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![Build Status](https://travis-ci.org/techouse/mysql-to-sqlite3.svg?branch=master)](https://travis-ci.org/techouse/mysql-to-sqlite3)
[![codecov](https://codecov.io/gh/techouse/mysql-to-sqlite3/branch/master/graph/badge.svg)](https://codecov.io/gh/techouse/mysql-to-sqlite3)

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

Options:
  -f, --sqlite-file PATH     SQLite3 database file  [required]
  -d, --mysql-database TEXT  MySQL database name  [required]
  -u, --mysql-user TEXT      MySQL user  [required]
  -p, --mysql-password TEXT  MySQL password
  -h, --mysql-host TEXT      MySQL host. Defaults to localhost.
  -P, --mysql-port INTEGER   MySQL port. Defaults to 3306.
  -c, --chunk INTEGER        Chunk reading/writing SQL records
  -l, --log-file PATH        Log file
  -V, --vacuum               Use the VACUUM command to rebuild the SQLite
                             database file, repacking it into a minimal amount
                             of disk space
  --use-buffered-cursors     Use MySQLCursorBuffered for reading the MySQL
                             database. This can be useful in situations where
                             multiple queries, with small result sets, need to
                             be combined or computed with each other.
  --help                     Show this message and exit.
```

### Testing
In order to run the test suite run these commands using a Docker MySQL image.

**Requires a running Docker instance!**

- using Python 2.7
```bash
git clone https://github.com/techouse/sqlite3-to-mysql
cd sqlite3-to-mysql
virtualenv -p $(which python2) env
source env/bin/activate
pip install -e .
pip install -r requirements_dev.txt
tox
```

- using Python 3.5+
```bash
git clone https://github.com/techouse/sqlite3-to-mysql
cd sqlite3-to-mysql                   
python3 -m venv env
source env/bin/activate
pip install -e .
pip install -r requirements_dev.txt
tox
```

### Note
Python **3.8** is currently **not** supported because at the moment of writing the most recent 
version of [MySQL Connector/Python](https://pypi.org/project/mysql-connector-python/) **v8.0.17** 
still contains a reference to a [feature deprecated in Python 3.8](https://bugs.python.org/issue1322):
```
mysql/connector/connection.py:126: DeprecationWarning: dist() and linux_distribution() functions are deprecated in Python 3.5
```