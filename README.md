![GitHub tag (latest by date)](https://img.shields.io/github/v/tag/techouse/mysql-to-sqlite3)
[![GitHub license](https://img.shields.io/github/license/techouse/mysql-to-sqlite3)](https://github.com/techouse/mysql-to-sqlite3/blob/master/LICENSE)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![Build Status](https://travis-ci.org/techouse/mysql-to-sqlite3.svg?branch=master)](https://travis-ci.org/techouse/mysql-to-sqlite3)
[![codecov](https://codecov.io/gh/techouse/mysql-to-sqlite3/branch/master/graph/badge.svg)](https://codecov.io/gh/techouse/mysql-to-sqlite3)

# MySQL to SQLite3

#### A simple Python script/class to transfer data from MySQL to SQLite 3.

This is the long overdue complimentary tool to my [SQLite3 to MySQL](https://github.com/techouse/sqlite3-to-mysql). It 
transfers all data from a MySQL database to a SQLite3 database.

#### Tested to work on Python 2.7 and 3.5+

### How to run

- using Python 2.7
```bash
virtualenv -p $(which python2) env
source env/bin/activate
pip install -r requirements.txt
python mysql2sqlite.py -h
```

- using Python 3.5+
```bash
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
python mysql2sqlite.py -h
```

### Usage
```
usage: mysql2sqlite.py [-h] [-f SQLITE_FILE] [-u MYSQL_USER]
                       [-p MYSQL_PASSWORD] [-d MYSQL_DATABASE]
                       [--mysql-host MYSQL_HOST] [--mysql-port MYSQL_PORT]
                       [-c CHUNK] [-l LOG_FILE] [-V] [--use-buffered-cursors]

optional arguments:
  -h, --help            show this help message and exit
  -f SQLITE_FILE, --sqlite-file SQLITE_FILE
                        SQLite3 db file
  -u MYSQL_USER, --mysql-user MYSQL_USER
                        MySQL user
  -p MYSQL_PASSWORD, --mysql-password MYSQL_PASSWORD
                        MySQL password
  -d MYSQL_DATABASE, --mysql-database MYSQL_DATABASE
                        MySQL database name
  --mysql-host MYSQL_HOST
                        MySQL host (default: localhost)
  --mysql-port MYSQL_PORT
                        MySQL port (default: 3306)
  -c CHUNK, --chunk CHUNK
                        Chunk reading/writing SQL records
  -l LOG_FILE, --log-file LOG_FILE
                        Log file
  -V, --vacuum          Use the VACUUM command to rebuild the SQLite database
                        file, repacking it into a minimal amount of disk space
  --use-buffered-cursors
                        Use MySQLCursorBuffered for reading the MySQL
                        database. This can be useful in situations where
                        multiple queries, with small result sets, need to be
                        combined or computed with each other.
```

### Testing
In order to run the test suite run these commands using a Docker MySQL image.

**Requires a running Docker instance!**

- using Python 2.7
```bash
virtualenv -p $(which python2) env
source env/bin/activate
pip install -r requirements-test.txt
pytest --cov=src
```

- using Python 3.5+
```bash
python3 -m venv env
source env/bin/activate
pip install -r requirements-test.txt
pytest --cov=src
```