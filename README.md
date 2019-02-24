# MySQL to SQLite3

#### A simple Python 3 script/class to transfer data from MySQL to SQLite 3.

This is the long overdue complimentary tool to my [SQLite3 to MySQL](https://github.com/techouse/sqlite3-to-mysql). It 
transfers all data from a MySQL database to a SQLite3 database.

### Installation
```bash
git clone https://github.com/techouse/mysql-to-sqlite3
cd mysql-to-sqlite3
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
python mysql2sqlite.py -h
```

### Usage
```
usage: mysql2sqlite.py [-h] [-f SQLITE_FILE] [-u MYSQL_USER]
                       [-p MYSQL_PASSWORD] [-d MYSQL_DATABASE]
                       [--mysql-host MYSQL_HOST] [-c CHUNK] [-l LOG_FILE]

optional arguments:
  -h, --help            show this help message and exit
  -f SQLITE_FILE, --sqlite-file SQLITE_FILE
                        SQLite3 db file
  -u MYSQL_USER, --mysql-user MYSQL_USER
                        MySQL user
  -p MYSQL_PASSWORD, --mysql-password MYSQL_PASSWORD
                        MySQL password
  -d MYSQL_DATABASE, --mysql-database MYSQL_DATABASE
                        MySQL host
  --mysql-host MYSQL_HOST
                        MySQL host
  -c CHUNK, --chunk CHUNK
                        Chunk reading/writing SQL records
  -l LOG_FILE, --log-file LOG_FILE
                        Log file

```