FROM python:3.15-alpine

LABEL maintainer="https://github.com/techouse"

RUN pip install --no-cache-dir mysql-to-sqlite3

ENTRYPOINT ["mysql2sqlite"]