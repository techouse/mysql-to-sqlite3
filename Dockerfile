FROM python:3.12

LABEL maintainer="https://github.com/techouse"

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir mysql-to-sqlite3

ENTRYPOINT ["mysql2sqlite"]