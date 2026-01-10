Usage
-----

Options
^^^^^^^

The command line options for the ``mysql2sqlite`` tool are as follows:

.. code-block:: bash

   mysql2sqlite [OPTIONS]

Required Options
""""""""""""""""

- ``-f, --sqlite-file PATH``: SQLite3 database file. This option is required.
- ``-d, --mysql-database TEXT``: MySQL database name. This option is required.
- ``-u, --mysql-user TEXT``: MySQL user. This option is required.

Password Options
""""""""""""""""

- ``-p, --prompt-mysql-password``: Prompt for MySQL password.
- ``--mysql-password TEXT``: MySQL password.

Table Options
"""""""""""""

- ``-t, --mysql-tables TUPLE``: Transfer only these specific tables (space separated table names). Implies --without-foreign-keys which inhibits the transfer of foreign keys. Can not be used together with --exclude-mysql-tables.
- ``-e, --exclude-mysql-tables TUPLE``: Transfer all tables except these specific tables (space separated table names). Implies --without-foreign-keys which inhibits the transfer of foreign keys. Can not be used together with --mysql-tables.

Transfer Options
""""""""""""""""

- ``-T, --mysql-views-as-tables``: Materialize MySQL VIEWs as SQLite tables (legacy behavior).
- ``-L, --limit-rows INTEGER``: Transfer only a limited number of rows from each table.
- ``-C, --collation [BINARY|NOCASE|RTRIM]``: Create datatypes of TEXT affinity using a specified collation sequence. The default is BINARY.
- ``-K, --prefix-indices``: Prefix indices with their corresponding tables. This ensures that their names remain unique across the SQLite database.
- ``-X, --without-foreign-keys``: Do not transfer foreign keys.
- ``-Z, --without-tables``: Do not transfer tables, data only.
- ``-W, --without-data``: Do not transfer table data, DDL only.
- ``-M, --strict``: Create SQLite STRICT tables when supported.

Connection Options
""""""""""""""""""

- ``-h, --mysql-host TEXT``: MySQL host. Defaults to localhost.
- ``-P, --mysql-port INTEGER``: MySQL port. Defaults to 3306.
- ``--mysql-charset TEXT``: MySQL database and table character set. The default is utf8mb4.
- ``--mysql-collation TEXT``: MySQL database and table collation
- ``-S, --skip-ssl``: Disable MySQL connection encryption.

Other Options
"""""""""""""

- ``-c, --chunk INTEGER``: Chunk reading/writing SQL records.
- ``-l, --log-file PATH``: Log file.
- ``--json-as-text``: Transfer JSON columns as TEXT.
- ``-V, --vacuum``: Use the VACUUM command to rebuild the SQLite database file, repacking it into a minimal amount of disk space.
- ``--use-buffered-cursors``: Use MySQLCursorBuffered for reading the MySQL database. This can be useful in situations where multiple queries, with small result sets, need to be combined or computed with each other.
- ``-q, --quiet``: Quiet. Display only errors.
- ``--debug``: Debug mode. Will throw exceptions.
- ``--version``: Show the version and exit.
- ``--help``: Show this message and exit.

Docker
^^^^^^

If you don’t want to install the tool on your system, you can use the
Docker image instead.

.. code:: bash

   docker run -it \
       --workdir $(pwd) \
       --volume $(pwd):$(pwd) \
       --rm ghcr.io/techouse/mysql-to-sqlite3:latest \
       --sqlite-file baz.db \
       --mysql-user foo \
       --mysql-password bar \
       --mysql-database baz \
       --mysql-host host.docker.internal

This will mount your host current working directory (pwd) inside the
Docker container as the current working directory. Any files Docker
would write to the current working directory are written to the host
directory where you did docker run. Note that you have to also use a
`special
hostname <https://docs.docker.com/desktop/networking/#use-cases-and-workarounds-for-all-platforms>`__
``host.docker.internal`` to access your host machine from inside the
Docker container.

Homebrew
^^^^^^^^

If you’re on macOS, you can install the tool using
`Homebrew <https://brew.sh/>`__.

.. code:: bash

   brew install mysql-to-sqlite3
   mysql2sqlite --help
