MySQL to SQLite3
================

A Python CLI for transferring MySQL or MariaDB schema and data to a SQLite 3
database file.

|PyPI| |PyPI - Downloads| |Homebrew Formula Downloads| |PyPI - Python Version|
|MySQL Support| |MariaDB Support| |GitHub license| |Contributor Covenant|
|PyPI - Format| |Code style: black| |Codacy Badge| |Test Status| |CodeQL Status|
|Publish PyPI Package Status| |codecov| |GitHub Sponsors| |GitHub stars|

Installation
------------

.. code-block:: bash

   pip install mysql-to-sqlite3

Basic Usage
-----------

Use the password prompt for interactive use:

.. code-block:: bash

   mysql2sqlite -f ./app.sqlite3 -d app_db -u app_user -p -h 127.0.0.1 -P 3306

Tested Databases
----------------

See the `GitHub Actions CI matrix
<https://github.com/techouse/mysql-to-sqlite3/blob/master/.github/workflows/test.yml>`__
for the current MySQL and MariaDB versions tested by the project.

Common Tasks
------------

- Use ``--without-data`` to create schema only.
- Use ``--without-tables`` to transfer data into an existing SQLite schema.
- Use ``--mysql-tables`` or ``--exclude-mysql-tables`` to transfer a table
  subset; this disables foreign key transfer.
- Use ``--mysql-ssl-ca``, ``--mysql-ssl-cert``, and ``--mysql-ssl-key`` for
  certificate-based MySQL connections.

See :doc:`README` for full recipes, option notes, and MySQL/MariaDB caveats.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   README
   modules

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. |PyPI| image:: https://img.shields.io/pypi/v/mysql-to-sqlite3?logo=pypi
   :target: https://pypi.org/project/mysql-to-sqlite3/
.. |PyPI - Downloads| image:: https://img.shields.io/pypi/dm/mysql-to-sqlite3?logo=pypi&label=PyPI%20downloads
   :target: https://pypistats.org/packages/mysql-to-sqlite3
.. |Homebrew Formula Downloads| image:: https://img.shields.io/homebrew/installs/dm/mysql-to-sqlite3?logo=homebrew&label=Homebrew%20downloads
   :target: https://formulae.brew.sh/formula/mysql-to-sqlite3
.. |PyPI - Python Version| image:: https://img.shields.io/pypi/pyversions/mysql-to-sqlite3?logo=python
   :target: https://pypi.org/project/mysql-to-sqlite3/
.. |MySQL Support| image:: https://img.shields.io/static/v1?logo=mysql&label=MySQL&message=5.5%7C5.6%7C5.7%7C8.0%7C8.4%7C9.7&color=2b5d80
   :target: https://github.com/techouse/mysql-to-sqlite3/actions/workflows/test.yml
.. |MariaDB Support| image:: https://img.shields.io/static/v1?logo=mariadb&label=MariaDB&message=5.5%7C10.0%7C10.6%7C10.11%7C11.4%7C11.8&color=C0765A
   :target: https://github.com/techouse/mysql-to-sqlite3/actions/workflows/test.yml
.. |GitHub license| image:: https://img.shields.io/github/license/techouse/mysql-to-sqlite3
   :target: https://github.com/techouse/mysql-to-sqlite3/blob/master/LICENSE
.. |Contributor Covenant| image:: https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg?logo=contributorcovenant
   :target: CODE-OF-CONDUCT.md
.. |PyPI - Format| image:: https://img.shields.io/pypi/format/mysql-to-sqlite3?logo=python
   :target: https://pypi.org/project/mysql-to-sqlite3/
.. |Code style: black| image:: https://img.shields.io/badge/code%20style-black-000000.svg?logo=python
   :target: https://github.com/ambv/black
.. |Codacy Badge| image:: https://api.codacy.com/project/badge/Grade/64aae8e9599746d58d277852b35cc2bd
   :target: https://www.codacy.com/manual/techouse/mysql-to-sqlite3?utm_source=github.com&utm_medium=referral&utm_content=techouse/mysql-to-sqlite3&utm_campaign=Badge_Grade
.. |Test Status| image:: https://github.com/techouse/mysql-to-sqlite3/actions/workflows/test.yml/badge.svg
   :target: https://github.com/techouse/mysql-to-sqlite3/actions/workflows/test.yml
.. |CodeQL Status| image:: https://github.com/techouse/mysql-to-sqlite3/actions/workflows/github-code-scanning/codeql/badge.svg
   :target: https://github.com/techouse/mysql-to-sqlite3/actions/workflows/github-code-scanning/codeql
.. |Publish PyPI Package Status| image:: https://github.com/techouse/mysql-to-sqlite3/actions/workflows/publish.yml/badge.svg
   :target: https://github.com/techouse/mysql-to-sqlite3/actions/workflows/publish.yml
.. |codecov| image:: https://codecov.io/gh/techouse/mysql-to-sqlite3/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/techouse/mysql-to-sqlite3
.. |GitHub Sponsors| image:: https://img.shields.io/github/sponsors/techouse?logo=github
   :target: https://github.com/sponsors/techouse
.. |GitHub stars| image:: https://img.shields.io/github/stars/techouse/mysql-to-sqlite3.svg?style=social&label=Star&maxAge=2592000
   :target: https://github.com/techouse/mysql-to-sqlite3/stargazers
