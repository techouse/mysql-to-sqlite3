MySQL to SQLite3
================

A simple Python tool to transfer data from MySQL to SQLite 3

|PyPI| |PyPI - Downloads| |Homebrew Formula Downloads| |PyPI - Python Version|
|MySQL Support| |MariaDB Support| |GitHub license| |Contributor Covenant|
|PyPI - Format| |Code style: black| |Codacy Badge| |Test Status| |CodeQL Status|
|Publish PyPI Package Status| |codecov| |GitHub Sponsors| |GitHub stars|

Installation
------------

.. code:: bash

   pip install mysql-to-sqlite3

Basic Usage
-----------

.. code:: bash

   mysql2sqlite -f path/to/foo.sqlite -d foo_db -u foo_user -p

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
.. |MySQL Support| image:: https://img.shields.io/static/v1?logo=mysql&label=MySQL&message=5.5+%7C+5.6+%7C+5.7+%7C+8.0&color=2b5d80
   :target: https://img.shields.io/static/v1?label=MySQL&message=5.6+%7C+5.7+%7C+8.0&color=2b5d80
.. |MariaDB Support| image:: https://img.shields.io/static/v1?logo=mariadb&label=MariaDB&message=5.5+%7C+10.0+%7C+10.1+%7C+10.2+%7C+10.3+%7C+10.4+%7C+10.5+%7C+10.6%7C+10.11&color=C0765A
   :target: https://img.shields.io/static/v1?label=MariaDB&message=10.0+%7C+10.1+%7C+10.2+%7C+10.3+%7C+10.4+%7C+10.5&color=C0765A
.. |GitHub license| image:: https://img.shields.io/github/license/techouse/mysql-to-sqlite3
   :target: https://github.com/techouse/mysql-to-sqlite3/blob/master/LICENSE
.. |Contributor Covenant| image:: https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg?logo=contributorcovenant
   :target: CODE-OF-CONDUCT.md
.. |PyPI - Format| image:: https://img.shields.io/pypi/format/mysql-to-sqlite3?logo=python
   :target: https://pypi.org/project/sqlite3-to-mysql/
.. |Code style: black| image:: https://img.shields.io/badge/code%20style-black-000000.svg?logo=python
   :target: https://github.com/ambv/black
.. |Codacy Badge| image:: https://api.codacy.com/project/badge/Grade/64aae8e9599746d58d277852b35cc2bd
   :target: https://www.codacy.com/manual/techouse/mysql-to-sqlite3?utm_source=github.com&utm_medium=referral&utm_content=techouse/mysql-to-sqlite3&utm_campaign=Badge_Grade
.. |Test Status| image:: https://github.com/techouse/mysql-to-sqlite3/actions/workflows/test.yml/badge.svg
   :target: https://github.com/techouse/mysql-to-sqlite3/actions/workflows/test.yml
.. |CodeQL Status| image:: https://github.com/techouse/mysql-to-sqlite3/actions/workflows/github-code-scanning/codeql/badge.svg
   :target: https://github.com/techouse/mysql-to-sqlite3/actions/workflows/codeql-analysis.yml
.. |Publish PyPI Package Status| image:: https://github.com/techouse/mysql-to-sqlite3/actions/workflows/publish.yml/badge.svg
   :target: https://github.com/techouse/mysql-to-sqlite3/actions/workflows/publish.yml
.. |codecov| image:: https://codecov.io/gh/techouse/mysql-to-sqlite3/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/techouse/mysql-to-sqlite3
.. |GitHub Sponsors| image:: https://img.shields.io/github/sponsors/techouse?logo=github
   :target: https://github.com/sponsors/techouse
.. |GitHub stars| image:: https://img.shields.io/github/stars/techouse/mysql-to-sqlite3.svg?style=social&label=Star&maxAge=2592000
   :target: https://github.com/techouse/mysql-to-sqlite3/stargazers