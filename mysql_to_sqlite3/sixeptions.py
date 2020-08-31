"""Wrappers for exceptions that exist in Python 3 are missing in Python 2."""


class ConnectionError(IOError):
    """Substitute missing exception for Python 2."""

    pass
