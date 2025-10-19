from unittest.mock import MagicMock, patch

from mysql_to_sqlite3.transporter import MySQLtoSQLite


def _make_instance_with_mocks():
    with patch.object(MySQLtoSQLite, "__init__", return_value=None):
        instance = MySQLtoSQLite()  # type: ignore[call-arg]
    instance._mysql_cur_dict = MagicMock()
    instance._mysql_database = "db"
    instance._sqlite_json1_extension_enabled = False
    instance._collation = "BINARY"
    instance._prefix_indices = False
    instance._without_tables = False
    instance._without_foreign_keys = True
    instance._logger = MagicMock()
    instance._sqlite_strict = False
    # Track index names for uniqueness
    instance._seen_sqlite_index_names = set()
    instance._sqlite_index_name_counters = {}
    return instance


def test_build_create_table_sql_prefix_indices_true_prefixes_index_names() -> None:
    inst = _make_instance_with_mocks()
    inst._prefix_indices = True

    # SHOW COLUMNS
    inst._mysql_cur_dict.fetchall.side_effect = [
        [
            {"Field": "id", "Type": "INT", "Null": "NO", "Default": None, "Key": "PRI", "Extra": ""},
            {"Field": "name", "Type": "VARCHAR(10)", "Null": "YES", "Default": None, "Key": "", "Extra": ""},
        ],
        # STATISTICS rows
        [
            {
                "name": "idx_name",
                "primary": 0,
                "unique": 0,
                "auto_increment": 0,
                "columns": "name",
                "types": "VARCHAR(10)",
            }
        ],
    ]
    # TABLE collision check -> 0
    inst._mysql_cur_dict.fetchone.return_value = {"count": 0}

    sql = inst._build_create_table_sql("users")

    # With prefix_indices=True, the index name should be prefixed with table name
    assert 'CREATE INDEX IF NOT EXISTS "users_idx_name" ON "users" ("name");' in sql


def test_build_create_table_sql_collision_renamed_and_uniqueness_suffix() -> None:
    inst = _make_instance_with_mocks()
    inst._prefix_indices = False

    # Pre-mark an index name as already used globally to force suffixing
    inst._seen_sqlite_index_names.add("dup")

    # SHOW COLUMNS
    inst._mysql_cur_dict.fetchall.side_effect = [
        [
            {"Field": "id", "Type": "INT", "Null": "NO", "Default": None, "Key": "", "Extra": ""},
        ],
        # STATISTICS rows
        [
            {
                "name": "dup",  # collides globally
                "primary": 0,
                "unique": 1,
                "auto_increment": 0,
                "columns": "id",
                "types": "INT",
            }
        ],
    ]
    # TABLE collision check -> 1 so we also prefix with table name before uniqueness
    inst._mysql_cur_dict.fetchone.return_value = {"count": 1}

    sql = inst._build_create_table_sql("accounts")

    # Proposed becomes accounts_dup, and since dup already used, unique name stays accounts_dup (no clash)
    # or if accounts_dup was in the seen set, it would become accounts_dup_2. We only asserted the presence of accounts_ prefix.
    assert 'CREATE UNIQUE INDEX IF NOT EXISTS "accounts_dup" ON "accounts" ("id");' in sql
