from unittest.mock import MagicMock, patch

from mysql_to_sqlite3.transporter import MySQLtoSQLite


def _make_base_instance():
    with patch.object(MySQLtoSQLite, "__init__", return_value=None):
        inst = MySQLtoSQLite()  # type: ignore[call-arg]
    inst._mysql_cur_dict = MagicMock()
    inst._mysql_database = "db"
    inst._sqlite_json1_extension_enabled = False
    inst._collation = "BINARY"
    inst._prefix_indices = False
    inst._without_tables = False
    inst._without_foreign_keys = True
    inst._logger = MagicMock()
    inst._sqlite_strict = False
    # Track index names for uniqueness
    inst._seen_sqlite_index_names = set()
    inst._sqlite_index_name_counters = {}
    return inst


def test_show_columns_backticks_are_escaped_in_mysql_query() -> None:
    inst = _make_base_instance()

    # Capture executed SQL
    executed_sql = []

    def capture_execute(sql: str, *args, **kwargs):
        executed_sql.append(sql)

    inst._mysql_cur_dict.execute.side_effect = capture_execute

    # SHOW COLUMNS -> then STATISTICS query
    inst._mysql_cur_dict.fetchall.side_effect = [
        [
            {
                "Field": "id",
                "Type": "INT",
                "Null": "NO",
                "Default": None,
                "Key": "PRI",
                "Extra": "",
            }
        ],
        [],
    ]
    # TABLE collision check -> 0
    inst._mysql_cur_dict.fetchone.return_value = {"count": 0}

    sql = inst._build_create_table_sql("we`ird")
    assert sql.startswith('CREATE TABLE IF NOT EXISTS "we`ird" (')

    # First executed SQL should be SHOW COLUMNS with backticks escaped
    assert executed_sql
    assert executed_sql[0] == "SHOW COLUMNS FROM `we``ird`"


def test_identifiers_with_double_quotes_are_safely_quoted_in_create_and_index() -> None:
    inst = _make_base_instance()
    inst._prefix_indices = True  # ensure an index is emitted with a deterministic name prefix

    # SHOW COLUMNS first call, then STATISTICS rows
    inst._mysql_cur_dict.fetchall.side_effect = [
        [
            {
                "Field": 'na"me',
                "Type": "VARCHAR(10)",
                "Null": "YES",
                "Default": None,
                "Key": "",
                "Extra": "",
            },
        ],
        [
            {
                "name": "idx",
                "primary": 0,
                "unique": 0,
                "auto_increment": 0,
                "columns": 'na"me',
                "types": "VARCHAR(10)",
            }
        ],
    ]
    inst._mysql_cur_dict.fetchone.return_value = {"count": 0}

    sql = inst._build_create_table_sql('ta"ble')

    # Column should be quoted with doubled quotes inside
    assert '"na""me" VARCHAR(10)' in sql or '"na""me" TEXT' in sql

    # Index should quote table and column names with doubled quotes
    assert (
        'CREATE  INDEX IF NOT EXISTS "ta""ble_idx" ON "ta""ble" ("na""me");' in sql
        or 'CREATE  INDEX IF NOT EXISTS "ta""ble_idx" ON "ta""ble" ("na""me")' in sql
    )
