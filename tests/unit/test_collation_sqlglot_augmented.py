import pytest

from mysql_to_sqlite3.sqlite_utils import CollatingSequences
from mysql_to_sqlite3.transporter import MySQLtoSQLite


class TestCollationSqlglotAugmented:
    @pytest.mark.parametrize(
        "mysql_type",
        [
            "char varying(12)",
            "CHARACTER VARYING(12)",
        ],
    )
    def test_collation_applied_for_char_varying_synonyms(self, mysql_type: str) -> None:
        out = MySQLtoSQLite._data_type_collation_sequence(collation=CollatingSequences.NOCASE, column_type=mysql_type)
        assert out == f"COLLATE {CollatingSequences.NOCASE}"

    def test_collation_applied_for_national_character_varying(self) -> None:
        out = MySQLtoSQLite._data_type_collation_sequence(
            collation=CollatingSequences.NOCASE, column_type="national character varying(15)"
        )
        assert out == f"COLLATE {CollatingSequences.NOCASE}"

    def test_no_collation_for_json(self) -> None:
        # Regardless of case or synonym handling, JSON should not have collation applied
        assert (
            MySQLtoSQLite._data_type_collation_sequence(collation=CollatingSequences.NOCASE, column_type="json") == ""
        )

    def test_no_collation_when_binary_collation(self) -> None:
        # BINARY collation disables COLLATE clause entirely
        assert (
            MySQLtoSQLite._data_type_collation_sequence(collation=CollatingSequences.BINARY, column_type="VARCHAR(10)")
            == ""
        )

    @pytest.mark.parametrize(
        "numeric_synonym",
        [
            "double precision",
            "FIXED(10,2)",
        ],
    )
    def test_no_collation_for_numeric_synonyms(self, numeric_synonym: str) -> None:
        assert (
            MySQLtoSQLite._data_type_collation_sequence(
                collation=CollatingSequences.NOCASE, column_type=numeric_synonym
            )
            == ""
        )
