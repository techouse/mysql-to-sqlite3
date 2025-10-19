import pytest

from mysql_to_sqlite3.transporter import MySQLtoSQLite


class TestSqlglotAugmentedTypeTranslation:
    @pytest.mark.parametrize("mysql_type", ["double precision", "DOUBLE PRECISION", "DoUbLe PrEcIsIoN"]) 
    def test_double_precision_maps_to_numeric_type(self, mysql_type: str) -> None:
        # Prior mapper would resolve this to TEXT; sqlglot fallback should improve it
        out = MySQLtoSQLite._translate_type_from_mysql_to_sqlite(mysql_type)
        assert out in {"DOUBLE", "REAL"}

    def test_fixed_maps_to_decimal(self) -> None:
        out = MySQLtoSQLite._translate_type_from_mysql_to_sqlite("fixed(10,2)")
        # Normalize to DECIMAL (without length) to match existing style
        assert out == "DECIMAL"

    def test_character_varying_keeps_length_as_varchar(self) -> None:
        out = MySQLtoSQLite._translate_type_from_mysql_to_sqlite("character varying(20)")
        assert out == "VARCHAR(20)"

    def test_char_varying_keeps_length_as_varchar(self) -> None:
        out = MySQLtoSQLite._translate_type_from_mysql_to_sqlite("char varying(12)")
        assert out == "VARCHAR(12)"

    def test_national_character_varying_maps_to_nvarchar(self) -> None:
        out = MySQLtoSQLite._translate_type_from_mysql_to_sqlite("national character varying(15)")
        assert out == "NVARCHAR(15)"

    def test_national_character_maps_to_nchar(self) -> None:
        out = MySQLtoSQLite._translate_type_from_mysql_to_sqlite("national character(5)")
        assert out == "NCHAR(5)"

    @pytest.mark.parametrize(
        "mysql_type,expected",
        [
            ("int unsigned", "INTEGER"),
            ("mediumint unsigned", "MEDIUMINT"),
            ("smallint unsigned", "SMALLINT"),
            ("tinyint unsigned", "TINYINT"),
            ("bigint unsigned", "BIGINT"),
        ],
    )
    def test_unsigned_variants_strip_unsigned(self, mysql_type: str, expected: str) -> None:
        out = MySQLtoSQLite._translate_type_from_mysql_to_sqlite(mysql_type)
        assert out == expected

    def test_timestamp_maps_to_datetime(self) -> None:
        out = MySQLtoSQLite._translate_type_from_mysql_to_sqlite("timestamp")
        assert out == "DATETIME"

    def test_varbinary_and_blobs_map_to_blob(self) -> None:
        assert MySQLtoSQLite._translate_type_from_mysql_to_sqlite("varbinary(16)") == "BLOB"
        assert MySQLtoSQLite._translate_type_from_mysql_to_sqlite("mediumblob") == "BLOB"

    def test_char_maps_to_character_with_length(self) -> None:
        out = MySQLtoSQLite._translate_type_from_mysql_to_sqlite("char(3)")
        assert out == "CHARACTER(3)"

    def test_json_mapping_respects_json1(self) -> None:
        assert (
            MySQLtoSQLite._translate_type_from_mysql_to_sqlite("json", sqlite_json1_extension_enabled=False) == "TEXT"
        )
        assert (
            MySQLtoSQLite._translate_type_from_mysql_to_sqlite("json", sqlite_json1_extension_enabled=True) == "JSON"
        )

    def test_fallback_to_text_on_unknown_type(self) -> None:
        out = MySQLtoSQLite._translate_type_from_mysql_to_sqlite("geography")
        assert out == "TEXT"

    def test_enum_remains_text(self) -> None:
        out = MySQLtoSQLite._translate_type_from_mysql_to_sqlite("enum('a','b')")
        assert out == "TEXT"
