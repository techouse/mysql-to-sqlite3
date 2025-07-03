"""Unit tests for the mysql_utils module."""

import typing as t
from unittest import mock
from unittest.mock import MagicMock

from mysql_to_sqlite3.mysql_utils import (
    CHARSET_INTRODUCERS,
    CharSet,
    compute_creation_order,
    fetch_schema_metadata,
    mysql_supported_character_sets,
    topo_sort_tables,
)


class TestMySQLUtils:
    """Unit tests for the mysql_utils module."""

    def test_charset_introducers(self) -> None:
        """Test that CHARSET_INTRODUCERS contains the expected values."""
        assert isinstance(CHARSET_INTRODUCERS, tuple)
        assert len(CHARSET_INTRODUCERS) > 0
        assert all(isinstance(intro, str) for intro in CHARSET_INTRODUCERS)
        assert all(intro.startswith("_") for intro in CHARSET_INTRODUCERS)

    def test_charset_named_tuple(self) -> None:
        """Test the CharSet named tuple."""
        charset = CharSet(id=1, charset="utf8", collation="utf8_general_ci")
        assert charset.id == 1
        assert charset.charset == "utf8"
        assert charset.collation == "utf8_general_ci"

    def test_mysql_supported_character_sets_with_charset(self) -> None:
        """Test mysql_supported_character_sets with a specific charset."""
        test_charset = "utf8mb4"
        results = list(mysql_supported_character_sets(test_charset))
        assert len(results) > 0
        for result in results:
            assert result.charset == test_charset
            assert isinstance(result.id, int)
            assert isinstance(result.collation, str)

    @mock.patch("mysql_to_sqlite3.mysql_utils.MYSQL_CHARACTER_SETS", [(None, None), ("utf8", "utf8_general_ci", True)])
    def test_mysql_supported_character_sets_with_charset_keyerror(self) -> None:
        """Test handling KeyError in mysql_supported_character_sets with charset."""
        # Override the MYSQL_CHARACTER_SETS behavior to raise KeyError
        with mock.patch("mysql_to_sqlite3.mysql_utils.MYSQL_CHARACTER_SETS") as mock_charset_sets:
            mock_charset_sets.__getitem__.side_effect = KeyError("Test KeyError")

            # This should not raise any exceptions
            results = list(mysql_supported_character_sets("utf8"))
            assert len(results) == 0

    @mock.patch("mysql_to_sqlite3.mysql_utils.CharacterSet")
    def test_mysql_supported_character_sets_no_charset(self, mock_charset_class) -> None:
        """Test mysql_supported_character_sets with no charset."""
        # Create a custom mock class for CharacterSet that has get_supported method
        mock_instance = mock.MagicMock()
        mock_instance.get_supported.return_value = ["utf8"]
        mock_charset_class.return_value = mock_instance

        with mock.patch(
            "mysql_to_sqlite3.mysql_utils.MYSQL_CHARACTER_SETS",
            [
                None,  # This will be skipped due to None check
                ("utf8", "utf8_general_ci", True),  # This will be processed
            ],
        ):
            results = list(mysql_supported_character_sets())

            mock_instance.get_supported.assert_called_once()

            assert len(results) > 0
            charset_results = [r.charset for r in results]
            assert "utf8" in charset_results

    @mock.patch("mysql_to_sqlite3.mysql_utils.CharacterSet")
    def test_mysql_supported_character_sets_no_charset_keyerror(self, mock_charset_class) -> None:
        """Test handling KeyError in mysql_supported_character_sets without charset."""
        # Setup mock to return specific values
        mock_instance = mock.MagicMock()
        mock_instance.get_supported.return_value = ["utf8"]
        mock_charset_class.return_value = mock_instance

        # Create a mock object to return either valid info or raise KeyError
        mock_char_sets = mock.MagicMock()
        mock_char_sets.__len__.return_value = 2

        # Make the first item None and the second one raise KeyError when accessed
        def getitem_side_effect(idx):
            if idx == 0:
                return (None, None, None)
            else:
                raise KeyError("Test KeyError")

        mock_char_sets.__getitem__.side_effect = getitem_side_effect

        # Patch with the mock object
        with mock.patch("mysql_to_sqlite3.mysql_utils.MYSQL_CHARACTER_SETS", mock_char_sets):
            # Should continue without raising exceptions despite KeyError
            results = list(mysql_supported_character_sets())
            assert len(results) == 0

            # Verify the get_supported method was called
            mock_instance.get_supported.assert_called_once()

    def test_mysql_supported_character_sets_complete_coverage(self) -> None:
        """Test mysql_supported_character_sets to target specific edge cases for full coverage."""
        # Test with a charset that doesn't match any entries
        with mock.patch(
            "mysql_to_sqlite3.mysql_utils.MYSQL_CHARACTER_SETS",
            [("utf8", "utf8_general_ci", True), None, ("latin1", "latin1_swedish_ci", True)],  # Test None handling
        ):
            # Should return empty when charset doesn't match any entries
            results = list(mysql_supported_character_sets("non_existent_charset"))
            assert len(results) == 0

            # Should process all valid charsets when no specific charset is requested
            with mock.patch("mysql_to_sqlite3.mysql_utils.CharacterSet") as mock_charset_class:
                mock_instance = mock.MagicMock()
                mock_instance.get_supported.return_value = ["utf8", "latin1", "invalid_charset"]
                mock_charset_class.return_value = mock_instance

                # Test when no charset is specified - should process all entries
                results = list(mysql_supported_character_sets())
                # Should have entries for utf8 and latin1
                assert len([r for r in results if r.charset in ["utf8", "latin1"]]) > 0

    def test_mysql_supported_character_sets_with_specific_keyerror(self) -> None:
        """Test mysql_supported_character_sets with specific KeyError scenarios."""
        # Test the specific KeyError scenario in the first branch (with charset specified)
        with mock.patch(
            "mysql_to_sqlite3.mysql_utils.MYSQL_CHARACTER_SETS",
            [
                None,
                mock.MagicMock(side_effect=KeyError("Key error in info[0]")),  # Trigger KeyError on info[0]
                ("latin1", "latin1_swedish_ci", True),
            ],
        ):
            # This should not raise exceptions despite the KeyError
            results = list(mysql_supported_character_sets("utf8"))
            assert len(results) == 0

    @mock.patch("mysql_to_sqlite3.mysql_utils.CharacterSet")
    def test_mysql_supported_character_sets_with_specific_info_keyerror(self, mock_charset_class) -> None:
        """Test mysql_supported_character_sets with KeyError on info[1] access."""
        mock_instance = mock.MagicMock()
        mock_instance.get_supported.return_value = ["utf8"]
        mock_charset_class.return_value = mock_instance

        # Create a special mock that will raise KeyError on accessing index 1
        class InfoMock:
            def __getitem__(self, key):
                if key == 0:
                    return "utf8"
                elif key == 1:
                    raise KeyError("Test KeyError on info[1]")
                return None

        # Set up MYSQL_CHARACTER_SETS with our special mock
        with mock.patch(
            "mysql_to_sqlite3.mysql_utils.MYSQL_CHARACTER_SETS",
            [
                None,
                InfoMock(),  # Will raise KeyError on info[1]
            ],
        ):
            # This should not raise exceptions despite the KeyError
            results = list(mysql_supported_character_sets())
            assert len(results) == 0

            # Now test with a specific charset to cover both branches
            results = list(mysql_supported_character_sets("utf8"))
            assert len(results) == 0

    def test_topo_sort_tables_acyclic(self) -> None:
        """Test topo_sort_tables with an acyclic graph."""
        # Setup a simple acyclic graph
        # users -> posts (posts references users)
        # users -> comments (comments references users)
        # posts -> comments (comments references posts)
        tables: t.Set[str] = {"users", "posts", "comments"}
        edges: t.List[t.Tuple[str, str]] = [
            ("posts", "users"),  # posts depends on users
            ("comments", "users"),  # comments depends on users
            ("comments", "posts"),  # comments depends on posts
        ]

        ordered: t.List[str]
        cyclic_edges: t.List[t.Tuple[str, str]]
        ordered, cyclic_edges = topo_sort_tables(tables, edges)

        # Check that the result is a valid topological sort
        assert len(ordered) == 3  # All tables are included
        assert len(cyclic_edges) == 0  # No cyclic edges

        # Check that dependencies are respected
        users_idx: int = ordered.index("users")
        posts_idx: int = ordered.index("posts")
        comments_idx: int = ordered.index("comments")

        assert users_idx < posts_idx  # users comes before posts
        assert users_idx < comments_idx  # users comes before comments
        assert posts_idx < comments_idx  # posts comes before comments

    def test_topo_sort_tables_cyclic(self) -> None:
        """Test topo_sort_tables with a cyclic graph."""
        # Setup a graph with a cycle
        # users -> posts -> comments -> users (circular dependency)
        tables: t.Set[str] = {"users", "posts", "comments"}
        edges: t.List[t.Tuple[str, str]] = [
            ("posts", "users"),  # posts depends on users
            ("comments", "posts"),  # comments depends on posts
            ("users", "comments"),  # users depends on comments (creates a cycle)
        ]

        ordered: t.List[str]
        cyclic_edges: t.List[t.Tuple[str, str]]
        ordered, cyclic_edges = topo_sort_tables(tables, edges)

        # In a fully cyclic graph, no tables can be ordered without breaking cycles
        # So the ordered list may be empty

        # Check that cyclic edges are detected
        assert len(cyclic_edges) > 0  # At least one cyclic edge

        # The cyclic edges should be from the edges we defined
        for edge in cyclic_edges:
            assert edge in edges

        # Verify that all tables in the cycle are accounted for in cyclic_edges
        cycle_tables: t.Set[str] = set()
        for child, parent in cyclic_edges:
            cycle_tables.add(child)
            cycle_tables.add(parent)

        # All tables should be part of the cycle or in the ordered list
        assert cycle_tables.union(set(ordered)) == tables

    def test_topo_sort_tables_empty(self) -> None:
        """Test topo_sort_tables with empty input."""
        tables: t.Set[str] = set()
        edges: t.List[t.Tuple[str, str]] = []

        ordered, cyclic_edges = topo_sort_tables(tables, edges)

        assert ordered == []
        assert cyclic_edges == []

    def test_fetch_schema_metadata(self) -> None:
        """Test fetch_schema_metadata function."""
        # Create a mock cursor
        mock_cursor: MagicMock = mock.MagicMock()

        # Mock the first query result (tables)
        mock_cursor.fetchall.side_effect = [
            # First call returns table names
            [("users",), ("posts",), ("comments",)],
            # Second call returns foreign key relationships
            [("posts", "users"), ("comments", "users"), ("comments", "posts")],
        ]

        # Call the function
        tables: t.Set[str]
        edges: t.List[t.Tuple[str, str]]
        tables, edges = fetch_schema_metadata(mock_cursor)

        # Verify the cursor was called with the expected queries
        assert mock_cursor.execute.call_count == 2

        # Check the results
        assert tables == {"users", "posts", "comments"}
        assert edges == [("posts", "users"), ("comments", "users"), ("comments", "posts")]

    def test_fetch_schema_metadata_with_different_row_formats(self) -> None:
        """Test fetch_schema_metadata with different row formats."""
        # Create a mock cursor
        mock_cursor: MagicMock = mock.MagicMock()

        # Create different types of row objects to test the robust row handling
        class DictLikeRow:
            def __init__(self, table_name=None, child=None, parent=None):
                self.TABLE_NAME = table_name
                self.child = child
                self.parent = parent

            def __str__(self):
                return f"DictLikeRow({self.TABLE_NAME or ''},{self.child or ''},{self.parent or ''})"

        # Mock the first query result with mixed row formats
        table_rows: t.List[t.Any] = [
            ("table1",),  # Tuple
            [b"table2"],  # List with bytes
            DictLikeRow(table_name="table3"),  # Object with attribute
            None,  # None should be handled
            42,  # Non-standard type
        ]

        # Mock the second query result with mixed row formats for FK relationships
        fk_rows: t.List[t.Any] = [
            ("child1", "parent1"),  # Tuple
            ["child2", "parent2"],  # List
            DictLikeRow(child="child3", parent="parent3"),  # Object with attributes
            {"child": "child4", "parent": "parent4"},  # Dictionary
            (None, "parent5"),  # Tuple with None
            ("child6", None),  # Tuple with None
            None,  # None should be skipped
            42,  # Non-standard type should be skipped
        ]

        mock_cursor.fetchall.side_effect = [table_rows, fk_rows]

        # Call the function
        tables: t.Set[str]
        edges: t.List[t.Tuple[str, str]]
        tables, edges = fetch_schema_metadata(mock_cursor)

        # Verify the cursor was called with the expected queries
        assert mock_cursor.execute.call_count == 2

        # Check that we have the expected number of tables and edges
        assert len(tables) >= 4  # At least our valid inputs
        assert len(edges) >= 4  # At least our valid edges

        # Check that our valid tables are included (using substring matching for flexibility)
        assert any("table1" in tbl for tbl in tables)
        assert any("table2" in tbl for tbl in tables)
        assert any("table3" in tbl for tbl in tables)

        # Check that our valid edges are included
        valid_edges: t.List[t.Tuple[str, str]] = [
            ("child1", "parent1"),
            ("child2", "parent2"),
            ("child3", "parent3"),
            ("child4", "parent4"),
        ]

        # For each valid edge, check that there's a corresponding edge in the result
        for valid_child, valid_parent in valid_edges:
            assert any(valid_child in child and valid_parent in parent for child, parent in edges)

    def test_compute_creation_order(self) -> None:
        """Test compute_creation_order function."""
        # Create a mock MySQL connection
        mock_conn: MagicMock = mock.MagicMock()
        mock_cursor: MagicMock = mock.MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Mock the fetch_schema_metadata function to return known values
        tables: t.Set[str] = {"users", "posts", "comments"}
        edges: t.List[t.Tuple[str, str]] = [
            ("posts", "users"),  # posts depends on users
            ("comments", "users"),  # comments depends on users
            ("comments", "posts"),  # comments depends on posts
        ]

        with mock.patch("mysql_to_sqlite3.mysql_utils.fetch_schema_metadata", return_value=(tables, edges)):
            # Call the function
            tables: t.Set[str]
            edges: t.List[t.Tuple[str, str]]
            ordered_tables, cyclic_edges = compute_creation_order(mock_conn)

            # Verify the connection's cursor was used
            mock_conn.cursor.assert_called_once()

            # Check the results
            assert len(ordered_tables) == 3
            assert len(cyclic_edges) == 0

            # Check that dependencies are respected
            users_idx: int = ordered_tables.index("users")
            posts_idx: int = ordered_tables.index("posts")
            comments_idx: int = ordered_tables.index("comments")

            assert users_idx < posts_idx  # users comes before posts
            assert users_idx < comments_idx  # users comes before comments
            assert posts_idx < comments_idx  # posts comes before comments

    def test_compute_creation_order_with_cycles(self) -> None:
        """Test compute_creation_order with circular dependencies."""
        # Create a mock MySQL connection
        mock_conn: MagicMock = mock.MagicMock()
        mock_cursor: MagicMock = mock.MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Mock the fetch_schema_metadata function to return a graph with a cycle
        tables: t.Set[str] = {"users", "posts", "comments"}
        edges: t.List[t.Tuple[str, str]] = [
            ("posts", "users"),  # posts depends on users
            ("comments", "posts"),  # comments depends on posts
            ("users", "comments"),  # users depends on comments (creates a cycle)
        ]

        with mock.patch("mysql_to_sqlite3.mysql_utils.fetch_schema_metadata", return_value=(tables, edges)):
            # Call the function
            tables: t.Set[str]
            edges: t.List[t.Tuple[str, str]]
            ordered_tables, cyclic_edges = compute_creation_order(mock_conn)

            # Verify the connection's cursor was used
            mock_conn.cursor.assert_called_once()

            # In a fully cyclic graph, no tables can be ordered without breaking cycles
            # So the ordered list may be empty

            # Check that cyclic edges are detected
            assert len(cyclic_edges) > 0

            # The cyclic edges should be from the edges we defined
            for edge in cyclic_edges:
                assert edge in edges

            # Verify that all tables in the cycle are accounted for in cyclic_edges
            cycle_tables: t.Set[str] = set()
            for child, parent in cyclic_edges:
                cycle_tables.add(child)
                cycle_tables.add(parent)

            # All tables should be part of the cycle or in the ordered list
            assert cycle_tables.union(set(ordered_tables)) == tables
