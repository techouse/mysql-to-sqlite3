"""Miscellaneous MySQL utilities."""

import typing as t
from collections import defaultdict, deque

from mysql.connector import CharacterSet
from mysql.connector.abstracts import MySQLConnectionAbstract, MySQLCursorAbstract
from mysql.connector.charsets import MYSQL_CHARACTER_SETS


CHARSET_INTRODUCERS: t.Tuple[str, ...] = tuple(
    f"_{charset[0]}" for charset in MYSQL_CHARACTER_SETS if charset is not None
)


class CharSet(t.NamedTuple):
    """MySQL character set as a named tuple."""

    id: int
    charset: str
    collation: str


def mysql_supported_character_sets(charset: t.Optional[str] = None) -> t.Iterator[CharSet]:
    """Get supported MySQL character sets."""
    index: int
    info: t.Optional[t.Tuple[str, str, bool]]
    if charset is not None:
        for index, info in enumerate(MYSQL_CHARACTER_SETS):
            if info is not None:
                try:
                    if info[0] == charset:
                        yield CharSet(index, charset, info[1])
                except KeyError:
                    continue
    else:
        for charset in CharacterSet().get_supported():
            for index, info in enumerate(MYSQL_CHARACTER_SETS):
                if info is not None:
                    try:
                        yield CharSet(index, charset, info[1])
                    except KeyError:
                        continue


def fetch_schema_metadata(cursor: MySQLCursorAbstract) -> t.Tuple[t.Set[str], t.List[t.Tuple[str, str]]]:
    """Fetch schema metadata from the database.

    Returns:
        tables: all base tables in `schema`
        edges: list of (child, parent) pairs for every FK
    """
    # 1. all ordinary tables
    cursor.execute(
        """
        SELECT TABLE_NAME
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = SCHEMA()
        AND TABLE_TYPE = 'BASE TABLE';
    """
    )
    # Use a more explicit approach to handle the row data
    tables: t.Set[str] = set()
    for row in cursor.fetchall():
        # Extract table name from row
        table_name: str
        try:
            # Try to get the first element
            first_element = row[0] if isinstance(row, (list, tuple)) else row
            table_name = str(first_element) if first_element is not None else ""
        except (IndexError, TypeError):
            # If that fails, try other approaches
            if hasattr(row, "TABLE_NAME"):
                table_name = str(row.TABLE_NAME) if row.TABLE_NAME is not None else ""
            else:
                table_name = str(row) if row is not None else ""
        tables.add(table_name)

    # 2. FK edges  (child -> parent)
    cursor.execute(
        """
        SELECT TABLE_NAME AS child, REFERENCED_TABLE_NAME AS parent
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = SCHEMA()
        AND REFERENCED_TABLE_NAME IS NOT NULL;
    """
    )
    # Use a more explicit approach to handle the row data
    edges: t.List[t.Tuple[str, str]] = []
    for row in cursor.fetchall():
        # Extract child and parent from row
        child: str
        parent: str
        try:
            # Try to get the elements as sequence
            if isinstance(row, (list, tuple)) and len(row) >= 2:
                child = str(row[0]) if row[0] is not None else ""
                parent = str(row[1]) if row[1] is not None else ""
            # Try to access as dictionary or object
            elif hasattr(row, "child") and hasattr(row, "parent"):
                child = str(row.child) if row.child is not None else ""
                parent = str(row.parent) if row.parent is not None else ""
            # Try to access as dictionary with string keys
            elif isinstance(row, dict) and "child" in row and "parent" in row:
                child = str(row["child"]) if row["child"] is not None else ""
                parent = str(row["parent"]) if row["parent"] is not None else ""
            else:
                # Skip if we can't extract the data
                continue
        except (IndexError, TypeError, KeyError):
            # Skip if any error occurs
            continue

        edges.append((child, parent))

    return tables, edges


def topo_sort_tables(
    tables: t.Set[str], edges: t.List[t.Tuple[str, str]]
) -> t.Tuple[t.List[str], t.List[t.Tuple[str, str]]]:
    """Perform a topological sort on tables based on foreign key dependencies.

    Returns:
        ordered: tables in FK-safe creation order
        cyclic_edges: any edges that keep the graph cyclic (empty if a pure DAG)
    """
    # dependency graph: child → {parents}
    deps: t.Dict[str, t.Set[str]] = {tbl: set() for tbl in tables}
    # reverse edges: parent → {children}
    rev: t.Dict[str, t.Set[str]] = defaultdict(set)

    for child, parent in edges:
        deps[child].add(parent)
        rev[parent].add(child)

    queue: deque[str] = deque(tbl for tbl, parents in deps.items() if not parents)
    ordered: t.List[str] = []

    while queue:
        table = queue.popleft()
        ordered.append(table)
        # "remove" table from graph
        for child in rev[table]:
            deps[child].discard(table)
            if not deps[child]:
                queue.append(child)

    # any table still having parents is in a cycle
    cyclic_edges: t.List[t.Tuple[str, str]] = [
        (child, parent) for child, parents in deps.items() if parents for parent in parents
    ]
    return ordered, cyclic_edges


def compute_creation_order(mysql_conn: MySQLConnectionAbstract) -> t.Tuple[t.List[str], t.List[t.Tuple[str, str]]]:
    """Compute the table creation order respecting foreign key constraints.

    Returns:
        A tuple (ordered_tables, cyclic_edges) where cyclic_edges is empty when the schema is acyclic.
    """
    with mysql_conn.cursor() as cur:
        tables: t.Set[str]
        edges: t.List[t.Tuple[str, str]]
        tables, edges = fetch_schema_metadata(cur)
    return topo_sort_tables(tables, edges)
