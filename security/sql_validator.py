"""
SQL injection prevention and validation module.
Implements parameterized queries and SQL keyword validation.
"""

import re
from typing import List, Tuple
from enum import Enum

from security.sql_parser import extract_symbol_filters, extract_table_refs


class SQLKeywordType(str, Enum):
    """Safe SQL keywords allowed in queries."""
    # SELECT operations
    SELECT = "SELECT"
    FROM = "FROM"
    WHERE = "WHERE"
    AND = "AND"
    OR = "OR"
    NOT = "NOT"
    
    # Joins
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"
    OUTER = "OUTER"
    CROSS = "CROSS"
    JOIN = "JOIN"
    ON = "ON"
    
    # Aggregations & grouping
    GROUP = "GROUP"
    BY = "BY"
    HAVING = "HAVING"
    ORDER = "ORDER"
    ASC = "ASC"
    DESC = "DESC"
    
    # Aggregation functions (will be checked separately)
    SUM = "SUM"
    COUNT = "COUNT"
    AVG = "AVG"
    MIN = "MIN"
    MAX = "MAX"
    
    # Other safe keywords
    AS = "AS"
    DISTINCT = "DISTINCT"
    LIMIT = "LIMIT"
    OFFSET = "OFFSET"
    CASE = "CASE"
    WHEN = "WHEN"
    THEN = "THEN"
    ELSE = "ELSE"
    END = "END"


# Dangerous keywords that could indicate SQL injection attempts
DANGEROUS_KEYWORDS = {
    "DROP", "DELETE", "INSERT", "UPDATE", "CREATE", "ALTER",
    "TRUNCATE", "EXEC", "EXECUTE", "SCRIPT", "UNION", "PRAGMA",
    "--", "/*", "*/", ";", "CAST", "CONVERT"
}


def validate_sql_query(sql: str) -> Tuple[bool, str]:
    """
    Validate SQL query for potential injection attacks.
    
    Args:
        sql: SQL query string to validate
        
    Returns:
        Tuple of (is_valid, message)
    """
    if not sql or not isinstance(sql, str):
        return False, "Query must be a non-empty string"
    
    # Check for dangerous keywords
    sql_upper = sql.upper().strip()
    for dangerous in DANGEROUS_KEYWORDS:
        if dangerous in sql_upper:
            return False, f"Dangerous keyword '{dangerous}' not allowed"
    
    # Check for common SQL injection patterns
    # patterns designed to catch naive injection attempts.  Most are
    # intentionally conservative; the second pattern in particular previously
    # matched any occurrence of ``'foo' OR 'bar'`` even when appearing as part
    # of a legitimate WHERE clause.  We tighten it to only trigger when two
    # quoted literals appear directly around the OR keyword with nothing but
    # whitespace between them.
    injection_patterns = [
        r"['\"]\s*;\s*",  # Quote followed by semicolon
        r"['\"][^'\"]*['\"]\s+OR\s+['\"][^'\"]*['\"]"  # literal OR literal
        r"UNION\s+SELECT",  # Union-based injection
        r"xp_",  # Extended stored procedures (SQL Server)
        r"sp_",  # System stored procedures
    ]
    
    for pattern in injection_patterns:
        if re.search(pattern, sql, re.IGNORECASE):
            return False, f"Potential SQL injection detected (pattern: {pattern})"
    
    # Ensure no comments
    if "--" in sql or "/*" in sql or "*/" in sql:
        return False, "SQL comments not allowed"
    
    # Basic validation: should start with SELECT
    if not sql_upper.startswith("SELECT"):
        return False, "Only SELECT queries are allowed"
    
    return True, "Query is valid"


def validate_parameters(params: List) -> Tuple[bool, str]:
    """
    Validate query parameters to prevent injection.
    
    Args:
        params: List of parameters for parameterized query
        
    Returns:
        Tuple of (is_valid, message)
    """
    if params is None:
        return True, "No parameters"
    
    if not isinstance(params, (list, tuple)):
        return False, "Parameters must be a list or tuple"
    
    # Check each parameter
    for i, param in enumerate(params):
        # Allowed types for parameters
        if not isinstance(param, (str, int, float, bool, type(None))):
            return False, f"Parameter {i} has invalid type: {type(param).__name__}"
        
        # Additional string validation
        if isinstance(param, str):
            # Strings should not contain dangerous keywords at param level
            # (DuckDB will handle them as data, not code)
            if len(param) > 10000:  # Prevent extremely long strings
                return False, f"Parameter {i} exceeds maximum length"
    
    return True, "Parameters are valid"


def _rewrite_ochlvf(sql: str, params: List) -> (str, List):
    """Rewrite ochlvf-related table references to parameterized GCS scans.

    There are two supported use‑cases:

    1. **Named tables** – the source SQL explicitly references
       ``ochlvf_{SYMBOL}`` in a FROM or JOIN clause.  These are rewritten
       directly and the ``{SYMBOL}`` portion is uppercased to form the
       storage path.

    2. **Generic table** – the SQL refers to ``ochlvf`` itself (optionally
       aliased) in FROM/JOIN.  In this case the set of symbols to read is
       inferred from the WHERE clause.  Any equality, IN-list, or LIKE
       conditions on the ``symbol`` column are collected; like patterns are
       translated into glob-style wildcards.  All discovered values are
       uppercased and combined into a single path parameter using brace
       expansion (DuckDB accepts paths like
       ``symbol={AAPL,TSLA}``).

    If neither pattern is present the input SQL is returned unchanged.
    """
    new_params = [] if params is None else list(params)

    # helper to create one or more GCS paths from symbol patterns
    def _make_paths(symbol_patterns: List[str]) -> List[str]:
        if not symbol_patterns:
            return []
        return [f"gcs://edge-lake/symbol={pat}/*.parquet" for pat in symbol_patterns]

    # first pass: explicit ochlvf_{SYMBOL} references in FROM/JOIN
    def _named_replacer(match: re.Match) -> str:
        prefix = match.group(1)  # "FROM " or "JOIN "
        symbol = match.group(2)
        paths = _make_paths([symbol.upper()])
        # single path only for named table
        new_params.extend(paths)
        return f"{prefix}read_parquet(?, hive_partitioning=true)"

    named_pattern = r"(\b(?:FROM|JOIN)\s+)ochlvf_([a-zA-Z0-9]+)\b"
    sql = re.sub(named_pattern, _named_replacer, sql, flags=re.IGNORECASE)

    # second pass: generic ``ochlvf`` table
    generic_pattern = r"(\b(?:FROM|JOIN)\s+)ochlvf\b"
    if re.search(generic_pattern, sql, flags=re.IGNORECASE):
        ochlvf_aliases = []
        for table_ref in extract_table_refs(sql):
            if table_ref.table_name.lower() == "ochlvf":
                ochlvf_aliases.extend([item for item in (table_ref.alias, table_ref.table_name) if item])

        symbols = extract_symbol_filters(sql, aliases=ochlvf_aliases or None)

        paths = _make_paths(sorted(symbols))
        if paths:
            # if only one path we can use the simple placeholder syntax
            if len(paths) == 1:
                new_params.append(paths[0])
                placeholder_expr = "?"
            else:
                # build list of placeholders
                placeholder_expr = ",".join("?" for _ in paths)
                new_params.extend(paths)
                placeholder_expr = f"[{placeholder_expr}]"
            sql = re.sub(generic_pattern,
                         rf"\1read_parquet({placeholder_expr}, hive_partitioning=true, union_by_name=true)",
                         sql, flags=re.IGNORECASE)
        else:
            # no paths discovered; still replace name but no params
            sql = re.sub(generic_pattern,
                         r"\1read_parquet(?, hive_partitioning=true, union_by_name=true)",
                         sql, flags=re.IGNORECASE)

    return sql, new_params


def prepare_query_for_duckdb(sql: str, params: List = None) -> Tuple[str, List]:
    """
    Prepare query for DuckDB execution with parameter substitution.
    Uses ? placeholders for parameterized queries.
    
    Args:
        sql: SQL query with ? placeholders
        params: List of parameters to bind
        
    Returns:
        Tuple of (processed_sql, params)
    """
    # Validate SQL
    is_valid, msg = validate_sql_query(sql)
    if not is_valid:
        raise ValueError(f"SQL validation failed: {msg}")
    
    # Validate parameters
    if params:
        is_valid, msg = validate_parameters(params)
        if not is_valid:
            raise ValueError(f"Parameter validation failed: {msg}")
    else:
        params = []
    
    # Rewrite any ochlvf_* table references to parameterized GCS parquet scans
    sql, params = _rewrite_ochlvf(sql, params)

    # Count placeholders vs parameters
    placeholder_count = sql.count("?")
    if placeholder_count != len(params):
        raise ValueError(
            f"Placeholder count ({placeholder_count}) doesn't match "
            f"parameter count ({len(params)})"
        )
    
    return sql, params
