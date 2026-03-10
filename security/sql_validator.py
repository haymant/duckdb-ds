"""
SQL injection prevention and validation module.
Implements parameterized queries and SQL keyword validation.
"""

import re
from typing import List, Tuple
from enum import Enum


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
    "--", "/*", "*/", ";", "LIKE", "CAST", "CONVERT"
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
    injection_patterns = [
        r"['\"]\s*;\s*",  # Quote followed by semicolon
        r"['\"].*['\"].*OR.*['\"]",  # OR with quotes
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
    """Rewrite ochlvf_* table names to parameterized GCS parquet scans.

    When a table name beginning with ``ochlvf_`` is seen we replace the
    literal reference with a ``read_parquet(?, hive_partitioning=true)``
    expression and append the corresponding GCS path to the parameter list.
    This keeps the path value out of the SQL string itself and allows DuckDB
    to treat it as a bound parameter.  Multiple occurrences are supported and
    each will add a new parameter in the order encountered.

    The symbol portion is captured case‑insensitively and interpolated as
    provided (``ochlvf_AAPL`` and ``ochlvf_aapl`` both work).

    Example::
        SELECT * FROM ochlvf_aapl =>
          SELECT * FROM read_parquet(?, hive_partitioning=true)
    """
    pattern = r"\bochlvf_([a-zA-Z0-9]+)\b"

    # ensure we have a mutable list to append to
    new_params = [] if params is None else list(params)

    def _replacer(match: re.Match) -> str:
        symbol = match.group(1)
        # GCS storage uses uppercase symbols; normalizing ensures the path
        # matches whatever is actually stored.  We still record the original
        # table name in SQL for readability, but the bound parameter uses
        # uppercase so that both ``ochlvf_aapl`` and ``ochlvf_AAPL`` resolve
        # to the same file set.
        path = f"gcs://edge-lake/symbol={symbol.upper()}/*.parquet"
        new_params.append(path)
        return "read_parquet(?, hive_partitioning=true)"

    new_sql = re.sub(pattern, _replacer, sql, flags=re.IGNORECASE)
    return new_sql, new_params


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
