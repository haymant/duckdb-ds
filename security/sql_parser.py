"""Shared SQL parsing helpers for lightweight table and predicate extraction.

These helpers intentionally support the subset of SQL patterns used by the
current `duck-server` service. They are not a full SQL parser, but they keep
table discovery and filter extraction consistent across `ochlvf` rewriting and
alpha table materialization.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Iterable, List, Optional, Sequence, Set, Tuple


_TABLE_REF_RE = re.compile(
    r"\b(?P<keyword>FROM|JOIN)\s+(?P<table>[a-zA-Z_][a-zA-Z0-9_]*)"
    r"(?:\s+(?:AS\s+)?(?P<alias>[a-zA-Z_][a-zA-Z0-9_]*))?",
    re.IGNORECASE,
)

_PROJECTION_RE = re.compile(r"^\s*SELECT\s+(?P<projection>.*?)\s+FROM\s+", re.IGNORECASE | re.DOTALL)


@dataclass(frozen=True)
class TableRef:
    table_name: str
    alias: Optional[str]
    source_keyword: str


def extract_table_refs(sql: str) -> List[TableRef]:
    refs: List[TableRef] = []
    for match in _TABLE_REF_RE.finditer(sql):
        table_name = match.group("table")
        alias = match.group("alias")
        keyword = match.group("keyword").upper()
        refs.append(TableRef(table_name=table_name, alias=alias, source_keyword=keyword))
    return refs


def extract_where_clause(sql: str) -> Optional[str]:
    where_match = re.search(r"\bWHERE\s+(.*)", sql, flags=re.IGNORECASE | re.DOTALL)
    if not where_match:
        return None
    where_clause = where_match.group(1)
    return re.split(r"\b(GROUP|ORDER|LIMIT|HAVING)\b", where_clause, flags=re.IGNORECASE)[0].strip()


def _aliases_for_matching(aliases: Optional[Sequence[str]]) -> List[str]:
    normalized = [alias for alias in (aliases or []) if alias]
    if not normalized:
        return []
    return normalized


def _prefixed_column_pattern(column: str, aliases: Optional[Sequence[str]] = None) -> str:
    alias_list = _aliases_for_matching(aliases)
    if not alias_list:
        return rf"(?:\b{column}\b)"
    alias_pattern = "|".join(re.escape(alias) for alias in alias_list)
    return rf"(?:\b(?:{alias_pattern})\.{column}\b|\b{column}\b)"


def extract_symbol_filters(sql: str, aliases: Optional[Sequence[str]] = None) -> Set[str]:
    where_clause = extract_where_clause(sql)
    if not where_clause:
        return set()

    symbol_pattern = _prefixed_column_pattern("symbol", aliases)
    symbols: Set[str] = set()

    eq_re = re.compile(rf"{symbol_pattern}\s*=\s*'([^']+)'", re.IGNORECASE)
    in_re = re.compile(rf"{symbol_pattern}\s+IN\s*\(([^)]+)\)", re.IGNORECASE)
    like_re = re.compile(rf"{symbol_pattern}\s+LIKE\s*'([^']+)'", re.IGNORECASE)

    for match in eq_re.finditer(where_clause):
        symbols.add(match.group(1).upper())

    for match in in_re.finditer(where_clause):
        for value in re.findall(r"'([^']+)'", match.group(1)):
            symbols.add(value.upper())

    for match in like_re.finditer(where_clause):
        symbols.add(translate_like_to_glob(match.group(1).upper()))

    return symbols


def extract_datetime_range(sql: str, aliases: Optional[Sequence[str]] = None) -> Tuple[Optional[str], Optional[str]]:
    where_clause = extract_where_clause(sql)
    if not where_clause:
        return None, None

    dt_pattern = _prefixed_column_pattern("datetime", aliases)
    start: Optional[str] = None
    end: Optional[str] = None

    between_re = re.compile(rf"{dt_pattern}\s+BETWEEN\s*'([^']+)'\s+AND\s*'([^']+)'", re.IGNORECASE)
    ge_re = re.compile(rf"{dt_pattern}\s*>=\s*'([^']+)'", re.IGNORECASE)
    gt_re = re.compile(rf"{dt_pattern}\s*>\s*'([^']+)'", re.IGNORECASE)
    le_re = re.compile(rf"{dt_pattern}\s*<=\s*'([^']+)'", re.IGNORECASE)
    lt_re = re.compile(rf"{dt_pattern}\s*<\s*'([^']+)'", re.IGNORECASE)
    eq_re = re.compile(rf"{dt_pattern}\s*=\s*'([^']+)'", re.IGNORECASE)

    between_match = between_re.search(where_clause)
    if between_match:
        return between_match.group(1), between_match.group(2)

    eq_match = eq_re.search(where_clause)
    if eq_match:
        value = eq_match.group(1)
        return value, value

    ge_match = ge_re.search(where_clause) or gt_re.search(where_clause)
    le_match = le_re.search(where_clause) or lt_re.search(where_clause)
    if ge_match:
        start = ge_match.group(1)
    if le_match:
        end = le_match.group(1)
    return start, end


def extract_projection_map(sql: str) -> dict[Optional[str], List[str]]:
    projection_match = _PROJECTION_RE.search(sql)
    if not projection_match:
        return {}
    projection_raw = projection_match.group("projection").strip()
    if projection_raw == "*":
        return {None: ["*"]}

    projection_map: dict[Optional[str], List[str]] = {}
    for part in [segment.strip() for segment in projection_raw.split(",") if segment.strip()]:
        alias_match = re.match(r"(?P<alias>[a-zA-Z_][a-zA-Z0-9_]*)\.(?P<column>[a-zA-Z_*][a-zA-Z0-9_]*)", part)
        if alias_match:
            alias = alias_match.group("alias")
            column = alias_match.group("column")
            projection_map.setdefault(alias, []).append(column)
        else:
            projection_map.setdefault(None, []).append(part)
    return projection_map


def translate_like_to_glob(pattern: str) -> str:
    return pattern.replace("%", "*").replace("_", "?")
