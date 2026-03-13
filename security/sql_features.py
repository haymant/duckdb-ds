"""Feature-table helpers built on the shared SQL parser."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import List, Optional, Set

from security.sql_parser import TableRef, extract_datetime_range, extract_symbol_filters, extract_table_refs


ALPHA_TABLE_RE = re.compile(r"^(alpha158|alpha360)(?:_([a-zA-Z0-9]+))?$", re.IGNORECASE)


@dataclass(frozen=True)
class AlphaRequest:
    feature_name: str
    sql_table_name: str
    alias: Optional[str]
    instruments: Set[str]
    start_time: Optional[str]
    end_time: Optional[str]


def find_alpha_table_refs(sql: str) -> List[TableRef]:
    refs: List[TableRef] = []
    for table_ref in extract_table_refs(sql):
        if ALPHA_TABLE_RE.match(table_ref.table_name):
            refs.append(table_ref)
    return refs


def infer_alpha_request(sql: str, table_ref: TableRef) -> AlphaRequest:
    match = ALPHA_TABLE_RE.match(table_ref.table_name)
    if not match:
        raise ValueError(f"Table '{table_ref.table_name}' is not an alpha table")

    feature_name = match.group(1).lower()
    explicit_symbol = match.group(2)
    aliases = [alias for alias in (table_ref.alias, table_ref.table_name) if alias]
    # alpha tables use the column name ``instrument`` rather than
    # ``symbol``.  ``extract_symbol_filters`` already handles the common
    # case where callers refer to ``symbol`` (including when the table name
    # embeds a symbol suffix), but we also need to accept predicates like
    # ``a.instrument = 'AAPL'`` so that validation logic in
    # ``FeatureManager`` can enforce explicit filters correctly.
    inferred = extract_symbol_filters(sql, aliases=aliases)
    # explicitly look for ``instrument`` clauses too and merge the results.
    # ``_extract_column_filters`` is imported via the public API of the
    # parser so we access it by name here.
    try:
        from security.sql_parser import _extract_column_filters
        inferred |= _extract_column_filters(sql, "instrument", aliases)
    except ImportError:
        pass

    if explicit_symbol:
        inferred.add(explicit_symbol.upper())
    start_time, end_time = extract_datetime_range(sql, aliases=aliases)

    return AlphaRequest(
        feature_name=feature_name,
        sql_table_name=table_ref.table_name,
        alias=table_ref.alias,
        instruments={item.upper() for item in inferred},
        start_time=start_time,
        end_time=end_time,
    )
