"""Helpers for executing DuckDB-backed SQL and returning pandas DataFrames."""

from __future__ import annotations

from typing import Any, List, Optional

import pandas as pd

from security.sql_validator import prepare_query_for_duckdb


class DataQueryService:
    """Execute validated SQL against the shared DuckDB connection."""

    def __init__(self, conn):
        self.conn = conn

    def query_dataframe(self, sql: str, params: Optional[List[Any]] = None) -> pd.DataFrame:
        prepared_sql, prepared_params = prepare_query_for_duckdb(sql, params or [])
        return self.conn.execute(prepared_sql, parameters=prepared_params).df()