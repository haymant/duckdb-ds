"""
DuckDB service for executing SQL queries on DataFrames.
Handles connection, table registration, and query execution.
"""

from typing import List, Dict, Any, Optional
import logging
import duckdb
import pandas as pd

from data.seed import create_dummy_users, create_dummy_orders
from security.sql_validator import prepare_query_for_duckdb

# optional qlib loader (reads env vars)
from services.qlib_service import load_qlib_dataframes


class DuckDBService:
    """Service for managing DuckDB connections and queries."""
    
    def __init__(self):
        """Initialize DuckDB service with in-memory database and seed data."""
        # Create in-memory DuckDB connection
        self.conn = duckdb.connect(":memory:")
        
        # Load dummy data
        self.users_df = create_dummy_users()
        self.orders_df = create_dummy_orders()
        
        # Register DataFrames as tables
        self._register_tables()
        # attempt to load qlib data if configured
        self._maybe_load_qlib_dataframes()
    
    def _register_tables(self) -> None:
        """Register DataFrames as DuckDB tables."""
        self.conn.register("users", self.users_df)
        self.conn.register("orders", self.orders_df)

    def _maybe_load_qlib_dataframes(self) -> None:
        """Load qlib DataFrames and register if available."""
        self.qlib_dfs = {}
        try:
            dfs = load_qlib_dataframes()
            for name, df in dfs.items():
                if df.empty:
                    logging.getLogger(__name__).warning(f"qlib dataframe {name} empty, skipping registration")
                else:
                    self.conn.register(name, df)
                    self.qlib_dfs[name] = df
                    logging.getLogger(__name__).info(
                        f"Loaded qlib dataframe {name} with {len(df):,} rows and {len(df.columns):,} columns"
                    )
        except Exception as e:
            # fail gracefully; we still have users/orders tables
            logging.getLogger(__name__).warning(f"Failed to load qlib data: {e}")
    
    def execute_query(
        self,
        sql: str,
        params: Optional[List[Any]] = None
    ) -> Dict[str, Any]:
        """
        Execute a SQL query with parameterized binding.
        
        Args:
            sql: SQL query string with ? placeholders
            params: Optional list of parameters
            
        Returns:
            Dict with columns, rows, and row count
            
        Raises:
            ValueError: If SQL validation fails
            Exception: If query execution fails
        """
        try:
            # Validate and prepare query
            prepared_sql, prepared_params = prepare_query_for_duckdb(sql, params or [])
            
            # Execute query with parameters
            result = self.conn.execute(prepared_sql, parameters=prepared_params)
            
            # Fetch all results
            df_result = result.df()
            
            return {
                "success": True,
                "columns": df_result.columns.tolist(),
                "rows": df_result.values.tolist(),
                "row_count": len(df_result),
                "data": df_result.to_dict(orient="records"),
            }
            
        except ValueError as e:
            # SQL injection prevention errors
            return {
                "success": False,
                "error": f"Validation error: {str(e)}",
                "error_type": "validation"
            }
        except Exception as e:
            # Query execution errors
            return {
                "success": False,
                "error": f"Query execution error: {str(e)}",
                "error_type": "execution"
            }
    
    def get_schema(self) -> Dict[str, List[Dict[str, str]]]:
        """
        Get schema information for all registered tables.
        
        Returns:
            Dict mapping table names to column information
        """
        schema_info = {}
        
        table_names = ["users", "orders"]
        if hasattr(self, 'qlib_dfs'):
            table_names.extend(list(self.qlib_dfs.keys()))

        for table_name in table_names:
            result = self.conn.execute(f"DESCRIBE {table_name}").df()
            schema_info[table_name] = result.to_dict(orient="records")
        
        return schema_info
    
    def get_table_sample(self, table_name: str, limit: int = 5) -> Dict[str, Any]:
        """
        Get sample data from a table.
        
        Args:
            table_name: Name of table to sample
            limit: Number of rows to return
            
        Returns:
            Dict with sample data
        """
        valid_tables = ["users", "orders"]
        if hasattr(self, 'qlib_dfs'):
            valid_tables.extend(list(self.qlib_dfs.keys()))
            
        if table_name not in valid_tables:
            return {
                "success": False,
                "error": f"Table '{table_name}' not found. Valid tables: {valid_tables}"
            }
        
        try:
            result = self.conn.execute(
                f"SELECT * FROM {table_name} LIMIT ?",
                parameters=[limit]
            ).df()
            
            return {
                "success": True,
                "table": table_name,
                "columns": result.columns.tolist(),
                "rows": result.values.tolist(),
                "row_count": len(result),
                "data": result.to_dict(orient="records"),
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }
