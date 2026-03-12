"""
DuckDB service for executing SQL queries on DataFrames.
Handles connection, table registration, and query execution.
"""

from typing import List, Dict, Any, Optional, Set
import logging
import duckdb
import pandas as pd
import os
import fsspec
import gcsfs

from data.seed import create_dummy_users, create_dummy_orders
from security.sql_features import find_alpha_table_refs, infer_alpha_request
from security.sql_parser import extract_table_refs
from security.sql_validator import prepare_query_for_duckdb
from services.data_query import DataQueryService
from services.feature_manager import FeatureManager
from services.market_data_service import MarketDataService, MARKET_DATA_DEFINITIONS


class DuckDBService:
    """Service for managing DuckDB connections and queries."""
    
    def __init__(self):
        """Initialize DuckDB service with in-memory database and seed data."""
        # Create in-memory DuckDB connection
        self.conn = duckdb.connect(":memory:")
        self.dynamic_tables: Set[str] = set()
        self.data_query_service = DataQueryService(self.conn)
        
        # Load dummy data
        self.users_df = create_dummy_users()
        self.orders_df = create_dummy_orders()
        
        # Register DataFrames as tables
        self._register_tables()
        # configure GCS credentials if environment variables are set
        self._maybe_configure_gcs()
        self.feature_manager = FeatureManager(
            self.conn,
            config={
                "provider_uri": os.getenv("ALPHA_PROVIDER_URI"),
                "region": os.getenv("ALPHA_REGION") or "REG_US",
                "freq": os.getenv("ALPHA_FREQ") or "day",
                "allow_full_scan": (os.getenv("ALPHA_ALLOW_FULL_SCAN") or "false").lower() == "true",
                "fit_start_time": os.getenv("ALPHA_FIT_START_TIME"),
                "fit_end_time": os.getenv("ALPHA_FIT_END_TIME"),
            },
            data_query_service=self.data_query_service,
        )
        self.market_data_service = MarketDataService(self.conn)
    
    def _register_tables(self) -> None:
        """Register DataFrames as DuckDB tables."""
        self.conn.register("users", self.users_df)
        self.conn.register("orders", self.orders_df)

    def _maybe_configure_gcs(self) -> None:
        """Create or update DuckDB GCS secret if credentials are provided.

        The service will look for `GCS_KEY_ID` and `GCS_KEY_SECRET` in the
        environment.  If both are defined the method issues a
        ``CREATE OR REPLACE SECRET`` statement using the active connection.
        Any errors are logged but do not prevent the service from functioning
        (queries against non-GCS tables still work).
        """
        key_id = os.getenv("GCS_KEY_ID")
        key_secret = os.getenv("GCS_KEY_SECRET")
        if not key_id or not key_secret:
            return

        try:
            # DuckDB requires the values to be quoted in the SQL string
            self.conn.sql("""
            INSTALL spatial; LOAD spatial;
                    SET enable_object_cache = true;SET home_directory='/tmp';
            """)

            self.conn.execute(f"""
            CREATE OR REPLACE SECRET gcs_secret (
                TYPE GCS,
                KEY_ID '{key_id}',
                SECRET '{key_secret}'
            );
            """)
            logging.getLogger(__name__).info("Configured GCS secret for DuckDB")
        except Exception as e:
            logging.getLogger(__name__).warning(f"Failed to configure GCS secret: {e}")
    
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
            self._prepare_alpha_tables(sql)
            self._prepare_market_data_tables(sql)
            
            # Execute query with parameters
            result = self.conn.execute(prepared_sql, parameters=prepared_params)
            
            # Fetch all results
            df_result = result.df()
            
            # convert pandas/numpy types to plain Python for JSON serialization
            def _to_python(val):
                # numpy scalar
                import numpy as _np
                import pandas as _pd
                # pandas NA/NaT
                if val is _pd.NaT:
                    return None
                # numpy nan -> convert to None; avoid calling isnat on plain floats
                if isinstance(val, float) and _np.isnan(val):
                    return None
                # numpy datetime64/timedelta64 may also be NaT
                if isinstance(val, (_np.datetime64, _np.timedelta64)):
                    if _np.isnat(val):
                        return None
                if isinstance(val, _np.generic):
                    try:
                        plain = val.item()
                    except Exception:
                        plain = float(val)
                    # convert the extracted scalar again (e.g. numpy datetime64)
                    return _to_python(plain)
                # pandas timestamp or timedelta
                if isinstance(val, (_pd.Timestamp, _pd.Timedelta)):
                    # NaT was handled above
                    return val.isoformat()
                # other pandas types
                if isinstance(val, _pd.Categorical):
                    return val.tolist()
                return val

            rows = []
            for row in df_result.values.tolist():
                rows.append([_to_python(x) for x in row])

            data = []
            for rec in df_result.to_dict(orient="records"):
                data.append({k: _to_python(v) for k, v in rec.items()})

            return {
                "success": True,
                "columns": df_result.columns.tolist(),
                "rows": rows,
                "row_count": len(df_result),
                "data": data,
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

    def query_dataframe(self, sql: str, params: Optional[List[Any]] = None) -> pd.DataFrame:
        return self.data_query_service.query_dataframe(sql, params)

    def _prepare_alpha_tables(self, sql: str) -> None:
        for table_ref in find_alpha_table_refs(sql):
            request = infer_alpha_request(sql, table_ref)
            self.feature_manager.ensure_registered(request)
            self.dynamic_tables.add(request.sql_table_name)

    def _prepare_market_data_tables(self, sql: str) -> None:
        """Ensure any market-data tables referenced by the SQL have views registered.

        Unlike alpha tables, market data must be explicitly synced (via the
        `/market-data/sync` endpoint) before it can be queried. Previously a
        missing view would result in a generic DuckDB "table does not exist"
        error during execution, which surfaced as a scary 400 in the API logs.
        
        To make the failure more actionable we attempt to register the view
        here and raise a ``ValueError`` when the underlying CSVs are not
        present.  This bubbles back through ``execute_query`` resulting in a
        validation error with a helpful message instead of the opaque catalog
        error.
        """
        # Detect which market-data table names appear in the query.
        refs = {ref.table_name.lower() for ref in extract_table_refs(sql)}
        for definition in MARKET_DATA_DEFINITIONS.values():
            if definition.table_name.lower() in refs:
                created = self.market_data_service.ensure_view(definition.table_name)
                if not created:
                    # view couldn't be created because files weren't found
                    raise ValueError(
                        f"Market data '{definition.asset_type}' is not available; "
                        "run /market-data/sync or place CSVs under the data root"
                    )
        # even if views were registered above we still update the dynamic set
        self.dynamic_tables.update(self.market_data_service.registered_tables)
    
    def get_schema(self) -> Dict[str, List[Dict[str, str]]]:
        """
        Get schema information for all registered tables.
        
        Returns:
            Dict mapping table names to column information
        """
        schema_info = {}
        
        self.market_data_service.register_all_views()
        self.dynamic_tables.update(self.market_data_service.registered_tables)
        table_names = ["users", "orders", *sorted(self.dynamic_tables)]

        for table_name in table_names:
            try:
                result = self.conn.execute(f"DESCRIBE {table_name}").df()
                schema_info[table_name] = result.to_dict(orient="records")
            except Exception:
                continue
        
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
        self.market_data_service.register_all_views()
        self.dynamic_tables.update(self.market_data_service.registered_tables)
        valid_tables = ["users", "orders", *sorted(self.dynamic_tables)]
            
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
