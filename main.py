"""
FastAPI REST API for DuckDB SQL queries on DataFrames.
Provides secure SQL query interface with injection prevention.
"""

import os
import logging

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Depends, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field
from typing import List, Optional, Any

from services.duckdb_service import DuckDBService

# load environment variables from `.env.local` first (used in
# local development) and then from a generic `.env` if present.  The
# explicit call avoids relying on the current working directory when the
# app is started by external tools.
load_dotenv(dotenv_path=".env.local", override=True)
load_dotenv()  # fallback to `.env`

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# helper to parse tokens from environment

def _load_tokens() -> List[str]:
    raw = os.getenv("CLIENT_TOKENS", "")
    return [tok.strip() for tok in raw.split(",") if tok.strip()]


# security dependency used by most routes (and drives OpenAPI docs)
security = HTTPBearer()

async def verify_token(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> None:
    """Dependency that validates a bearer token against CLIENT_TOKENS.

    Raises an HTTPException if no token is provided or if it is not found
    in the comma‑separated environment variable.  The dependency is added
    globally on the app so every route is protected by default.
    """
    if credentials.scheme.lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid authentication scheme.",
        )
    if credentials.credentials not in _load_tokens():
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing bearer token.",
        )


# Initialize FastAPI app with global auth dependency.  The presence of
# the dependency also updates the generated OpenAPI spec so that the
# Swagger UI shows a lock icon and requires you to supply the token
# before executing requests.
app = FastAPI(
    title="DuckDB Query API",
    description="REST API for SQL queries on in-memory DataFrames using DuckDB",
    version="1.0.0",
    dependencies=[Depends(verify_token)],
)

# Add CORS middleware.  For security we don't allow every origin;
# only requests from lizhao.net subdomains are permitted.  FastAPI's
# `CORSMiddleware` supports a regex for this purpose.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "https://localhost:3000"],
    allow_origin_regex=r"^https?://([a-z0-9-]+\.)*lizhao\.net$",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)



# Initialize DuckDB service (singleton)
db_service = DuckDBService()


# Pydantic models
class QueryRequest(BaseModel):
    """Request body for SQL queries."""
    sql: str = Field(
        ...,
        description="SQL SELECT query with ? placeholders for parameters",
        json_schema_extra={"example": "SELECT * FROM users WHERE country = ?"},
    )
    params: Optional[List[Any]] = Field(
        None,
        description="List of parameters to bind to ? placeholders",
        json_schema_extra={"example": ["USA"]},
    )


class QueryResponse(BaseModel):
    """Response body for successful queries."""
    success: bool
    columns: List[str]
    rows: List[List[Any]]
    row_count: int
    data: List[dict]


class QueryErrorResponse(BaseModel):
    """Response body for query errors."""
    success: bool
    error: str
    error_type: str


class HealthResponse(BaseModel):
    """Response body for health check."""
    status: str
    message: str


class SchemaResponse(BaseModel):
    """Response body for schema endpoint."""
    success: bool
    schema_info: dict  # renamed from `schema` to avoid BaseModel warning


class MarketDataSyncRequest(BaseModel):
    asset_type: str = Field(..., description="Market data type such as equity, fx, ir, vol, correlation, option")
    symbols: Optional[List[str] | str] = Field(None, description="Symbols or pair identifiers to sync")
    start: Optional[str] = Field(None, description="Inclusive start date")
    end: Optional[str] = Field(None, description="Inclusive end date")
    mode: str = Field("history", description="history or spot")
    out_format: str = Field("csv", description="csv, parquet, or bin depending on asset type")
    store_type: str = Field(
        "gcs",
        description=(
            "fs or gcs; defaults to gcs to keep buckets optimised. "
            "If you are running locally without GCS credentials you can set this to 'fs', "
            "otherwise a malformed GCS JSON will produce a 400 complaining about a missing key."
        ),
    )
    data_path: Optional[str] = Field(
        None,
        description="Local root or GCS bucket/prefix (bucket name when using gcs)",
    )
    correlation_window: int = Field(20, description="Rolling window for derived correlations")
    generate_ir_curve: bool = Field(True, description="Whether IR sync also writes ir_curve.csv")
    generate_vol_surface: bool = Field(True, description="Whether option sync also writes vol_surface.csv")


class MarketDataCatalogResponse(BaseModel):
    success: bool
    items: List[dict]


class MarketDataSyncResponse(BaseModel):
    success: bool
    result: dict
    # ``result`` may contain a ``warnings`` list when the downloader reported
    # non-fatal issues (e.g. rate limiting or missing symbols).  The UI should
    # display these messages to help users understand why no files appeared.


# Routes

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Health check endpoint.
    
    Returns:
        Health status and message
    """
    return {
        "status": "healthy",
        "message": "DuckDB Query API is running"
    }


@app.post("/query", response_model=QueryResponse)
async def execute_query(request: QueryRequest):
    """
    Execute a SQL query on the in-memory database.
    
    **Security Notes:**
    - Only SELECT queries allowed
    - Parameters are bound using ? placeholders (parameterized queries)
    - SQL injection attempts are blocked
    - Dangerous keywords are rejected
    
    **Example:**
    ```json
    {
        "sql": "SELECT * FROM users WHERE country = ? ORDER BY age DESC",
        "params": ["USA"]
    }
    ```
    
    Args:
        request: QueryRequest with SQL and optional parameters
        
    Returns:
        QueryResponse with results or error
        
    Raises:
        HTTPException: If query validation or execution fails
    """
    try:
        logger.info(f"Executing query: {request.sql[:100]}...")
        
        result = db_service.execute_query(request.sql, request.params)
        
        if not result["success"]:
            logger.warning(f"Query validation failed: {result['error']}")
            raise HTTPException(
                status_code=400,
                detail=result["error"]
            )
        
        logger.info(f"Query executed successfully, returned {result['row_count']} rows")
        
        return QueryResponse(
            success=True,
            columns=result["columns"],
            rows=result["rows"],
            row_count=result["row_count"],
            data=result["data"]
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


@app.get("/schema", response_model=SchemaResponse)
async def get_schema():
    """
    Get schema information for all available tables.
    
    Returns:
        SchemaResponse with table schemas
    """
    try:
        schema = db_service.get_schema()
        return {
            "success": True,
            "schema_info": schema
        }
    except Exception as e:
        logger.error(f"Error retrieving schema: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving schema: {str(e)}"
        )


@app.get("/market-data/catalog", response_model=MarketDataCatalogResponse)
async def get_market_data_catalog():
    try:
        return {
            "success": True,
            "items": db_service.market_data_service.get_catalog(),
        }
    except Exception as e:
        logger.error(f"Error retrieving market data catalog: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error retrieving market data catalog: {str(e)}")


@app.post("/market-data/sync", response_model=MarketDataSyncResponse)
async def sync_market_data(request: MarketDataSyncRequest):
    try:
        result = db_service.market_data_service.sync_market_data(
            request.asset_type,
            symbols=request.symbols,
            start=request.start,
            end=request.end,
            mode=request.mode,
            out_format=request.out_format,
            store_type=request.store_type,
            data_path=request.data_path,
            correlation_window=request.correlation_window,
            generate_ir_curve=request.generate_ir_curve,
            generate_vol_surface=request.generate_vol_surface,
        )
        db_service.dynamic_tables.update(db_service.market_data_service.registered_tables)
        return {"success": True, "result": result}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error syncing market data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error syncing market data: {str(e)}")


@app.get("/tables/{table_name}")
async def get_table_sample(table_name: str, limit: int = 5):
    """
    Get sample data from a specific table.
    
    Args:
        table_name: Name of the table (users or orders)
        limit: Number of rows to return (default: 5)
        
    Returns:
        Sample data from the table
    """
    if limit < 1 or limit > 1000:
        raise HTTPException(
            status_code=400,
            detail="Limit must be between 1 and 1000"
        )
    
    try:
        result = db_service.get_table_sample(table_name, limit)
        
        if not result.get("success"):
            raise HTTPException(
                status_code=404,
                detail=result.get("error", "Table not found")
            )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving table sample: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving table sample: {str(e)}"
        )


@app.get("/")
async def root():
    """Root endpoint with API documentation."""
    return {
        "message": "DuckDB Query API",
        "version": "1.0.0",
        "endpoints": {
            "health": "/health",
            "schema": "/schema",
            "market_data_catalog": "/market-data/catalog",
            "market_data_sync": "/market-data/sync (POST)",
            "tables": "/tables/{table_name}?limit=5",
            "query": "/query (POST)",
            "docs": "/docs",
            "openapi": "/openapi.json"
        },
        "available_tables": ["users", "orders"]
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
