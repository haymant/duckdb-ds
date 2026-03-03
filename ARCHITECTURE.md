# Architecture Overview

## System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      CLIENT LAYER                            │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │   cURL/REST  │  │   Python     │  │   Browser    │       │
│  │   Client     │  │   Client     │  │   UI (/docs) │       │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘       │
│         │                 │                 │                │
│         └─────────────────┼─────────────────┘                │
│                           │                                  │
│                    HTTP/JSON POST                            │
│                           │                                  │
└───────────────────────────┼──────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                     API LAYER (FastAPI)                      │
├─────────────────────────────────────────────────────────────┤
│  main.py (239 lines)                                         │
│                                                               │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ Routes & Endpoints                                     │ │
│  ├────────────────────────────────────────────────────────┤ │
│  │  GET  /              → API info                         │ │
│  │  GET  /health        → Status check                    │ │
│  │  GET  /schema        → Database schema                 │ │
│  │  GET  /tables/{name} → Table sample                    │ │
│  │  POST /query         → Execute SQL (MAIN)              │ │
│  └────────────────────────────────────────────────────────┘ │
│                                                               │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ Request Validation (Pydantic)                          │ │
│  ├────────────────────────────────────────────────────────┤ │
│  │  • Query validation                                     │ │
│  │  • Parameter type checking                              │ │
│  │  • Error handling                                       │ │
│  └────────────────────────────────────────────────────────┘ │
│                                                               │
└───────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│              SECURITY LAYER (sql_validator.py)               │
├─────────────────────────────────────────────────────────────┤
│  (175 lines)                                                 │
│                                                               │
│  ┌─ Validation Pipeline ────────────────────────────────┐   │
│  │                                                       │   │
│  │  [1] Dangerous Keyword Check                         │   │
│  │      ├─ Rejects: DROP, DELETE, INSERT, UPDATE, etc. │   │
│  │      └─ Only allows SELECT queries                  │   │
│  │                      ↓                               │   │
│  │  [2] SQL Injection Pattern Detection                │   │
│  │      ├─ Blocks: OR 'x'='x', UNION SELECT, etc.     │   │
│  │      └─ Detects: Comments, quotes, special chars    │   │
│  │                      ↓                               │   │
│  │  [3] Parameter Type Validation                       │   │
│  │      ├─ Allows: str, int, float, bool, None         │   │
│  │      └─ Rejects: objects, arrays, functions         │   │
│  │                      ↓                               │   │
│  │  [4] Placeholder Matching                            │   │
│  │      ├─ Counts: ? in SQL vs params array            │   │
│  │      └─ Must match exactly                           │   │
│  │                      ↓                               │   │
│  │  [5] String Length Check                             │   │
│  │      ├─ Max: 10,000 chars per parameter              │   │
│  │      └─ Prevents memory exhaustion                   │   │
│  │                                                       │   │
│  │  ✅ Pass → Continue to Service Layer                 │   │
│  │  ❌ Fail → Return error to client                    │   │
│  │                                                       │   │
│  └───────────────────────────────────────────────────────┘   │
│                                                               │
└───────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│             SERVICE LAYER (duckdb_service.py)                │
├─────────────────────────────────────────────────────────────┤
│  (138 lines)                                                 │
│                                                               │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ DuckDB Service Manager                                 │ │
│  ├────────────────────────────────────────────────────────┤ │
│  │  • Connection Management                               │ │
│  │  • Query Execution with Parameters                     │ │
│  │  • Result Formatting                                   │ │
│  │  • Error Handling                                      │ │
│  └────────────────────────────────────────────────────────┘ │
│                                                               │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ Parameterized Query Execution                          │ │
│  ├────────────────────────────────────────────────────────┤ │
│  │                                                         │ │
│  │  Input:  sql = "SELECT * FROM users WHERE id = ?"     │ │
│  │          params = [123]                                │ │
│  │                          ↓                             │ │
│  │  Execute: conn.execute(sql, parameters=params)        │ │
│  │                          ↓                             │ │
│  │  DuckDB: Binds 123 as DATA, not CODE                  │ │
│  │          (prevents SQL injection)                      │ │
│  │                          ↓                             │ │
│  │  Output: DataFrame → JSON                              │ │
│  │                                                         │ │
│  └────────────────────────────────────────────────────────┘ │
│                                                               │
└───────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│              DATA LAYER (DuckDB In-Memory)                   │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌────────────────────┐  ┌────────────────────┐             │
│  │   USERS TABLE      │  │   ORDERS TABLE     │             │
│  ├────────────────────┤  ├────────────────────┤             │
│  │ user_id (PK)      │  │ order_id (PK)     │             │
│  │ username          │  │ user_id (FK)      │             │
│  │ email             │  │ product_name      │             │
│  │ country           │  │ amount            │             │
│  │ signup_date       │  │ quantity          │             │
│  │ age               │  │ order_date        │             │
│  │                   │  │ status            │             │
│  │ Records: 8        │  │ Records: 12       │             │
│  └────────────────────┘  └────────────────────┘             │
│                                                               │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ Data Sources                                          │   │
│  ├──────────────────────────────────────────────────────┤   │
│  │                                                       │   │
│  │  data/seed.py → Pandas DataFrames                    │   │
│  │        └─ create_dummy_users()                        │   │
│  │        └─ create_dummy_orders()                       │   │
│  │                                                       │   │
│  │  Loaded into DuckDB on service initialization        │   │
│  │  Available as SQL-queryable tables                   │   │
│  │                                                       │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                               │
└───────────────────────────────────────────────────────────────┘
                            │
                            ▼
                   DuckDB SQL Engine
                   (Query Execution)
                            │
                            ▼
                     Results → JSON
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                   RESPONSE LAYER                             │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  {                                                            │
│    "success": true,                                          │
│    "columns": ["col1", "col2"],                              │
│    "rows": [[val1, val2], ...],                              │
│    "row_count": 42,                                          │
│    "data": [{"col1": val1, "col2": val2}, ...]               │
│  }                                                            │
│                                                               │
│  OR on error:                                                │
│                                                               │
│  {                                                            │
│    "detail": "Validation error: SQL injection detected"      │
│  }                                                            │
│                                                               │
└───────────────────────────────────────────────────────────────┘
                            │
                            ▼
                    Back to Client
```

## Request Flow Diagram

```
Client Request
      │
      ▼
┌─────────────────────────────────────────────┐
│ FastAPI Route Handler                       │
│ (GET /health, POST /query, etc.)            │
└─────────────┬───────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────┐
│ Pydantic Request Validation                 │
│ (Schema validation, type checking)          │
└─────────────┬───────────────────────────────┘
              │
         ┌────┴────┐
         ▼         ▼
      VALID?    INVALID?
         │         │
         ▼         ▼
       YES    Return Error (400)
         │
         ▼
┌─────────────────────────────────────────────┐
│ Security Module: validate_sql_query()       │
│ • Check dangerous keywords                  │
│ • Check injection patterns                  │
│ • Verify SELECT-only                        │
└─────────────┬───────────────────────────────┘
              │
         ┌────┴────┐
         ▼         ▼
      VALID?    INVALID?
         │         │
         ▼         ▼
       YES    Return Error (400)
         │
         ▼
┌─────────────────────────────────────────────┐
│ Security Module: validate_parameters()      │
│ • Check type (str, int, float, bool, None) │
│ • Check length (< 10,000 chars)             │
└─────────────┬───────────────────────────────┘
              │
         ┌────┴────┐
         ▼         ▼
      VALID?    INVALID?
         │         │
         ▼         ▼
       YES    Return Error (400)
         │
         ▼
┌─────────────────────────────────────────────┐
│ Placeholder Matching                        │
│ count(?) must equal len(params)             │
└─────────────┬───────────────────────────────┘
              │
         ┌────┴────┐
         ▼         ▼
      MATCH?   MISMATCH?
         │         │
         ▼         ▼
       YES    Return Error (400)
         │
         ▼
┌─────────────────────────────────────────────┐
│ DuckDB Query Execution                      │
│ conn.execute(sql, parameters=params)        │
│ (DuckDB binds values safely)                │
└─────────────┬───────────────────────────────┘
              │
         ┌────┴────────────┐
         ▼                 ▼
      SUCCESS?          ERROR?
         │                │
         ▼                ▼
    Convert to      Return Error (400/500)
    DataFrame
         │
         ▼
┌─────────────────────────────────────────────┐
│ Format Results                              │
│ • Extract columns                           │
│ • Convert to rows & dict format             │
│ • Count results                             │
└─────────────┬───────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────┐
│ Return JSON Response (200)                  │
│ {                                           │
│   "success": true,                          │
│   "columns": [...],                         │
│   "rows": [...],                            │
│   "row_count": N,                           │
│   "data": [...]                             │
│ }                                           │
└─────────────────────────────────────────────┘
              │
              ▼
          Client
```

## Security Validation Pipeline

```
     Raw User Input
            │
            ▼
    ┌──────────────────────────────┐
    │ Step 1: Keyword Check        │
    ├──────────────────────────────┤
    │ • Is it SELECT?              │
    │ • No DROP/DELETE/INSERT?     │
    │ • No comments (--/*)?        │
    │ • No dangerous keywords?     │
    └───────────┬──────────────────┘
                │
         ┌──────┴──────┐
         ▼             ▼
       PASS         FAIL →  Reject
         │
         ▼
    ┌──────────────────────────────┐
    │ Step 2: Pattern Detection    │
    ├──────────────────────────────┤
    │ • Check: ' OR '1'='1'        │
    │ • Check: UNION SELECT        │
    │ • Check: xp_, sp_ (exec)     │
    │ • Check: Quote+semicolon     │
    └───────────┬──────────────────┘
                │
         ┌──────┴──────┐
         ▼             ▼
       PASS         FAIL →  Reject
         │
         ▼
    ┌──────────────────────────────┐
    │ Step 3: Parameter Validation │
    ├──────────────────────────────┤
    │ For each parameter:          │
    │ • Check type (safe types?)   │
    │ • Check length (< 10k?)      │
    │ • Check count matches (?)    │
    └───────────┬──────────────────┘
                │
         ┌──────┴──────┐
         ▼             ▼
       PASS         FAIL →  Reject
         │
         ▼
    ┌──────────────────────────────┐
    │ Step 4: Execute with DuckDB  │
    ├──────────────────────────────┤
    │ • sql + params are separated │
    │ • DuckDB binds safely        │
    │ • Values treated as DATA     │
    │ • Never as CODE              │
    └──────────────┬───────────────┘
                   │
            ┌──────┴──────┐
            ▼             ▼
         SUCCESS       ERROR
            │             │
            ▼             ▼
        Results     Error Message
            │             │
            └──────┬──────┘
                   ▼
            Return to Client
```

## Technology Stack

```
┌─────────────────────────────────────────────────────────┐
│                   Presentation Layer                    │
├─────────────────────────────────────────────────────────┤
│  FastAPI (Swagger UI: /docs)                            │
│  Uvicorn (ASGI Server)                                  │
└─────────────────────────────────────────────────────────┘
                          │
┌─────────────────────────────────────────────────────────┐
│                   Application Layer                     │
├─────────────────────────────────────────────────────────┤
│  FastAPI Routes                                         │
│  Pydantic Models (validation)                           │
│  Custom Security Module                                 │
└─────────────────────────────────────────────────────────┘
                          │
┌─────────────────────────────────────────────────────────┐
│                   Data Access Layer                     │
├─────────────────────────────────────────────────────────┤
│  DuckDB (SQL Query Engine)                              │
│  Pandas (DataFrame Creation)                            │
└─────────────────────────────────────────────────────────┘
                          │
┌─────────────────────────────────────────────────────────┐
│                   Data Layer                            │
├─────────────────────────────────────────────────────────┤
│  In-Memory DataFrames (Users, Orders)                   │
│  Generated from seed.py                                 │
└─────────────────────────────────────────────────────────┘
```

## Deployment Architecture

```
Development Environment:
  Local Machine
    └─ Python 3.9+
    └─ uvicorn dev server
    └─ Auto-reload enabled
    └─ Swagger UI at /docs

Production Environment (Vercel):
  Vercel Serverless
    └─ Python runtime (3.11)
    └─ Automatic scaling
    └─ HTTPS enabled
    └─ vercel.json configuration
    └─ Cold start (data reloads)

Alternative Production:
  Docker / VPS
    └─ Python 3.9+ runtime
    └─ uvicorn server
    └─ External database (optional)
    └─ Persistent data (optional)
```

## File Dependencies

```
main.py
  ├─ depends on: FastAPI
  ├─ imports: services/duckdb_service.py
  ├─ imports: Pydantic models
  └─ exposes: All API routes

services/duckdb_service.py
  ├─ depends on: DuckDB, Pandas
  ├─ imports: data/seed.py
  ├─ imports: security/sql_validator.py
  └─ provides: Query execution

security/sql_validator.py
  ├─ no external imports
  ├─ pure Python validation
  └─ provides: SQL validation functions

data/seed.py
  ├─ depends on: Pandas
  └─ provides: Dummy DataFrames
```

---

This architecture ensures:
- ✅ **Security** - Multi-layer validation
- ✅ **Separation of Concerns** - Clean layered design
- ✅ **Scalability** - Modular components
- ✅ **Maintainability** - Clear flow and structure
- ✅ **Testability** - Isolated layers
