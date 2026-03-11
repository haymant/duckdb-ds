# DuckDB Query API

A secure FastAPI REST API service for executing SQL queries on in-memory DataFrames using DuckDB. Includes built-in SQL injection prevention and parameterized query support.

## Features

✨ **Core Features:**
- 🔒 SQL injection prevention with parameterized queries
- 📊 In-memory DuckDB with dummy users and orders tables
- 📝 Support for advanced SQL features (JOINs, GROUP BY, aggregations, subqueries)
- 🌐 CORS configured to allow `*.lizhao.net` origins plus localhost:3000 for local testing
- 📚 Interactive API documentation (Swagger UI)
- 🚀 Vercel serverless deployment ready
- 🛠️ Local development with `uv` package manager

## Project Structure

```
fastapi-backend/
├── main.py                 # FastAPI application and routes
├── pyproject.toml          # Project configuration and dependencies
├── requirements.txt        # pip requirements
├── vercel.json            # Vercel deployment configuration
├── data/
│   └── seed.py            # Dummy data generation (users, orders)
├── services/
│   └── duckdb_service.py  # DuckDB connection and query execution
├── security/
│   └── sql_validator.py   # SQL injection prevention
└── README.md              # This file
```

## Data Schema

### Users Table

```sql
CREATE TABLE users (
    user_id INTEGER PRIMARY KEY,
    username VARCHAR,
    email VARCHAR,
    country VARCHAR,
    signup_date TIMESTAMP,
    age INTEGER
);
```

**Sample Data:**
- 8 users with various countries (USA, Canada, Spain, UK, Australia)
- Age range: 26-52 years
- Signup dates: Jan 2023 - Aug 2023

### Orders Table

```sql
CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    user_id INTEGER,
    product_name VARCHAR,
    amount DECIMAL,
    quantity INTEGER,
    order_date TIMESTAMP,
    status VARCHAR
);
```

**Sample Data:**
- 12 orders from various users
- Product variety: electronics and accessories
- Order dates: Jan 2024 - Mar 2024
- Statuses: completed, pending, shipped

## Getting Started

> 🔐 **Note:** the API is protected by a simple bearer-token scheme.
> Requests must include an `Authorization: Bearer <token>` header.  The
> Swagger UI (`/docs`) is publicly accessible so you can see the
> documentation, but you still need to click **Authorize** and provide a
> valid token before executing any operations.  Valid tokens are provided
> via the `CLIENT_TOKENS` environment variable (a comma-separated list).
>
> For local development the project loads values from `.env.local` if
> present; an example file is included in the repository.


### Local Development

All data used by the service is seeded automatically; there is no
external dependency required to run the server locally.

> **Google Cloud Storage**  
> If you need to query Parquet files stored in GCS the service can
> automatically configure DuckDB with the appropriate credentials.  Set
> `GCS_KEY_ID` and `GCS_KEY_SECRET` (for a service account) in your
> environment before startup and the server will issue a `CREATE OR
> REPLACE SECRET` statement on its own connection.  Once configured you
> may use `read_parquet('gcs://...')` and similar functions in your SQL
> queries.


#### Authentication

Create a `.env.local` file in the project root (this file is ignored by
Git).  Define `CLIENT_TOKENS` with one or more comma-separated bearer
keys:

```bash
# .env.local
CLIENT_TOKENS=sdk-1234567890abcdef,sdk-abcdef1234567890
```

The application reads this variable at startup and applies the same
list to both the runtime and the documentation UI.

When making requests you must send the header:

```
Authorization: Bearer <token>
```

You can also use the **Authorize** button in the Swagger UI; the value
appears in the “locked” icon next to each operation.  Note that `/docs`
and `/openapi.json` themselves remain open so you can view the API
specification without a token.


#### Prerequisites
- Python 3.12+ 
- `uv` package manager ([install uv](https://docs.astral.sh/uv/getting-started/installation/))

#### Setup

1. **Navigate to the project:**
```bash
cd fastapi-backend
```

2. **Create virtual environment with `uv`.**
`uv` bundles its own lightweight Python distribution and manages the
venv for you.  Run:

```bash
uv venv
```

If a `.venv` already exists you will be prompted to replace it.

3. **Activate the environment.**

```bash
# Unix/macOS:
source .venv/bin/activate

# Windows PowerShell/CMD:
.venv\Scripts\activate
```

Once activated the `uv`-managed Python is on your `PATH` and you can
use `uv pip` to install packages inside it.

4. **Install dependencies.**

```bash
# preferred (editable install of the current project):
uv pip install -e .

# or install from requirements.txt:
uv pip install -r requirements.txt
```

> **Tip:** do **not** run `pip install` directly unless the venv has
> been created by another tool.  Using `uv pip` avoids the PEP-668
> "externally managed environment" errors that occur when uv’s
> Python is used.

> A missing `fastapi` or similar `ModuleNotFoundError` usually means the
> above install step was skipped or executed outside the activated
> environment.

#### Run Local Server




With the environment active you can start the server directly,
or use `uv run` which ensures the correct Python interpreter is used.

```bash
# after `source .venv/bin/activate`
uvicorn main:app --reload --host 0.0.0.0 --port 8000

# alternatively (no activation required):
uv run uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

> **Debugging in VS Code**
>
> A `launch.json` has been added under `.vscode/` with a configuration that
> starts the FastAPI app under the debugger using the `uvicorn` module.
> Open the Run view (Ctrl+Shift+D), select **Python: Debug FastAPI
> (module uvicorn)** and press F5.  Breakpoints in `main.py` (and other
> modules) will be hit, and hot‑reload works as usual.  The configuration
> already sets `"subProcess": true` so the debugger follows the reloader
> child process.
>
> You may still need to activate the `.venv` before debugging if VS Code
> doesn’t auto‑activate it.

The most common startup error is a `ModuleNotFoundError` for a
dependency — this indicates you either forgot to install the requirements
or you’re running the command outside the uv-managed environment.

The API will be available at `http://localhost:8000`

Access interactive documentation:
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

## API Endpoints

### Health Check

```bash
curl http://localhost:8000/health
```

**Response:**
```json
{
  "status": "healthy",
  "message": "DuckDB Query API is running"
}
```

### Get Schema

```bash
curl http://localhost:8000/schema
```

**Response:** Shows column names and types for all tables.



### Get Table Sample

```bash
curl "http://localhost:8000/tables/users?limit=3"
```

### Query Endpoint (Main)

**URL:** `POST /query`

**Request Body:**
```json
{
  "sql": "SELECT * FROM users WHERE country = ?",
  "params": ["USA"]
}
```

## Query Examples

All examples use curl. Copy and paste into terminal.

### 1. Simple SELECT (All Users)

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT user_id, username, country FROM users",
    "params": []
  }'
```

### 2. WHERE Clause (Filter by Country)

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT username, email, country FROM users WHERE country = ?",
    "params": ["USA"]
  }'
```

### 3. AND/OR Conditions

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT username, age FROM users WHERE country = ? AND age > ?",
    "params": ["USA", 30]
  }'
```

### 4. ORDER BY

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT username, age FROM users ORDER BY age DESC LIMIT 5",
    "params": []
  }'
```

### 5. GROUP BY with Aggregation

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT country, COUNT(*) as user_count FROM users GROUP BY country ORDER BY user_count DESC",
    "params": []
  }'
```

### 6. COUNT and SUM Aggregations

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT COUNT(*) as total_orders, SUM(amount) as total_revenue, AVG(amount) as avg_order_value FROM orders",
    "params": []
  }'
```

### 7. INNER JOIN

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT u.username, o.product_name, o.amount, o.order_date FROM users u INNER JOIN orders o ON u.user_id = o.user_id WHERE u.country = ?",
    "params": ["USA"]
  }'
```

### 8. LEFT JOIN

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT u.username, COUNT(o.order_id) as order_count FROM users u LEFT JOIN orders o ON u.user_id = o.user_id GROUP BY u.username ORDER BY order_count DESC",
    "params": []
  }'
```

### 9. Complex JOIN with WHERE and GROUP BY

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT u.country, COUNT(o.order_id) as total_orders, SUM(o.amount) as total_spent FROM users u LEFT JOIN orders o ON u.user_id = o.user_id WHERE o.status = ? GROUP BY u.country",
    "params": ["completed"]
  }'
```

### 10. DISTINCT Values

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT DISTINCT country FROM users ORDER BY country",
    "params": []
  }'
```

### 11. CASE Statement (Conditional Logic)

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT username, age, CASE WHEN age < 30 THEN '\''Young'\'' WHEN age < 45 THEN '\''Adult'\'' ELSE '\''Senior'\'' END as age_group FROM users",
    "params": []
  }'
```

### 12. Subquery

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT username FROM users WHERE user_id IN (SELECT user_id FROM orders WHERE amount > ? GROUP BY user_id HAVING COUNT(*) > ?)",
    "params": [500, 1]
  }'
```

### 13. AVG, MIN, MAX with HAVING

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT product_name, COUNT(*) as sales_count, AVG(amount) as avg_price, MIN(amount) as min_price, MAX(amount) as max_price FROM orders GROUP BY product_name HAVING COUNT(*) > ?",
    "params": [1]
  }'
```

### 14. Order Status Summary

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT status, COUNT(*) as count, SUM(amount) as total_amount FROM orders GROUP BY status",
    "params": []
  }'
```

### 15. User Purchase History (Multiple Joins)


```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT u.username, u.email, o.order_id, o.product_name, o.amount, o.order_date, o.status FROM users u INNER JOIN orders o ON u.user_id = o.user_id WHERE u.age > ? ORDER BY o.order_date DESC",
    "params": [30]
  }'
```

## Security Features
- ✅ Cross-Origin Resource Sharing (CORS) is restricted by default to
  any subdomain of `lizhao.net` via a regex so only approved frontends can
  make browser requests to the API.

### SQL Injection Prevention

The API uses **parameterized queries** to prevent SQL injection attacks:

✅ **Safe - Use parameterized queries:**
```json
{
  "sql": "SELECT * FROM users WHERE country = ?",
  "params": ["USA"]
}
```

❌ **Unsafe - Never concatenate strings:**
```json
{
  "sql": "SELECT * FROM users WHERE country = 'USA' OR '1'='1'"
}
```

### Validation Mechanisms

1. **Parameterized Query Binding** - All parameters are bound using `?` placeholders
2. **SQL Keyword Whitelist** - Only SELECT queries allowed, dangerous keywords (DROP, DELETE, INSERT, etc.) are rejected
3. **Pattern Detection** - Common SQL injection patterns are detected and blocked
4. **Type Checking** - Parameters are validated for correct data types

### Dangerous Keywords Blocked

- Data modification: `DROP`, `DELETE`, `INSERT`, `UPDATE`, `CREATE`, `ALTER`, `TRUNCATE`
- Execution: `EXEC`, `EXECUTE`, `SCRIPT`
- Union attacks: `UNION`
- Database control: `PRAGMA`
- SQL comments: `--`, `/*`, `*/`

## Deployment

### Vercel Deployment

1. **Install Vercel CLI (optional):**
```bash
npm i -g vercel
```

2. **Deploy from project root:**
```bash
cd fastapi-backend
vercel
```

3. **Environment Configuration:**
   - No environment variables needed for basic setup
   - Vercel uses the `@vercel/python` builder defined in `vercel.json`.
     (the `runtime` property is no longer supported and has been removed.)

The `vercel.json` file should look like this:

```json
{
  "version": 2,
  "env": { "PYTHONUNBUFFERED": "1" },
  "builds": [
    { "src": "main.py", "use": "@vercel/python" }
  ],
  "routes": [
    { "src": "/(.*)", "dest": "main.py" }
  ]
}
```

> If you see an error about an “additional property `runtime`”, simply
> delete that field and re‑run `vercel`.

### Alternative: Manual Vercel Setup

Create `vercel.json` in your project root (already included):
```json
{
  "version": 2,
  "runtime": "python@3.11",
  "builds": [{"src": "main.py", "use": "@vercel/python"}],
  "routes": [{"src": "/(.*)", "dest": "main.py"}]
}
```

Visit your deployment URL and append `/docs` for interactive API documentation.

## Performance Notes

- **In-Memory Database** - Data is loaded on each app restart
- **Single Query Execution** - Each request executes one query
- **No Persistence** - Data is not saved to disk (for demo purposes)

For production use with persistent data:
1. Add database persistence layer (SQLite, PostgreSQL, etc.)
2. Implement connection pooling
3. Add query caching/memoization
4. Set up monitoring and logging

## Error Handling

The API returns detailed error messages for debugging:

**SQL Validation Error:**
```json
{
  "detail": "Validation error: Dangerous keyword 'DROP' not allowed"
}
```

**Query Execution Error:**
```json
{
  "detail": "Query execution error: No attribute with name col_name"
}
```

## Testing

Run the following curl commands to test all features:

```bash
# Test health check
curl http://localhost:8000/health

# Test schema
curl http://localhost:8000/schema

# Test table sample
curl "http://localhost:8000/tables/users?limit=3"

# Test simple query
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT * FROM users LIMIT 2","params":[]}'
```

## Troubleshooting

### Authentication errors

- **401 Unauthorized** – the `Authorization` header is missing or the
  bearer token is not listed in `CLIENT_TOKENS`.
- **403 Forbidden** – the scheme was not `Bearer` or the token is
  invalid.

Make sure your environment variable is correctly set and that you send
`Authorization: Bearer ...` on every request (including requests made by
Swagger UI).


## Troubleshooting

### Port Already in Use

If port 8000 is already in use:
```bash
uvicorn main:app --reload --port 8001
```

### Import Errors

Ensure all dependencies are installed:
```bash
uv pip install -r requirements.txt
```

### ModuleNotFoundError

Make sure you're in the correct directory and virtual environment is activated:
```bash
cd fastapi-backend
source .venv/bin/activate  # or .venv\Scripts\activate on Windows
```

## API Response Format

### Successful Query Response

```json
{
  "success": true,
  "columns": ["user_id", "username", "country"],
  "rows": [[1, "alice_smith", "USA"], [2, "bob_jones", "USA"]],
  "row_count": 2,
  "data": [
    {"user_id": 1, "username": "alice_smith", "country": "USA"},
    {"user_id": 2, "username": "bob_jones", "country": "USA"}
  ]
}
```

### Error Response

```json
{
  "detail": "Validation error: Dangerous keyword 'DROP' not allowed"
}
```

## License

MIT

## Support

For issues or questions:
1. Check the error message details
2. Review the SQL query syntax in DuckDB documentation
3. Ensure parameters match the `?` placeholders in your query
4. Verify that table names and column names are correct

## Next Steps

- Add authentication (JWT tokens)
- Implement query result caching
- Add data export endpoints (CSV, JSON)
- Add query history logging
- Implement rate limiting
- Add transaction support
