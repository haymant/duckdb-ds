# DuckDB Query API - Quick Reference

## Installation & Setup

```bash
# Navigate to project
cd fastapi-backend

# Option 1: With uv (recommended)
uv venv
source .venv/bin/activate
uv pip install -e .

# Option 2: Traditional pip
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Option 3: Automated
bash setup.sh
```

## Running the API

```bash
# Development (with auto-reload)
uvicorn main:app --reload

# Production
uvicorn main:app --host 0.0.0.0 --port 8000

# Different port
uvicorn main:app --reload --port 8001
```

## API Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| GET | `http://localhost:8000/` | API info |
| GET | `http://localhost:8000/health` | Health check |
| GET | `http://localhost:8000/schema` | Database schema |
| GET | `http://localhost:8000/tables/{table}` | Table sample |
| POST | `http://localhost:8000/query` | Execute query |
| GET | `http://localhost:8000/docs` | Swagger UI |
| GET | `http://localhost:8000/redoc` | ReDoc |

## Quick Queries

### Health Check
```bash
curl http://localhost:8000/health
```

### Get Schema
```bash
curl http://localhost:8000/schema
```

### Simple SELECT
```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT * FROM users LIMIT 3","params":[]}'
```

### WHERE Clause
```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT * FROM users WHERE country = ?","params":["USA"]}'
```

### JOIN
```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT u.username, o.product_name FROM users u INNER JOIN orders o ON u.user_id = o.user_id","params":[]}'
```

### GROUP BY
```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT status, COUNT(*) as count FROM orders GROUP BY status","params":[]}'
```

### Multiple Parameters
```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT * FROM users WHERE country = ? AND age > ?","params":["USA", 30]}'
```

## Data Schema

### Users Table
```
user_id     | INTEGER
username    | VARCHAR
email       | VARCHAR
country     | VARCHAR
signup_date | TIMESTAMP
age         | INTEGER
```

**Sample:** 8 users from USA, Canada, Spain, UK, Australia

### Orders Table
```
order_id    | INTEGER
user_id     | INTEGER
product_name| VARCHAR
amount      | DECIMAL
quantity    | INTEGER
order_date  | TIMESTAMP
status      | VARCHAR
```

**Sample:** 12 orders with statuses: completed, pending, shipped

## SQL Syntax Rules

### Allowed Keywords
- SELECT, FROM, WHERE, AND, OR, NOT
- INNER, LEFT, RIGHT, FULL, OUTER, CROSS, JOIN, ON
- GROUP, BY, HAVING, ORDER, ASC, DESC
- SUM, COUNT, AVG, MIN, MAX
- AS, DISTINCT, LIMIT, OFFSET
- CASE, WHEN, THEN, ELSE, END

### Blocked Keywords
- DROP, DELETE, INSERT, UPDATE, CREATE, ALTER, TRUNCATE
- EXEC, EXECUTE, SCRIPT, UNION, PRAGMA
- Comments: `--`, `/*`, `*/`, `;`

### Query Format
```json
{
  "sql": "SELECT * FROM users WHERE country = ?",
  "params": ["USA"]
}
```

**Rules:**
- Only SELECT queries allowed
- Use `?` placeholders for parameters
- Parameters in separate array
- Must match number of placeholders

## Parameter Rules

### Allowed Types
- `string` - Text values
- `number` - Integer or decimal
- `boolean` - true/false
- `null` - SQL NULL

### Validation
- Max 10,000 characters per string
- Placeholder count must equal parameter count
- No special types (objects, arrays, etc.)

### Examples
✅ Safe:
```json
{"sql":"SELECT * FROM users WHERE age > ?","params":[30]}
{"sql":"SELECT * FROM orders WHERE status = ? AND amount < ?","params":["completed",500]}
```

❌ Unsafe:
```json
{"sql":"SELECT * FROM users WHERE age > 30 OR 1=1","params":[]}
{"sql":"SELECT * FROM users","params":[{"key":"value"}]}
```

## Python Client

```python
from examples.client import DuckDBAPIClient

client = DuckDBAPIClient("http://localhost:8000")

# Health check
status = client.health_check()

# Get schema
schema = client.get_schema()

# Query
result = client.query(
    "SELECT * FROM users WHERE country = ?",
    params=["USA"]
)

# Get as list of dicts
data = client.query_to_list(
    "SELECT * FROM users WHERE country = ?",
    params=["USA"]
)
```

## Common Query Patterns

### Count Records
```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT COUNT(*) as total FROM users","params":[]}'
```

### Filter with Multiple Conditions
```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT * FROM users WHERE country = ? AND age > ? AND signup_date > ?","params":["USA", 30, "2023-01-01"]}'
```

### Aggregate by Group
```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT country, AVG(age) as avg_age, COUNT(*) as count FROM users GROUP BY country ORDER BY count DESC","params":[]}'
```

### Join with Aggregation
```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT u.country, COUNT(o.order_id) as orders, SUM(o.amount) as revenue FROM users u LEFT JOIN orders o ON u.user_id = o.user_id GROUP BY u.country","params":[]}'
```

### Top N Records
```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT username, age FROM users ORDER BY age DESC LIMIT 5","params":[]}'
```

## Error Responses

### SQL Injection Attempt
```json
{
  "detail": "Validation error: Dangerous keyword 'DROP' not allowed"
}
```

### Query Error
```json
{
  "detail": "Query execution error: No attribute with name col_name"
}
```

### Invalid Request
```json
{
  "detail": "Placeholder count (1) doesn't match parameter count (2)"
}
```

## Troubleshooting

| Problem | Solution |
|---------|----------|
| Port 8000 in use | Use `--port 8001` |
| ModuleNotFoundError | Install dependencies: `uv pip install -e .` |
| Can't connect | Verify API running: `curl http://localhost:8000/health` |
| Query fails | Check syntax at `/docs`, review examples |
| Auth error | Check query uses parameterized format |

## Deployment

### Vercel
```bash
vercel
```

### Docker (example)
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY . .
RUN pip install -r requirements.txt
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

### Local Production
```bash
source .venv/bin/activate
uvicorn main:app --host 0.0.0.0 --port 8000
```

## Files Overview

| File | Lines | Purpose |
|------|-------|---------|
| `main.py` | 239 | FastAPI app |
| `services/duckdb_service.py` | 138 | Query execution |
| `security/sql_validator.py` | 175 | SQL injection prevention |
| `data/seed.py` | 123 | Dummy data |
| `README.md` | 523 | Full documentation |
| `SECURITY.md` | 502 | Security guide |
| `examples/CURL_EXAMPLES.sh` | 290 | 36 query examples |
| `examples/client.py` | 205 | Python client |

## Documentation Links

- **Setup:** `QUICKSTART.md`
- **Full Docs:** `README.md`
- **Security:** `SECURITY.md`
- **Project Info:** `PROJECT_SUMMARY.md`
- **Examples:** `examples/CURL_EXAMPLES.sh` or `examples/client.py`
- **Interactive:** `/docs` (Swagger UI)

## Key Features

✅ **Security**
- Parameterized queries (SQL injection proof)
- Keyword whitelist
- Pattern detection
- Type validation

✅ **SQL Support**
- SELECT, WHERE, JOIN, GROUP BY, ORDER BY
- Aggregations (COUNT, SUM, AVG, MIN, MAX)
- Subqueries, CASE statements
- DISTINCT, LIMIT, OFFSET

✅ **Data**
- 8 sample users
- 12 sample orders
- Realistic test data

✅ **Development**
- Fast local development
- Auto-reload
- Interactive /docs UI
- Python client library

## Need Help?

1. Check `/docs` in browser
2. Review `QUICKSTART.md` for setup
3. Check `README.md` for details
4. See `SECURITY.md` for SQL rules
5. Run examples from `examples/CURL_EXAMPLES.sh`

---

**Version:** 1.0.0
**Last Updated:** 2024
**Status:** Production Ready
