# Quick Start Guide

Get the DuckDB Query API running in 2 minutes!

## Option 1: Local Development (Recommended)

### 1. Install uv (One-time setup)
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 2. Setup and Run
```bash
# Navigate to the backend directory
cd fastapi-backend

# Create and activate virtual environment
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
uv pip install -e .

# Start the server
uvicorn main:app --reload
```

The API is now running at `http://localhost:8000` 🎉

### 3. Test It

**Health Check:**
```bash
curl http://localhost:8000/health
```

**Interactive Docs:**
Open browser to: `http://localhost:8000/docs`

**Sample Query:**
```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT * FROM users WHERE country = ?","params":["USA"]}'
```

## Option 2: Using pip (Traditional way)

```bash
cd fastapi-backend

# Create virtual environment
python -m venv venv
source venv/bin/activate  # or: venv\Scripts\activate on Windows

# Install from requirements
pip install -r requirements.txt

# Run
uvicorn main:app --reload
```

## Option 3: Deploy to Vercel

```bash
# From fastapi-backend directory
vercel
```

Follow the Vercel CLI prompts. Your API will be live in seconds!

## Key Files

| File | Purpose |
|------|---------|
| `main.py` | FastAPI application with all endpoints |
| `data/seed.py` | Dummy users & orders tables |
| `security/sql_validator.py` | SQL injection prevention |
| `services/duckdb_service.py` | DuckDB connection & queries |

## Available Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/` | API info |
| GET | `/health` | Health check |
| GET | `/schema` | Database schema |
| GET | `/tables/{name}` | Sample table data |
| POST | `/query` | Execute SQL query |

## Example Queries

**All Users:**
```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT * FROM users","params":[]}'
```

**Users by Country:**
```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT * FROM users WHERE country = ?","params":["USA"]}'
```

**User Orders (JOIN):**
```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT u.username, o.product_name, o.amount FROM users u INNER JOIN orders o ON u.user_id = o.user_id","params":[]}'
```

**Orders Summary (GROUP BY):**
```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT status, COUNT(*) as count, SUM(amount) as total FROM orders GROUP BY status","params":[]}'
```

## Need Help?

1. Check `/docs` endpoint in browser (Swagger UI)
2. Review `README.md` for detailed docs
3. Check error messages - they tell you what went wrong
4. See `README.md` for 15+ query examples

## Next Steps

- ✅ Run locally and explore the API
- ✅ Try different SQL queries
- ✅ Review the security implementation
- ✅ Deploy to Vercel
- ✅ Integrate with a frontend

Happy querying! 🚀
