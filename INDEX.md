# DuckDB Query API - Documentation Index

Welcome! This guide will help you navigate the project documentation and get started quickly.

## 🚀 Start Here

### Just Want to Run It? (2 minutes)
👉 **Read:** [`QUICKSTART.md`](./QUICKSTART.md)
- Installation steps
- Run the server
- First test

### Want a Reference Card?
👉 **Read:** [`REFERENCE.md`](./REFERENCE.md)
- Commands & endpoints
- Common queries
- Query patterns

### Need Full Documentation?
👉 **Read:** [`README.md`](./README.md)
- Complete API docs
- 15+ detailed examples
- Troubleshooting
- Performance notes

## 🔒 Security & Implementation

### Want to Understand Security?
👉 **Read:** [`SECURITY.md`](./SECURITY.md)
- How SQL injection prevention works
- Security validation flow
- Testing security
- Production recommendations

### Want Project Overview?
👉 **Read:** [`PROJECT_SUMMARY.md`](./PROJECT_SUMMARY.md)
- What was built
- Key features
- File statistics
- Next steps

## 📚 Code & Examples

### Python Examples
👉 **File:** [`examples/client.py`](./examples/client.py)
```python
from examples.client import DuckDBAPIClient
client = DuckDBAPIClient()
result = client.query("SELECT * FROM users WHERE country = ?", ["USA"])
```

### cURL Examples (36 total)
👉 **File:** [`examples/CURL_EXAMPLES.sh`](./examples/CURL_EXAMPLES.sh)

Run individual commands to test features:
```bash
# Simple query
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT * FROM users","params":[]}'
```

## 📁 Project Structure

```
fastapi-backend/
├── main.py                    # FastAPI app (239 lines)
├── data/seed.py              # Sample data (123 lines)
├── services/duckdb_service.py # Query execution (138 lines)
├── security/sql_validator.py  # SQL injection prevention (175 lines)
│
├── 📖 DOCUMENTATION
│   ├── INDEX.md              # ← You are here
│   ├── QUICKSTART.md         # 2-minute setup
│   ├── README.md             # Full documentation
│   ├── REFERENCE.md          # Quick reference
│   ├── SECURITY.md           # Security details
│   └── PROJECT_SUMMARY.md    # Project overview
│
├── 📝 EXAMPLES
│   ├── client.py             # Python client library
│   └── CURL_EXAMPLES.sh      # 36 curl examples
│
└── ⚙️ CONFIG
    ├── pyproject.toml        # Dependencies (uv)
    ├── requirements.txt      # Dependencies (pip)
    ├── vercel.json          # Vercel deployment
    └── .gitignore           # Git config
```

## 🎯 Quick Links by Use Case

### "I want to use this API"
1. Run: [`QUICKSTART.md`](./QUICKSTART.md)
2. Query: [`REFERENCE.md`](./REFERENCE.md)
3. Details: [`README.md`](./README.md)

### "I want to understand the code"
1. Overview: [`PROJECT_SUMMARY.md`](./PROJECT_SUMMARY.md)
2. Security: [`SECURITY.md`](./SECURITY.md)
3. Code: Check `main.py`, `services/`, `security/`

### "I want to deploy this"
1. Quick: [`QUICKSTART.md`](./QUICKSTART.md) (Vercel section)
2. Config: Check `vercel.json`
3. Docs: [`README.md`](./README.md) (Deployment section)

### "I want example queries"
1. Quick: [`REFERENCE.md`](./REFERENCE.md) (Common Patterns)
2. Full: [`examples/CURL_EXAMPLES.sh`](./examples/CURL_EXAMPLES.sh) (36 examples)
3. Programmatic: [`examples/client.py`](./examples/client.py) (Python)

### "I'm concerned about security"
1. Read: [`SECURITY.md`](./SECURITY.md)
2. Understand: How parameterized queries work
3. Test: Security test cases in SECURITY.md

## ⚡ Commands Reference

```bash
# Setup
cd fastapi-backend
uv venv && source .venv/bin/activate
uv pip install -e .

# Run
uvicorn main:app --reload

# Test
curl http://localhost:8000/health

# Interactive UI
# Open: http://localhost:8000/docs
```

## 📊 Data Overview

### Users Table
- 8 dummy users
- From: USA, Canada, Spain, UK, Australia
- Ages: 26-52 years
- Columns: user_id, username, email, country, signup_date, age

### Orders Table
- 12 dummy orders
- Products: electronics and accessories
- Amounts: $15.99 - $1200.50
- Statuses: completed, pending, shipped
- Columns: order_id, user_id, product_name, amount, quantity, order_date, status

## 🔍 Find What You Need

| What I Need | Go To |
|------------|-------|
| Get started NOW | [`QUICKSTART.md`](./QUICKSTART.md) |
| Setup help | [`QUICKSTART.md`](./QUICKSTART.md) |
| API reference | [`REFERENCE.md`](./REFERENCE.md) |
| Full docs | [`README.md`](./README.md) |
| Security info | [`SECURITY.md`](./SECURITY.md) |
| Query examples | [`examples/CURL_EXAMPLES.sh`](./examples/CURL_EXAMPLES.sh) |
| Python client | [`examples/client.py`](./examples/client.py) |
| Project info | [`PROJECT_SUMMARY.md`](./PROJECT_SUMMARY.md) |
| Interactive UI | `http://localhost:8000/docs` |
| Schema info | `curl http://localhost:8000/schema` |

## ✨ Key Features

✅ **Secure** - Multi-layer SQL injection prevention
✅ **Fast** - In-memory DuckDB queries
✅ **Simple** - Parameterized query interface
✅ **Well-Documented** - 2000+ lines of docs
✅ **Ready-to-Deploy** - Vercel serverless ready
✅ **Well-Tested** - 36 example queries included

## 🎓 Learning Path

### Beginner
1. Read: `QUICKSTART.md` (5 min)
2. Run: `setup.sh` (2 min)
3. Test: Simple query from `REFERENCE.md` (2 min)
4. Explore: `/docs` UI (5 min)

### Intermediate
1. Read: `README.md` sections (30 min)
2. Try: Different queries from `examples/` (15 min)
3. Build: Your own queries (15 min)

### Advanced
1. Read: `SECURITY.md` (30 min)
2. Review: Code in `main.py` and `security/` (20 min)
3. Deploy: To Vercel (10 min)
4. Extend: Add custom features (varies)

## 📞 Support

### Error Messages?
Check: `README.md` - Troubleshooting section

### Query Syntax Issues?
Check: `REFERENCE.md` - Common Query Patterns

### Security Questions?
Check: `SECURITY.md`

### Want More Examples?
Check: `examples/CURL_EXAMPLES.sh` (36 examples)

### Deployment Help?
Check: `README.md` - Deployment section

## 🚀 Next Steps

1. **Get it running** → [`QUICKSTART.md`](./QUICKSTART.md)
2. **Learn the API** → [`REFERENCE.md`](./REFERENCE.md) + `/docs`
3. **Understand security** → [`SECURITY.md`](./SECURITY.md)
4. **Deploy** → `vercel`
5. **Integrate** → Use with your frontend

## 📈 Project Stats

| Metric | Value |
|--------|-------|
| Total Files | 20+ |
| Code Lines | 675 |
| Doc Lines | 2000+ |
| Example Queries | 36 |
| API Endpoints | 5 |
| Sample Records | 20 |
| Security Layers | 5 |

## 🎉 You're Ready!

Pick a guide above and get started. Most users are up and running in 2-5 minutes.

**Recommended first step:** [`QUICKSTART.md`](./QUICKSTART.md)

---

**Last Updated:** 2024
**Documentation Version:** 1.0
**API Version:** 1.0.0
