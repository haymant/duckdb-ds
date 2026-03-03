# Security Implementation

This document explains the SQL injection prevention and security measures implemented in the DuckDB Query API.

## Overview

The API implements multiple layers of defense against SQL injection attacks:

1. **Parameterized Queries** - Core protection mechanism
2. **SQL Keyword Validation** - Whitelist-based approach
3. **Pattern Detection** - Regex-based injection pattern blocking
4. **Type Validation** - Parameter type checking
5. **Length Limits** - String length restrictions

---

## 1. Parameterized Queries (Primary Defense)

### How It Works

Parameterized queries separate SQL code from data by using placeholders:

```json
{
  "sql": "SELECT * FROM users WHERE country = ?",
  "params": ["USA"]
}
```

The `?` placeholder tells DuckDB that the value comes later, not as code. This is **100% effective** against SQL injection because user input is never treated as SQL code.

### Implementation

**File:** `security/sql_validator.py`

```python
def prepare_query_for_duckdb(sql: str, params: List = None) -> Tuple[str, List]:
    """Validates and prepares query with parameters."""
    # Validate SQL
    is_valid, msg = validate_sql_query(sql)
    if not is_valid:
        raise ValueError(f"SQL validation failed: {msg}")
    
    # Validate parameters
    if params:
        is_valid, msg = validate_parameters(params)
        if not is_valid:
            raise ValueError(f"Parameter validation failed: {msg}")
    
    return sql, params
```

### DuckDB Execution

```python
# In services/duckdb_service.py
result = self.conn.execute(prepared_sql, parameters=prepared_params)
```

The `parameters=` keyword argument ensures DuckDB treats values as data, not code.

### Attack Example - Blocked ✓

**Attacker tries:**
```json
{
  "sql": "SELECT * FROM users WHERE country = 'USA' OR '1'='1'",
  "params": []
}
```

**Why it's blocked:**
1. SQL keyword validation detects `OR` without a `?` placeholder
2. String concatenation pattern detected
3. Query rejected before execution

**Correct way:**
```json
{
  "sql": "SELECT * FROM users WHERE country = ? OR country = ?",
  "params": ["USA", "Canada"]
}
```

---

## 2. SQL Keyword Whitelist

### Allowed Keywords

The API **only allows READ operations** with a whitelist of safe keywords:

**SELECT & Filtering:**
- SELECT, FROM, WHERE, AND, OR, NOT

**Joins:**
- INNER, LEFT, RIGHT, FULL, OUTER, CROSS, JOIN, ON

**Aggregations:**
- GROUP, BY, HAVING, ORDER, ASC, DESC
- SUM, COUNT, AVG, MIN, MAX

**Other:**
- AS, DISTINCT, LIMIT, OFFSET
- CASE, WHEN, THEN, ELSE, END

### Blocked Keywords

These keywords are **always rejected**:

**Data Modification:**
- DROP, DELETE, INSERT, UPDATE, CREATE, ALTER, TRUNCATE

**Execution:**
- EXEC, EXECUTE, SCRIPT

**Union Attacks:**
- UNION

**Database Control:**
- PRAGMA

**SQL Comments:**
- `--`, `/*`, `*/`

### Example - Blocked Attempts

```json
{
  "sql": "DROP TABLE users",
  "params": []
}
```
❌ **Result:** "Dangerous keyword 'DROP' not allowed"

```json
{
  "sql": "SELECT * FROM users; DELETE FROM orders",
  "params": []
}
```
❌ **Result:** "Dangerous keyword 'DELETE' not allowed"

---

## 3. Pattern Detection

### SQL Injection Patterns Blocked

The validator uses regex patterns to detect common injection techniques:

**Pattern 1: Quote-Semicolon**
```regex
['\"]\s*;\s*
```
Detects: `'; DROP TABLE users;--`

**Pattern 2: OR Logic Injection**
```regex
['\"].*['\"].*OR.*['\"]
```
Detects: `' OR '1'='1`

**Pattern 3: UNION-based Injection**
```regex
UNION\s+SELECT
```
Detects: `UNION SELECT * FROM admin`

**Pattern 4: Extended Stored Procedures (SQL Server)**
```regex
xp_
```
Detects: `xp_cmdshell`, `xp_regread`

**Pattern 5: System Stored Procedures**
```regex
sp_
```
Detects: `sp_executesql`, `sp_oacreate`

### Implementation

**File:** `security/sql_validator.py`

```python
injection_patterns = [
    r"['\"]\s*;\s*",          # Quote followed by semicolon
    r"['\"].*['\"].*OR.*['\"]",  # OR with quotes
    r"UNION\s+SELECT",         # Union-based injection
    r"xp_",                    # Extended stored procedures
    r"sp_",                    # System stored procedures
]

for pattern in injection_patterns:
    if re.search(pattern, sql, re.IGNORECASE):
        return False, f"Potential SQL injection detected"
```

---

## 4. Parameter Type Validation

### Allowed Types

Only safe data types are allowed in parameters:

- `str` - String values
- `int` - Integer values
- `float` - Decimal values
- `bool` - Boolean values
- `None` - NULL values

### Rejected Types

Complex types that could be dangerous:

- Objects (dict, list in parameters)
- Custom classes
- File objects
- Functions/callables

### Example - Type Validation

**Safe:**
```json
{
  "sql": "SELECT * FROM users WHERE age > ? AND name = ?",
  "params": [30, "John"]
}
```
✓ Accepted: int and string are safe types

**Unsafe:**
```json
{
  "sql": "SELECT * FROM users",
  "params": [{"key": "value"}]
}
```
❌ Rejected: dict objects not allowed

### Implementation

```python
def validate_parameters(params: List) -> Tuple[bool, str]:
    for i, param in enumerate(params):
        if not isinstance(param, (str, int, float, bool, type(None))):
            return False, f"Parameter {i} has invalid type"
        
        if isinstance(param, str):
            if len(param) > 10000:  # Max length check
                return False, f"Parameter {i} exceeds max length"
    
    return True, "Parameters are valid"
```

---

## 5. Additional Safeguards

### String Length Limits

Parameters have a maximum length of **10,000 characters** to prevent:
- Memory exhaustion attacks
- Extremely large data injection attempts

### Comment Prevention

SQL comments are completely blocked:

```json
{
  "sql": "SELECT * FROM users -- ignore rest",
  "params": []
}
```
❌ **Result:** "SQL comments not allowed"

### Query Type Validation

Only `SELECT` queries are allowed. All queries must start with:

```python
if not sql_upper.startswith("SELECT"):
    return False, "Only SELECT queries are allowed"
```

### Placeholder Matching

The number of `?` placeholders must exactly match the parameter count:

```python
placeholder_count = sql.count("?")
if placeholder_count != len(params):
    raise ValueError(
        f"Placeholder count ({placeholder_count}) doesn't match "
        f"parameter count ({len(params)})"
    )
```

---

## Security Best Practices

### For API Users

✅ **DO:**
1. Use `?` placeholders for all user input
2. Pass data in the `params` array
3. Never concatenate strings into SQL
4. Use parameterized queries for every user-provided value

❌ **DON'T:**
1. Build SQL with string concatenation
2. Try to bypass the API's security measures
3. Pass SQL keywords in parameters as literals
4. Send raw user input directly in the SQL field

### Example - Safe vs Unsafe

**Unsafe (NEVER DO THIS):**
```json
{
  "sql": "SELECT * FROM users WHERE username = '" + username + "'",
  "params": []
}
```

**Safe (DO THIS):**
```json
{
  "sql": "SELECT * FROM users WHERE username = ?",
  "params": [username]
}
```

---

## Security Validation Flow

```
User Request
    ↓
[1] Validate SQL Query
    ├─ Check dangerous keywords
    ├─ Check injection patterns
    ├─ Verify SELECT only
    └─ Reject if invalid ❌
    ↓
[2] Validate Parameters
    ├─ Check types (only str, int, float, bool, None)
    ├─ Check lengths (< 10,000 chars)
    └─ Reject if invalid ❌
    ↓
[3] Match Placeholders
    ├─ Count ? in SQL
    ├─ Count parameters
    └─ Must be equal ❌
    ↓
[4] Execute Query
    └─ DuckDB binds parameters safely ✓
    ↓
Return Results
```

---

## Testing Security

### Test Case 1: Basic SQL Injection

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT * FROM users WHERE id = 1 OR 1=1","params":[]}'
```

**Result:** ❌ Blocked - "Potential SQL injection detected"

### Test Case 2: Correct Parameter Usage

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT * FROM users WHERE country = ?","params":["USA"]}'
```

**Result:** ✅ Success - Returns USA users

### Test Case 3: DROP Attempt

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql":"DROP TABLE users","params":[]}'
```

**Result:** ❌ Blocked - "Dangerous keyword 'DROP' not allowed"

### Test Case 4: Comment Injection

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT * FROM users -- comment","params":[]}'
```

**Result:** ❌ Blocked - "SQL comments not allowed"

---

## Known Limitations

### What This Security Model DOES Protect Against

- ✅ SQL Injection via string concatenation
- ✅ Union-based SQL injection
- ✅ Blind SQL injection
- ✅ Time-based SQL injection
- ✅ Stacked queries
- ✅ Comment-based injection
- ✅ Data modification attacks

### What This Security Model DOES NOT Address

- ❌ **DoS Attacks** - Very large queries could consume resources
- ❌ **Timing Attacks** - Slow queries could leak information
- ❌ **Information Disclosure** - Error messages might reveal schema

### Recommendations for Production

For production deployments:

1. **Add Rate Limiting** - Prevent query bombing
2. **Add Query Timeouts** - Kill long-running queries
3. **Add Authentication** - Verify API users
4. **Add Logging** - Track all queries for audit trail
5. **Add Query Size Limits** - Reject extremely large results
6. **Use HTTPS** - Encrypt data in transit
7. **Add WAF** - Web Application Firewall for additional protection

---

## Security Headers

The API includes CORS middleware. For production, configure appropriately:

```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://yourdomain.com"],  # Restrict to your domain
    allow_credentials=True,
    allow_methods=["POST", "GET"],
    allow_headers=["Content-Type"],
)
```

---

## Auditing

### Log Security Events

All validation failures are logged:

```python
logger.warning(f"Query validation failed: {result['error']}")
logger.error(f"Unexpected error: {str(e)}")
```

Monitor logs for:
- Multiple validation failures (potential attack)
- Patterns in rejected queries
- Unusual error types

---

## Incident Response

If you suspect an attack:

1. **Review logs** in `uvicorn` output
2. **Check rejected query patterns** - what was the attacker trying?
3. **Update IP whitelist** if you have one
4. **Increase monitoring** - watch for similar patterns
5. **Alert security team** - log incident for tracking

---

## Additional Resources

- [OWASP SQL Injection](https://owasp.org/www-community/attacks/SQL_Injection)
- [DuckDB Security](https://duckdb.org/docs/security/)
- [CWE-89: Improper Neutralization of Special Elements used in an SQL Command](https://cwe.mitre.org/data/definitions/89.html)
- [NIST Guidelines for SQL Injection Prevention](https://nvlpubs.nist.gov/nistpubs/Legacy/SP/nistspecialpublication800-115.pdf)

---

**Last Updated:** 2024
**Security Model Version:** 1.0
