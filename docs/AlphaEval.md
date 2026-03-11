# Alpha Feature On-Demand Evaluation in `duck-server`

## Problem Statement

`duck-server` currently executes validated `SELECT` statements against:

- in-memory seeded tables: `users`, `orders`
- rewritten `ochlvf` parquet sources via `read_parquet(...)`

The missing capability is to let SQL reference `alpha158` and `alpha360` in `FROM` and `JOIN` clauses, for example:

```sql
SELECT o.symbol, o.datetime, o.close, a.MA5
FROM ochlvf_AAPL o
JOIN alpha158_AAPL a
  ON o.symbol = a.instrument AND o.datetime = a.datetime
WHERE o.datetime BETWEEN '2019-01-01' AND '2019-12-31';
```

When such a query arrives, `duck-server` should:

1. detect alpha tables during SQL preprocessing
2. infer the required instruments and date range from SQL
3. lazily initialize the matching `featureHandler` data handler
4. materialize `dh._data` into a pandas `DataFrame`
5. cache the computed alpha data by feature type and query-relevant parameters
6. compute only missing instruments for later queries and merge them into the cached dataframe
7. register the dataframe as a DuckDB table before query execution

## Current Context Constraints

This design must reflect the actual `duck-server` codebase today:

- `duck-server` is a synchronous FastAPI + DuckDB service.
- SQL validation and `ochlvf` rewriting currently live in `security/sql_validator.py`.
- `duck-server` does not currently declare `featureHandler` in its own `pyproject.toml`, but local development can install it explicitly with `uv pip install -e ../featureHandler`.
- `featureHandler` requires explicit initialization via `featureHandler.init(...)` before `Alpha158` / `Alpha360` can be used.
- `Alpha158` and `Alpha360` return processed data via `dh._data`; their shape is driven by `featureHandler` internals and must be normalized in `duck-server` before DuckDB registration.

Because of this, alpha integration must stay optional and lazy. When `ALPHA_PROVIDER_URI` is absent, the default path should switch to a duck-server-backed raw-data provider rather than failing immediately.

## Goals

- Support `alpha158` and `alpha360` in `FROM` / `JOIN` clauses.
- Reuse SQL parsing logic already needed for `ochlvf`.
- Materialize alpha data only when referenced by a query.
- Cache alpha results by feature kind and materialization parameters.
- Incrementally extend cached data when a later query asks for new instruments.
- Support joins between alpha data and `ochlvf` on symbol/instrument and datetime.
- Keep the current SQL validation model intact.

## Non-Goals

- Persist alpha cache across process restarts.
- Rewrite `featureHandler` internals.
- Support unrestricted whole-universe alpha evaluation by default.
- Change FastAPI route contracts unless required for configuration.

## High-Level Design

### Execution Flow

1. `DuckDBService.execute_query()` receives SQL.
2. `prepare_query_for_duckdb()` validates the SQL and rewrites `ochlvf` references.
3. A shared SQL parsing module extracts:
   - referenced tables and aliases
   - projected columns where useful
   - instrument predicates
   - datetime predicates
4. If alpha tables are referenced, `DuckDBService` asks `FeatureManager` to ensure each alpha table is materialized and registered.
5. `FeatureManager`:
   - computes a cache key
   - checks which instruments are already cached
   - computes only the missing instruments
  - when `ALPHA_PROVIDER_URI` is present, uses the existing `featureHandler.init(...)` path
  - when `ALPHA_PROVIDER_URI` is absent, swaps in a duck-server-backed provider so `featureHandler` evaluates expressions against `ochlvf` data returned by duck-server
  - normalizes and merges the new dataframe into the cached dataframe
   - registers the dataframe in DuckDB under the SQL-visible table name
6. DuckDB executes the prepared SQL.

### Why the duck-server-backed provider is the default fallback

`featureHandler` already contains the expression engine for Alpha158 and Alpha360. The hard dependency on `ALPHA_PROVIDER_URI` comes from the default provider `D`, which reads local qlib bin files. The most accurate low-risk change is therefore:

- keep `featureHandler`'s expression engine and processors unchanged
- replace only the provider/storage layer when no provider URI is configured
- fetch raw `ochlvf` rows from duck-server for the requested symbols and date window only
- keep those raw rows in-memory for the lifetime of the alpha materialization call

This avoids duplicating the alpha formulas in DuckDB or pandas while also avoiding a greedy load of all symbols.

### Why shared SQL parsing

`ochlvf` already needs SQL parsing to derive symbol-specific parquet reads. Alpha support needs the same categories of parsing:

- table discovery in `FROM` / `JOIN`
- alias resolution
- symbol predicate extraction
- datetime predicate extraction
- optional projection discovery for optimization later

Duplicating that logic in multiple places would drift quickly. The plan is to extract shared parsing helpers into `security/sql_parser.py` and make both `ochlvf` rewriting and alpha table materialization depend on it.

## Proposed Modules

### `security/sql_parser.py`

Reusable, side-effect-free helpers:

- `extract_table_refs(sql) -> list[TableRef]`
- `extract_where_clause(sql) -> str | None`
- `extract_symbol_filters(sql, aliases=None) -> set[str]`
- `extract_datetime_range(sql, aliases=None) -> tuple[str | None, str | None]`
- `extract_projection_map(sql) -> dict[str | None, list[str]]`
- `translate_like_to_glob(pattern) -> str`

`TableRef` should at least contain:

- `table_name`
- `alias`
- `source_keyword` (`FROM` or `JOIN`)

This parser does not need to be a full SQL grammar. It only needs to cover the query forms supported by `duck-server` today.

### `security/sql_features.py`

Thin alpha-specific logic built on top of the shared parser:

- `find_alpha_table_refs(sql)`
- `infer_alpha_request(sql, table_ref)`

`infer_alpha_request(...)` returns a normalized request object containing:

- feature kind: `alpha158` or `alpha360`
- sql-visible table name: e.g. `alpha158`, `alpha158_AAPL`
- requested instruments
- inferred date range

### `services/feature_manager.py`

Responsible for lazy evaluation, cache management, normalization, and DuckDB registration.

Public API:

- `ensure_registered(alpha_request) -> pd.DataFrame`
- `clear_cache()`
- `get_cache_snapshot()`

Runtime behavior:

- if `feature_factory` is injected, use it first for tests
- else if `provider_uri` is configured, initialize `featureHandler` normally
- else use a duck-server-backed provider that queries `ochlvf` through the shared DuckDB connection

New collaborator:

- `DataQueryService.query_dataframe(sql, params) -> pd.DataFrame`

New adapter module:

- `services/feature_handler_adapter.py`
  - `DuckServerFeatureProvider`
  - `feature_handler_provider_context(...)`

Internal cache key:

```text
(feature_name, start_time, end_time, freq, fit_start_time, fit_end_time, provider_uri, region)
```

The key must include any parameter that changes alpha values or the backing data source.

### `services/duckdb_service.py`

Changes:

- initialize `FeatureManager`
- add optional alpha runtime configuration
- preprocess alpha requests before executing the query
- include registered alpha tables in schema and sampling endpoints when present

## Alpha Runtime Configuration

Because `duck-server` does not currently own `featureHandler` runtime setup, the alpha path needs explicit configuration.

Environment variables or constructor config should provide:

- `ALPHA_PROVIDER_URI`
- `ALPHA_REGION` (default `REG_US`)
- `ALPHA_FREQ` (default `day`)
- `ALPHA_ALLOW_FULL_SCAN` (default `false`)

Behavior:

- If alpha tables are referenced and `ALPHA_PROVIDER_URI` is defined, initialize `featureHandler` only once per process.
- If alpha tables are referenced and `ALPHA_PROVIDER_URI` is not defined, use duck-server as the raw market-data source.
- Avoid importing `featureHandler` until the first alpha query so the base `duck-server` startup stays unchanged for non-alpha use.

Code defaults when env variables are absent:

```bash
ALPHA_REGION='REG_US'
ALPHA_FREQ='day'
ALPHA_ALLOW_FULL_SCAN='false'
```

## Data Normalization Rules

`featureHandler` returns a pandas dataframe from `dh._data`, but `duck-server` needs a stable DuckDB-facing shape.

Normalization rules:

1. Convert index levels into columns if needed.
2. Standardize instrument column naming to `instrument`.
3. Keep a `datetime` column for joins.
4. Uppercase instrument values for cache membership and joins.
5. Preserve all alpha feature columns as returned.
6. Deduplicate on `(instrument, datetime)` when merging incremental results.

## Cache Design

### Cache Entry

- `dataframe`: merged normalized alpha dataframe
- `instruments`: set of already materialized instruments
- `lock`: per-entry lock for concurrent requests
- `last_updated`: timestamp

### Incremental load behavior

For a request on `alpha158`:

1. compute `missing = requested_instruments - cached_instruments`
2. if empty, reuse cached dataframe
3. if not empty:
   - instantiate alpha handler only for missing instruments
   - read `dh._data`
   - normalize new rows
   - merge into cached dataframe
   - register updated dataframe

This ensures evaluation happens once per instrument per cache key.

## Join Semantics with `ochlvf`

Supported primary join pattern:

```sql
... JOIN alpha158 a
ON o.symbol = a.instrument AND o.datetime = a.datetime
```

Requirements:

- `ochlvf` rewrite and alpha materialization must agree on uppercase symbol normalization.
- `datetime` types must be compatible for DuckDB joins.
- the alpha materialization window should be restricted by the SQL datetime filter when present to avoid unnecessary compute.
- duck-server must only read the requested symbols' `ochlvf` rows for feature materialization; no fallback path should greedily load all symbols.

## Error Handling

- Validation error:
  - SQL fails base validation
  - alpha query omits symbol restrictions while full scan is disabled
- Execution error:
  - `featureHandler` is not importable
  - alpha runtime configuration is missing
  - alpha materialization fails
  - normalized alpha dataframe lacks required join columns

## Documentation Corrections From Earlier Draft

The earlier draft was too generic in a few places. The corrected assumptions are:

- `duck-server` is not yet packaged with `featureHandler`; local editable installation is expected during development and runtime import remains lazy.
- current testing lives in `test_duckdb_service.py`, not a dedicated `tests/` package.
- current service schema endpoints only know about `users` and `orders`; alpha/registered tables must be added dynamically.
- the `ochlvf` rewrite exists inside `security/sql_validator.py`, so the first refactor step is shared parsing extraction rather than feature logic alone.
- the public HTTP contract already exists at `POST /query`; curl-based SIT coverage should target that route instead of inventing a new alpha-only API.

## Epics and Stories

### Epic 1: Shared SQL Parsing

- Story 1.1: Create `security/sql_parser.py` with reusable table and predicate extraction helpers.
- Story 1.2: Refactor `sql_validator._rewrite_ochlvf()` to use `sql_parser`.
- Story 1.3: Add unit tests for parser behavior across aliases, `IN`, `LIKE`, and datetime range predicates.

### Epic 2: Alpha Materialization Infrastructure

- Story 2.1: Add `services/feature_manager.py`.
- Story 2.2: Add lazy `featureHandler` import and one-time initialization support.
- Story 2.3: Normalize `dh._data` into a DuckDB-joinable dataframe.
- Story 2.4: Add incremental cache merge logic.
- Story 2.5: Add a duck-server-backed provider adapter so `featureHandler` can evaluate alpha expressions from `ochlvf` rows without `ALPHA_PROVIDER_URI`.

### Epic 2A: Duck-Server Raw Data Access

- Story 2A.1: Add `services/data_query.py` to execute validated SQL and return pandas DataFrames.
- Story 2A.2: Ensure provider queries hit generic `ochlvf` with explicit symbol filters so only required parquet partitions are scanned.
- Story 2A.3: Keep the raw-data query path internal to `DuckDBService` and reuse the existing SQL rewrite instead of adding a parallel storage reader.

### Epic 3: DuckDB Service Integration

- Story 3.1: Detect alpha table refs before execution.
- Story 3.2: Materialize and register alpha tables on demand.
- Story 3.3: Extend schema and sample endpoints to surface dynamic registered tables.

### Epic 4: Tests

- Story 4.1: Unit tests for shared SQL parser.
- Story 4.2: Unit tests for `FeatureManager` cache and merge behavior.
- Story 4.3: Integration tests for alpha query registration.
- Story 4.4: SIT coverage for alpha + `ochlvf` joins.
- Story 4.5: API test coverage for `POST /query` with alpha SQL payloads.

## Required Test Cases

### Unit tests

1. parser extracts `ochlvf_AAPL` and `alpha158_AAPL` correctly from joins
2. parser extracts instruments from:
   - `symbol = 'AAPL'`
   - `symbol IN ('AAPL', 'MSFT')`
   - `symbol LIKE 'AA%'`
3. parser extracts datetime ranges from `BETWEEN`, `>=`, `<=`
4. feature manager caches previously computed instruments
5. feature manager computes only missing instruments on second request
6. feature manager deduplicates merged rows on `(instrument, datetime)`

### Service integration tests

1. `SELECT * FROM alpha158_AAPL` registers and queries alpha data
2. `SELECT * FROM alpha158 WHERE symbol = 'AAPL'` infers symbol correctly
3. querying `alpha158` without symbol restriction fails when full scan is disabled
4. two sequential alpha queries compute only incremental instruments
5. when `ALPHA_PROVIDER_URI` is absent, the service still materializes alpha data via the duck-server-backed provider
6. default config values are `REG_US`, `day`, and `false` when the env vars are not set

### SIT tests

1. `ochlvf` rewrite still works after parser refactor
2. alpha query can join with `ochlvf` by symbol/date and return columns from both sources
3. incremental alpha cache survives across sequential requests in one service instance
4. `POST /query` accepts alpha SQL in the request body and returns materialized rows

Representative SIT join query:

```sql
SELECT o.symbol, o.datetime, o.close, a.MA5
FROM ochlvf_AAPL o
JOIN alpha158_AAPL a
  ON o.symbol = a.instrument AND o.datetime = a.datetime
WHERE o.datetime BETWEEN '2019-01-01' AND '2019-12-31'
LIMIT 10;
```

Expected result:

- rows contain `ochlvf` columns plus alpha feature columns
- join keys align on uppercase symbol/instrument and datetime
- alpha materialization happens before execution

Representative API-level SIT test:

```bash
curl -X POST 'http://localhost:8000/query' \
  -H 'Authorization: Bearer <token>' \
  -H 'Content-Type: application/json' \
  -d '{
    "sql": "SELECT * FROM alpha158 WHERE symbol = '\''AAPL'\''"
  }'
```

Expected result:

- HTTP 200 when alpha materialization succeeds
- JSON body with `success=true`
- at least one returned row for the test fixture or configured market data source
- no requirement for `ALPHA_PROVIDER_URI` when the duck-server-backed provider path is active

## Implementation Plan

1. Add `services/data_query.py` and route all internal raw data reads through `prepare_query_for_duckdb(...)` so `ochlvf` still loads symbol-scoped parquet paths only.
2. Add `services/feature_handler_adapter.py` with a provider compatible with `featureHandler.provider.D`.
3. Update `FeatureManager` so the runtime order is: injected factory, configured provider URI, then duck-server-backed provider.
4. Keep `DuckDBService` defaults in code for `ALPHA_REGION`, `ALPHA_FREQ`, and `ALPHA_ALLOW_FULL_SCAN`.
5. Expose a small `DuckDBService.query_dataframe(...)` helper for internal use and tests.
6. Preserve current public API contracts and keep alpha queries going through the existing `POST /query` route.
7. Defer any new internal ingestion endpoints until there is a concrete need for out-of-process feature writers.

## Test Plan

### Unit

1. verify `FeatureManager` falls back to the duck-server-backed provider when `ALPHA_PROVIDER_URI` is absent
2. verify the duck-server-backed provider queries `ochlvf` with explicit symbol filters and does not request all symbols
3. verify config defaults are applied when env vars are missing

### Integration

1. verify `DuckDBService.execute_query(...)` materializes `alpha158` through the existing query path
2. verify incremental cache behavior still works after the provider fallback is added
3. verify schema exposure still includes dynamically registered alpha tables

### API / SIT

1. call `POST /query` with `{"sql": "SELECT * FROM alpha158 WHERE symbol = 'AAPL'"}` and verify a 200 response
2. call `POST /query` without a symbol filter for `alpha158` and verify the service returns a validation failure when full scan is disabled
3. call `POST /query` with an alpha + `ochlvf` join and verify the response contains columns from both sides
4. confirm the service continues to rewrite `ochlvf` into symbol-scoped parquet reads rather than scanning the full universe

## Implementation Notes

- Keep all new modules synchronous to match the current service.
- Keep the default service startup unchanged for users who never query alpha tables.
- Add clear logging for cache hit/miss and alpha initialization.
- Prefer dependency injection in tests so alpha materialization can be mocked without requiring a live qlib dataset.

## Delivery Sequence

1. refactor shared SQL parsing
2. add `FeatureManager`
3. add `DataQueryService` and the duck-server-backed feature provider
4. wire alpha materialization fallback into `DuckDBService`
5. add unit tests
6. add integration and API/SIT tests against `POST /query`
7. run full `duck-server` test suite

