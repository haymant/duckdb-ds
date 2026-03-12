# Duck Server Market Data API Design

## Objective

Extend duck-server so it can orchestrate FeatureSQL-backed market-data sync jobs and expose the resulting datasets as DuckDB-queryable tables for the trading UI.

> **Note:** the service prefers the local `featureSQL` checkout over any
> installed package.  This prevents runtime mismatches (e.g. missing
> `asset_type` parameter) and ensures the code under development is the one
> actually executed.  See ``services/market_data_service.py`` for the
> import helper.

## FeatureSQL Integration

### Runtime expectation

Duck-server should run with FeatureSQL available from the local repo checkout.

Recommended setup:

```bash
cd /home/data/git/haymant/trading-edge/duck-server
uv pip install ../featureSQL
```

The implementation also includes a sibling-repo import fallback so local development and tests can resolve FeatureSQL without a published package.

## REST API Extensions

### `GET /market-data/catalog`

Returns supported market-data types, table names, file availability, sample files, and registration status.

### `POST /market-data/sync`

Triggers FeatureSQL-backed retrieval for:

- `equity`
- `fx`
- `ir`
- `vol`
- `correlation`
- `option`

Optional derived outputs:

- `ir_curve.csv` after IR sync
- `vol_surface.csv` after option sync

Supported request fields:

- `asset_type`
- `symbols`
- `start`
- `end`
- `mode`
- `out_format`
- `store_type`  (defaults to `gcs`; specify `fs` to override — note the OpenAPI schema has changed accordingly)
  > ⚠️ **Common pitfall:** running the service without valid GCS
  > credentials will result in a `400` error with message
  > `storage configuration error: missing key 'refresh_token'`.  This
  > typically means the `GCS_SC_JSON` payload is malformed or an
  > OAuth2 user‐credential file was supplied instead of a service account
  > key.  If you're developing locally and don't need cloud storage,
  > simply set `store_type` to `fs` or remove the field entirely and the
  > sync will write to the local filesystem.
- `data_path`   (for GCS this should be a bucket name; if omitted the
  `GCS_BUCKET_NAME` environment variable is consulted)
- `correlation_window`
- `generate_ir_curve`
- `generate_vol_surface`

## DuckDB Table Model

The service registers or refreshes these views when data exists:

- `market_ochlvf`
- `market_fx`
- `market_ir`
- `market_vol`
- `market_correlation`
- `market_option_chain`
- `market_ir_curve`
- `market_vol_surface`

Views are backed by `read_csv_auto(..., union_by_name=true, filename=true)` so they remain lightweight and queryable through the existing `/query` endpoint.

## Query Lifecycle

1. The UI sends predefined SQL through the existing query route.
2. `DuckDBService` inspects table references before execution.
3. If a market-data view is referenced, `MarketDataService` creates or refreshes the view from the configured root.
4. DuckDB executes the validated SQL.

## Storage Model

Default local root:

- `MARKET_DATA_ROOT`, fallback `/tmp/duck-server-market-data`

Expected layout:

- `feature-csv/*.csv` for OCHLVF
- `feature-csv/fx/*.csv`
- `feature-csv/ir/*.csv`
- `feature-csv/vol/*.csv`
- `feature-csv/correlation/*.csv`
- `option-chain/*.csv`
- `analytics/ir_curve.csv`
- `analytics/vol_surface.csv`

GCS writes are triggered by passing `store_type=gcs` and a suitable `data_path` to the sync endpoint; since store_type now defaults to `gcs` the bucket name may also be provided via the
`GCS_BUCKET_NAME` environment variable. DuckDB table registration is currently oriented around the local root unless the server is explicitly configured to point at GCS-backed paths.

## Implementation Plan

### Phase 1

- add `MarketDataService` with catalog, sync, and view registration responsibilities.
- add FeatureSQL runtime resolution.

### Phase 2

- integrate market-data view preparation into `DuckDBService.execute_query`, schema, and table sampling.

### Phase 3

- expose catalog and sync routes from FastAPI.
- document the local FeatureSQL install flow.

### Phase 4

- add backend unit tests for market-data views and new API routes.
- validate UI-driven sync and query flows.

## Unit Test Plan

- verify a local FX CSV is surfaced as `market_fx`.
- verify market-data tables appear in schema output after registration.
- verify `/market-data/catalog` returns supported items.
- verify `/market-data/sync` returns the sync payload and registered table names.

## UAT Plan

1. Install FeatureSQL into duck-server from the local repo.
2. Start duck-server with a valid API token.
3. Call `POST /market-data/sync` for `equity` and `fx` subsets.
4. Query `market_ochlvf` and `market_fx` through `POST /query`.
5. Sync `ir` and confirm `market_ir_curve` is queryable.
6. Sync `option` and confirm `market_option_chain` and `market_vol_surface` are queryable.
7. Open the book UI and verify the market-data pages load previews from duck-server.