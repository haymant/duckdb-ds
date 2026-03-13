import os
import duckdb
import pandas as pd
from fastapi.testclient import TestClient

from services.duckdb_service import DuckDBService
from security.sql_features import find_alpha_table_refs, infer_alpha_request
from security.sql_parser import extract_datetime_range, extract_symbol_filters, extract_table_refs
from services.feature_manager import FeatureManager
import main as api_main
import pytest
from pathlib import Path


class DummyAlphaHandler:
    def __init__(self, dataframe):
        self._data = dataframe


def build_dummy_alpha_handler(feature_name, instruments, start_time=None, end_time=None, freq="day"):
    rows = []
    for instrument in instruments:
        rows.append(
            {
                "instrument": instrument,
                "datetime": "2019-01-02",
                "MA5": float(len(instrument)),
                "feature_name": feature_name,
                "freq": freq,
            }
        )
    return DummyAlphaHandler(pd.DataFrame(rows))


class DummyDataQueryService:
    def __init__(self):
        self.calls = []

    def query_dataframe(self, sql, params=None):
        self.calls.append((sql, [] if params is None else list(params)))
        # if parameters were supplied we assume they contain the requested
        # symbol names; if not we fall back to a simple regex from the SQL
        symbols = []
        if params:
            symbols = [value for value in params if isinstance(value, str) and value.isupper()]
        else:
            # look for patterns like symbol = 'AAPL' or symbol IN ('AAPL','MSFT')
            import re
            symbols = re.findall(r"symbol\s*(?:=|IN)\s*\(?['\"]([A-Z0-9]+)['\"]", sql, re.IGNORECASE)
        rows = []
        for symbol in symbols:
            rows.append(
                {
                    "symbol": symbol,
                    "datetime": pd.Timestamp("2019-01-02"),
                    "open": 99.0,
                    "high": 101.0,
                    "low": 98.0,
                    "close": 100.0,
                    "volume": 1000.0,
                    "factor": 1.0,
                    "change": 0.01,
                    "vwap": 100.0,
                }
            )
            rows.append(
                {
                    "symbol": symbol,
                    "datetime": pd.Timestamp("2019-01-03"),
                    "open": 100.0,
                    "high": 102.0,
                    "low": 99.0,
                    "close": 101.0,
                    "volume": 1100.0,
                    "factor": 1.0,
                    "change": 0.01,
                    "vwap": 101.0,
                }
            )
        return pd.DataFrame(rows)

def test_gcs_secret_creation(monkeypatch):
    # set env vars so the service will configure GCS secret
    monkeypatch.setenv("GCS_KEY_ID", "test-id")
    monkeypatch.setenv("GCS_KEY_SECRET", "test-secret")

    svc = DuckDBService()
    # query the DuckDB secrets table function to verify secret exists
    df = svc.conn.sql("SELECT * FROM duckdb_secrets() WHERE name = 'gcs_secret'").df()
    assert not df.empty
    assert df.iloc[0]["name"] == "gcs_secret"
    assert "type=gcs" in df.iloc[0]["secret_string"]


def test_ochlvf_rewrite(monkeypatch):
    # create service normally (no envvars required for this test)
    svc = DuckDBService()

    # monkeypatch the connection to capture SQL calls and ignore installs
    executed = {}
    class DummyResult:
        def __init__(self):
            self._df = pd.DataFrame()
        def df(self):
            return self._df

    class FakeConn:
        def sql(self, *args, **kwargs):
            # installation calls are ignored
            return None
        def execute(self, sql, parameters=None):
            executed['sql'] = sql
            executed['params'] = [] if parameters is None else list(parameters)
            return DummyResult()

    monkeypatch.setattr(svc, 'conn', FakeConn())

    # run a query that references an ochlvf table uppercase symbol
    svc.execute_query("SELECT col FROM ochlvf_AAPL WHERE col > ?", params=[1])

    sql_executed = executed.get('sql', '')
    params_executed = executed.get('params', [])

    assert 'read_parquet' in sql_executed
    # ensure the FROM keyword is still present when rewriting
    assert 'FROM read_parquet' in sql_executed
    # path should have been turned into a parameter, not hard‑coded in SQL
    assert "symbol=AAPL" not in sql_executed
    assert sql_executed.count('?') >= 1
    # original table name should no longer appear
    assert 'ochlvf_AAPL' not in sql_executed

    # verify the path parameter was appended and is normalized to uppercase
    assert any(isinstance(p, str) and 'symbol=AAPL' in p for p in params_executed)

    # clear captured state and try lowercase table name; path should still
    # normalize to uppercase
    executed.clear()
    svc.execute_query("SELECT * FROM ochlvf_aapl")
    sql_executed = executed.get('sql', '')
    params_executed = executed.get('params', [])
    assert 'read_parquet' in sql_executed
    assert 'ochlvf_aapl' not in sql_executed
    assert any('symbol=AAPL' in p for p in params_executed)

    # generic ochlvf with multiple symbols in WHERE should combine paths
    executed.clear()
    svc.execute_query(
        "SELECT * FROM ochlvf WHERE symbol = 'NVDA' OR symbol = 'TSLA'"
    )
    sql_executed = executed.get('sql', '')
    params_executed = executed.get('params', [])
    assert 'read_parquet' in sql_executed
    # multi-path case should generate bracketed placeholders and two params
    assert 'read_parquet([?,?]' in sql_executed
    assert 'union_by_name=true' in sql_executed
    assert len(params_executed) == 2
    assert 'symbol=NVDA' in params_executed[0]
    assert 'symbol=TSLA' in params_executed[1]

    # generic ochlvf with LIKE pattern
    executed.clear()
    svc.execute_query("SELECT * FROM ochlvf WHERE symbol LIKE 'NV%' ")
    params_executed = executed.get('params', [])
    assert any('symbol=NV*' in p for p in params_executed)

    # regression test for datetime filter: path parameter must precede
    # any other parameters.  using literal dates exercises the same
    # ordering behaviour even though no date values appear in `params`.
    executed.clear()
    svc.execute_query(
        "SELECT * FROM ochlvf WHERE symbol = 'AAPL' "
        "AND datetime >= '2026-03-01' AND datetime <= '2026-03-12'"
    )
    params_executed = executed.get('params', [])
    # only the parquet path should be present and it must be first
    assert params_executed and isinstance(params_executed[0], str)
    assert 'symbol=AAPL' in params_executed[0]
    assert len(params_executed) == 1


def test_execute_query_serializes_timestamps(monkeypatch):
    # ensure execute_query converts pandas Timestamp to plain strings
    svc = DuckDBService()

    class FakeResult:
        def __init__(self):
            self._df = pd.DataFrame([
                {"date": pd.Timestamp("2026-03-01"), "value": 1},
            ])
        def df(self):
            return self._df

    class FakeConn:
        def execute(self, sql, parameters=None):
            return FakeResult()

    monkeypatch.setattr(svc, "conn", FakeConn())
    res = svc.execute_query("select date, value from ochlvf", params=None)
    assert res["success"] is True
    # row value should be a string not Timestamp
    assert isinstance(res["rows"][0][0], str)
    assert isinstance(res["data"][0]["date"], str)


def test_query_endpoint_handles_timestamp_rows(monkeypatch):
    api_main._load_tokens = lambda: ["test-token"]
    svc = api_main.db_service

    # patch the underlying connection to return a timestamp-containing row
    class FakeResult:
        def __init__(self):
            self._df = pd.DataFrame([{"date": pd.Timestamp("2026-03-02"), "val": 42}])
        def df(self):
            return self._df

    class FakeConn:
        def execute(self, sql, parameters=None):
            return FakeResult()

    monkeypatch.setattr(svc, "conn", FakeConn())

    client = TestClient(api_main.app)
    response = client.post(
        "/query",
        json={"sql": "select date,val from ochlvf"},
        headers={"Authorization": "Bearer test-token"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["success"] is True
    assert isinstance(payload["rows"][0][0], str)
    assert isinstance(payload["data"][0]["date"], str)


def test_sql_parser_extracts_table_refs_and_filters():
    sql = (
        "SELECT o.symbol, a.MA5 FROM ochlvf_AAPL o "
        "JOIN alpha158 a ON o.symbol = a.instrument AND o.datetime = a.datetime "
        "WHERE a.symbol IN ('AAPL', 'MSFT') AND o.datetime BETWEEN '2019-01-01' AND '2019-12-31'"
    )
    table_refs = extract_table_refs(sql)
    assert [table_ref.table_name for table_ref in table_refs] == ["ochlvf_AAPL", "alpha158"]
    assert table_refs[0].alias == "o"
    assert table_refs[1].alias == "a"

    symbols = extract_symbol_filters(sql, aliases=["a"])
    assert symbols == {"AAPL", "MSFT"}

    start_time, end_time = extract_datetime_range(sql, aliases=["o"])
    assert start_time == "2019-01-01"
    assert end_time == "2019-12-31"


def test_sql_features_infers_alpha_request():
    sql = "SELECT * FROM alpha158_AAPL a WHERE a.datetime >= '2019-01-01'"
    table_ref = find_alpha_table_refs(sql)[0]
    request = infer_alpha_request(sql, table_ref)
    assert request.feature_name == "alpha158"
    assert request.sql_table_name == "alpha158_AAPL"
    assert request.instruments == {"AAPL"}
    assert request.start_time == "2019-01-01"


def test_infer_alpha_request_respects_instrument_predicate():
    sql = "SELECT * FROM alpha158 a WHERE a.instrument = 'AAPL' AND a.datetime >= '2020-01-01'"
    table_ref = find_alpha_table_refs(sql)[0]
    request = infer_alpha_request(sql, table_ref)
    assert request.feature_name == "alpha158"
    assert request.instruments == {"AAPL"}
    assert request.start_time == "2020-01-01"


def test_feature_manager_incremental_cache():
    conn = duckdb.connect(":memory:")
    manager = FeatureManager(conn, feature_factory=build_dummy_alpha_handler, config={"freq": "day"})
    request_aapl = infer_alpha_request("SELECT * FROM alpha158_AAPL", find_alpha_table_refs("SELECT * FROM alpha158_AAPL")[0])
    manager.ensure_registered(request_aapl)
    snapshot = manager.get_cache_snapshot()
    assert len(snapshot) == 1
    cached_instruments = next(iter(snapshot.values()))
    assert cached_instruments == {"AAPL"}


def test_vol_surface_view_aliasing(monkeypatch):
    # ensure the view SQL for vol_surface includes a fallback for
    # impliedVolatility -> implied_vol
    svc = MarketDataService(None)
    executed = []
    class FakeConn:
        def execute(self, sql, *args, **kwargs):
            executed.append(sql)
    svc.conn = FakeConn()
    # simulate empty local files and a single bucket file
    monkeypatch.setattr(svc, '_resolve_local_files', lambda d, r: [])
    class FakeStore:
        def glob(self, prefix, pattern):
            return ['analytics/vol_surface.csv']
    import featureSQL.storage
    monkeypatch.setattr(featureSQL.storage, 'get_storage', lambda st, name: FakeStore())
    monkeypatch.setenv('GCS_BUCKET_NAME', 'bucket')

    svc.ensure_view('market_vol_surface', root='ignored')
    assert executed, "no view SQL produced"
    sql = executed[0]
    assert 'AS t' in sql, "view should alias the CSV source as 't'"
    assert 'COALESCE(t.implied_vol, t.impliedVolatility) AS implied_vol' in sql
    assert 'COALESCE(t.moneyness, t.moneyness) AS moneyness' in sql
    assert 'COALESCE(t.total_variance, t.total_variance) AS total_variance' in sql

    request_multi = infer_alpha_request(
        "SELECT * FROM alpha158 WHERE symbol IN ('AAPL', 'MSFT')",
        find_alpha_table_refs("SELECT * FROM alpha158 WHERE symbol IN ('AAPL', 'MSFT')")[0],
    )
    df = manager.ensure_registered(request_multi)
    assert set(df["instrument"].tolist()) == {"AAPL", "MSFT"}


def test_feature_manager_uses_duck_server_data_query_when_provider_uri_missing():
    conn = duckdb.connect(":memory:")
    data_query_service = DummyDataQueryService()
    manager = FeatureManager(conn, config={"freq": "day", "allow_full_scan": False}, data_query_service=data_query_service)

    request = infer_alpha_request(
        "SELECT * FROM alpha158 WHERE symbol = 'AAPL'",
        find_alpha_table_refs("SELECT * FROM alpha158 WHERE symbol = 'AAPL'")[0],
    )
    df = manager.ensure_registered(request)

    assert not df.empty
    assert "MA5" in df.columns
    assert len(data_query_service.calls) == 1
    sql, params = data_query_service.calls[0]
    assert "FROM ochlvf" in sql
    # with the new provider the symbols are embedded directly in the SQL
    assert "AAPL" in sql


def test_ensure_view_prefers_bucket_when_local_missing(monkeypatch):
    svc = DuckDBService()
    # capture any SQL executed
    executed = []
    class FakeConn:
        def execute(self, sql, *args, **kwargs):
            executed.append(sql)
    monkeypatch.setattr(svc, "conn", FakeConn())

    # simulate no local CSVs
    monkeypatch.setattr(svc, "_resolve_local_files", lambda definition, root: [])

    # fake storage that returns a single file path
    class FakeStore:
        def glob(self, prefix, pattern):
            assert prefix == "feature-csv/fx"
            assert pattern == "*.csv"
            return ["feature-csv/fx/bucket.csv"]
    import featureSQL.storage
    monkeypatch.setattr(featureSQL.storage, "get_storage", lambda st, name: FakeStore())

    monkeypatch.setenv("GCS_BUCKET_NAME", "mybucket")

    svc.ensure_view("market_fx", root="/tmp/unused")
    assert executed, "no view SQL was generated"
    assert "gs://mybucket/feature-csv/fx/bucket.csv" in executed[0]
    # in the feature handler succeeds
    import pandas as pd
    data_dir = tmp_path / "market-data" / "symbol=AAPL"
    data_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame({"symbol": ["AAPL"], "date": [pd.Timestamp("2026-01-01")]})
    df.to_parquet(data_dir / "dummy.parquet")

    svc = DuckDBService()
    manager = FeatureManager(
        svc.conn,
        config={"freq": "day", "allow_full_scan": False},
        data_query_service=svc.data_query_service,
    )

    request = infer_alpha_request(
        "SELECT * FROM alpha158 WHERE symbol = 'AAPL'",
        find_alpha_table_refs("SELECT * FROM alpha158 WHERE symbol = 'AAPL'")[0],
    )
    # should not raise
    df = manager.ensure_registered(request)
    assert not df.empty
    assert "instrument" in df.columns


def test_duckdb_service_alpha_registration_and_schema(monkeypatch):
    svc = DuckDBService()
    svc.feature_manager = FeatureManager(svc.conn, feature_factory=build_dummy_alpha_handler, config={"freq": "day"})

    result = svc.execute_query("SELECT instrument, datetime, MA5 FROM alpha158_AAPL")
    assert result["success"] is True
    assert result["row_count"] == 1
    assert result["data"][0]["instrument"] == "AAPL"

    schema = svc.get_schema()
    assert "alpha158_AAPL" in schema


def test_duckdb_service_alpha_requires_symbol_filter_when_generic():
    svc = DuckDBService()
    svc.feature_manager = FeatureManager(svc.conn, feature_factory=build_dummy_alpha_handler, config={"freq": "day", "allow_full_scan": False})

    result = svc.execute_query("SELECT * FROM alpha158")
    assert result["success"] is False
    assert result["error_type"] == "validation"
    assert "explicit symbol filters" in result["error"]


def test_duckdb_service_defaults_alpha_config_when_env_missing(monkeypatch):
    monkeypatch.delenv("ALPHA_REGION", raising=False)
    monkeypatch.delenv("ALPHA_FREQ", raising=False)
    monkeypatch.delenv("ALPHA_ALLOW_FULL_SCAN", raising=False)
    svc = DuckDBService()

    assert svc.feature_manager.config["region"] == "REG_US"
    assert svc.feature_manager.config["freq"] == "day"
    assert svc.feature_manager.config["allow_full_scan"] is False


def test_sit_alpha_join_with_ochlvf(monkeypatch):
    svc = DuckDBService()
    svc.feature_manager = FeatureManager(svc.conn, feature_factory=build_dummy_alpha_handler, config={"freq": "day"})

    executed = {}

    class DummyResult:
        def __init__(self):
            self._df = pd.DataFrame(
                [
                    {
                        "symbol": "AAPL",
                        "datetime": pd.Timestamp("2019-01-02"),
                        "close": 100.0,
                        "MA5": 4.0,
                    }
                ]
            )

        def df(self):
            return self._df

    class FakeConn:
        def __init__(self):
            self.registered = {}

        def sql(self, *args, **kwargs):
            return None

        def register(self, name, dataframe):
            self.registered[name] = dataframe.copy()

        def execute(self, sql, parameters=None):
            executed["sql"] = sql
            executed["params"] = [] if parameters is None else list(parameters)
            return DummyResult()

    fake_conn = FakeConn()
    monkeypatch.setattr(svc, "conn", fake_conn)
    svc.feature_manager.conn = fake_conn

    query = (
        "SELECT o.symbol, o.datetime, o.close, a.MA5 "
        "FROM ochlvf_AAPL o "
        "JOIN alpha158_AAPL a ON o.symbol = a.instrument AND o.datetime = a.datetime "
        "WHERE o.datetime BETWEEN '2019-01-01' AND '2019-12-31'"
    )
    result = svc.execute_query(query)

    assert result["success"] is True
    assert "alpha158_AAPL" in fake_conn.registered
    assert "read_parquet" in executed["sql"]
    assert result["data"][0]["symbol"] == "AAPL"
    assert "MA5" in result["data"][0]


def test_query_endpoint_accepts_alpha_payload(monkeypatch):
    api_main._load_tokens = lambda: ["test-token"]
    api_main.db_service.feature_manager = FeatureManager(
        api_main.db_service.conn,
        feature_factory=build_dummy_alpha_handler,
        config={"freq": "day", "allow_full_scan": False},
        data_query_service=api_main.db_service.data_query_service,
    )
    client = TestClient(api_main.app)

    response = client.post(
        "/query",
        headers={"Authorization": "Bearer test-token"},
        json={"sql": "SELECT * FROM alpha158 WHERE symbol = 'AAPL'"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["success"] is True
    assert payload["row_count"] >= 1


def test_duckdb_service_registers_market_data_views(tmp_path, monkeypatch):
    root = tmp_path / "market-data"
    (root / "feature-csv" / "fx").mkdir(parents=True)
    (root / "feature-csv" / "fx" / "EURUSD_X.csv").write_text(
        "symbol,date,close\nEURUSD=X,2026-03-10,1.09\n"
    )

    monkeypatch.setenv("MARKET_DATA_ROOT", str(root))
    svc = DuckDBService()

    result = svc.execute_query("SELECT symbol, close FROM market_fx")
    assert result["success"] is True
    assert result["row_count"] == 1
    assert result["data"][0]["symbol"] == "EURUSD=X"

    schema = svc.get_schema()
    assert "market_fx" in schema


def test_query_fails_if_market_data_not_synced(monkeypatch, tmp_path):
    """If the market-data root is empty the service should return a clear
    validation error instead of a generic DuckDB catalog exception.
    """
    # ensure the default root is something that exists but has no files
    monkeypatch.setenv("MARKET_DATA_ROOT", str(tmp_path))
    svc = DuckDBService()
    result = svc.execute_query("SELECT symbol FROM market_fx")
    assert result["success"] is False
    assert result["error_type"] == "validation"
    assert "Market data 'fx' is not available" in result["error"]


def test_market_data_catalog_endpoint(monkeypatch):
    api_main._load_tokens = lambda: ["test-token"]
    monkeypatch.setattr(
        api_main.db_service.market_data_service,
        "get_catalog",
        lambda: [{"asset_type": "fx", "table_name": "market_fx", "available": True}],
    )
    client = TestClient(api_main.app)

    response = client.get(
        "/market-data/catalog",
        headers={"Authorization": "Bearer test-token"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["success"] is True
    assert payload["items"][0]["table_name"] == "market_fx"


def test_market_data_sync_endpoint(monkeypatch):
    api_main._load_tokens = lambda: ["test-token"]

    monkeypatch.setattr(
        api_main.db_service.market_data_service,
        "sync_market_data",
        lambda *args, **kwargs: {
            "asset_type": "fx",
            "table_name": "market_fx",
            "generated_files": ["/tmp/market-data/feature-csv/fx/EURUSD_X.csv"],
            "registered_tables": ["market_fx"],
            "warnings": ["some warning"],
        },
    )
    client = TestClient(api_main.app)

    response = client.post(
        "/market-data/sync",
        headers={"Authorization": "Bearer test-token"},
        json={"asset_type": "fx", "symbols": ["EURUSD=X"], "start": "2026-03-01", "end": "2026-03-10"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["success"] is True
    assert payload["result"]["table_name"] == "market_fx"
    assert payload["result"].get("warnings") == ["some warning"]


def test_sync_returns_error_on_download_failure(monkeypatch):
    api_main._load_tokens = lambda: ["test-token"]
    # patch the underlying featureSQL runner to throw FileNotFoundError
    import services.market_data_service as mdmod
    class DummyRun:
        def download(self, *args, **kwargs):
            raise FileNotFoundError("no CSV files found")
    monkeypatch.setattr(
        mdmod,
        "_ensure_feature_sql_importable",
        lambda: type("M", (), {"Run": DummyRun}),
    )
    # clear any previously cached module so our patch takes effect
    api_main.db_service.market_data_service._feature_sql = None

    client = TestClient(api_main.app)
    response = client.post(
        "/market-data/sync",
        headers={"Authorization": "Bearer test-token"},
        json={"asset_type": "option"},
    )
    assert response.status_code == 400
    payload = response.json()
    assert payload["detail"] == "no CSV files found"


def test_sync_returns_error_on_missing_gcs_key(monkeypatch):
    """A KeyError originating from the storage layer should become a 400.
    """
    api_main._load_tokens = lambda: ["test-token"]
    # monkeypatch the runner so download raises KeyError (simulating
    # malformed credentials deep in the gcs client).  Our service should
    # catch it and report as a ValueError.
    import services.market_data_service as mdmod
    class DummyRun:
        def download(self, *args, **kwargs):
            raise KeyError("refresh_token")
    monkeypatch.setattr(
        mdmod,
        "_ensure_feature_sql_importable",
        lambda: type("M", (), {"Run": DummyRun}),
    )
    api_main.db_service.market_data_service._feature_sql = None

    client = TestClient(api_main.app)
    response = client.post(
        "/market-data/sync",
        headers={"Authorization": "Bearer test-token"},
        json={"asset_type": "equity"},
    )
    assert response.status_code == 400
    assert "refresh_token" in response.json().get("detail", "")


def test_sync_skips_analytics_on_missing_data(monkeypatch):
    """Even if the main download succeeds, the analytics step may have no
    data and should not cause a 500.  This test simulates that by hooking the
    runner to raise FileNotFoundError during the IR curve and vol surface
    calls while leaving the initial download working.
    """
    api_main._load_tokens = lambda: ["test-token"]
    class DummyRun:
        def download(self, *args, **kwargs):
            # do nothing, pretend csvs exist
            return
        def boost_ir_curve(self, *args, **kwargs):
            raise FileNotFoundError("no CSV files found under /tmp/duck-server-market-data/ir")
        def calibrate_vol_surface(self, *args, **kwargs):
            raise FileNotFoundError("no CSV files found under /tmp/duck-server-market-data/option-chain")
    import services.market_data_service as mdmod
    monkeypatch.setattr(
        mdmod,
        "_ensure_feature_sql_importable",
        lambda: type("M", (), {"Run": DummyRun}),
    )
    svc = api_main.db_service.market_data_service
    svc._feature_sql = None

    client = TestClient(api_main.app)
    response = client.post(
        "/market-data/sync",
        headers={"Authorization": "Bearer test-token"},
        json={"asset_type": "option"},
    )
    # should still be 200 although analytics were skipped
    assert response.status_code == 200
    payload = response.json()
    assert payload["success"] is True
    assert payload["result"]["analytics_files"] == []


def test_sync_market_data_passes_asset_type_to_runner(monkeypatch):
    """Ensure `sync_market_data` invokes the featureSQL runner with the
    requested asset_type argument (regression test for upstream package
    mismatches).
    """
    dummy = {}

    class DummyRun:
        def __init__(self):
            pass

        def download(self, *args, **kwargs):
            dummy["called"] = True
            dummy["kwargs"] = kwargs

    # monkeypatch the module-level helper so sync_market_data uses our stub
    import services.market_data_service as mdmod
    monkeypatch.setattr(
        mdmod,
        "_ensure_feature_sql_importable",
        lambda: type("M", (), {"Run": DummyRun}),
    )
    svc = api_main.db_service.market_data_service
    svc._feature_sql = None  # clear cache so patched module is loaded

    # perform the sync, which should call our dummy runner
    # set a bucket name so the service can fill in the path
    monkeypatch.setenv("GCS_BUCKET_NAME", "bucket123")
    svc.sync_market_data("fx", symbols=["EURUSD=X"])

    assert dummy.get("called")
    assert dummy["kwargs"].get("asset_type") == "fx"
    # ensure default store type and path behaviour
    assert dummy["kwargs"].get("store_type") == "gcs"
    assert dummy["kwargs"].get("data_path") == "bucket123"


def test_api_default_store_type_is_gcs(monkeypatch):
    """POSTing to the sync endpoint without a store_type should still use
    GCS.  We likewise verify that the data_path is pulled from the
    environment when missing."""
    dummy = {}
    class DummyRun:
        def download(self, *args, **kwargs):
            dummy["kwargs"] = kwargs
    import services.market_data_service as mdmod
    monkeypatch.setattr(
        mdmod,
        "_ensure_feature_sql_importable",
        lambda: type("M", (), {"Run": DummyRun}),
    )
    svc = api_main.db_service.market_data_service
    svc._feature_sql = None

    api_main._load_tokens = lambda: ["test-token"]
    client = TestClient(api_main.app)
    response = client.post(
        "/market-data/sync",
        headers={"Authorization": "Bearer test-token"},
        json={"asset_type": "equity"},
    )
    assert response.status_code == 200
    assert dummy.get("kwargs", {}).get("store_type") == "gcs"

    dummy.clear()
    monkeypatch.setenv("GCS_BUCKET_NAME", "bucket-env")
    response = client.post(
        "/market-data/sync",
        headers={"Authorization": "Bearer test-token"},
        json={"asset_type": "equity"},
    )
    assert response.status_code == 200
    assert dummy.get("kwargs", {}).get("data_path") == "bucket-env"


def test_sync_market_data_propagates_runner_warnings(monkeypatch):
    """Any warnings returned by featureSQL downloader should appear in the result."""
    api_main._load_tokens = lambda: ["test-token"]
    class DummyRun:
        def download(self, *args, **kwargs):
            return {"warnings": ["foo"]}
    import services.market_data_service as mdmod
    monkeypatch.setattr(
        mdmod,
        "_ensure_feature_sql_importable",
        lambda: type("M", (), {"Run": DummyRun}),
    )
    svc = api_main.db_service.market_data_service
    svc._feature_sql = None

    client = TestClient(api_main.app)
    response = client.post(
        "/market-data/sync",
        headers={"Authorization": "Bearer test-token"},
        json={"asset_type": "equity"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["result"].get("warnings") == ["foo"]


def test_sync_parquet_generation_for_gcs(monkeypatch):
    """When syncing with `store_type=gcs` the service should invoke the
    runner's `dump_parquet` helper and propagate any errors as warnings.
    """
    api_main._load_tokens = lambda: ["test-token"]
    calls = {}
    class DummyRun:
        def download(self, *args, **kwargs):
            return {}  # no warnings
        def dump_parquet(self, *args, **kwargs):
            calls['dump'] = kwargs
            # simulate failure to trigger warning
            raise RuntimeError("upload failed")
        # stub parser used by service logic
        def _parse_symbols_arg(self, symbols, asset_type=None):
            # mimic Run._parse_symbols_arg behaviour minimally
            if symbols is None:
                return None
            if isinstance(symbols, (list, tuple)):
                return list(symbols)
            return [s for s in str(symbols).split(",") if s]
    import services.market_data_service as mdmod
    monkeypatch.setattr(
        mdmod,
        "_ensure_feature_sql_importable",
        lambda: type("M", (), {"Run": DummyRun}),
    )
    svc = api_main.db_service.market_data_service
    svc._feature_sql = None

    client = TestClient(api_main.app)
    response = client.post(
        "/market-data/sync",
        headers={"Authorization": "Bearer test-token"},
        json={"asset_type": "equity", "symbols": ["AAPL"]},
    )
    assert response.status_code == 200
    payload = response.json()
    assert 'warnings' in payload['result']
    assert any('upload failed' in w for w in payload['result']['warnings'])
    assert 'dump' in calls
    assert calls['dump'].get('upload_gcs') is True
    # gcs_bucket should be set to the base path used by the service (either
    # bucket name or MARKET_DATA_ROOT)
    assert calls['dump'].get('gcs_bucket')
    # even though dump_parquet failed we should still return generated_files
    assert isinstance(payload['result'].get('generated_files'), list)


def test_ensure_feature_sql_importable_prefers_local():
    """The import helper should return the copy of `featureSQL` from the
    repository rather than any installed package.
    """
    import services.market_data_service as mdmod
    mod = mdmod._ensure_feature_sql_importable()
    from pathlib import Path

    # The returned module should come from the workspace tree rather than
    # site-packages.  We simply verify that the file path contains the
    # repository's `featureSQL` directory and does not reference 'site-packages'.
    path_str = str(Path(mod.__file__).resolve())
    assert "/featureSQL/featureSQL" in path_str
    assert "site-packages" not in path_str


def test_sync_market_data_invalid_type():
    """Requests for unsupported market data types should raise ValueError."""
    svc = api_main.db_service.market_data_service
    with pytest.raises(ValueError):
        svc.sync_market_data("bogus")


def test_sync_market_data_alpha_rejected():
    """Alpha assets are not syncable via the market-data endpoint."""
    svc = api_main.db_service.market_data_service
    for alpha in ("alpha158", "alpha360"):
        with pytest.raises(ValueError) as exc:
            svc.sync_market_data(alpha)
        assert "not supported" in str(exc.value)


def test_alpha360_query_without_fit(monkeypatch):
    """Executing an alpha360 query with only a symbol filter must not raise.

    Previously the feature handler asserted that fit_start_time and
    fit_end_time were non-null, leading to an opaque ``AssertionError``
    bubbling through the DuckDB service.
    """
    svc = api_main.db_service
    svc.feature_manager = FeatureManager(svc.conn, feature_factory=build_dummy_alpha_handler, config={"freq": "day", "allow_full_scan": False})

    # running the query should succeed and register a table
    result = svc.execute_query("SELECT * FROM alpha360 WHERE symbol = 'AAPL'")
    assert result["success"] is True
    # query may return zero rows, we just care that it executed without
    # throwing an AssertionError.
    assert "row_count" in result


def test_table_ref_alias_ignores_keywords():
    sql = "SELECT * FROM alpha360 WHERE symbol='AAPL'"
    from security.sql_parser import extract_table_refs
    refs = extract_table_refs(sql)
    assert len(refs) == 1
    assert refs[0].table_name.lower() == "alpha360"
    assert refs[0].alias is None


def test_feature_sql_module_is_cached(monkeypatch):
    """Accessing ``feature_sql`` twice should reuse the cached module."""
    import services.market_data_service as mdmod
    sentinel = object()
    monkeypatch.setattr(mdmod, "_ensure_feature_sql_importable", lambda: sentinel)
    svc = api_main.db_service.market_data_service
    svc._feature_sql = None

    first = svc.feature_sql
    assert first is sentinel
    # change helper so that a second access would return something else
    monkeypatch.setattr(mdmod, "_ensure_feature_sql_importable", lambda: None)
    second = svc.feature_sql
    assert second is sentinel  # still the original cached object


def test_execute_query_serializes_nat_and_nan():
    """Rows containing pandas NaT or numpy NaN should round-trip as nulls.
    """
    svc = api_main.db_service
    # register a small table with NaT/NaN values
    import pandas as pd
    df = pd.DataFrame({"a": [pd.NaT], "b": [float("nan")]})
    svc.conn.register("foo", df)
    result = svc.execute_query("SELECT * FROM foo")
    assert result["success"] is True
    # check python-level extraction
    assert result["rows"][0] == [None, None]
    assert result["data"][0]["a"] is None
    assert result["data"][0]["b"] is None

    # constructing the pydantic response should not raise
    from main import QueryResponse
    resp = QueryResponse(
        success=True,
        columns=result["columns"],
        rows=result["rows"],
        row_count=result["row_count"],
        data=result["data"],
    )
    # ensure json() works
    _ = resp.json()


def test_execute_query_handles_plain_floats():
    """Ensure float values are returned intact and do not raise serialization errors.
    """
    svc = api_main.db_service
    # create simple table with a float column
    import pandas as pd
    df = pd.DataFrame({"x": [1.23, 4.56]})
    svc.conn.register("bar", df)
    result = svc.execute_query("SELECT * FROM bar")
    assert result["success"] is True
    assert result["rows"] == [[1.23], [4.56]]
    # response object should build cleanly
    from main import QueryResponse
    resp = QueryResponse(
        success=True,
        columns=result["columns"],
        rows=result["rows"],
        row_count=result["row_count"],
        data=result["data"],
    )
    _ = resp.json()


def test_ensure_feature_sql_importable_falls_back(monkeypatch):
    """When no local checkout exists we still call import_module and return whatever it gives."""
    import services.market_data_service as mdmod
    # make candidate check always false
    orig_exists = Path.exists
    def fake_exists(self):
        if self.name == "featureSQL":
            return False
        return orig_exists(self)
    monkeypatch.setattr(Path, "exists", fake_exists)
    dummy = object()
    monkeypatch.setattr(mdmod.importlib, "import_module", lambda name: dummy)

    assert mdmod._ensure_feature_sql_importable() is dummy


def test_sync_market_data_files_and_analytics(monkeypatch, tmp_path):
    """``sync_market_data`` should return generated and analytics paths when using fs store."""
    class DummyRun:
        def download(self, *args, **kwargs):
            return {}
        def boost_ir_curve(self, data_path, output_path, store_type):
            # create the expected output file when running on filesystem
            if store_type == "fs":
                Path(output_path).parent.mkdir(parents=True, exist_ok=True)
                Path(output_path).write_text("")
        def calibrate_vol_surface(self, data_path, output_path, store_type):
            if store_type == "fs":
                Path(output_path).parent.mkdir(parents=True, exist_ok=True)
                Path(output_path).write_text("")

    import services.market_data_service as mdmod
    monkeypatch.setattr(mdmod, "_ensure_feature_sql_importable", lambda: type("M", (), {"Run": DummyRun}))
    svc = api_main.db_service.market_data_service
    svc._feature_sql = None

    # stub out file resolution to avoid touching the real filesystem
    monkeypatch.setattr(svc, "_resolve_local_files", lambda definition, root: [f"{root}/{definition.table_name}.csv"])
    monkeypatch.setenv("MARKET_DATA_ROOT", str(tmp_path))
    # avoid creating real DuckDB views during this unit test
    monkeypatch.setattr(svc, "register_all_views", lambda root=None: None)

    result = svc.sync_market_data("ir", store_type="fs")
    base = str(tmp_path)
    assert result["store_type"] == "fs"
    # generated files should come from our fake resolver
    assert result["generated_files"] == [f"{base}/market_ir.csv"]
    # analytics path should be returned as well
    assert result["analytics_files"] == [f"{base}/analytics/ir_curve.csv"]
