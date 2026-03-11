import os
import duckdb
import pandas as pd
from fastapi.testclient import TestClient

from services.duckdb_service import DuckDBService
from security.sql_features import find_alpha_table_refs, infer_alpha_request
from security.sql_parser import extract_datetime_range, extract_symbol_filters, extract_table_refs
from services.feature_manager import FeatureManager
import main as api_main


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


def test_feature_manager_incremental_cache():
    conn = duckdb.connect(":memory:")
    manager = FeatureManager(conn, feature_factory=build_dummy_alpha_handler, config={"freq": "day"})
    request_aapl = infer_alpha_request("SELECT * FROM alpha158_AAPL", find_alpha_table_refs("SELECT * FROM alpha158_AAPL")[0])
    manager.ensure_registered(request_aapl)
    snapshot = manager.get_cache_snapshot()
    assert len(snapshot) == 1
    cached_instruments = next(iter(snapshot.values()))
    assert cached_instruments == {"AAPL"}

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


def test_feature_manager_real_query_no_placeholder_mismatch():
    # this regression test exercises the real DataQueryService, which uses
    # prepare_query_for_duckdb and therefore performs the ochlvf rewrite.
    # prior to the fix the internal provider query would produce two '?' but
    # only one parameter (symbol), leading to a validation error bubbled to
    # the outer execute_query call.  verify that no exception is thrown and
    # some data is returned.

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
