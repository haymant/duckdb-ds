import os
import duckdb
import pandas as pd
from services.duckdb_service import DuckDBService

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
