"""Market-data orchestration for FeatureSQL-backed datasets."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import importlib
import logging
import os
from pathlib import Path
import sys
from typing import Any, Iterable

from security.sql_parser import extract_table_refs


LOGGER = logging.getLogger(__name__)


def _ensure_feature_sql_importable():
    """Return the ``featureSQL`` module, preferring the local checkout.

    During development we want to exercise the repository code rather than a
    possibly out-of-date package that happens to be installed in the virtual
    environment.  The original implementation only added the repo path when an
    ``ImportError`` occurred; however, when the package is installed the
    import succeeds and the old version is used, leading to mysterious
    ``unexpected keyword argument`` errors (see bug report).

    To avoid this we always insert the local ``featureSQL`` directory at the
    front of ``sys.path`` if it exists.  Python will then import from the
    workspace before falling back to site-packages.
    """
    repo_root = Path(__file__).resolve().parents[2]
    candidate = repo_root / "featureSQL"
    if candidate.exists():
        # put repo path at beginning so it overrides any installed package
        if str(candidate) not in sys.path:
            sys.path.insert(0, str(candidate))
    return importlib.import_module("featureSQL")


@dataclass(frozen=True)
class MarketDataDefinition:
    asset_type: str
    label: str
    table_name: str
    path_parts: tuple[str, ...]
    description: str
    common_keys: tuple[str, ...]
    default_symbols: tuple[str, ...]


MARKET_DATA_DEFINITIONS: dict[str, MarketDataDefinition] = {
    "equity": MarketDataDefinition(
        asset_type="equity",
        label="OCHLVF",
        table_name="market_ochlvf",
        path_parts=("feature-csv", "*.csv"),
        description="Yahoo equity OCHLVF history.",
        common_keys=("symbol", "date"),
        default_symbols=("AAPL", "MSFT", "NVDA"),
    ),
    "fx": MarketDataDefinition(
        asset_type="fx",
        label="FX",
        table_name="market_fx",
        path_parts=("feature-csv", "fx", "*.csv"),
        description="Yahoo FX spot and history data.",
        common_keys=("symbol", "date"),
        default_symbols=("EURUSD=X", "GBPUSD=X", "USDJPY=X"),
    ),
    "ir": MarketDataDefinition(
        asset_type="ir",
        label="Interest Rates",
        table_name="market_ir",
        path_parts=("feature-csv", "ir", "*.csv"),
        description="Yahoo interest-rate instrument history.",
        common_keys=("symbol", "date"),
        default_symbols=("^IRX", "^FVX", "^TNX"),
    ),
    "vol": MarketDataDefinition(
        asset_type="vol",
        label="Volatility",
        table_name="market_vol",
        path_parts=("feature-csv", "vol", "*.csv"),
        description="Yahoo volatility index history.",
        common_keys=("symbol", "date"),
        default_symbols=("^VIX",),
    ),
    "correlation": MarketDataDefinition(
        asset_type="correlation",
        label="Correlation",
        table_name="market_correlation",
        path_parts=("feature-csv", "correlation", "*.csv"),
        description="Derived rolling correlations from Yahoo histories.",
        common_keys=("pair", "date"),
        default_symbols=("SPY:QQQ",),
    ),
    "option": MarketDataDefinition(
        asset_type="option",
        label="Option Chain",
        table_name="market_option_chain",
        path_parts=("option-chain", "*.csv"),
        description="Yahoo option-chain snapshots.",
        common_keys=("symbol", "expiration", "snapshot_at"),
        default_symbols=("AAPL",),
    ),
    "ir_curve": MarketDataDefinition(
        asset_type="ir_curve",
        label="IR Curve",
        table_name="market_ir_curve",
        path_parts=("analytics", "ir_curve.csv"),
        description="Bootstrapped IR zero curve derived from market rates.",
        common_keys=("maturity_years",),
        default_symbols=("^IRX", "^FVX", "^TNX"),
    ),
    "vol_surface": MarketDataDefinition(
        asset_type="vol_surface",
        label="Vol Surface",
        table_name="market_vol_surface",
        path_parts=("analytics", "vol_surface.csv"),
        description="Implied volatility surface calibrated from option chains.",
        common_keys=("symbol", "expiration", "strike"),
        default_symbols=("AAPL",),
    ),
}


class MarketDataService:
    def __init__(self, conn):
        self.conn = conn
        self.registered_tables: set[str] = set()
        self._feature_sql = None

    @property
    def feature_sql(self):
        if self._feature_sql is None:
            self._feature_sql = _ensure_feature_sql_importable()
        return self._feature_sql

    def get_default_root(self) -> str:
        return os.getenv("MARKET_DATA_ROOT") or "/tmp/duck-server-market-data"

    def get_catalog(self) -> list[dict[str, Any]]:
        root = Path(self.get_default_root())
        items: list[dict[str, Any]] = []
        for definition in MARKET_DATA_DEFINITIONS.values():
            files = self._resolve_local_files(definition, str(root))
            items.append(
                {
                    **asdict(definition),
                    "available": bool(files),
                    "registered": definition.table_name in self.registered_tables,
                    "root": str(root),
                    "file_count": len(files),
                    "sample_files": files[:5],
                }
            )
        return items

    def sync_market_data(
        self,
        asset_type: str,
        *,
        symbols: Iterable[str] | str | None = None,
        start: str | None = None,
        end: str | None = None,
        mode: str = "history",
        out_format: str = "csv",
        store_type: str = "gcs",
        data_path: str | None = None,
        correlation_window: int = 20,
        generate_ir_curve: bool = True,
        generate_vol_surface: bool = True,
    ) -> dict[str, Any]:
        normalized = asset_type.strip().lower()
        if normalized not in MARKET_DATA_DEFINITIONS:
            raise ValueError(f"Unsupported market data type: {asset_type}")

        # when using GCS we prefer the explicit data_path argument, but
        # fall back to the GCS_BUCKET_NAME environment variable if nothing
        # was provided.  ``get_default_root`` still returns the local
        # filesystem path for backwards compatibility but is ignored by the
        # downloader when ``store_type`` is not "fs".
        if store_type == "gcs" and not data_path:
            data_path = os.environ.get("GCS_BUCKET_NAME")
        base = data_path or self.get_default_root()

        # guard against potentially confusing KeyErrors coming from deep in
        # the storage/auth libraries (e.g. malformed GCS credentials).  We
        # translate those into a ValueError so that the FastAPI layer returns
        # a 400 instead of an opaque 500.
        try:
            feature_module = self.feature_sql
            runner = feature_module.Run()
        except KeyError as e:
            raise ValueError(f"storage configuration error: missing key {e!r}") from e

        if normalized in {"ir_curve", "vol_surface"}:
            generated = []
            warnings: list[str] = []
            result = None
        else:
            warnings = []
            result = None
            try:
                result = runner.download(
                    asset_type="equity" if normalized == "equity" else normalized,
                    start=start,
                    end=end,
                    symbols=symbols,
                    data_path=base,
                    store_type=store_type,
                    out_format=out_format,
                    mode=mode,
                    correlation_window=correlation_window,
                )
                if isinstance(result, dict):
                    warnings = result.get("warnings", [])
            except KeyError as e:
                raise ValueError(f"storage configuration error: missing key {e!r}") from e
            except FileNotFoundError as e:
                raise ValueError(str(e))
            # other exceptions bubble up

            generated = self._resolve_local_files(MARKET_DATA_DEFINITIONS[normalized], base) if store_type == "fs" else []

        # log result for debugging when using GCS
        if store_type == "gcs":
            LOGGER.info(f"market-data sync result for {normalized}: runner returned {result}, warnings={warnings}")
            if not warnings and not generated:
                LOGGER.info(
                    "gcs sync produced no warnings and no local files; "
                    "verify that the bucket '%s' contains the expected data "
                    "or run with store_type='fs' for local debugging",
                    base,
                )
            # attempt to create parquet dataset and upload it, mirroring CLI
            try:
                import shutil

                if store_type == "fs":
                    parquet_root = os.path.join(base, "parquet")
                else:
                    parquet_root = os.path.join(os.getcwd(), "_parquet_temp")
                    try:
                        shutil.rmtree(parquet_root)
                    except Exception:
                        pass
                # symbols filter logic same as CLI (used only to decide if we
                # should call dump_parquet; dump_parquet itself will detect
                # missing CSVs and no-op accordingly)
                sym_filter = None
                if symbols is not None:
                    # use the same parsing logic as CLI Run
                    sym_filter = set(runner._parse_symbols_arg(symbols, asset_type=normalized) or [])
                # Note: CLI only invoked dump_parquet when sym_filter is not
                # None (i.e. symbols explicitly supplied, even if empty).
                if sym_filter is not None:
                    try:
                        runner.dump_parquet(
                            data_path=base,
                            out_root=parquet_root,
                            upload_gcs=True,
                            gcs_bucket=base,
                            store_type=store_type,
                            symbols=symbols if symbols is not None else None,
                            csv_subdir="feature-csv" if normalized == "equity" else f"feature-csv/{normalized}",
                        )
                        # record uploaded parquet paths for return value
                        if store_type == "gcs":
                            uploaded = []
                            for p in Path(parquet_root).rglob("*.parquet"):
                                rel = p.relative_to(parquet_root)
                                uploaded.append(f"gs://{base}/{rel}")
                            generated = uploaded
                    except Exception as e:
                        warnings.append(f"parquet generation/upload failed: {e}")
            except Exception as e:
                warnings.append(f"parquet generation/upload failed: {e}")

        analytics: list[str] = []
        if normalized == "ir" and generate_ir_curve:
            output_path = self._join_output(base, store_type, "analytics", "ir_curve.csv")
            try:
                runner.boost_ir_curve(data_path=base, output_path=output_path, store_type=store_type)
                if store_type == "fs":
                    analytics.append(output_path)
            except FileNotFoundError as e:
                LOGGER.warning(f"ir curve generation skipped: {e}")
        if normalized == "option" and generate_vol_surface:
            output_path = self._join_output(base, store_type, "analytics", "vol_surface.csv")
            try:
                runner.calibrate_vol_surface(data_path=base, output_path=output_path, store_type=store_type)
                if store_type == "fs":
                    analytics.append(output_path)
            except FileNotFoundError as e:
                LOGGER.warning(f"vol surface generation skipped: {e}")

        self.register_all_views(base)
        definition = MARKET_DATA_DEFINITIONS[normalized]
        result: dict[str, Any] = {
            "asset_type": normalized,
            "table_name": definition.table_name,
            "store_type": store_type,
            "data_path": base,
            "generated_files": generated,
            "analytics_files": analytics,
            "registered_tables": sorted(self.registered_tables),
        }
        if warnings:
            result["warnings"] = warnings
        return result

    def register_all_views(self, root: str | None = None) -> None:
        base = root or self.get_default_root()
        for definition in MARKET_DATA_DEFINITIONS.values():
            self.ensure_view(definition.table_name, root=base)

    def ensure_views_for_sql(self, sql: str, root: str | None = None) -> None:
        refs = {ref.table_name.lower() for ref in extract_table_refs(sql)}
        for definition in MARKET_DATA_DEFINITIONS.values():
            if definition.table_name.lower() in refs:
                self.ensure_view(definition.table_name, root=root)

    def ensure_view(self, table_name: str, root: str | None = None) -> bool:
        definition = self._definition_by_table(table_name)
        if definition is None:
            return False
        base = root or self.get_default_root()
        if not self._local_source_exists(definition, base):
            return False

        source = self._duckdb_source(definition, base)
        escaped_source = source.replace("'", "''")
        self.conn.execute(
            f"CREATE OR REPLACE VIEW {definition.table_name} AS "
            f"SELECT * FROM read_csv_auto('{escaped_source}', union_by_name=true, filename=true, sample_size=-1)"
        )
        self.registered_tables.add(definition.table_name)
        return True

    def _definition_by_table(self, table_name: str) -> MarketDataDefinition | None:
        for definition in MARKET_DATA_DEFINITIONS.values():
            if definition.table_name.lower() == table_name.lower():
                return definition
        return None

    def _join_output(self, base: str, store_type: str, *parts: str) -> str:
        if store_type != "fs":
            return "/".join([base.rstrip("/")] + [part.strip("/") for part in parts])
        return str(Path(base).joinpath(*parts))

    def _local_source_exists(self, definition: MarketDataDefinition, root: str) -> bool:
        return bool(self._resolve_local_files(definition, root))

    def _resolve_local_files(self, definition: MarketDataDefinition, root: str) -> list[str]:
        pattern_path = Path(root)
        for part in definition.path_parts:
            pattern_path = pattern_path / part
        return sorted(str(path) for path in Path(root).glob(str(Path(*definition.path_parts))))

    def _duckdb_source(self, definition: MarketDataDefinition, root: str) -> str:
        pattern = Path(root)
        for part in definition.path_parts:
            pattern = pattern / part
        return str(pattern)