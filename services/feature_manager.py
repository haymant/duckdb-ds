"""Alpha feature materialization and in-memory caching for DuckDB queries."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import importlib
import logging
import os
import threading
from typing import Any, Callable, Dict, Optional, Set, Tuple

import pandas as pd

from security.sql_features import AlphaRequest
from services.feature_handler_adapter import DuckServerFeatureProvider, feature_handler_provider_context


LOGGER = logging.getLogger(__name__)


CacheKey = Tuple[str, Optional[str], Optional[str], str, Optional[str], Optional[str], str, str]


@dataclass
class CacheEntry:
    dataframe: pd.DataFrame
    instruments: Set[str]
    lock: threading.Lock
    last_updated: Optional[datetime]


class FeatureManager:
    def __init__(self, conn, feature_factory: Optional[Callable[..., Any]] = None, config: Optional[Dict[str, Any]] = None, data_query_service=None):
        self.conn = conn
        self.feature_factory = feature_factory
        self.config = config or {}
        self.data_query_service = data_query_service
        self.cache: Dict[CacheKey, CacheEntry] = {}
        self._init_lock = threading.Lock()
        self._cache_lock = threading.Lock()
        self._feature_runtime_ready = False

    def clear_cache(self):
        self.cache.clear()

    def get_cache_snapshot(self) -> Dict[CacheKey, Set[str]]:
        return {key: set(entry.instruments) for key, entry in self.cache.items()}

    def ensure_registered(self, alpha_request: AlphaRequest) -> pd.DataFrame:
        if not alpha_request.instruments and not self.config.get("allow_full_scan", False):
            raise ValueError("Alpha queries require explicit symbol filters when full scan is disabled")

        key = self._make_cache_key(alpha_request)
        entry = self._ensure_entry(key)
        missing = set(alpha_request.instruments) - set(entry.instruments)

        if missing:
            with entry.lock:
                missing = set(alpha_request.instruments) - set(entry.instruments)
                if missing:
                    LOGGER.info("Materializing %s for missing instruments: %s", alpha_request.feature_name, sorted(missing))
                    df_new = self._materialize(alpha_request, missing)
                    df_new = self._normalize_dataframe(df_new)
                    if entry.dataframe.empty:
                        entry.dataframe = df_new
                    else:
                        combined = pd.concat([entry.dataframe, df_new], axis=0, ignore_index=True, sort=False)
                        if {"instrument", "datetime"}.issubset(combined.columns):
                            combined = combined.drop_duplicates(subset=["instrument", "datetime"], keep="first")
                        else:
                            combined = combined.drop_duplicates(keep="first")
                        entry.dataframe = combined
                    entry.instruments.update(missing)
                    entry.last_updated = datetime.now(timezone.utc)
        else:
            LOGGER.info("Alpha cache hit for %s and instruments %s", alpha_request.feature_name, sorted(alpha_request.instruments))

        if not entry.dataframe.empty:
            self.conn.register(alpha_request.sql_table_name, entry.dataframe.copy())
        return entry.dataframe

    def _ensure_entry(self, key: CacheKey) -> CacheEntry:
        with self._cache_lock:
            if key not in self.cache:
                self.cache[key] = CacheEntry(
                    dataframe=pd.DataFrame(),
                    instruments=set(),
                    lock=threading.Lock(),
                    last_updated=None,
                )
            return self.cache[key]

    def _make_cache_key(self, alpha_request: AlphaRequest) -> CacheKey:
        freq = self.config.get("freq", "day")
        provider_uri = self.config.get("provider_uri", "")
        region = self.config.get("region", "REG_US")
        fit_start_time = self.config.get("fit_start_time")
        fit_end_time = self.config.get("fit_end_time")
        return (
            alpha_request.feature_name,
            alpha_request.start_time,
            alpha_request.end_time,
            freq,
            fit_start_time,
            fit_end_time,
            provider_uri,
            region,
        )

    def _materialize(self, alpha_request: AlphaRequest, instruments: Set[str]) -> pd.DataFrame:
        if self.feature_factory is not None:
            handler = self.feature_factory(alpha_request.feature_name, instruments=sorted(instruments), start_time=alpha_request.start_time, end_time=alpha_request.end_time, freq=self.config.get("freq", "day"))
            dataframe = getattr(handler, "_data", None)
            if dataframe is None:
                raise RuntimeError("Feature factory returned handler without `_data`")
            return dataframe

        feature_handler = importlib.import_module("featureHandler")
        handler_class = getattr(feature_handler, "Alpha158" if alpha_request.feature_name == "alpha158" else "Alpha360")

        if self.config.get("provider_uri") or os.getenv("ALPHA_PROVIDER_URI"):
            self._ensure_runtime_initialized()
            handler = handler_class(**self._build_handler_kwargs(alpha_request, instruments))
        else:
            if self.data_query_service is None:
                raise RuntimeError("Alpha runtime is not configured. Set ALPHA_PROVIDER_URI or configure a DataQueryService.")
            provider = DuckServerFeatureProvider(
                self.data_query_service,
                instruments=sorted(instruments),
                start_time=alpha_request.start_time,
                end_time=alpha_request.end_time,
            )
            provider.reset()
            with feature_handler_provider_context(provider):
                handler = handler_class(**self._build_handler_kwargs(alpha_request, instruments))

        dataframe = getattr(handler, "_data", None)
        if dataframe is None:
            raise RuntimeError("Alpha handler did not expose `_data`")
        return dataframe

    def _build_handler_kwargs(self, alpha_request: AlphaRequest, instruments: Set[str]) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {
            "instruments": sorted(instruments),
            "start_time": alpha_request.start_time,
            "end_time": alpha_request.end_time,
            "freq": self.config.get("freq", "day"),
        }
        if alpha_request.feature_name == "alpha360":
            # alpha360 processors may insist on having fit window values; do
            # not pass the keys at all if we don't know them, avoiding the
            # assertion raised by featureHandler.  callers can set
            # ``ALPHA_FIT_START_TIME``/``_END_TIME`` in config when required.
            fit_start = self.config.get("fit_start_time") or alpha_request.start_time
            fit_end = self.config.get("fit_end_time") or alpha_request.end_time
            if fit_start is not None and fit_end is not None:
                kwargs["fit_start_time"] = fit_start
                kwargs["fit_end_time"] = fit_end
        return kwargs

    def _ensure_runtime_initialized(self):
        if self._feature_runtime_ready:
            return
        with self._init_lock:
            if self._feature_runtime_ready:
                return
            provider_uri = self.config.get("provider_uri") or os.getenv("ALPHA_PROVIDER_URI")
            if not provider_uri:
                raise RuntimeError("Alpha runtime is not configured. Set ALPHA_PROVIDER_URI or configure a DataQueryService.")

            feature_handler = importlib.import_module("featureHandler")
            region_name = self.config.get("region", os.getenv("ALPHA_REGION", "REG_US"))
            region = getattr(feature_handler, region_name, region_name)
            feature_handler.init(provider_uri=provider_uri, region=region)
            self._feature_runtime_ready = True
            LOGGER.info("Initialized featureHandler runtime for alpha queries")

    def _normalize_dataframe(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        if isinstance(dataframe.index, pd.MultiIndex) or dataframe.index.name is not None:
            dataframe = dataframe.reset_index()
        df = dataframe.copy()

        if isinstance(df.columns, pd.MultiIndex):
            flattened_columns = []
            for column in df.columns:
                if not isinstance(column, tuple):
                    flattened_columns.append(column)
                    continue
                parts = [str(part) for part in column if part not in (None, "")]
                if parts[:1] in (["feature"], ["label"]):
                    flattened_columns.append(parts[-1])
                else:
                    flattened_columns.append("_".join(parts) if parts else "")
            df.columns = flattened_columns

        rename_map = {}
        if "symbol" in df.columns and "instrument" not in df.columns:
            rename_map["symbol"] = "instrument"
        if rename_map:
            df = df.rename(columns=rename_map)

        if "instrument" in df.columns:
            df["instrument"] = df["instrument"].astype(str).str.upper()
            if "symbol" not in df.columns:
                df["symbol"] = df["instrument"]

        if "datetime" in df.columns:
            df["datetime"] = pd.to_datetime(df["datetime"]) 

        return df
