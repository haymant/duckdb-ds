"""Adapters that let featureHandler source raw market data from duck-server."""

from __future__ import annotations

from contextlib import contextmanager
import importlib
from typing import Dict, Iterable, Optional

import numpy as np
import pandas as pd


RAW_OCHLVF_COLUMNS = ["open", "high", "low", "close", "volume", "factor", "change", "vwap"]


class DuckServerFeatureProvider:
    """Minimal provider implementation compatible with featureHandler.provider.D."""

    def __init__(self, data_query_service, instruments: Iterable[str], start_time: Optional[str], end_time: Optional[str]):
        self.data_query_service = data_query_service
        self._requested_instruments = sorted({str(symbol).upper() for symbol in instruments})
        self._requested_start_time = start_time
        self._requested_end_time = end_time
        self._instrument_frames: Dict[str, pd.DataFrame] = {}
        self._calendar: Optional[pd.DatetimeIndex] = None
        self._calendar_index: Optional[Dict[pd.Timestamp, int]] = None
        self._expr_cache = {}
        self._field_cache = {}

    def reset(self):
        self._instrument_frames.clear()
        self._calendar = None
        self._calendar_index = None
        self._expr_cache.clear()
        self._field_cache.clear()
        provider_module = importlib.import_module("featureHandler.provider")
        provider_module.Expression._cache.clear()

    def calendar(self):
        if self._calendar is None:
            frame = self._load_raw_frame()
            if frame.empty:
                self._calendar = pd.DatetimeIndex([])
            else:
                self._calendar = pd.DatetimeIndex(sorted(frame["datetime"].dropna().unique()))
            self._calendar_index = {timestamp: index for index, timestamp in enumerate(self._calendar)}
        return self._calendar

    def locate_index(self, start_time=None, end_time=None):
        calendar = self.calendar()
        if len(calendar) == 0:
            return 0, -1

        start_ts = pd.Timestamp(start_time) if start_time is not None else calendar[0]
        end_ts = pd.Timestamp(end_time) if end_time is not None else calendar[-1]

        if start_ts not in self._calendar_index:
            position = int(np.searchsorted(calendar, start_ts))
            start_ts = calendar[min(position, len(calendar) - 1)]
        if end_ts not in self._calendar_index:
            position = int(np.searchsorted(calendar, end_ts, side="right") - 1)
            end_ts = calendar[max(position, 0)]
        return self._calendar_index[start_ts], self._calendar_index[end_ts]

    def instruments(self, market="all", filter_pipe=None, start_time=None, end_time=None, as_list=False):
        _ = filter_pipe, start_time, end_time, as_list
        if isinstance(market, list):
            return [str(symbol).upper() for symbol in market]
        if isinstance(market, str):
            if market.lower() == "all":
                return list(self._requested_instruments)
            return [market.upper()]
        raise TypeError(f"Unsupported instruments type: {type(market)}")

    def get_expression_instance(self, field):
        if field not in self._expr_cache:
            provider_module = importlib.import_module("featureHandler.provider")
            self._expr_cache[field] = eval(
                provider_module.parse_field(field),
                {"Feature": provider_module.Feature, "Operators": provider_module.Operators},
            )
        return self._expr_cache[field]

    def expression(self, instrument, field, start_time=None, end_time=None, freq="day"):
        _ = freq
        expression = self.get_expression_instance(field)
        start_index, end_index = self.locate_index(start_time, end_time)
        if end_index < start_index:
            return pd.Series(dtype=np.float32)

        left_extend, right_extend = expression.get_extended_window_size()
        calendar = self.calendar()
        query_start = max(0, start_index - left_extend)
        query_end = min(len(calendar) - 1, end_index + right_extend)
        series = expression.load(str(instrument).upper(), query_start, query_end, freq)
        series = series.astype(np.float32)
        if not series.empty:
            series = series.iloc[start_index - query_start : end_index - query_start + 1]
        return series

    def features(self, instruments, exprs, start_time=None, end_time=None, freq="day", inst_processors=None):
        _ = inst_processors
        instrument_list = self.instruments(instruments) if not isinstance(instruments, list) else [str(item).upper() for item in instruments]
        frames = []
        for instrument in instrument_list:
            frame = pd.DataFrame({expr: self.expression(instrument, expr, start_time, end_time, freq) for expr in exprs})
            frame.index.name = "datetime"
            frame["instrument"] = instrument
            frames.append(frame.reset_index().set_index(["instrument", "datetime"]))
        if not frames:
            return pd.DataFrame(columns=exprs)
        return pd.concat(frames).sort_index()

    def feature(self, instrument, field, start_index, end_index):
        instrument = str(instrument).upper()
        cache_key = (instrument, field)
        if cache_key not in self._field_cache:
            calendar = self.calendar()
            instrument_frame = self._instrument_frame(instrument)
            full_series = pd.Series(np.nan, index=calendar, dtype=np.float32)
            if field in instrument_frame.columns and not instrument_frame.empty:
                values = instrument_frame.set_index("datetime")[field].astype(np.float32)
                full_series.loc[values.index] = values.values
            self._field_cache[cache_key] = full_series
        return self._field_cache[cache_key].iloc[start_index : end_index + 1]

    def _instrument_frame(self, instrument: str) -> pd.DataFrame:
        if not self._instrument_frames:
            self._load_raw_frame()
        return self._instrument_frames.get(instrument, pd.DataFrame(columns=["datetime", *RAW_OCHLVF_COLUMNS]))

    def _load_raw_frame(self) -> pd.DataFrame:
        if self._instrument_frames:
            frames = []
            for symbol in self._requested_instruments:
                frame = self._instrument_frames.get(symbol)
                if frame is not None and not frame.empty:
                    frames.append(frame.assign(symbol=symbol))
            return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["symbol", "datetime", *RAW_OCHLVF_COLUMNS])

        if not self._requested_instruments:
            return pd.DataFrame(columns=["symbol", "datetime", *RAW_OCHLVF_COLUMNS])

        # build literal symbol list to keep the SQL rewrite happy.  the
        # external request has already been through our SQL parser so the
        # instruments are uppercased and safe.  using literals means
        # ``extract_symbol_filters`` will see them and generate correct
        # parquet path parameters; we avoid the placeholder-count mismatch
        # that occurs when the WHERE clause is parameterized.
        symbol_list = ", ".join(f"'{sym}'" for sym in self._requested_instruments)
        where_parts = [f"symbol IN ({symbol_list})"]
        params: list = []
        if self._requested_start_time is not None:
            where_parts.append("datetime >= ?")
            params.append(self._requested_start_time)
        if self._requested_end_time is not None:
            where_parts.append("datetime <= ?")
            params.append(self._requested_end_time)

        # parquet files expose a `date` column; alias it to ``datetime``
        # and select all other fields so the expression engine can compute any
        # feature it wants.  we will drop the original ``date`` column afterwards
        sql = (
            "SELECT *, date AS datetime "
            "FROM ochlvf "
            f"WHERE {' AND '.join(where_parts)} "
            "ORDER BY symbol, datetime"
        )
        frame = self.data_query_service.query_dataframe(sql, params)
        if frame.empty:
            self._instrument_frames = {symbol: pd.DataFrame(columns=["datetime", *RAW_OCHLVF_COLUMNS]) for symbol in self._requested_instruments}
            return frame

        frame = frame.copy()
        frame["symbol"] = frame["symbol"].astype(str).str.upper()
        # the alias above ensures ``datetime`` column exists; normalise it
        frame["datetime"] = pd.to_datetime(frame["datetime"])
        # remove any rows where datetime couldn't be parsed or is missing; they
        # are useless for calendar/index computations and cause later KeyErrors
        frame = frame.dropna(subset=["datetime"])
        # drop raw 'date' column if it came back; we've renamed it
        if "date" in frame.columns:
            frame = frame.drop(columns=["date"])
        for symbol, symbol_frame in frame.groupby("symbol", sort=False):
            ordered = symbol_frame.sort_values("datetime").reset_index(drop=True)
            # keep all fields except the `symbol` column itself; the provider
            # consumers will look for whatever feature names they need, and
            # missing columns simply produce NaNs.
            self._instrument_frames[symbol] = ordered.drop(columns=["symbol"], errors="ignore").copy()
        for symbol in self._requested_instruments:
            self._instrument_frames.setdefault(symbol, pd.DataFrame(columns=["datetime", *RAW_OCHLVF_COLUMNS]))
        return frame


@contextmanager
def feature_handler_provider_context(provider):
    """Temporarily swap featureHandler's global provider with a duck-server-backed one."""

    provider_module = importlib.import_module("featureHandler.provider")
    loader_module = importlib.import_module("featureHandler.loader")

    old_provider = provider_module.D
    old_loader_provider = loader_module.D
    provider_module.D = provider
    loader_module.D = provider
    try:
        yield
    finally:
        provider_module.D = old_provider
        loader_module.D = old_loader_provider