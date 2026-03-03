"""
Utility for loading qlib alpha feature data based on environment variables.
The configuration mirrors the `.env.local` settings used by the project and
returns a pandas DataFrame suitable for registration with DuckDB.

Environment variables:

- QLIB_DATA_DIR     : provider_uri passed to `qlib.init`
- QLIB_DATA_REGION  : either `cn` or `us` (mapped to REG_CN/REG_US)
- QLIB_START_DATE   : YYYYMMDD or YYYY-MM-DD string for start of features
- QLIB_END_DATE     : same format for end date
- QLIB_FREQ         : frequency string passed to handler (e.g. `day`)
- QLIB_ALPHA_TYPE   : "158" or "360"
- QLIB_INSTRUMENTS  : instruments argument ("all", "csi300", list, etc.)

If `QLIB_DATA_DIR` is not set the loader returns None and nothing will be
registered with DuckDB.
"""
import os
import logging
from typing import Optional
import qlib
from qlib.constant import REG_CN, REG_US  # or REG_US if you use US market
from qlib.contrib.data.handler import Alpha158, Alpha360
from qlib.data import D
import pandas as pd


def _fmt_date(s: str) -> str:
    # convert YYYYMMDD to YYYY-MM-DD if necessary
    if s and len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    return s


def load_qlib_dataframe() -> Optional[pd.DataFrame]:
    """Attempt to load qlib alpha features according to environment.

    Returns a DataFrame or None if the configuration is incomplete.
    """
    provider = os.getenv("QLIB_DATA_DIR")
    if not provider:
        return None
    # avoid expensive initialization if the directory doesn't actually exist
    if not os.path.isdir(provider):
        logging.getLogger(__name__).warning(
            "QLIB_DATA_DIR '%s' does not exist, skipping qlib data load", provider
        )
        return None


    region_str = os.getenv("QLIB_DATA_REGION", "").lower()
    region = REG_CN if region_str == "cn" else REG_US

    try:
        # temporarily quiet qlib's own logger to avoid spammy warnings such
        # as "auto_path is False" or other notices during initialization.
        ql_logger = logging.getLogger("qlib")
        old_level = ql_logger.level
        ql_logger.setLevel(logging.ERROR)
        try:
            qlib.init(provider_uri=provider, region=region)
        finally:
            ql_logger.setLevel(old_level)

        # handler parameters
        alpha_type = os.getenv("QLIB_ALPHA_TYPE", "158")
        instruments = os.getenv("QLIB_INSTRUMENTS", "all")
        start_time = _fmt_date(os.getenv("QLIB_START_DATE", ""))
        end_time = _fmt_date(os.getenv("QLIB_END_DATE", ""))
        freq = os.getenv("QLIB_FREQ", "day")

        from qlib.contrib.data.handler import Alpha158, Alpha360

        HandlerClass = Alpha158 if alpha_type == "158" else Alpha360

        handler = HandlerClass(
            start_time=start_time or None,
            end_time=end_time or None,
            fit_start_time=start_time or None,
            fit_end_time=end_time or None,
            instruments=instruments,
            freq=freq,
        )

        df = handler.fetch()
        # flatten the multi-index into columns
        df = df.reset_index().rename(columns={"datetime": "date", "instrument": "symbol"})
        df["date"] = pd.to_datetime(df["date"])

        # log size for diagnostics
        logging.getLogger(__name__).info(
            f"qlib handler returned {len(df):,} rows and {len(df.columns):,} columns"
        )

        return df
    except Exception as e:
        logging.getLogger(__name__).warning(f"error while initializing/reading qlib data: {e}")
        return None
