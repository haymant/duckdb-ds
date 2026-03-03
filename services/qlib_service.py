"""
Utility for loading qlib alpha feature data based on environment variables.
The configuration mirrors the `.env.local` settings used by the project and
returns dictionaries of pandas DataFrames suitable for registration with DuckDB.
"""
import os
import logging
from typing import Dict, Tuple, List
import qlib
from qlib.constant import REG_CN, REG_US
from qlib.contrib.data.handler import Alpha158, Alpha360
import pandas as pd

class Alpha158e(Alpha158):
    """Alpha158 extended with raw OHLCV(F) fields."""
    def get_feature_config(self) -> Tuple[List[str], List[str]]:
        fields, names = super().get_feature_config()
        raw_fields = ["$open", "$high", "$low", "$close", "$volume", "$factor"]
        raw_names = ["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "FACTOR"]
        return fields + raw_fields, names + raw_names

def _fmt_date(s: str) -> str:
    if s and len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    return s

def _fetch_df(HandlerClass, kwargs) -> pd.DataFrame:
    handler = HandlerClass(**kwargs)
    df = handler.fetch()
    df = df.reset_index().rename(columns={"datetime": "date", "instrument": "symbol"})
    df["date"] = pd.to_datetime(df["date"])
    logging.getLogger(__name__).info(
        f"qlib handler {HandlerClass.__name__} returned {len(df):,} rows and {len(df.columns):,} columns"
    )
    return df

def _load_region(prefix: str, region_constant) -> Dict[str, pd.DataFrame]:
    data_dir = os.getenv(f"QLIB_{prefix}_DATA_DIR")
    if not data_dir or not os.path.isdir(data_dir):
        if data_dir:
            logging.getLogger(__name__).warning(f"Directory {data_dir} not found for {prefix}")
        return {}

    alpha_types_str = os.getenv(f"QLIB_{prefix}_ALPHA_TYPE", "158")
    alpha_types = [t.strip() for t in alpha_types_str.split(",") if t.strip()]
    if not alpha_types:
        return {}

    instruments = os.getenv(f"QLIB_{prefix}_INSTRUMENTS", "all")
    start_time = _fmt_date(os.getenv(f"QLIB_{prefix}_START_DATE", ""))
    end_time = _fmt_date(os.getenv(f"QLIB_{prefix}_END_DATE", ""))
    freq = os.getenv(f"QLIB_{prefix}_FREQ", "day")

    ql_logger = logging.getLogger("qlib")
    old_level = ql_logger.level
    ql_logger.setLevel(logging.ERROR)
    try:
        qlib.init(provider_uri=data_dir, region=region_constant)
    except Exception as e:
        logging.getLogger(__name__).warning(f"qlib.init failed for {prefix}: {e}")
        return {}
    finally:
        ql_logger.setLevel(old_level)

    kwargs = {
        "start_time": start_time or None,
        "end_time": end_time or None,
        "fit_start_time": start_time or None,
        "fit_end_time": end_time or None,
        "instruments": instruments,
        "freq": freq,
    }

    results = {}
    region_lbl = "us" if region_constant == REG_US else "cn"

    for a in alpha_types:
        df_name = f"alpha{a}_{region_lbl}"
        try:
            if a == "158":
                results[df_name] = _fetch_df(Alpha158, kwargs)
            elif a == "158e":
                results[df_name] = _fetch_df(Alpha158e, kwargs)
            elif a == "360":
                results[df_name] = _fetch_df(Alpha360, kwargs)
            else:
                logging.getLogger(__name__).warning(f"Unsupported alpha type '{a}' for {prefix}")
        except Exception as e:
            logging.getLogger(__name__).warning(f"Error fetching {df_name}: {e}")

    return results

def load_qlib_dataframes() -> Dict[str, pd.DataFrame]:
    results = {}
    
    us_data = _load_region("US", REG_US)
    results.update(us_data)
    
    cn_data = _load_region("CN", REG_CN)
    results.update(cn_data)
    
    return results

