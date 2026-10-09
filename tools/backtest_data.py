"""Shared input preparation for the current 15-minute SuperTrend backtest."""
from __future__ import annotations

from pathlib import Path
import pandas as pd


def load_5m_csv(path: Path, session_open: str = "09:00",
                session_close: str = "23:30") -> pd.DataFrame:
    frame = pd.read_csv(path)
    aliases = {str(column).strip().lower(): column for column in frame.columns}
    time_column = next((aliases[name] for name in
                        ("datetime", "date_time", "timestamp", "time", "date")
                        if name in aliases), None)
    if time_column is None:
        raise ValueError(f"{path} has no recognizable timestamp column")
    rename = {}
    for name in ("open", "high", "low", "close", "volume"):
        if name in aliases:
            rename[aliases[name]] = name
    missing = {"open", "high", "low", "close"} - set(rename.values())
    if missing:
        raise ValueError(f"{path} is missing OHLC columns: {sorted(missing)}")
    frame = frame.rename(columns=rename)
    index = pd.to_datetime(frame.pop(time_column), errors="coerce")
    if getattr(index.dt, "tz", None) is not None:
        index = index.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    frame.index = pd.DatetimeIndex(index)
    frame = frame.loc[~frame.index.isna()].sort_index()
    for column in ("open", "high", "low", "close"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(subset=["open", "high", "low", "close"])
    if "volume" not in frame:
        frame["volume"] = 0
    frame["volume"] = pd.to_numeric(frame["volume"], errors="coerce").fillna(0)
    frame["source_rows"] = 1
    start, end = pd.to_timedelta(session_open + ":00"), pd.to_timedelta(session_close + ":00")
    times = frame.index.time
    open_time = pd.Timestamp("2000-01-01") + start
    close_time = pd.Timestamp("2000-01-01") + end
    mask = (times >= open_time.time()) & (times < close_time.time())
    filtered = frame.loc[mask].copy()
    filtered.attrs["filtered_rows"] = int(len(frame) - len(filtered))
    return filtered


def resample_clock_bars(base: pd.DataFrame, minutes: int,
                        session_close: str = "23:30") -> pd.DataFrame:
    if minutes <= 0:
        raise ValueError("minutes must be positive")
    if base.empty:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume",
                                     "source_rows"], index=base.index)
    offset = "9h"
    bars = base.resample(f"{minutes}min", origin="start_day", offset=offset,
                         closed="left", label="left").agg({
        "open": "first", "high": "max", "low": "min", "close": "last",
        "volume": "sum", "source_rows": "sum",
    })
    bars = bars.dropna(subset=["open", "high", "low", "close"])
    close = pd.to_timedelta(session_close + ":00")
    session_end = bars.index.normalize() + close
    bars = bars.loc[bars.index < session_end]
    bars.attrs["filtered_rows"] = base.attrs.get("filtered_rows", 0)
    return bars


def fees(symbol: str, entry_price: float, exit_price: float, quantity: int,
         contract_multiplier: float, schedule: dict, sell_side: str) -> float:
    cfg = schedule.get(symbol, {})
    entry_turnover = abs(float(entry_price) * quantity * contract_multiplier)
    exit_turnover = abs(float(exit_price) * quantity * contract_multiplier)
    brokerage = float(cfg.get("brokerage_per_side", 0.0)) * 2
    sell_turnover = exit_turnover if str(sell_side).upper() == "SELL" else entry_turnover
    buy_turnover = entry_turnover if str(sell_side).upper() == "SELL" else exit_turnover
    stt = sell_turnover * float(cfg.get("stt_sell_pct", 0.0)) / 100.0
    exchange = (entry_turnover + exit_turnover) * float(cfg.get("exchange_pct", 0.0)) / 100.0
    sebi = (entry_turnover + exit_turnover) * float(cfg.get("sebi_pct", 0.0)) / 100.0
    stamp = buy_turnover * float(cfg.get("stamp_duty_pct", 0.0)) / 100.0
    gst = (brokerage + exchange + sebi) * float(cfg.get("gst_pct", 0.0)) / 100.0
    return brokerage + stt + exchange + sebi + stamp + gst
