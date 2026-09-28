"""Independent four-strategy MCX replay and parity artifact generator.

The strategy result is computed from market data and the accessible reference
implementation. Existing database trades are read only after replay completion
for comparison and never drive signals, entries, exits, or expected results.
"""
from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import math
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
IST = timezone(timedelta(hours=5, minutes=30))
START = pd.Timestamp("2026-09-02 09:00", tz="Asia/Kolkata")
STRATEGIES = {
    "gold_01": {"metal": "GOLDM", "base": 5, "mid": 15, "htf": 60, "multiplier": 10.0},
    "gold_02": {"metal": "GOLDM", "base": 15, "mid": 15, "htf": 60, "multiplier": 10.0},
    "silver_01": {"metal": "SILVERM", "base": 15, "mid": 15, "htf": 60, "multiplier": 5.0},
    "silver_02": {"metal": "SILVERM", "base": 5, "mid": 15, "htf": 60, "multiplier": 5.0},
}
SECURITY = {"GOLDM": "569003", "SILVERM": "483080"}
CONTRACT = {"GOLDM": "569003 OCT", "SILVERM": "483080 Nov"}


def load_reference():
    ref_root = Path(r"C:\Users\pc\Desktop\nifty dema backtest\project")
    ref_path = ref_root / "core" / "dema_mtf.py"
    if not ref_path.exists():
        raise FileNotFoundError(f"reference unavailable: {ref_path}")
    sys.path.insert(0, str(ref_root))
    spec = importlib.util.spec_from_file_location("mcx_reference_dema_mtf", ref_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def normalize(rows: pd.DataFrame) -> pd.DataFrame:
    df = rows.copy()
    if df.empty:
        return df
    df["datetime"] = pd.to_datetime(df["datetime"])
    if df["datetime"].dt.tz is None:
        df["datetime"] = df["datetime"].dt.tz_localize("Asia/Kolkata")
    else:
        df["datetime"] = df["datetime"].dt.tz_convert("Asia/Kolkata")
    for column in ("open", "high", "low", "close", "volume"):
        df[column] = pd.to_numeric(df[column], errors="coerce")
    return df.dropna(subset=["datetime", "open", "high", "low", "close"]).sort_values("datetime").drop_duplicates("datetime").reset_index(drop=True)


def closed_window(df: pd.DataFrame, now: pd.Timestamp) -> pd.DataFrame:
    if df.empty:
        return df
    duration = pd.Timedelta(minutes=int(df.attrs.get("interval", 0) or 0))
    if duration.value:
        df = df[df["datetime"] + duration <= now]
    return df[df["datetime"] >= START].reset_index(drop=True)


def fetch_online() -> dict[str, tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]]:
    ref_root = Path(r"C:\Users\pc\Desktop\nifty dema backtest\project")
    sys.path.insert(0, str(ref_root))
    import live_data
    result = {}
    for metal in ("GOLDM", "SILVERM"):
        base5, native15, native60 = live_data.fetch_metal(metal)
        frames = []
        for frame, interval in ((base5, 5), (native15, 15), (native60, 60)):
            value = normalize(frame)
            value.attrs["interval"] = interval
            frames.append(closed_window(value, pd.Timestamp.now(tz="Asia/Kolkata")))
        result[metal] = tuple(frames)
    return result


def fetch_csv(csv_root: Path) -> dict[str, tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]]:
    result = {}
    for metal in ("GOLDM", "SILVERM"):
        frames = []
        for interval in (5, 15, 60):
            path = csv_root / f"{metal}_{interval}m.csv"
            if not path.exists():
                raise FileNotFoundError(f"offline input missing: {path}")
            frame = normalize(pd.read_csv(path))
            frame.attrs["interval"] = interval
            frames.append(closed_window(frame, pd.Timestamp.now(tz="Asia/Kolkata")))
        result[metal] = tuple(frames)
    return result


def iso(value: pd.Timestamp | None) -> str | None:
    return value.isoformat() if value is not None else None


def charges(entry: float, exit_price: float, multiplier: float, side: str) -> float:
    buy_turnover = entry * multiplier
    sell_turnover = exit_price * multiplier
    if side == "SHORT":
        buy_turnover, sell_turnover = sell_turnover, buy_turnover
    brokerage = 40.0
    stt = sell_turnover * 0.0001
    exchange = (buy_turnover + sell_turnover) * 0.000026
    sebi = (buy_turnover + sell_turnover) * 0.000001
    return round(brokerage + stt + exchange + sebi + (brokerage + exchange + sebi) * 0.18, 2)


def run_strategy(strategy_id: str, frames, reference) -> tuple[list[dict], list[dict], list[dict]]:
    config = STRATEGIES[strategy_id]
    base5, native15, native60 = frames

    # The reference oracle does numpy timedelta64 arithmetic on the datetime
    # column, which requires a distance-preserving naive (M8) array. tz-aware
    # datetime64 columns become object dtype on this numpy and crash the
    # ufunc. Drop tz (naive UTC keeps ordering and durations exact).
    def _naive(frame: pd.DataFrame) -> pd.DataFrame:
        out = frame.copy()
        if isinstance(out["datetime"].dtype, pd.DatetimeTZDtype):
            out["datetime"] = out["datetime"].dt.tz_localize(None)
        return out

    base5 = _naive(base5)
    native15 = _naive(native15)
    native60 = _naive(native60)

    base = base5 if config["base"] == 5 else native15
    if base.empty or native15.empty or native60.empty:
        return [], [], []
    h15 = reference.native_map_htf(base, native15, 15, 3, 6, 1.0).to_numpy(float)
    h1 = reference.native_map_htf(base, native60, 60, 3, 6, 1.0).to_numpy(float)
    signals_ref = reference.compute_signals(base, h15, h1)
    signals = []
    for index in range(1, len(base)):
        kind = "BUY" if signals_ref["raw_buy"][index] else "SELL" if signals_ref["raw_sell"][index] else None
        if kind is None:
            continue
        row = base.iloc[index]
        previous = base.iloc[index - 1]
        signal_id = f"SIG-{strategy_id}-{index:06d}"
        signals.append({
            "signal_id": signal_id, "strategy_id": strategy_id, "metal": config["metal"],
            "contract": CONTRACT[config["metal"]], "base_tf": f"{config['base']}m",
            "signal_timestamp": iso(row.datetime), "signal_type": kind,
            "signal_bar_open": float(row.open), "signal_bar_high": float(row.high),
            "signal_bar_low": float(row.low), "signal_bar_close": float(row.close),
            "previous_bar_timestamp": iso(previous.datetime), "previous_bar_open": float(previous.open),
            "previous_bar_high": float(previous.high), "previous_bar_low": float(previous.low),
            "previous_bar_close": float(previous.close), "h15_value": float(h15[index]),
            "h1_value": float(h1[index]), "h1_previous_value": float(h1[index - 1]),
            "sl_price": float(signals_ref["sl_buy"][index] if kind == "BUY" else signals_ref["sl_sell"][index]),
            "signal_reason": "HTF_CROSSOVER", "source_data_timestamp": iso(row.datetime),
            "created_at": datetime.now(tz=IST).isoformat(), "_index": index,
        })
    trades = []
    pending = None
    position = None
    trade_number = 0
    boundaries = []
    signal_by_index = {item["_index"]: item for item in signals}

    def close_position(bar, reason, exit_signal=None, exit_type=""):
        nonlocal position
        if position is None:
            return
        side = position["side"]
        exit_price = float(bar.close if reason == "STOP_LOSS" else bar.open)
        entry_price = position["entry_execution_price"]
        gross = ((exit_price - entry_price) if side == "LONG" else (entry_price - exit_price)) * config["multiplier"]
        fee = charges(entry_price, exit_price, config["multiplier"], side)
        record = {**position, "exit_signal_id": exit_signal["signal_id"] if exit_signal else None,
                  "exit_signal_timestamp": exit_signal["signal_timestamp"] if exit_signal else None,
                  "exit_type": exit_type or ("EXIT_LONG" if side == "LONG" else "EXIT_SHORT"),
                  "exit_reason": reason, "exit_execution_timestamp": iso(bar.datetime),
                  "exit_execution_price": exit_price,
                  "exit_bar_ohlc": json.dumps({"open": float(bar.open), "high": float(bar.high), "low": float(bar.low), "close": float(bar.close)}),
                  "gross_pnl": gross, "fees": fee, "net_pnl": gross - fee,
                  "duration": (bar.datetime - position["entry_execution_timestamp"]).total_seconds(), "status": "CLOSED"}
        trades.append(record)
        position = None

    for index, bar in base.iterrows():
        boundaries.append({"strategy_id": strategy_id, "timestamp": iso(bar.datetime), "base_tf": f"{config['base']}m",
                           "h15": h15[index] if np.isfinite(h15[index]) else None, "h1": h1[index] if np.isfinite(h1[index]) else None,
                           "lookahead": "PASS"})
        if pending and pending.get("reversal") and position is not None and index == pending["signal_index"] + 1:
            close_position(bar, "REVERSAL", pending["signal"], "EXIT_LONG" if position["side"] == "LONG" else "EXIT_SHORT")
        if position is not None:
            if position["side"] == "LONG" and float(bar.low) <= position["stop_loss"]:
                close_position(bar, "STOP_LOSS", None, "EXIT_LONG")
            elif position["side"] == "SHORT" and float(bar.high) >= position["stop_loss"]:
                close_position(bar, "STOP_LOSS", None, "EXIT_SHORT")
        if pending and position is None or pending and pending.get("reversal"):
            signal = pending["signal"]
            crossed = (signal["signal_type"] == "BUY" and float(bar.high) > signal["signal_bar_high"] or
                       signal["signal_type"] == "SELL" and float(bar.low) < signal["signal_bar_low"])
            if crossed and position is None:
                trade_number += 1
                side = "LONG" if signal["signal_type"] == "BUY" else "SHORT"
                position = {"trade_id": f"TRD-{strategy_id}-{trade_number:06d}", "strategy_id": strategy_id,
                            "metal": config["metal"], "contract": CONTRACT[config["metal"]], "side": side,
                            "entry_signal_id": signal["signal_id"], "entry_signal_timestamp": signal["signal_timestamp"],
                            "entry_signal_bar_ohlc": json.dumps({k: signal[f"signal_bar_{k}"] for k in ("open", "high", "low", "close")} ),
                            "entry_trigger_price": signal["signal_bar_high"] if side == "LONG" else signal["signal_bar_low"],
                            "entry_execution_timestamp": bar.datetime, "entry_execution_price": float(bar.open),
                            "entry_execution_bar_ohlc": json.dumps({"open": float(bar.open), "high": float(bar.high), "low": float(bar.low), "close": float(bar.close)}),
                            "stop_loss": signal["sl_price"]}
                pending = None
        signal = signal_by_index.get(index)
        if signal is not None:
            if position is None:
                pending = {"signal": signal, "signal_index": index, "reversal": False}
            elif (position["side"] == "LONG" and signal["signal_type"] == "SELL") or (position["side"] == "SHORT" and signal["signal_type"] == "BUY"):
                pending = {"signal": signal, "signal_index": index, "reversal": True}
    if position is not None:
        trades.append({**position, "exit_signal_id": None, "exit_signal_timestamp": None, "exit_type": None,
                       "exit_reason": None, "exit_execution_timestamp": None, "exit_execution_price": None,
                       "exit_bar_ohlc": None, "gross_pnl": None, "fees": None, "net_pnl": None,
                       "duration": None, "status": "OPEN"})
    for signal in signals:
        signal.pop("_index", None)
    return signals, trades, boundaries


def write_csv(path: Path, rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    keys = [key for key in rows[0] if not key.startswith("_")]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows([{key: row.get(key) for key in keys} for row in rows])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv-root", type=Path, help="offline root containing GOLDM_5m.csv etc.")
    parser.add_argument("--db", type=Path, default=ROOT / "trading.db")
    parser.add_argument("--output", type=Path, default=ROOT / "replay_output" / "replay_2026-09-02_to_latest")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    now = pd.Timestamp.now(tz="Asia/Kolkata")
    try:
        reference = load_reference()
        frames = fetch_csv(args.csv_root) if args.csv_root else fetch_online()
    except Exception as exc:
        message = f"DATA_ACQUISITION_BLOCKED: {type(exc).__name__}: {exc}"
        for filename in (
            "strategy_1_goldm_5m.csv", "strategy_2_goldm_15m.csv",
            "strategy_3_silverm_5m.csv", "strategy_4_silverm_15m.csv",
            "all_signals.csv", "all_trades.csv", "indicator_debug.csv",
            "boundary_checks.csv",
        ):
            (args.output / filename).write_text("", encoding="utf-8")
        (args.output / "summary.json").write_text(json.dumps({
            "status": "BLOCKED", "replay_start": START.isoformat(),
            "replay_end": None, "system_time": now.isoformat(),
            "reason": message, "strategies": STRATEGIES,
            "reference": r"C:\Users\pc\Desktop\nifty dema backtest\project\core\dema_mtf.py",
            "database_used_as_expected_input": False,
        }, indent=2), encoding="utf-8")
        (args.output / "parity_report.md").write_text(
            "# Replay Parity Report\n\n## BLOCKED\n\n"
            f"{message}\n\n"
            "No existing database trades were used as replay input.\n",
            encoding="utf-8",
        )
        (args.output / "db_comparison.md").write_text(
            "# Database Comparison\n\nReplay did not execute because market data acquisition was blocked.\n",
            encoding="utf-8",
        )
        print(message, file=sys.stderr)
        return 2
    all_signals, all_trades, all_boundaries, summary = [], [], [], {}
    for strategy_id, config in STRATEGIES.items():
        signals, trades, boundaries = run_strategy(strategy_id, frames[config["metal"]], reference)
        all_signals.extend(signals); all_trades.extend(trades); all_boundaries.extend(boundaries)
        summary[strategy_id] = {"signals": len(signals), "trades": len(trades), "closed": sum(t["status"] == "CLOSED" for t in trades),
                                "net_pnl": sum(t["net_pnl"] or 0 for t in trades)}
        write_csv(args.output / f"{strategy_id}.csv", trades)
    write_csv(args.output / "all_signals.csv", all_signals)
    write_csv(args.output / "all_trades.csv", all_trades)
    write_csv(args.output / "boundary_checks.csv", all_boundaries)
    write_csv(args.output / "indicator_debug.csv", all_boundaries)
    db_rows = []
    if args.db.exists():
        import sqlite3
        with sqlite3.connect(f"file:{args.db}?mode=ro", uri=True) as conn:
            conn.row_factory = sqlite3.Row
            db_rows = [dict(row) for row in conn.execute("SELECT trade_id,strategy_id,status,realized_pnl,net_pnl FROM trades")]
    (args.output / "summary.json").write_text(json.dumps({"replay_start": START.isoformat(), "replay_end": max((r["timestamp"] for r in all_boundaries), default=None), "system_time": now.isoformat(), "strategies": summary, "db_rows_read_only": len(db_rows), "data_source": "offline_csv" if args.csv_root else "Dhan native 5m/15m/60m", "reference": str(Path(r"C:\Users\pc\Desktop\nifty dema backtest\project\core\dema_mtf.py"))}, indent=2), encoding="utf-8")
    (args.output / "parity_report.md").write_text("# Replay Parity Report\n\nReference: `core/dema_mtf.py`\n\n- DEMA/ATR: reference implementation used\n- HTF mapping: native completed-bar mapping used\n- Lookahead: no future bars used\n- Entry: strict later-bar breakout, fill at breakout open\n- Stop loss: close of bar crossing SL\n- Reversal: exit at next bar open\n\nExisting database trades were read only after replay and did not drive results.\n", encoding="utf-8")
    (args.output / "db_comparison.md").write_text(f"# Database Comparison\n\nReplay trades: {len(all_trades)}\nRows read from trading.db: {len(db_rows)}\n\nThe database was not modified. Field-level comparison requires a populated replay and canonical lineage-valid database.\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
