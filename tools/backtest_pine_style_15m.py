"""Backtest the attached Pine strategy logic on 15m/15m/1h data in Backtrader.

The signal and state rules follow the supplied Pine script. Only its timeframe
inputs are set to the production configuration: trade=15m, fast=15m, slow=1h.
This is an offline simulation and never uses a live broker connection.
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any

import backtrader as bt
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.backtest_live_15m_csv import (  # noqa: E402
    Live15mFeed, add_dema_atr, fees, load_5m_csv, resample_clock_bars,
)


class PineStyle15m(bt.Strategy):
    params = dict(instrument="", quantity=1, multiplier=1.0, tick_size=1.0,
                  initial_capital=300000.0, charges=None, warmup_hours=20)

    def __init__(self) -> None:
        self.position_side: str | None = None
        self.entry_price: float | None = None
        self.entry_time: pd.Timestamp | None = None
        self.entry_kind = ""
        self.entry_reversal = False
        self.position_signal: dict[str, Any] = {}
        self.entry_bar: dict[str, float] | None = None
        self.active_sl: float | None = None
        self.pending: dict[str, Any] | None = None
        self.previous_stored_slow: float | None = None
        self.previous_stored_fast: float | None = None
        self.previous_close: float | None = None
        self._last_htf_seq: float | None = None
        self._hour_count = 0
        self.trades: list[dict[str, Any]] = []
        self.events: list[dict[str, Any]] = []

    def _dt(self) -> pd.Timestamp:
        return pd.Timestamp(bt.num2date(self.data.datetime[0])).tz_localize(None)

    def _row(self) -> dict[str, float]:
        return {key: float(getattr(self.data, key)[0])
                for key in ("open", "high", "low", "close", "fast", "htf", "htf_seq")}

    @staticmethod
    def _bar_fields(prefix: str, bar: dict[str, float] | None) -> dict[str, Any]:
        if not bar:
            return {}
        return {f"{prefix}_{key}": bar.get(key) for key in ("open", "high", "low", "close")}

    def _close(self, price: float, ts: pd.Timestamp, reason: str,
               bar: dict[str, float] | None = None) -> None:
        side = self.position_side
        if side is None or self.entry_price is None or self.entry_time is None:
            return
        gross = ((price - self.entry_price) if side == "LONG"
                 else (self.entry_price - price)) * self.p.quantity * self.p.multiplier
        # STT is charged on the sell leg (entry sell for shorts, exit sell for longs).
        charge = fees(self.p.instrument, self.entry_price, price, self.p.quantity,
                      self.p.multiplier, self.p.charges,
                      "SELL" if side == "LONG" else "BUY")
        trade = {
            **self.position_signal,
            "instrument": self.p.instrument, "side": side,
            "entry_time": self.entry_time.isoformat(sep=" "),
            "entry_price": self.entry_price, "entry_type": self.entry_kind,
            "entry_trigger_price": self.entry_price,
            "exit_time": ts.isoformat(sep=" "), "exit_price": price,
            "exit_reason": reason, "stop_price": self.active_sl,
            "quantity": self.p.quantity, "multiplier": self.p.multiplier,
            "gross_pnl": gross, "fees": charge, "net_pnl": gross - charge,
            "reversal_entry": self.entry_reversal,
            "status": "CLOSED",
            "duration_minutes": (ts - self.entry_time).total_seconds() / 60.0,
        }
        trade.update(self._bar_fields("entry_bar", self.entry_bar))
        trade.update(self._bar_fields("exit_bar", bar))
        self.trades.append(trade)
        self.events.append({"time": ts.isoformat(sep=" "), "event": "position_closed",
                            "side": side, "price": price, "reason": reason})
        self.position_side = None
        self.entry_price = None
        self.entry_time = None
        self.entry_kind = ""
        self.entry_reversal = False
        self.active_sl = None
        self.position_signal = {}
        self.entry_bar = None

    def _open(self, side: str, price: float, stop: float, ts: pd.Timestamp,
              kind: str, reversal: bool, bar: dict[str, float],
              signal: dict[str, Any]) -> None:
        # The requested OHLC model fills every entry exactly at the trigger,
        # including gaps; there is no entry slippage.
        self.position_side = side
        self.entry_price = price
        self.entry_time = ts
        self.entry_kind = kind
        self.entry_reversal = reversal
        self.active_sl = stop
        self.position_signal = dict(signal)
        self.entry_bar = dict(bar)
        self.events.append({"time": ts.isoformat(sep=" "), "event": "position_opened",
                            "side": side, "price": price, "stop": stop,
                            "type": kind, "reversal": reversal})
        # A new position's stop can be touched within the same OHLC bar as its
        # entry. With lost tick ordering, assume it is stopped immediately.
        if side == "LONG" and bar["low"] <= stop:
            self._close(stop, ts, "same_bar_stop_first", bar)
        elif side == "SHORT" and bar["high"] >= stop:
            self._close(stop, ts, "same_bar_stop_first", bar)

    def _signal(self, row: dict[str, float]) -> str | None:
        # Pine logic: previous close/fast/slow vs signal close/current slow.
        if (self.previous_close is None or self.previous_stored_slow is None
                or self.previous_stored_fast is None):
            return None
        if row["close"] > row["htf"] and self.previous_close < self.previous_stored_slow and self.previous_stored_fast < self.previous_stored_slow:
            return "LONG"
        if row["close"] < row["htf"] and self.previous_close > self.previous_stored_slow and self.previous_stored_fast > self.previous_stored_slow:
            return "SHORT"
        return None

    def next(self) -> None:
        row, ts = self._row(), self._dt()
        if not all(math.isfinite(row[k]) for k in ("open", "high", "low", "close", "fast", "htf", "htf_seq")):
            return
        if self._last_htf_seq is None or row["htf_seq"] != self._last_htf_seq:
            self._hour_count += 1
            self._last_htf_seq = row["htf_seq"]

        # Work existing orders through this bar before evaluating its close.
        # On an ambiguous bar, the current position's stop has priority.
        if self.position_side and self.active_sl is not None:
            sl_hit = (row["low"] <= self.active_sl if self.position_side == "LONG"
                      else row["high"] >= self.active_sl)
            if sl_hit:
                self._close(self.active_sl, ts, "stop_loss", row)

        if self.pending:
            side = str(self.pending["side"])
            trigger = float(self.pending["trigger"])
            crossed = row["high"] >= trigger if side == "LONG" else row["low"] <= trigger
            if crossed:
                order = self.pending
                self.pending = None
                reverses_open_position = self.position_side is not None and self.position_side != side
                if reverses_open_position:
                    self._close(trigger, ts, "reversal_exit", row)
                if self.position_side is None:
                    self._open(side, trigger, float(order["stop"]), ts,
                               "reversal_entry" if order.get("is_reversal") else "entry_trigger",
                               bool(order.get("is_reversal")), row, order)

        signal = self._signal(row)
        if self._hour_count >= int(self.p.warmup_hours) and signal:
            if signal == "LONG" and self.position_side != "LONG":
                replaced = self.pending is not None
                is_reversal = self.position_side == "SHORT"
                self.pending = self._signal_details("LONG", row, ts, is_reversal)
                event = ("reversal_replaced" if replaced and is_reversal else
                         "trigger_replaced" if replaced else
                         "reversal_armed" if is_reversal else "signal_armed")
                self.events.append({"time": ts.isoformat(sep=" "),
                                    "event": event,
                                    "side": "LONG", "trigger": self.pending["trigger"],
                                    "stop": self.pending["stop"], **self.pending})
            elif signal == "SHORT" and self.position_side != "SHORT":
                replaced = self.pending is not None
                is_reversal = self.position_side == "LONG"
                self.pending = self._signal_details("SHORT", row, ts, is_reversal)
                event = ("reversal_replaced" if replaced and is_reversal else
                         "trigger_replaced" if replaced else
                         "reversal_armed" if is_reversal else "signal_armed")
                self.events.append({"time": ts.isoformat(sep=" "),
                                    "event": event,
                                    "side": "SHORT", "trigger": self.pending["trigger"],
                                    "stop": self.pending["stop"], **self.pending})

        # Pine stores this candle's completed indicator values only after
        # evaluating its signal, so they become the prior values next bar.
        self.previous_close = row["close"]
        self.previous_stored_slow = row["htf"]
        self.previous_stored_fast = row["fast"]

    def _signal_details(self, side: str, row: dict[str, float], ts: pd.Timestamp,
                        is_reversal: bool) -> dict[str, Any]:
        previous = ({key: float(getattr(self.data, key)[-1])
                     for key in ("open", "high", "low", "close")}
                    if len(self) > 1 else {})
        stop = (min(row["low"], previous.get("low", row["low"])) if side == "LONG"
                else max(row["high"], previous.get("high", row["high"])))
        result: dict[str, Any] = {
            "side": side, "trigger": row["high"] if side == "LONG" else row["low"],
            "stop": stop, "is_reversal": is_reversal,
            "signal_time": ts.isoformat(sep=" "),
            "signal_type": "REVERSAL" if is_reversal else "INITIAL",
            "signal_fast_dema_atr_15m": row["fast"],
            "signal_slow_dema_atr_1h": row["htf"],
            "previous_fast_dema_atr_15m": self.previous_stored_fast,
            "previous_slow_dema_atr_1h": self.previous_stored_slow,
            "previous_close": self.previous_close,
        }
        result.update(self._bar_fields("signal_bar", row))
        result.update(self._bar_fields("previous_bar", previous))
        return result

    def stop(self) -> None:
        if self.position_side is not None:
            row, ts = self._row(), self._dt()
            unrealized = ((row["close"] - float(self.entry_price)) if self.position_side == "LONG"
                          else (float(self.entry_price) - row["close"])) * self.p.quantity * self.p.multiplier
            self.events.append({"time": ts.isoformat(sep=" "), "event": "open_position_mark",
                                "side": self.position_side, "entry": self.entry_price,
                                "mark": row["close"], "unrealized_gross": unrealized,
                                "stop": self.active_sl})
            self.trades.append({
                **self.position_signal, "instrument": self.p.instrument,
                "side": self.position_side, "entry_time": self.entry_time.isoformat(sep=" "),
                "entry_price": self.entry_price, "entry_trigger_price": self.entry_price,
                "entry_type": self.entry_kind, "exit_time": "", "exit_price": None,
                "exit_reason": "OPEN_AT_DATA_END", "stop_price": self.active_sl,
                "quantity": self.p.quantity, "multiplier": self.p.multiplier,
                "gross_pnl": 0.0, "fees": 0.0, "net_pnl": 0.0,
                "unrealized_gross": unrealized, "mark_price": row["close"],
                "reversal_entry": self.entry_reversal, "status": "OPEN",
                "duration_minutes": (ts - self.entry_time).total_seconds() / 60.0,
                **self._bar_fields("entry_bar", self.entry_bar),
                **self._bar_fields("mark_bar", row),
            })


def run_one(instrument: str, csv_path: Path, config: dict[str, Any], out: Path,
            warmup_hours: int) -> dict[str, Any]:
    inst = config["instruments"][instrument]
    base = load_5m_csv(csv_path, inst.get("session_open", "09:00"),
                       inst.get("session_close", "23:30"))
    fast = resample_clock_bars(base, 15, inst.get("session_close", "23:30"))
    slow = resample_clock_bars(base, 60, inst.get("session_close", "23:30"))
    ind = config["indicators"]
    fast["fast"] = add_dema_atr(fast, ind["dema_period"], ind["atr_period"], ind["atr_factor"])
    fast["mid"] = fast["fast"]
    slow["slow"] = add_dema_atr(slow, ind["dema_period"], ind["atr_period"], ind["atr_factor"])
    slow_end_ns = [int(x.value) for x in slow["end"]]
    slow_vals = slow["slow"].tolist()
    import bisect
    mapped, seq = [], []
    for end in fast["end"]:
        idx = bisect.bisect_right(slow_end_ns, int(end.value)) - 1
        mapped.append(float("nan") if idx < 0 or pd.isna(slow_vals[idx]) else float(slow_vals[idx]))
        seq.append(float(idx))
    fast["htf"], fast["htf_seq"] = mapped, seq
    fast = fast.dropna(subset=["fast", "htf"])
    strat_key = "gold_02" if instrument == "GOLDM" else "silver_01"
    scfg = config["strategies"][strat_key]
    feed_df = fast[["open", "high", "low", "close", "volume", "fast", "mid", "htf", "htf_seq"]].copy()
    cerebro = bt.Cerebro(stdstats=False)
    cerebro.adddata(Live15mFeed(dataname=feed_df), name=instrument)
    cerebro.addstrategy(PineStyle15m, instrument=instrument,
                        quantity=int(scfg.get("quantity", 1)),
                        multiplier=float(inst["multiplier"]), tick_size=float(inst["tick_size"]),
                        initial_capital=float(scfg.get("capital", 300000)),
                        charges=config["charges"], warmup_hours=warmup_hours)
    cerebro.broker.setcash(float(scfg.get("capital", 300000)))
    strategy: PineStyle15m = cerebro.run(runonce=False, preload=True)[0]
    out.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(strategy.trades).to_csv(out / f"{instrument.lower()}_pine_trades.csv", index=False)
    pd.DataFrame(strategy.events).to_csv(out / f"{instrument.lower()}_pine_events.csv", index=False)
    closed = [t for t in strategy.trades if t.get("status") == "CLOSED"]
    open_trades = [t for t in strategy.trades if t.get("status") == "OPEN"]
    wins = sum(t["net_pnl"] > 0 for t in closed)
    losses = sum(t["net_pnl"] < 0 for t in closed)
    gross = sum(t["gross_pnl"] for t in closed)
    total_fees = sum(t["fees"] for t in closed)
    net = sum(t["net_pnl"] for t in closed)
    open_mark = next((e["unrealized_gross"] for e in reversed(strategy.events)
                      if e["event"] == "open_position_mark"), 0.0)
    counts: dict[str, int] = {}
    for event in strategy.events:
        counts[event["event"]] = counts.get(event["event"], 0) + 1
    return {
        "instrument": instrument, "data_start": str(base.index.min()),
        "data_end": str(base.index.max()), "source_5m_rows": int(base["source_rows"].sum()),
        "filtered_out_of_session_rows": int(base.attrs.get("filtered_rows", 0)),
        "derived_15m_bars": int(len(fast)), "derived_1h_bars": int(len(slow)),
        "warmup_hours": warmup_hours, "quantity": int(scfg.get("quantity", 1)),
        "entries_including_open": len(closed) + len(open_trades),
        "multiplier": float(inst["multiplier"]), "closed_trades": len(closed),
        "open_trades_at_data_end": len(open_trades),
        "wins": wins, "losses": losses,
        "win_rate_pct": round(100 * wins / len(closed), 2) if closed else 0,
        "gross_realized_pnl": round(gross, 2), "fees": round(total_fees, 2),
        "net_realized_pnl": round(net, 2),
        "open_unrealized_gross_at_end": round(open_mark, 2),
        "net_including_open_mark_before_exit_fees": round(net + open_mark, 2),
        "event_counts": counts,
        "trades_file": str(out / f"{instrument.lower()}_pine_trades.csv"),
        "events_file": str(out / f"{instrument.lower()}_pine_events.csv"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--config", type=Path, default=ROOT / "config" / "live_settings.json")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "backtest_results_pine_15m")
    parser.add_argument("--warmup-hours", type=int, default=20)
    args = parser.parse_args()
    config = json.loads(args.config.read_text(encoding="utf-8"))
    results = [run_one(sym, args.data_dir / f"{sym}_5m_mcx.csv", config,
                       args.output_dir, args.warmup_hours)
               for sym in ("GOLDM", "SILVERM")]
    metadata = {
        "strategy": "Attached Pine strategy logic, parameterized trade=15m, fast=15m, slow=1h",
        "signal_logic": "Pine previous close/previous fast/previous slow versus current close/current slow conditions",
        "execution": "future 15m candle high/low crossing, exact trigger price, zero slippage for entry/stop/reversal",
        "reversal": "opposite stop-entry at its signal candle high/low; no additional 2-point gap; one pending opposite order replaces the other",
        "ambiguous_bar": "active stop is evaluated before a pending reversal trigger; if a new entry and its stop are both touched, assume entry then stop-out",
        "indicator_parameters": config["indicators"],
        "warmup_hours": args.warmup_hours,
        "costs": "per-instrument fees from config/live_settings.json",
        "capital_and_margin": "₹300,000 reference capital per strategy; margin constraints are not simulated",
        "data_note": "15m and 1h bars are derived from provided 5m CSVs; expiry/security-id continuity is not identifiable from their columns.",
    }
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "summary.json").write_text(json.dumps(results, indent=2), encoding="utf-8")
    (args.output_dir / "run_metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    print(json.dumps({"results": results, "output_dir": str(args.output_dir)}, indent=2))


if __name__ == "__main__":
    main()
