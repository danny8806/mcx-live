"""Offline 15-minute backtest of the supplied Pine v4 SuperTrend strategy."""
from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.backtest_data import fees, load_5m_csv, resample_clock_bars  # noqa: E402


def pine_atr(data: pd.DataFrame, period: int, change_atr: bool = True) -> pd.Series:
    """Pine v4 atr() (Wilder RMA) or SMA(true range) when changeATR is false."""
    previous_close = data["close"].shift(1)
    tr = pd.concat([
        data["high"] - data["low"],
        (data["high"] - previous_close).abs(),
        (data["low"] - previous_close).abs(),
    ], axis=1).max(axis=1)
    if change_atr:
        values = [float("nan")] * len(tr)
        if len(tr) >= period:
            seed = float(tr.iloc[:period].mean())
            values[period - 1] = seed
            for i in range(period, len(tr)):
                values[i] = (values[i - 1] * (period - 1) + float(tr.iloc[i])) / period
        return pd.Series(values, index=data.index, dtype="float64")
    return tr.rolling(period, min_periods=period).mean()


def supertrend(data: pd.DataFrame, period: int = 10, multiplier: float = 3.0,
               change_atr: bool = True) -> pd.DataFrame:
    """Reproduce the supplied Pine v4 recursive band/trend calculation."""
    if period < 1 or multiplier <= 0:
        raise ValueError("period must be >= 1 and multiplier must be > 0")
    atr = pine_atr(data, period, change_atr)
    src = (data["high"] + data["low"]) / 2.0
    basic_up = src - multiplier * atr
    basic_dn = src + multiplier * atr
    up = [float("nan")] * len(data)
    dn = [float("nan")] * len(data)
    trend = [1] * len(data)
    buy = [False] * len(data)
    sell = [False] * len(data)
    for i in range(len(data)):
        if not math.isfinite(float(atr.iloc[i])):
            if i:
                trend[i] = trend[i - 1]
            continue
        u, d = float(basic_up.iloc[i]), float(basic_dn.iloc[i])
        up1 = up[i - 1] if i and math.isfinite(up[i - 1]) else u
        dn1 = dn[i - 1] if i and math.isfinite(dn[i - 1]) else d
        prev_close = float(data["close"].iloc[i - 1]) if i else float("nan")
        up[i] = max(u, up1) if i and prev_close > up1 else u
        dn[i] = min(d, dn1) if i and prev_close < dn1 else d
        prior_trend = trend[i - 1] if i else 1
        close = float(data["close"].iloc[i])
        if prior_trend == -1 and close > dn1:
            trend[i] = 1
        elif prior_trend == 1 and close < up1:
            trend[i] = -1
        else:
            trend[i] = prior_trend
        buy[i] = trend[i] == 1 and prior_trend == -1
        sell[i] = trend[i] == -1 and prior_trend == 1
    return pd.DataFrame({"atr": atr, "up": up, "dn": dn, "trend": trend,
                         "buy_signal": buy, "sell_signal": sell}, index=data.index)


def _bar(data: pd.DataFrame, i: int) -> dict[str, Any]:
    return {f"{k}": float(data[k].iloc[i]) for k in ("open", "high", "low", "close")}


def run_one(symbol: str, data_dir: Path, config: dict[str, Any], out_dir: Path,
            period: int, multiplier: float, change_atr: bool, fill_model: str,
            apply_live_fees: bool) -> dict[str, Any]:
    instrument = config["instruments"][symbol]
    base = load_5m_csv(data_dir / f"{symbol}_5m_mcx.csv", instrument.get("session_open", "09:00"),
                       instrument.get("session_close", "23:30"))
    bars = resample_clock_bars(base, 15, instrument.get("session_close", "23:30"))
    bars = bars.join(supertrend(bars, period, multiplier, change_atr))
    bars = bars.dropna(subset=["atr"])
    strategy_cfg = config["strategies"]["gold_02" if symbol == "GOLDM" else "silver_01"]
    qty = int(strategy_cfg.get("quantity", 1))
    contract_multiplier = float(instrument["multiplier"])
    tick_size = float(instrument["tick_size"])
    position: dict[str, Any] | None = None
    pending: dict[str, Any] | None = None
    trades: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []

    def close_position(price: float, ts: pd.Timestamp, bar: dict[str, Any], reason: str) -> None:
        nonlocal position
        if position is None:
            return
        side = position["side"]
        gross = (price - position["entry_price"] if side == "LONG" else position["entry_price"] - price) * qty * contract_multiplier
        sell_side = "SELL" if side == "LONG" else "BUY"
        charge = (fees(symbol, position["entry_price"], price, qty, contract_multiplier,
                       config["charges"], sell_side) if apply_live_fees else 0.0)
        row = {**position, "instrument": symbol, "status": "CLOSED",
               "exit_time": ts.isoformat(sep=" "), "exit_price": price,
               "exit_reason": reason, "exit_bar_open": bar["open"],
               "exit_bar_high": bar["high"], "exit_bar_low": bar["low"],
               "exit_bar_close": bar["close"], "quantity": qty,
               "contract_multiplier": contract_multiplier, "gross_pnl": gross,
               "fees": charge, "net_pnl": gross - charge,
               "duration_minutes": (ts - pd.Timestamp(position["entry_time"])).total_seconds() / 60}
        trades.append(row)
        events.append({"time": ts.isoformat(sep=" "), "instrument": symbol,
                       "event": "position_closed", "side": side, "price": price,
                       "reason": reason})
        position = None

    def enter(side: str, signal_i: int, fill_i: int, price: float, reversal: bool) -> None:
        nonlocal position
        sig_ts, fill_ts = bars.index[signal_i], bars.index[fill_i]
        signal_bar, fill_bar = _bar(bars, signal_i), _bar(bars, fill_i)
        position = {
            "instrument": symbol, "side": side,
            "signal_time": sig_ts.isoformat(sep=" "),
            "signal_type": "REVERSAL" if reversal else "INITIAL",
            "signal_open": signal_bar["open"], "signal_high": signal_bar["high"],
            "signal_low": signal_bar["low"], "signal_close": signal_bar["close"],
            "signal_atr": float(bars["atr"].iloc[signal_i]),
            "signal_up_band": float(bars["up"].iloc[signal_i]) if math.isfinite(float(bars["up"].iloc[signal_i])) else None,
            "signal_down_band": float(bars["dn"].iloc[signal_i]) if math.isfinite(float(bars["dn"].iloc[signal_i])) else None,
            "signal_trend": int(bars["trend"].iloc[signal_i]),
            "previous_trend": int(bars["trend"].iloc[signal_i - 1]) if signal_i else 1,
            "previous_atr": float(bars["atr"].iloc[signal_i - 1]) if signal_i else None,
            "previous_up_band": float(bars["up"].iloc[signal_i - 1]) if signal_i and math.isfinite(float(bars["up"].iloc[signal_i - 1])) else None,
            "previous_down_band": float(bars["dn"].iloc[signal_i - 1]) if signal_i and math.isfinite(float(bars["dn"].iloc[signal_i - 1])) else None,
            "entry_time": fill_ts.isoformat(sep=" "), "entry_price": price,
            "entry_type": "next_bar_market_open" if fill_model == "next_open" else "signal_bar_market_close",
            "entry_open": fill_bar["open"], "entry_high": fill_bar["high"],
            "entry_low": fill_bar["low"], "entry_close": fill_bar["close"],
            "reversal_entry": reversal,
        }
        events.append({"time": fill_ts.isoformat(sep=" "), "instrument": symbol,
                       "event": "position_opened", "side": side, "price": price,
                       "signal_time": sig_ts.isoformat(sep=" "), "reversal": reversal})

    indices = list(range(len(bars)))
    for i in indices:
        ts = bars.index[i]
        signal = "LONG" if bool(bars["buy_signal"].iloc[i]) else "SHORT" if bool(bars["sell_signal"].iloc[i]) else None

        # By default, the signal is produced at this bar's close and the
        # broker emulator fills the resulting market order at next bar open.
        if pending is not None and pending["fill_i"] == i:
            order = pending; pending = None
            if position is not None and position["side"] != order["side"]:
                close_position(order["price"], ts, _bar(bars, i), "opposite_signal")
            if position is None:
                enter(order["side"], order["signal_i"], i, order["price"], order["reversal"])

        if signal:
            events.append({"time": ts.isoformat(sep=" "), "instrument": symbol,
                           "event": "buy_signal" if signal == "LONG" else "sell_signal",
                           "side": signal, "trend": int(bars["trend"].iloc[i]),
                           "atr": float(bars["atr"].iloc[i])})
            if fill_model == "signal_close":
                price = float(bars["close"].iloc[i])
                reversal = position is not None and position["side"] != signal
                if reversal:
                    close_position(price, ts, _bar(bars, i), "opposite_signal")
                if position is None:
                    enter(signal, i, i, price, reversal)
            elif i + 1 < len(bars):
                pending = {"side": signal, "signal_i": i, "fill_i": i + 1,
                           "price": float(bars["open"].iloc[i + 1]),
                           "reversal": position is not None and position["side"] != signal}
            else:
                events.append({"time": ts.isoformat(sep=" "), "instrument": symbol,
                               "event": "unfilled_last_bar_signal", "side": signal})

    if position is not None:
        last_i = len(bars) - 1
        last_ts = bars.index[last_i]
        mark = float(bars["close"].iloc[last_i])
        side = position["side"]
        unrealized = (mark - position["entry_price"] if side == "LONG" else position["entry_price"] - mark) * qty * contract_multiplier
        trades.append({**position, "instrument": symbol, "status": "OPEN",
                       "exit_time": "", "exit_price": None, "exit_reason": "OPEN_AT_DATA_END",
                       "exit_bar_open": None, "exit_bar_high": None, "exit_bar_low": None, "exit_bar_close": None,
                       "quantity": qty, "contract_multiplier": contract_multiplier,
                       "gross_pnl": 0.0, "fees": 0.0, "net_pnl": 0.0,
                       "unrealized_gross": unrealized, "mark_price": mark,
                       "mark_time": last_ts.isoformat(sep=" "),
                       "mark_bar_open": float(bars["open"].iloc[last_i]),
                       "mark_bar_high": float(bars["high"].iloc[last_i]),
                       "mark_bar_low": float(bars["low"].iloc[last_i]),
                       "mark_bar_close": float(bars["close"].iloc[last_i]),
                       "duration_minutes": (last_ts - pd.Timestamp(position["entry_time"])).total_seconds() / 60})

    out_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(trades).to_csv(out_dir / f"{symbol.lower()}_supertrend_15m_trades.csv", index=False)
    pd.DataFrame(events).to_csv(out_dir / f"{symbol.lower()}_supertrend_15m_events.csv", index=False)
    closed = [r for r in trades if r["status"] == "CLOSED"]
    wins = sum(r["net_pnl"] > 0 for r in closed)
    losses = sum(r["net_pnl"] < 0 for r in closed)
    winners = [r["net_pnl"] for r in closed if r["net_pnl"] > 0]
    losers = [r["net_pnl"] for r in closed if r["net_pnl"] < 0]
    equity = peak = 1000000.0
    max_drawdown = 0.0
    for trade in sorted(closed, key=lambda r: r["exit_time"]):
        equity += trade["net_pnl"]
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
    return {
        "instrument": symbol, "data_start": str(base.index.min()), "data_end": str(base.index.max()),
        "source_5m_rows": int(base["source_rows"].sum()),
        "filtered_out_of_session_rows": int(base.attrs.get("filtered_rows", 0)),
        "derived_15m_bars": int(len(bars)), "atr_period": period,
        "atr_multiplier": multiplier, "change_atr": change_atr,
        "entry_fill_model": fill_model, "quantity": qty, "contract_multiplier": contract_multiplier,
        "initial_capital": 1000000.0,
        "commission_model": "live fee schedule" if apply_live_fees else "TradingView default: no commission",
        "closed_trades": len(closed), "open_trades_at_data_end": sum(r["status"] == "OPEN" for r in trades),
        "wins": wins, "losses": losses,
        "win_rate_pct": round(wins * 100 / len(closed), 2) if closed else 0.0,
        "average_net_pnl_per_closed_trade": round(sum(r["net_pnl"] for r in closed) / len(closed), 2) if closed else 0.0,
        "profit_factor": round(sum(winners) / abs(sum(losers)), 4) if losers else None,
        "max_realized_drawdown": round(max_drawdown, 2),
        "largest_win": round(max(winners), 2) if winners else 0.0,
        "largest_loss": round(min(losers), 2) if losers else 0.0,
        "reversal_entries": sum(bool(r["reversal_entry"]) for r in closed),
        "gross_realized_pnl": round(sum(r["gross_pnl"] for r in closed), 2),
        "fees": round(sum(r["fees"] for r in closed), 2),
        "net_realized_pnl": round(sum(r["net_pnl"] for r in closed), 2),
        "open_unrealized_gross_at_end": round(sum(r.get("unrealized_gross", 0) for r in trades if r["status"] == "OPEN"), 2),
        "trade_file": str(out_dir / f"{symbol.lower()}_supertrend_15m_trades.csv"),
        "events_file": str(out_dir / f"{symbol.lower()}_supertrend_15m_events.csv"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data-dir', type=Path, required=True)
    parser.add_argument('--config', type=Path, default=ROOT / 'config' / 'live_settings.json')
    parser.add_argument('--output-dir', type=Path, default=ROOT / 'backtest_results_supertrend_15m')
    parser.add_argument('--atr-period', type=int, default=10)
    parser.add_argument('--atr-multiplier', type=float, default=3.0)
    parser.add_argument('--change-atr', action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument('--fill-model', choices=('next_open', 'signal_close'), default='next_open')
    parser.add_argument('--apply-live-fees', action='store_true',
                        help='Use configured Dhan/MCX fees instead of TradingView default zero commission.')
    args = parser.parse_args()
    config = json.loads(args.config.read_text(encoding='utf-8'))
    results = [run_one(sym, args.data_dir, config, args.output_dir,
                       args.atr_period, args.atr_multiplier, args.change_atr,
                       args.fill_model, args.apply_live_fees) for sym in ('GOLDM', 'SILVERM')]
    metadata = {
        'strategy': 'Supplied Pine v4 SuperTrend strategy, 15-minute timeframe',
        'atr_period': args.atr_period, 'atr_multiplier': args.atr_multiplier,
        'change_atr': args.change_atr,
        'source': 'hl2',
        'atr_method': 'Wilder ATR (RMA of true range)' if args.change_atr else 'SMA of true range',
        'entry_fill_model': args.fill_model,
        'fill_model_note': 'Pine default next-bar open' if args.fill_model == 'next_open' else 'Signal-bar close sensitivity case',
        'strategy_entry_behavior': 'Opposite strategy.entry closes the prior position and opens the new direction at the same fill price.',
        'exit_rule': 'No fixed stop or target in supplied Pine code; positions exit/reverse only on the opposite SuperTrend signal.',
        'quantity': 'TradingView default fixed quantity: 1. Contract multipliers from the instrument configuration are applied to P&L.',
        'initial_capital': 'TradingView default: 1,000,000 account-currency units (treated as INR for these MCX prices).',
        'commission': 'TradingView default zero commission.' if not args.apply_live_fees else 'Configured live fee schedule applied as a non-default comparison.',
        'slippage': 'TradingView default zero slippage.',
        'date_window': 'Pine inputs default to 2018-09-01 through 9999-01-01; all provided data falls inside this range.',
        'visual_inputs': 'showsignals, highlighting, and barcoloring affect chart display only, not trade decisions.',
        'data_note': '5-minute supplied CSVs resampled to session-anchored 15-minute candles; contract expiry continuity is not identified by source CSV columns.',
        'execution_limit': 'No slippage; market order price uses selected fill model. OHLC history cannot model spread, queue, rejection, or partial fills.',
    }
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / 'summary.json').write_text(json.dumps(results, indent=2), encoding='utf-8')
    (args.output_dir / 'run_metadata.json').write_text(json.dumps(metadata, indent=2), encoding='utf-8')
    print(json.dumps({'results': results, 'output_dir': str(args.output_dir)}, indent=2))


if __name__ == '__main__':
    main()
