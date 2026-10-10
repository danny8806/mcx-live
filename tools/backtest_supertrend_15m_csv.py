"""Offline 15-minute backtest of the supplied Pine v4 SuperTrend strategy."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.backtest_data import fees, load_5m_csv, resample_clock_bars  # noqa: E402
from indicators.supertrend import SuperTrend  # noqa: E402


def supertrend(data: pd.DataFrame, period: int, multiplier: float,
               change_atr: bool | None = None, *, source: str = "hl2",
               atr_method: str = "sma_tr", first_tr: str = "high_low") -> pd.DataFrame:
    """Run the production indicator over bars in timestamp order.

    ``change_atr`` remains a compatibility switch for command-line overrides;
    the normal run takes source/ATR/initial-TR settings from the active profile.
    """
    if period < 1 or multiplier <= 0:
        raise ValueError("period must be >= 1 and multiplier must be > 0")
    if change_atr is not None:
        atr_method = "wilder" if change_atr else "sma_tr"
    indicator = SuperTrend(period, multiplier, source, atr_method,
                           first_tr=first_tr)
    values = []
    for ts, row in data.iterrows():
        value = indicator.update(row.open, row.high, row.low, row.close,
                                 ts.timestamp())
        values.append({
            "atr": value.atr, "up": value.upper, "dn": value.lower,
            "trend": value.trend, "buy_signal": value.buy_signal,
            "sell_signal": value.sell_signal,
        })
    return pd.DataFrame(values, index=data.index)


def _bar(data: pd.DataFrame, i: int) -> dict[str, Any]:
    return {f"{k}": float(data[k].iloc[i]) for k in ("open", "high", "low", "close")}


def run_one(symbol: str, data_dir: Path, config: dict[str, Any], out_dir: Path,
            period: int | None, multiplier: float | None, change_atr: bool | None,
            fill_model: str,
            apply_live_fees: bool) -> dict[str, Any]:
    instrument = config["instruments"][symbol]
    base = load_5m_csv(data_dir / f"{symbol}_5m_mcx.csv", instrument.get("session_open", "09:00"),
                       instrument.get("session_close", "23:30"))
    bars = resample_clock_bars(base, 15, instrument.get("session_close", "23:30"))
    strategy_cfg = config["strategies"]["gold_02" if symbol == "GOLDM" else "silver_01"]
    params = strategy_cfg["supertrend"]
    period = int(period if period is not None else params["atr_period"])
    multiplier = float(multiplier if multiplier is not None else params["multiplier"])
    source = str(params["source"])
    atr_method = ("wilder" if change_atr else "sma_tr") if change_atr is not None else str(params["atr_method"])
    first_tr = str(params["first_tr"])
    bars = bars.join(supertrend(bars, period, multiplier, source=source,
                                atr_method=atr_method, first_tr=first_tr))
    bars = bars.dropna(subset=["atr"])
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
            "entry_type": "next_bar_market_open" if fill_model == "next_open" else "signal_bar_close_proxy",
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

        # Live submits a market order immediately after the completed signal
        # candle. Historical OHLC has no broker fill; signal_close is only a
        # proxy. next_open is an explicitly non-live sensitivity model.
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
    equity = peak = float(strategy_cfg.get("capital", 300000.0))
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
        "atr_multiplier": multiplier, "change_atr": atr_method == "wilder",
        "entry_fill_model": fill_model, "quantity": qty, "contract_multiplier": contract_multiplier,
        "initial_capital": float(strategy_cfg.get("capital", 300000.0)),
        "source": source, "atr_method": atr_method, "first_tr": first_tr,
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
    parser.add_argument('--atr-period', type=int, default=None,
                        help='Override both current profile periods (normally read from config).')
    parser.add_argument('--atr-multiplier', type=float, default=None,
                        help='Override both current profile multipliers (normally read from config).')
    parser.add_argument('--change-atr', action=argparse.BooleanOptionalAction, default=None,
                        help='Override profile ATR method: Wilder when on, SMA(TR) when off.')
    parser.add_argument('--fill-model', choices=('signal_close', 'next_open'), default='signal_close')
    parser.add_argument('--apply-live-fees', action='store_true',
                        help='Use configured Dhan/MCX fees instead of TradingView default zero commission.')
    args = parser.parse_args()
    config = json.loads(args.config.read_text(encoding='utf-8'))
    results = [run_one(sym, args.data_dir, config, args.output_dir,
                       args.atr_period, args.atr_multiplier, args.change_atr,
                       args.fill_model, args.apply_live_fees) for sym in ('GOLDM', 'SILVERM')]
    metadata = {
        'strategy': 'Current production SuperTrend strategy, 15-minute timeframe',
        'indicator_implementation': 'indicators.supertrend.SuperTrend shared with live execution',
        'profile_parameters': {k: v['supertrend'] for k, v in config['strategies'].items()},
        'atr_period_override': args.atr_period,
        'atr_multiplier_override': args.atr_multiplier,
        'change_atr_override': args.change_atr,
        'entry_fill_model': args.fill_model,
        'fill_model_note': 'Signal-close price proxy; live submits a market order immediately after candle close, actual broker fill can differ.' if args.fill_model == 'signal_close' else 'Next-bar open sensitivity model; not the live execution timing.',
        'strategy_entry_behavior': 'Live entry is a market order after a completed SuperTrend flip candle. Opposite flip closes old exposure and submits the opposite entry after broker-flat confirmation.',
        'exit_rule': 'No separate stop or target; exit/reversal only on opposite SuperTrend flip.',
        'quantity': 'Current profile quantity: 1 for each instrument. Contract multipliers from instrument configuration are applied to P&L.',
        'initial_capital': 'Current profile accounting capital: ₹300,000 per strategy.',
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
