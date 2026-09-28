"""LIVE (production MCX-TRADER code) vs BACKTEST (nifty reference) signal parity.

Feeds actual MCX 5m CSV data through BOTH pipelines:
  backtest side : reference core/dema_mtf.py (resample_ohlcv, htf_dema_line, compute_signals)
  live side     : production indicators/dema_atr.py + htf/backtest_style_htf.py
                  + strategies/base_dema_strategy.py (on_bar + _check_*_cross)
Compares bar-by-bar: base OHLC, h1 (1H DEMA-ATR line), h15, signal direction,
trigger and SL. Reports first divergence + classification.

Also computes a FORCED-GRID reference line (reference DEMA-ATR but with the
mapping grid set to the true fast timeframe, bypassing the reference's
auto-detected base_min which is corrupted by irregular head rows in the CSVs).
This isolates the DATA-artifact (reference base_min=1) from a genuine
indicator/mapping divergence.
"""
import importlib.util
import os
import sys

import numpy as np
import pandas as pd

NIFTY = r"C:\Users\pc\Desktop\nifty dema backtest\project"
LIVE = r"C:\Users\pc\Desktop\MCX-TRADER"
sys.path.insert(0, LIVE)
sys.path.append(NIFTY)  # end of path: only for utils.logger, never shadows LIVE's core

def _load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod

# Load reference modules by file to avoid the `core` package-name collision
# between the backtest project and MCX-TRADER.
build_15min_enriched = _load_module("build_15min_enriched", os.path.join(NIFTY, "build_15min_enriched.py"))
ref_dema_mtf = _load_module("ref_dema_mtf", os.path.join(NIFTY, "core", "dema_mtf.py"))
resample_ohlcv = ref_dema_mtf.resample_ohlcv
htf_dema_line = ref_dema_mtf.htf_dema_line
compute_signals = ref_dema_mtf.compute_signals

from core.timeframe_engine import Bar, BarState  # noqa: E402
from htf.backtest_style_htf import BacktestStyleHTFEngine  # noqa: E402
from strategies.base_dema_strategy import BaseDEMAStrategy  # noqa: E402

GOLD_CSV = os.path.join(NIFTY, "data_mcx", "GOLDM_5m_mcx.csv")
SILVER_CSV = os.path.join(NIFTY, "data_mcx", "SILVERM_5m_mcx.csv")

SESSION_OPEN = "09:00"
DEMA_P, ATR_P, ATR_F = 3, 6, 1.0


def htf_line_forced(df_base, rule, base_min, dema_p=DEMA_P, atr_p=ATR_P, atr_f=ATR_F,
                    session_open=SESSION_OPEN):
    """Reference DEMA-ATR line with the mapping grid explicitly set to base_min
    (normalising the reference's auto-detected base_min, which is corrupted by
    irregular head rows in the 5m CSVs)."""
    rule_min = ref_dema_mtf._rule_minutes(rule)
    htf = resample_ohlcv(df_base, rule, session_open=session_open)
    dt = htf["datetime"].to_numpy()
    src_avail = dt + np.timedelta64(rule_min, "m")
    vals = build_15min_enriched.dema_atr(
        pd.DataFrame({"open": htf["open"].to_numpy(), "high": htf["high"].to_numpy(),
                      "low": htf["low"].to_numpy(), "close": htf["close"].to_numpy(),
                      "volume": htf["volume"].to_numpy()}),
        dema_p, atr_p, atr_f,
    ).to_numpy()
    base_dt = pd.to_datetime(df_base["datetime"]).to_numpy()
    target = base_dt + np.timedelta64(base_min, "m")
    idx = np.searchsorted(src_avail, target, side="right") - 1
    out = np.full(len(base_dt), np.nan)
    mask = idx >= 0
    out[mask] = vals[np.clip(idx[mask], 0, len(src_avail) - 1)]
    return out


def load_df(path: str):
    df = pd.read_csv(path, parse_dates=["datetime"])
    return df.sort_values("datetime").reset_index(drop=True)


def epoch(dt_series: pd.Series) -> np.ndarray:
    """naive wall-clock epoch (consistent unit for both sides)."""
    return (dt_series - pd.Timestamp("1970-01-01")).dt.total_seconds().to_numpy()


def warmup_htf_bars(df5: pd.DataFrame, tf_minutes: int, keep_partial: bool = True) -> pd.DataFrame:
    """Production warmup/offline HTF aggregation (trading_engine 09:00 session anchor)."""
    dt = pd.to_datetime(df5["datetime"])
    d = df5.copy()
    d["datetime"] = dt
    dates = dt.dt.date.astype(str)
    session_start = pd.to_datetime(dates + f" {SESSION_OPEN}")
    mins = ((dt - session_start).dt.total_seconds() // 60).astype(int)
    d["_bucket"] = session_start + pd.to_timedelta((mins // tf_minutes) * tf_minutes, unit="m")
    d = d[mins >= 0]
    if not keep_partial:
        d = d[d.groupby("_bucket")["datetime"].transform("size") == tf_minutes // 5]
    htf = (
        d.groupby("_bucket", sort=True)
        .agg({"open": "first", "high": "max", "low": "min",
              "close": "last", "volume": "sum"})
        .reset_index()
        .rename(columns={"_bucket": "datetime"})
    )
    return htf[["datetime", "open", "high", "low", "close", "volume"]]


def df_to_bars(df: pd.DataFrame, instrument: str, timeframe: str, minutes: int):
    bars = []
    dts = pd.to_datetime(df["datetime"])
    eps = epoch(dts)
    for i in range(len(df)):
        bars.append(Bar(
            instrument=instrument,
            timeframe=timeframe,
            start_ts=float(eps[i]),
            end_ts=float(eps[i]) + minutes * 60.0,
            open=float(df["open"].iloc[i]),
            high=float(df["high"].iloc[i]),
            low=float(df["low"].iloc[i]),
            close=float(df["close"].iloc[i]),
            volume=int(df["volume"].iloc[i]) if "volume" in df else 0,
            state=BarState.CLOSED,
        ))
    return bars


def run_strategy(name, metal, csv_path, base_minutes):
    print(f"\n{'='*90}\nSTRATEGY {name}: {metal} base={base_minutes}m (mid 15m / htf 1h)\n{'='*90}")
    df5 = load_df(csv_path)
    print(f"raw 5m rows: {len(df5)}  ts {df5['datetime'].iloc[0]} .. {df5['datetime'].iloc[-1]}")

    # ---- base series ----
    if base_minutes == 5:
        df_base = df5.copy()
    else:
        df_base = resample_ohlcv(df5, "15min", session_open=SESSION_OPEN)
    print(f"base rows: {len(df_base)} ({base_minutes}m)")

    # ---- BACKTEST side (reference code, as-is: auto-detected base_min) ----
    h15_ref = htf_dema_line(df_base, "15min", DEMA_P, ATR_P, ATR_F, SESSION_OPEN).to_numpy()
    h1_ref = htf_dema_line(df_base, "60min", DEMA_P, ATR_P, ATR_F, SESSION_OPEN).to_numpy()
    sigs = compute_signals(df_base, h15_ref, h1_ref)
    buy_ref, sell_ref = sigs["raw_buy"], sigs["raw_sell"]
    sl_ref = np.where(buy_ref, sigs["sl_buy"], np.where(sell_ref, sigs["sl_sell"], np.nan))
    trig_ref = np.where(buy_ref, df_base["high"], np.where(sell_ref, df_base["low"], np.nan))
    n_ref = sum(buy_ref) + sum(sell_ref)
    nm_ref_b = int(sum(buy_ref)); nm_ref_s = int(sum(sell_ref))
    print(f"BACKTEST signals (auto base_min): LONG={nm_ref_b} SHORT={nm_ref_s} total={n_ref}")

    # ---- BACKTEST side, FORCED grid (base_min = true fast timeframe) ----
    h15_forced = htf_line_forced(df_base, "15min", base_minutes)
    h1_forced = htf_line_forced(df_base, "60min", base_minutes)
    sigs_f = compute_signals(df_base, h15_forced, h1_forced)
    buy_f, sell_f = sigs_f["raw_buy"], sigs_f["raw_sell"]
    n_f = sum(buy_f) + sum(sell_f)
    print(f"BACKTEST signals (forced grid {base_minutes}m): LONG={int(sum(buy_f))} SHORT={int(sum(sell_f))} total={n_f}")

    # ---- LIVE side: build 15m/1h bars with production aggregation ----
    htf15 = warmup_htf_bars(df5, 15, keep_partial=True)
    htf60 = warmup_htf_bars(df5, 60, keep_partial=True)

    # resampling parity vs reference resample_ohlcv
    ref15 = resample_ohlcv(df5, "15min", SESSION_OPEN)
    ref60 = resample_ohlcv(df5, "60min", SESSION_OPEN)
    merge15 = htf15.merge(ref15, on="datetime", suffixes=("_l", "_r"))
    ohlc_diffs = 0
    for c in ["open", "high", "low", "close", "volume"]:
        ohlc_diffs += int((merge15[f"{c}_l"] != merge15[f"{c}_r"]).sum())
    print(f"resample parity 15m: rows {len(htf15)} vs {len(ref15)}, OHLC/vol diffs={ohlc_diffs}")
    merge60 = htf60.merge(ref60, on="datetime", suffixes=("_l", "_r"))
    ohlc_diffs60 = 0
    for c in ["open", "high", "low", "close", "volume"]:
        ohlc_diffs60 += int((merge60[f"{c}_l"] != merge60[f"{c}_r"]).sum())
    print(f"resample parity 1h : rows {len(htf60)} vs {len(ref60)}, OHLC/vol diffs={ohlc_diffs60}")

    # ---- live engine feed ----
    eng = BacktestStyleHTFEngine()
    eng.register(metal, "1h", DEMA_P, ATR_P, ATR_F, session_open=SESSION_OPEN)
    eng.register(metal, "15m", DEMA_P, ATR_P, ATR_F, session_open=SESSION_OPEN)
    for b in df_to_bars(htf15, metal, "15m", 15):
        eng.on_htf_bar_closed(b)
    for b in df_to_bars(htf60, metal, "1h", 60):
        eng.on_htf_bar_closed(b)

    base_bars = df_to_bars(df_base, metal, f"{base_minutes}m", base_minutes)
    h1_live = np.full(len(base_bars), np.nan)
    h15_live = np.full(len(base_bars), np.nan)

    close_a = df_base["close"].to_numpy(float)
    high_a = df_base["high"].to_numpy(float)
    low_a = df_base["low"].to_numpy(float)

    strat = BaseDEMAStrategy(strategy_id=name, instrument=metal,
                             fast_timeframe=f"{base_minutes}m", htf_timeframe="1h",
                             quantity=1)
    h1_mismatch = 0; first_h1 = None
    h15_mismatch = 0; first_h15 = None
    live_signal_bars = []   # (idx, side, trigger, sl) from the state machine
    live_xs_long = np.zeros(len(base_bars), dtype=bool)   # production _check_long_cross per bar
    live_xs_short = np.zeros(len(base_bars), dtype=bool)
    prevh1 = None
    prevh15 = None

    for i, bar in enumerate(base_bars):
        hm = eng.map_to_fast_bar(bar, strat.fast_timeframe)
        mm = eng.map_mid_to_fast_bar(bar, strat.fast_timeframe)
        h1_l = hm.htf_value if hm.htf_confirmed else None
        h15_l = mm.htf_value if mm.htf_confirmed else None
        h1_live[i] = np.nan if h1_l is None else h1_l
        h15_live[i] = np.nan if h15_l is None else h15_l

        if not (np.isnan(h1_live[i]) and np.isnan(h1_ref[i])):
            if (np.isnan(h1_live[i]) != np.isnan(h1_ref[i])) or \
               (not np.isnan(h1_live[i]) and abs(h1_live[i] - h1_ref[i]) > 1e-9):
                h1_mismatch += 1
                if first_h1 is None:
                    first_h1 = (i, str(df_base["datetime"].iloc[i]), h1_live[i], h1_ref[i])
        if not (np.isnan(h15_live[i]) and np.isnan(h15_ref[i])):
            if (np.isnan(h15_live[i]) != np.isnan(h15_ref[i])) or \
               (not np.isnan(h15_live[i]) and abs(h15_live[i] - h15_ref[i]) > 1e-9):
                h15_mismatch += 1
                if first_h15 is None:
                    first_h15 = (i, str(df_base["datetime"].iloc[i]), h15_live[i], h15_ref[i])

        # Production cross functions driven directly with this bar's values
        # (prev_close/prev_htf from the previous bar, exactly like on_bar).
        pc = close_a[i - 1] if i > 0 else close_a[i]
        if h1_l is not None and prevh1 is not None and h15_l is not None and i > 0:
            live_xs_long[i] = bool(strat._check_long_cross(
                float(close_a[i]), float(pc), h1_l, prevh1, h15_l, prevh15))
            live_xs_short[i] = bool(strat._check_short_cross(
                float(close_a[i]), float(pc), h1_l, prevh1, h15_l, prevh15))

        # strategy on_bar (production state machine)
        fast_val = np.nan
        strat.on_bar(bar, hm, fast_val, mm)
        for ev in strat._events:
            if ev["event_type"] in ("PENDING_ENTRY_CREATED", "REVERSAL_SIGNAL"):
                if ev.get("_seen"):
                    continue
                ev["_seen"] = True
                live_signal_bars.append((i, ev["side"], ev["trigger"], ev["stop"]))

        prevh1 = h1_l
        prevh15 = h15_l

    def _cmp_arrays(a, b):
        mm = 0
        first = None
        for i in range(len(a)):
            if a[i] != b[i]:
                mm += 1
                if first is None:
                    first = i
        return mm, first

    dir_mm_auto, first_dir_auto = _cmp_arrays(
        np.where(buy_ref, "LONG", np.where(sell_ref, "SHORT", "")),
        np.where(live_xs_long, "LONG", np.where(live_xs_short, "SHORT", "")))
    dir_mm_forced, first_dir_forced = _cmp_arrays(
        np.where(buy_f, "LONG", np.where(sell_f, "SHORT", "")),
        np.where(live_xs_long, "LONG", np.where(live_xs_short, "SHORT", "")))

    if dir_mm_auto:
        print("  AUTO-ref residuals (stale base_min=1 mapping suspects):")
        ref_dir = np.where(buy_ref, "LONG", np.where(sell_ref, "SHORT", ""))
        live_dir_x = np.where(live_xs_long, "LONG", np.where(live_xs_short, "SHORT", ""))
        cnt = 0
        for i in range(len(base_bars)):
            if ref_dir[i] != live_dir_x[i]:
                cnt += 1
                print(f"    bar {i} {df_base['datetime'].iloc[i]}: ref={ref_dir[i] or '-'} live={live_dir_x[i] or '-'}"
                      f" h1_auto={h1_ref[i]:.2f} h1_forced={h1_forced[i]:.2f} h1_live={h1_live[i]:.2f}"
                      f" close={close_a[i]:.2f} prev_close={close_a[i-1]:.2f}")
                if cnt >= 6:
                    break

    # line parity vs forced-grid reference (isolates base_min artifact)
    h1_mm_forced = 0; first_h1_f = None
    h15_mm_forced = 0; first_h15_f = None
    for i in range(len(base_bars)):
        if not (np.isnan(h1_live[i]) and np.isnan(h1_forced[i])):
            if (np.isnan(h1_live[i]) != np.isnan(h1_forced[i])) or \
               (not np.isnan(h1_live[i]) and abs(h1_live[i] - h1_forced[i]) > 1e-9):
                h1_mm_forced += 1
                if first_h1_f is None:
                    first_h1_f = (i, str(df_base["datetime"].iloc[i]), h1_live[i], h1_forced[i])
        if not (np.isnan(h15_live[i]) and np.isnan(h15_forced[i])):
            if (np.isnan(h15_live[i]) != np.isnan(h15_forced[i])) or \
               (not np.isnan(h15_live[i]) and abs(h15_live[i] - h15_forced[i]) > 1e-9):
                h15_mm_forced += 1
                if first_h15_f is None:
                    first_h15_f = (i, str(df_base["datetime"].iloc[i]), h15_live[i], h15_forced[i])

    print(f"\nLIVE (post-fix production) per-bar crosses LONG={int(live_xs_long.sum())} "
          f"SHORT={int(live_xs_short.sum())}")
    print(f"dir mismatches vs AUTO reference: {dir_mm_auto} (first idx {first_dir_auto})")
    print(f"dir mismatches vs FORCED grid ref: {dir_mm_forced} (first idx {first_dir_forced})")
    print(f"line parity vs FORCED grid ref   : h1_mm={h1_mm_forced} (first {first_h1_f})  "
          f"h15_mm={h15_mm_forced} (first {first_h15_f})")

    live_dir = {idx: side for idx, side, *_ in live_signal_bars}
    dir_mm_state = 0
    first_dir_state = None
    for i in range(len(base_bars)):
        ref = "LONG" if buy_ref[i] else ("SHORT" if sell_ref[i] else None)
        liv = live_dir.get(i)
        if ref != liv:
            dir_mm_state += 1
            if first_dir_state is None:
                first_dir_state = (i, str(df_base["datetime"].iloc[i]), ref, liv)

    print(f"state-machine signals: {len(live_signal_bars)}; dir mismatches vs AUTO ref: {dir_mm_state} "
          f"(first {first_dir_state})")

    # trigger/SL parity on common signal bars
    trig_sl_mm = 0
    first_ts = None
    for i in range(len(base_bars)):
        if i not in live_dir:
            continue
        ref_sig = "LONG" if buy_ref[i] else ("SHORT" if sell_ref[i] else None)
        if ref_sig is None:
            continue
        if live_dir[i] != ref_sig:
            continue
        if ref_sig == "LONG":
            ok_t = abs(trig_ref[i] - float(high_a[i])) < 1e-9
            ok_s = abs(sl_ref[i] - float(min(low_a[i], low_a[i - 1]))) < 1e-9
        else:
            ok_t = abs(trig_ref[i] - float(low_a[i])) < 1e-9
            ok_s = abs(sl_ref[i] - float(max(high_a[i], high_a[i - 1]))) < 1e-9
        if not (ok_t and ok_s):
            trig_sl_mm += 1
            if first_ts is None:
                first_ts = (i, str(df_base["datetime"].iloc[i]), ref_sig)
    print(f"trigger/SL mismatches on common signal bars: {trig_sl_mm} (first {first_ts})")

    print(f"h1 line mismatched bars (vs AUTO ref): {h1_mismatch} (first: {first_h1})")
    print(f"h15 line mismatched bars (vs AUTO ref): {h15_mismatch} (first: {first_h15})")
    return {
        "name": name, "metal": metal, "base": base_minutes,
        "n_raw": len(df_base), "n_bt": n_ref, "n_bt_forced": n_f,
        "n_xs_long": int(live_xs_long.sum()), "n_xs_short": int(live_xs_short.sum()),
        "h1_mm_auto": h1_mismatch, "h15_mm_auto": h15_mismatch,
        "h1_mm_forced": h1_mm_forced, "h15_mm_forced": h15_mm_forced,
        "dir_mm_auto": dir_mm_auto, "dir_mm_forced": dir_mm_forced,
        "dir_mm_state": dir_mm_state,
        "trigsl_mm": trig_sl_mm,
        "first_dir_auto": first_dir_auto, "first_dir_forced": first_dir_forced,
    }


if __name__ == "__main__":
    results = []
    results.append(run_strategy("gold_01 (GOLDM 5m)", "GOLDM", GOLD_CSV, 5))
    results.append(run_strategy("gold_02 (GOLDM 15m)", "GOLDM", GOLD_CSV, 15))
    results.append(run_strategy("silver_02 (SILVERM 5m)", "SILVERM", SILVER_CSV, 5))
    results.append(run_strategy("silver_01 (SILVERM 15m)", "SILVERM", SILVER_CSV, 15))
    print("\n" + "=" * 90)
    print("SUMMARY")
    print("=" * 90)
    for r in results:
        print(f"{r['name']:<24} bt_auto={r['n_bt']:<4} bt_forced={r['n_bt_forced']:<4} "
              f"xslong={r['n_xs_long']:<4} xsshort={r['n_xs_short']:<4}")
        print(f"{'':24} dir_auto={r['dir_mm_auto']:<4} dir_forced={r['dir_mm_forced']:<4} "
              f"dir_state={r['dir_mm_state']:<4} trigsl={r['trigsl_mm']:<4}")
        print(f"{'':24} line auto h1={r['h1_mm_auto']} h15={r['h15_mm_auto']} | "
              f"forced h1={r['h1_mm_forced']} h15={r['h15_mm_forced']}")