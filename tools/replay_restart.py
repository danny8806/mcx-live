"""RESTART-CONTINUATION FORENSIC REPLAY (mission Phase 30/31/32/60/61).

Proves the Part 60 contract with executable evidence:

  restart reproduces the SAME strategy decisions, signals, signal candles,
  trigger, SL, and logical lifecycle as an uninterrupted run — with only
  explicitly nondeterministic wall-clock fields differing.

Procedure (all against one isolated workdir + canonical trading.db):
  A_pre   engine #1 feeds bars up to checkpoint Tchk (real _on_bar_closed path)
          -> trades/orders/fills/positions/signals persisted to trading.db;
          snapshot saved (system_state.json) -> simulate clean process stop.
  A_cont  the SAME engine #1 continues feeding bars after Tchk (uninterrupted).
  B       fresh engine #2 on the SAME db: set_persistence -> restore(saved)
          -> _warmup_from_rest (warmup ingest limited to <= Tchk, i.e. history
          available at the restart instant) -> continues feeding bars after Tchk.

Assertions:
  1. checkpoint continuity: after restore, engine #2 holds the same open
     positions / trades / fills as engine #1 had at Tchk (nothing lost, nothing
     duplicated).
  2. A_cont == B_cont under the deterministic projection (signals + trades +
     crossover context), proving restart did not change decisions.
  3. final canonical DB row sets after A_cont and after B_cont are identical
     (same normalized identity set; no duplicates, no lost entities).
"""
from __future__ import annotations

import json
import shutil
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import tools.replay_live_architecture as live

IST = timezone(timedelta(hours=5, minutes=30))
SIDS = live.SIDS

# Checkpoint: end of the first trading day (2026-09-02 23:30 IST).
CHK_ISO = "2026-09-02T23:30:00+05:30"
CHK = datetime.fromisoformat(CHK_ISO).timestamp()

NORMALIZE_SKIP = {"created_at", "updated_at", "event_id", "id", "timestamp",
                  "signal_id", "trade_id", "order_id", "fill_id", "position_id",
                  "pending_order_id", "broker_order_id", "entry_order_id",
                  "exit_order_id", "entry_fill_id", "exit_fill_id"}


def norm_row(row: dict) -> dict:
    return {k: v for k, v in sorted(row.items())
            if k not in NORMALIZE_SKIP and not isinstance(v, (dict, list))}


def table_rows(db_path: Path, table: str) -> list[dict]:
    import sqlite3
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in con.execute(f"SELECT * FROM {table}")]
    finally:
        con.close()


def strip_anchor_bars(bars_by_key):
    """Drop the synthetic seed-anchor candle (the one that closes at 09:00 IST
    and does not exist in real broker history) from every (instrument, tf)
    stream. Without this, the canonical feed lets the router pre-emptively
    synthesize a 09:00 HTF partial bar (dedup keeps it over the stored CSV
    row), so the warmup fetch (which reads the stored CSV rows) never sees the
    same history the live engine consumed — breaking the Dhan-API warmup
    contract (warmup must be fed the identical native history the live stream
    received). Normalizing BOTH sides to the real bars makes warmup == live."""
    out = {}
    for key, rows in bars_by_key.items():
        kept = []
        for b in rows:
            dt = datetime.fromtimestamp(b["end_ts"], tz=IST)
            if dt.hour == 9 and dt.minute == 0:
                continue
            kept.append(b)
        out[key] = kept
    return out


def offline_fetch(bars_by_key, *, chk):
    """Build a fetch_historical_candles(name, tf, from_date, to_date) callable
    that returns the REAL historical CSV rows from replay_input up to the
    checkpoint (rows as tuples (ts, o, h, l, c, v), ascending) — i.e. exactly
    the recorded history available at the restart instant, for every
    timeframe including native 15m/1h."""
    tf_map = {"5": "5m", "15": "15m", "60": "1h"}

    def fetch(name: str, tf_id: str, from_date: date, to_date: date):
        key = (name, tf_map.get(str(tf_id), tf_id))
        rows = []
        for b in bars_by_key.get(key, []):
            ts = b["end_ts"] - live.TF_MIN[b["timeframe"]] * 60  # bar open (IST)
            rows.append((ts, b["open"], b["high"], b["low"], b["close"],
                         int(b["volume"])))
        out_rows = []
        for ts, o, h, l, c, v in rows:
            if ts > chk:
                continue  # not yet available at the restart instant
            d = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(IST).date()
            if from_date <= d <= to_date:
                out_rows.append((ts, o, h, l, c, v))
        out_rows.sort(key=lambda r: r[0])
        return out_rows

    return fetch


def boundary_diag(engine) -> dict[str, Any]:
    """Snapshot per-strategy state + indicator-stream state at a boundary
    (used to compare the engine-run A at Tchk vs restart-warmup B)."""
    out = {}
    for sid in SIDS:
        strat = engine.strategies.get(sid)
        if strat is None:
            continue
        out[sid] = {
            "state": strat.state.value if getattr(strat, "state", None) else None,
            "position_side": strat.position_side,
            "current_trade_id": strat.current_trade_id,
            "bars_processed": strat._bars_processed,
            "signals_count": len(getattr(strat, "_signals", [])),
            "pending_entry": bool(strat.pending_entry),
            "stop_price": strat.stop_price,
            "last_exit_reason": strat.last_exit_reason,
            "just_entered": strat.just_entered,
            "same_bar_stop": strat.same_bar_stop,
            "prev_fast_close": strat._prev_fast_close,
            "prev_htf_value": strat._prev_htf_value,
            "prev_mid_value": strat._prev_mid_value,
            "fast_count": strat.fast_indicator._count,
            "fast_value": strat.fast_indicator.value,
            "mid_count": strat.mid_htf_state.bar_count(),
            "mid_value": strat.mid_indicator.value,
            "slow_count": strat.slow_htf_state.bar_count(),
            "slow_value": strat.slow_indicator.value,
        }
    return out


def collect_decisions(out: dict[str, Any]) -> dict[str, list[dict]]:
    return {
        "signals": [norm_row(s) for s in out["signals"]],
        "trades": [norm_row(t) for t in out["trades"]],
        "orders": [norm_row(o) for o in out["orders"]],
        "fills": [norm_row(f) for f in out["fills"]],
        "positions": [norm_row(p) for p in out["positions"]],
    }


def main() -> int:
    import tempfile
    workdir_root = Path(tempfile.gettempdir()) / "opencode" / "restart_replay_work"
    workdir = live.fresh_workdir(workdir_root / "engine")
    csv_root = ROOT / "replay_input"
    out_dir = ROOT / "replay_output" / "live_replay" / "restart"
    out_dir.mkdir(parents=True, exist_ok=True)

    bars = live.load_candles(csv_root)
    bars = strip_anchor_bars(bars)
    stream = live.chronological(bars)
    cutoff = next((i for i, b in enumerate(stream) if b["end_ts"] > CHK),
                  len(stream))
    pre, post = stream[:cutoff], stream[cutoff:]
    print(f"[restart] total={len(stream)} pre-chk={len(pre)} post-chk={len(post)}")

    result: dict[str, Any] = {"checkpoint_iso": CHK_ISO}

    # ── Phase A: interrupted engine ─────────────────────────────────────────
    engA, persA, _ = live.build_engine(workdir)
    outA: dict[str, Any] = {}
    try:
        live.run_replay(engA, persA, pre, outA)
        outA.update(live.collect(engA, persA, workdir))
        # checkpoint snapshot persisted (clean stop simulation)
        engA_boundary = boundary_diag(engA)
        result["boundary_A_at_chk"] = engA_boundary
        persA.save_state(engA.snapshot())
        chk_pos = [p.position_id for p in engA.position_manager.open_positions]
        chk_trades = [t.get("trade_id") for t in outA["trades"]]
        result["checkpoint"] = {"open_positions": sorted(chk_pos),
                                "trades": sorted(chk_trades),
                                "signals": outA["signals"]}
        db_pre = live.collect(engA, persA, workdir)
        result["checkpoint_db_rows"] = {t: len(db_pre.get(t, [])) for t in
                                        ("trades", "orders", "fills", "positions")}
        print(f"[A] checkpoint open_pos={len(chk_pos)} trades={len(chk_trades)} "
              f"db={result['checkpoint_db_rows']}")

        # uninterrupted continuation
        live.run_replay(engA, persA, post, outA)
        outA.update(live.collect(engA, persA, workdir))
        dbA_rows = {t: table_rows(Path(persA.db_path), t)
                    for t in ("trades", "orders", "fills", "positions",
                              "signals")}
        result["A_cont"] = collect_decisions(outA)
        result["A_final_db_counts"] = {t: len(rows) for t, rows in dbA_rows.items()}
        print(f"[A] uninterrupted final signals={len(outA['signals'])} "
              f"trades={len(outA['trades'])} db={result['A_final_db_counts']}")
    finally:
        try:
            engA.stop()
        except Exception:
            pass
        try:
            persA.close()
        except Exception:
            pass

    # ── Phase B: fresh process, same canonical DB ───────────────────────────
    engB, persB, _ = live.build_engine(workdir)  # same trading.db + state file
    outB: dict[str, Any] = {}
    try:
        saved = persB.load_state()
        assert saved is not None, "checkpoint state missing"
        engB.restore(saved)
        engB.data_adapter.fetch_historical_candles = offline_fetch(
            bars, chk=CHK)
        engB._warmup_from_rest()
        engB_boundary = boundary_diag(engB)
        result["boundary_B_after_warmup"] = engB_boundary
        result["boundary_diff"] = {
            sid: {k: {"A": engA_boundary.get(sid, {}).get(k),
                      "B": v.get(k)}
                  for k in set(engA_boundary.get(sid, {})) | set(v)}
            for sid, v in engB_boundary.items()
            if engA_boundary.get(sid) != v
        }
        fast_counts = {sid: engB.strategies.get(sid).fast_indicator._count
                       for sid in SIDS}
        result["B_warmup"] = {"fast_indicator_counts": fast_counts}
        rec_pos = sorted(p.position_id
                         for p in engB.position_manager.open_positions)
        restore_ok = (rec_pos == sorted(chk_pos) and
                      len(persB.load_state() or {}) > 0)
        result["checkpoint_continuity"] = {
            "restored_positions": rec_pos,
            "expected_positions": sorted(chk_pos),
            "ok": restore_ok,
        }
        print(f"[B] restored positions={len(rec_pos)} "
              f"(expected {len(chk_pos)}) warmup={fast_counts}")

        live.run_replay(engB, persB, post, outB)
        outB.update(live.collect(engB, persB, workdir))
        dbB_rows = {t: table_rows(Path(persB.db_path), t)
                    for t in ("trades", "orders", "fills", "positions",
                              "signals")}
        result["B_cont"] = collect_decisions(outB)
        result["B_final_db_counts"] = {t: len(rows) for t, rows in dbB_rows.items()}
        print(f"[B] continued final signals={len(outB['signals'])} "
              f"trades={len(outB['trades'])} db={result['B_final_db_counts']}")
    finally:
        try:
            engB.stop()
        except Exception:
            pass
        try:
            persB.close()
        except Exception:
            pass

    # ── Compare ─────────────────────────────────────────────────────────────
    def compare(key: str) -> dict:
        a = [json.dumps(r, sort_keys=True, default=str) for r in
             result["A_cont"][key]]
        b = [json.dumps(r, sort_keys=True, default=str) for r in
             result["B_cont"][key]]
        return {"A": len(a), "B": len(b), "identical": sorted(a) == sorted(b),
                "ct": sum(1 for x in a if x in b)}

    result["comparison"] = {k: compare(k) for k in
                            ("signals", "trades", "orders", "fills", "positions")}
    result["db_equality"] = {
        t: sorted([json.dumps(norm_row(r), sort_keys=True, default=str)
                   for r in dbA_rows[t]]) ==
           sorted([json.dumps(norm_row(r), sort_keys=True, default=str)
                   for r in dbB_rows[t]])
        for t in ("trades", "orders", "fills", "positions", "signals")
    }
    # Extra trades in B not present in A (normalized): side + strategy + reason
    a_norm = {json.dumps(r, sort_keys=True, default=str)
              for r in result["A_cont"]["trades"]}
    b_norm = {json.dumps(r, sort_keys=True, default=str)
              for r in result["B_cont"]["trades"]}
    b_extra = [r for r in result["B_cont"]["trades"]
               if json.dumps(norm_row(r), sort_keys=True, default=str) not in a_norm]
    a_extra = [r for r in result["A_cont"]["trades"]
               if json.dumps(norm_row(r), sort_keys=True, default=str) not in b_norm]
    from collections import Counter
    result["extra_trades_B_vs_A"] = list(dict(Counter(
        (r.get("strategy_id"), r.get("status"), r.get("exit_reason"))
        for r in b_extra).items()))
    result["extra_trades_A_vs_B"] = list(dict(Counter(
        (r.get("strategy_id"), r.get("status"), r.get("exit_reason"))
        for r in a_extra).items()))
    passed = (result["checkpoint_continuity"]["ok"] and
              all(v["identical"] for k, v in result["comparison"].items()) and
              all(result["db_equality"].values()))
    result["all_identical"] = passed

    (out_dir / "restart_replay.json").write_text(
        json.dumps(result, indent=1, default=str), encoding="utf-8")
    print(json.dumps({
        "checkpoint_continuity": result["checkpoint_continuity"],
        "comparison": result["comparison"],
        "db_equality": result["db_equality"],
        "ALL_IDENTICAL": passed,
    }, indent=1))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())