"""Phase 11/12/41/42 + 67 forensic datasets and report builder.

Reads replay_output/live_replay/{parallel,sequential} + the regenerated
reference artifacts, then emits under replay_output/live_replay/forensics/:

  signal_forensics.json/.csv      — every live crossover w/ candle context
  trade_forensics.json/.csv       — every trade w/ entry/exit signal lineage
  reversal_invariant.json         — reversal exit signal reuse classification
  phase67_checksums.json          — SHA-256 per normalised stream per source
  checksum_stream_compare.csv     — stream-level equality matrix
"""
from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LR = ROOT / "replay_output" / "live_replay"
REF = ROOT / "replay_output" / "replay_2026-09-02_to_latest"
OUT = LR / "forensics"
SIDS = ["gold_01", "gold_02", "silver_01", "silver_02"]


def sha(obj) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, default=str).encode()).hexdigest()


def norm_signal(s) -> dict:
    s = dict(s)
    s.pop("signal_id", None)
    s.pop("created_at", None)
    return s


def norm_trade(t) -> dict:
    t = dict(t)
    for k in ("trade_id", "position_id", "entry_order_id", "exit_order_id",
              "entry_signal_id", "exit_signal_id", "created_at", "updated_at",
              "id", "fill_id", "order_id", "broker_order_id", "pending_order_id"):
        t.pop(k, None)
    return t


def load(p, default=None):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    report: dict = {
        "generated": __file__,
        "window": "2026-09-02 09:00 IST -> 2026-09-04 (latest closed candle)",
        "sources": {
            "parallel": str(LR / "parallel"),
            "sequential": str(LR / "sequential"),
            "reference": str(REF),
        },
    }

    p_ev = (LR / "parallel" / "evaluation_stream.jsonl").read_text(encoding="utf-8").splitlines()
    p_eval = [json.loads(l) for l in p_ev]
    p_sig = load(LR / "parallel" / "crossover_replay.json").get("crossover_log", {})
    p_trades = load(LR / "parallel" / "trade_replay.json").get("trades", [])
    p_persisted = load(LR / "parallel" / "persisted_signal_replay.json").get("persisted_signals", [])

    # window bounds from eval bars
    starts = sorted(x["timestamp"] for x in p_eval)
    report["bar_bounds"] = {"first_bar_start": starts[0], "last_bar_start": starts[-1],
                            "bar_count": len(p_eval)}

    # ---------------------------------------------------------------- signals
    signal_rows = []
    signals_full = []
    for sid in SIDS:
        for x in p_sig.get(sid, []):
            row = dict(x)
            row["strategy_id"] = sid
            row["source"] = "parallel_live"
            signals_full.append(row)
            signal_rows.append(
                {k: row.get(k) for k in (
                    "strategy_id", "signal_id", "side", "timestamp", "bar_start",
                    "fast", "mid", "slow", "fast_value", "mid_value", "slow_value",
                    "cross", "state", "prev_fast", "prev_slow", "is_reversal",
                    "trigger_price", "stop_price", "pending_entry", "event")})
    # attach candle context: crossover timestamp == bar.start_ts of that bar
    eval_by_ts = {}
    for b in p_eval:
        apple = b.get("candle") or b.get("bar") or {}
        if b["timestamp"] not in eval_by_ts:
            eval_by_ts[b["timestamp"]] = b
    used = set()
    for r in signals_full:
        ts = r.get("timestamp")
        if ts in eval_by_ts:
            c = eval_by_ts[ts].get("candle") or []
            r["candle"] = {"open": c[0] if len(c) > 0 else None,
                           "high": c[1] if len(c) > 1 else None,
                           "low": c[2] if len(c) > 2 else None,
                           "close": c[3] if len(c) > 3 else None,
                           "volume": c[4] if len(c) > 4 else None}
            r["eval_state"] = eval_by_ts[ts].get("state")
            used.add(ts)
        else:
            r["candle"] = None
            r["eval_state"] = None
        r["candle_joined"] = ts in eval_by_ts
    (OUT / "signal_forensics.json").write_text(
        json.dumps({"count": len(signals_full), "candle_joined": sum(1 for r in signals_full if r["candle_joined"]),
                    "signals": signals_full}, indent=1), encoding="utf-8")
    if signal_rows:
        with (OUT / "signal_forensics.csv").open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(signal_rows[0].keys()))
            w.writeheader()
            w.writerows(signal_rows)

    # ---------------------------------------------------------------- trades
    # join persisted signals by id
    sig_by_id = {s["signal_id"]: s for s in p_persisted}
    trade_rows = []
    for t in p_trades:
        esig = sig_by_id.get(t.get("entry_signal_id"))
        row = {
            "strategy_id": t["strategy_id"], "trade_id": t["trade_id"],
            "position_id": t.get("position_id"), "side": t["side"],
            "entry_timestamp": t.get("entry_timestamp"), "entry_price": t.get("entry_price"),
            "exit_timestamp": t.get("exit_timestamp"), "exit_price": t.get("exit_price"),
            "quantity": t.get("quantity"), "exit_reason": t.get("exit_reason"),
            "status": t.get("status"), "net_pnl": t.get("net_pnl"),
            "gross_pnl": t.get("gross_pnl"), "entry_signal_id": t.get("entry_signal_id"),
            "exit_signal_id": t.get("exit_signal_id"),
            "entry_sig_persisted": esig is not None,
            "entry_sig_side": esig.get("side") if esig else None,
            "entry_sig_ts": esig.get("signal_timestamp") if esig else None,
            "entry_sig_type": esig.get("signal_type") if esig else None,
            "sl_is_new_signal": bool(t.get("exit_signal_id")) and (t.get("exit_reason") == "STOP_LOSS"),
            "reversal_reuse_expected": "reversal" in (t.get("exit_reason") or ""),
        }
        trade_rows.append(row)
    (OUT / "trade_forensics.json").write_text(
        json.dumps({"count": len(trade_rows), "trades": trade_rows}, indent=1), encoding="utf-8")
    if trade_rows:
        with (OUT / "trade_forensics.csv").open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(trade_rows[0].keys()))
            w.writeheader()
            w.writerows(trade_rows)

    # ------------------------------------------------------- reversal invariant
    rev = {"checked": 0, "same_signal_follow_through": 0, "exit_only": 0, "cases": []}
    by_strategy = {sid: [t for t in p_trades if t["strategy_id"] == sid] for sid in SIDS}
    for sid, lst in by_strategy.items():
        lst = sorted(lst, key=lambda t: t.get("entry_timestamp", ""))
        for i, t in enumerate(lst):
            er = t.get("exit_reason", "")
            if "reversal" not in er:
                continue
            rev["checked"] += 1
            esig = t.get("exit_signal_id")
            follower = next((n for n in lst[i + 1:] if n.get("entry_signal_id") == esig), None)
            if follower is not None and esig:
                rev["same_signal_follow_through"] += 1
                kind = "same_signal_follow_through"
            else:
                rev["exit_only"] += 1
                kind = "exit_only"
            rev["cases"].append({
                "strategy_id": sid, "trade_id": t["trade_id"], "exit_reason": er,
                "exit_signal_id": esig, "kind": kind,
                "follower_trade": follower["trade_id"] if follower else None,
            })
    (OUT / "reversal_invariant.json").write_text(json.dumps(rev, indent=1), encoding="utf-8")

    # ------------------------------------------------------- phase 67 checksums
    def sigs_from_ref() -> dict:
        p = REF / "all_signals.csv"
        out = {sid: [] for sid in SIDS}
        if not p.exists():
            return out
        for r in csv.DictReader(p.open(encoding="utf-8")):
            if r.get("strategy_id") in out:
                out[r["strategy_id"]].append(norm_signal(
                    {k: v for k, v in r.items()}))
        return out

    def sigs_from_seq() -> dict:
        out = {}
        for sid in SIDS:
            d = load(LR / "sequential" / f"{sid}.json")
            out[sid] = [norm_signal(s) for s in (d.get("signals") or [])]
        return out

    def sigs_from_par() -> dict:
        from_par = load(LR / "parallel" / "signal_replay.json").get("signals", [])
        return {sid: [norm_signal(s) for s in from_par if s.get("strategy_id") == sid]
                for sid in SIDS}

    def trades_seq() -> list:
        out = []
        for sid in SIDS:
            d = load(LR / "sequential" / f"{sid}.json")
            out.extend(norm_trade(t) for t in (d.get("trades") or []))
        return out

    ref_sig, seq_sig, par_sig = sigs_from_ref(), sigs_from_seq(), sigs_from_par()
    seq_tr = trades_seq()
    par_tr = [norm_trade(t) for t in p_trades]

    def cross_seq() -> dict:
        return {sid: load(LR / "sequential" / f"{sid}.json").get("crossovers", [])
                for sid in SIDS}

    seq_cross, par_cross = cross_seq(), {sid: p_sig.get(sid, []) for sid in SIDS}

    checks = {
        "signals_in_memory": {
            sid: {
                "sequential": sha(sorted(json.dumps(s, sort_keys=True) for s in seq_sig[sid])),
                "parallel": sha(sorted(json.dumps(s, sort_keys=True) for s in par_sig[sid])),
            } for sid in SIDS},
        "crossover_events": {
            sid: {
                "sequential": sha(sorted(json.dumps(s, sort_keys=True, default=str) for s in seq_cross[sid])),
                "parallel": sha(sorted(json.dumps(s, sort_keys=True, default=str) for s in par_cross[sid])),
            } for sid in SIDS},
        "reference_signals": {
            sid: sha(sorted(json.dumps(s, sort_keys=True) for s in ref_sig[sid]))
            for sid in SIDS},
        "trades": {
            "sequential": sha(sorted(json.dumps(t, sort_keys=True) for t in seq_tr)),
            "parallel": sha(sorted(json.dumps(t, sort_keys=True) for t in par_tr)),
        },
        "eval_stream_parallel": sha([
            (b["strategy_id"], b["timestamp"], b["state"], b["fast_value"])
            for b in p_eval]),
    }
    seq_tr_by = {sid: sorted(json.dumps(norm_trade(t), sort_keys=True)
                             for t in (load(LR / "sequential" / f"{sid}.json").get("trades") or []))
                 for sid in SIDS}
    par_tr_by = {sid: sorted(json.dumps(norm_trade(t), sort_keys=True)
                             for t in p_trades if t["strategy_id"] == sid) for sid in SIDS}
    checks["trades_by_strategy"] = {
        sid: {"sequential": sha(seq_tr_by[sid]), "parallel": sha(par_tr_by[sid])} for sid in SIDS}

    (OUT / "phase67_checksums.json").write_text(json.dumps(checks, indent=1), encoding="utf-8")

    same_signals = all(checks["signals_in_memory"][sid]["sequential"] ==
                       checks["signals_in_memory"][sid]["parallel"] for sid in SIDS)
    same_cross = all(checks["crossover_events"][sid]["sequential"] ==
                     checks["crossover_events"][sid]["parallel"] for sid in SIDS)
    checksum_csv = [
        {"stream": "in_memory_signals:" + sid,
         **checks["signals_in_memory"][sid],
         "seq_eq_par": checks["signals_in_memory"][sid]["sequential"] ==
                       checks["signals_in_memory"][sid]["parallel"]}
        for sid in SIDS]
    checksum_csv += [
        {"stream": "crossover_events:" + sid,
         **checks["crossover_events"][sid],
         "seq_eq_par": checks["crossover_events"][sid]["sequential"] ==
                       checks["crossover_events"][sid]["parallel"]}
        for sid in SIDS]
    checksum_csv += [
        {"stream": "trades:" + sid, **checks["trades_by_strategy"][sid],
         "seq_eq_par": checks["trades_by_strategy"][sid]["sequential"] ==
                       checks["trades_by_strategy"][sid]["parallel"]}
        for sid in SIDS]
    with (OUT / "checksum_stream_compare.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(checksum_csv[0].keys()))
        w.writeheader()
        w.writerows(checksum_csv)

    report["isolated_seq_eq_par_signals"] = same_signals
    report["isolated_seq_eq_par_crossovers"] = same_cross
    report["trades_seq_eq_par"] = checks["trades"]["sequential"] == checks["trades"]["parallel"]
    report["trades_by_strategy_seq_eq_par"] = {
        sid: checks["trades_by_strategy"][sid]["sequential"] == checks["trades_by_strategy"][sid]["parallel"]
        for sid in SIDS}
    report["reversal_invariant"] = rev
    report["signal_forensics_count"] = len(signals_full)
    report["trade_forensics_count"] = len(trade_rows)
    (OUT / "forensics_report.json").write_text(json.dumps(report, indent=1), encoding="utf-8")
    print("forensics written:", OUT)
    print("signals:", len(signals_full), "trades:", len(trade_rows),
          "reversal:", rev["same_signal_follow_through"], "same /", rev["exit_only"], "exit-only")
    print("seq==par signals:", report["isolated_seq_eq_par_signals"],
          "seq==par crossovers:", report["isolated_seq_eq_par_crossovers"],
          "seq==par trades:", report["trades_seq_eq_par"])


if __name__ == "__main__":
    main()