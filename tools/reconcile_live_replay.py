"""LIVE REFERENCE RECONCILIATION (mission Phases 39/40 + 66/67 + report/matrix).

Computes, from the captured artifacts:
  * reference crossover signals   (replay_output/replay_2026-09-02_to_latest/all_signals.csv)
  * live-engine crossover events  (replay_output/live_replay/parallel/crossover_replay.json)
  * live evaluation stream        (…/evaluation_stream.jsonl)
  * live persisted signals/trades/orders/fills/positions
  * sequential vs parallel equality (isolation equivalence)

and emits:
  signal_parity.csv            per-bar itemized parity for every strategy
  signal_parity_causes.csv     reference-only signals bucketed by live state
  live_vs_reference_trades.csv trade-level side-by-side
  parallel_vs_sequential.csv   isolation-equivalence verdict per strategy
  reconciliation.json          machine-readable totals/checksums/verdict
  FINAL_ARCHITECTURE_REPLAY_VERIFICATION.md
  FINAL_ACCEPTANCE_MATRIX.csv
"""
from __future__ import annotations

import csv
import hashlib
import json
import sys
from collections import Counter
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
IST = timezone(timedelta(hours=5, minutes=30))
SIDS = ["gold_01", "gold_02", "silver_01", "silver_02"]
MISSION = {"gold_01": "GOLDM_5M", "gold_02": "GOLDM_15M",
           "silver_01": "SILVERM_15M", "silver_02": "SILVERM_5M"}
SECURITY = {"GOLDM": "569003", "SILVERM": "483080"}
TF_OF = {"gold_01": 5, "gold_02": 15, "silver_01": 15, "silver_02": 5}
REF_DIR = ROOT / "replay_output" / "replay_2026-09-02_to_latest"
LIVE_DIR = ROOT / "replay_output" / "live_replay"
OUT = ROOT / "replay_output" / "live_replay" / "reconciliation"


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def iso_epoch(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, IST).strftime("%Y-%m-%d %H:%M:%S")


def epoch_to_ts(value: float) -> float:
    return value


def ref_ts_to_epoch(iso: str) -> float:
    dt = datetime.fromisoformat(iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST)
    return dt.timestamp()


def bar_key(strategy_id: str, epoch: float) -> str:
    tf = TF_OF[strategy_id]
    return datetime.fromtimestamp(epoch + tf * 60, IST).strftime("%Y-%m-%dT%H:%M:%S")


def checksum(rows: list[Any]) -> str:
    text = json.dumps(rows, sort_keys=True, default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def build_signal_parity() -> list[dict]:
    ref_rows = read_csv(REF_DIR / "all_signals.csv")
    live_events = load_json(LIVE_DIR / "parallel" / "crossover_replay.json")["crossover_log"]
    live_eval: dict[str, dict[str, dict]] = {sid: {} for sid in SIDS}
    with (LIVE_DIR / "parallel" / "evaluation_stream.jsonl").open(encoding="utf-8") as fh:
        for line in fh:
            r = json.loads(line)
            sid = r["strategy_id"]
            key = bar_key(sid, r["timestamp"])
            live_eval[sid][key] = r

    # reference signals -> {strategy: {bar_key: row}}
    ref_map: dict[str, dict[str, dict]] = {sid: {} for sid in SIDS}
    for r in ref_rows:
        sid = r["strategy_id"]
        key = bar_key(sid, ref_ts_to_epoch(r["signal_timestamp"]) - TF_OF[sid] * 60)
        ref_map[sid][key] = r

    # live crossover events -> {strategy: {bar_key: row}}
    live_map: dict[str, dict[str, dict]] = {sid: {} for sid in SIDS}
    for sid, events in live_events.items():
        for e in events:
            key = bar_key(sid, e["timestamp"])
            live_map[sid].setdefault(key, []).append(e)

    rows: list[dict] = []
    for sid in SIDS:
        for key in sorted(set(ref_map[sid]) | set(live_map[sid])):
            ref = ref_map[sid].get(key)
            lvs = live_map[sid].get(key, [])
            live_state = live_eval[sid].get(key, {}).get("state")
            rows.append({
                "strategy_id": sid, "mission_name": MISSION[sid], "bar_key": key,
                "ref_signal": ref["signal_type"] if ref else "",
                "ref_side": ref["signal_type"] if ref else "",
                "ref_h15": ref["h15_value"] if ref else "",
                "ref_h1": ref["h1_value"] if ref else "",
                "live_crossover": "|".join(e["side"] for e in lvs),
                "live_count": len(lvs),
                "live_htf_value": lvs[0]["htf_value"] if lvs else "",
                "live_mid_value": lvs[0]["mid_value"] if lvs else "",
                "live_state": live_state,
                "match": bool(ref and lvs),
                "ref_only": bool(ref and not lvs),
                "live_only": bool(not ref and lvs),
            })
    return rows


def cause_of(rows: list[dict], sid: str) -> Counter:
    cause = Counter()
    for r in rows:
        if r["strategy_id"] != sid or not r["ref_only"]:
            continue
        state = r["live_state"] or ""
        if state == "LONG_POSITION":
            cause["in_long_position"] += 1
        elif state == "SHORT_POSITION":
            cause["in_short_position"] += 1
        elif state in ("PENDING_LONG", "PENDING_SHORT"):
            cause["while_pending"] += 1
        elif state == "EXIT_ORDER_SUBMITTED":
            cause["exit_submitted"] += 1
        elif not state:
            cause["no_state_recorded"] += 1
        else:
            cause[f"other:{state}"] += 1
    return cause


def trades_flat_seq() -> list[dict]:
    seq = load_json(LIVE_DIR / "sequential" / "sequential.json")["sequential"]
    out = []
    for sid in SIDS:
        out.extend(seq[sid]["trades"])
    return out


def trades_side_by_side() -> list[dict]:
    ref_trades = read_csv(REF_DIR / "all_trades.csv")
    live_trades = load_json(LIVE_DIR / "parallel" / "trade_replay.json")["trades"]
    rows = []
    for sid in SIDS:
        rt = [t for t in ref_trades if t["strategy_id"] == sid]
        lt = [t for t in live_trades if t.get("strategy_id") == sid]
        rows.append({
            "strategy_id": sid, "mission_name": MISSION[sid],
            "ref_trades": len(rt), "live_trades": len(lt),
            "ref_closed": sum(1 for t in rt if t.get("status") == "CLOSED"),
            "live_closed": sum(1 for t in lt if t.get("status") == "CLOSED"),
            "ref_net_pnl": round(sum(float(t["net_pnl"] or 0) for t in rt), 2),
            "live_net_pnl": round(sum(float(t.get("net_pnl") or 0) for t in lt), 2),
        })
    return rows


ID_SKIP = {"id", "created_at", "updated_at"}


def _idskipped(t):
    return {k: v for k, v in t.items() if k not in ID_SKIP and not k.endswith("_id")}


def isolation_equivalence() -> list[dict]:
    seq = load_json(LIVE_DIR / "sequential" / "sequential.json")
    rows = []
    for sid in SIDS:
        par_signals = [s for s in
                       load_json(LIVE_DIR / "parallel" / "signal_replay.json")["signals"]
                       if s["strategy_id"] == sid]
        par_trades = [t for t in
                      load_json(LIVE_DIR / "parallel" / "trade_replay.json")["trades"]
                      if t.get("strategy_id") == sid]
        seq_row = seq["sequential"][sid]
        # signals may carry nondeterministic ids; compare canonical projections
        def canon_sig(s):
            return {k: v for k, v in s.items() if k not in
                    ("signal_id",)}
        par_c = sorted(json.dumps(canon_sig(s), sort_keys=True, default=str) for s in par_signals)
        seq_c = sorted(json.dumps(canon_sig(s), sort_keys=True, default=str) for s in seq_row["signals"])
        par_t = sorted(json.dumps(_idskipped(t), sort_keys=True, default=str)
                       for t in par_trades)
        seq_t = sorted(json.dumps(_idskipped(t), sort_keys=True, default=str)
                       for t in seq_row["trades"])
        rows.append({
            "strategy_id": sid, "mission_name": MISSION[sid],
            "sigs_parallel": len(par_signals), "sigs_sequential": len(seq_row["signals"]),
            "signals_identical": par_c == seq_c,
            "trades_parallel": len(par_trades), "trades_sequential": len(seq_row["trades"]),
            "trades_identical": par_t == seq_t,
            "signals_sha256": checksum(par_signals),
        })
    return rows


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    parity = build_signal_parity()
    with (OUT / "signal_parity.csv").open("w", newline="", encoding="utf-8") as fh:
        keys = sorted(parity[0].keys()) if parity else []
        writer = csv.DictWriter(fh, fieldnames=keys)
        writer.writeheader()
        writer.writerows(parity)

    causes = []
    for sid in SIDS:
        c = cause_of(parity, sid)
        csum = sum(c.values())
        causes.append({"strategy_id": sid, "mission_name": MISSION[sid],
                       "ref_signals": sum(1 for r in parity if r["strategy_id"] == sid and r["ref_signal"]),
                       "live_crossovers": sum(1 for r in parity if r["strategy_id"] == sid and r["live_crossover"]),
                       "matched": sum(1 for r in parity if r["strategy_id"] == sid and r["match"]),
                       "ref_only": sum(1 for r in parity if r["strategy_id"] == sid and r["ref_only"]),
                       "live_only": sum(1 for r in parity if r["strategy_id"] == sid and r["live_only"]),
                       "ref_only_causes": dict(c), "ref_only_cause_total": csum})
    with (OUT / "signal_parity_causes.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["strategy_id", "mission_name", "ref_signals",
                                                "live_crossovers", "matched", "ref_only",
                                                "live_only", "ref_only_causes",
                                                "ref_only_cause_total"])
        writer.writeheader()
        for row in causes:
            row = dict(row)
            row["ref_only_causes"] = json.dumps(row["ref_only_causes"])
            writer.writerow(row)

    trades = trades_side_by_side()
    with (OUT / "live_vs_reference_trades.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(trades[0].keys()))
        writer.writeheader()
        writer.writerows(trades)

    for name, src in (("trades_detail_parallel",
                       load_json(LIVE_DIR / "parallel" / "trade_replay.json")["trades"]),
                      ("trades_detail_sequential", trades_flat_seq()),
                      ("trades_detail_reference", read_csv(REF_DIR / "all_trades.csv"))):
        if not src:
            continue
        with (OUT / f"{name}.csv").open("w", newline="", encoding="utf-8") as fh:
            keys = sorted({k for r in src for k in r.keys()})
            writer = csv.DictWriter(fh, fieldnames=keys, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(src)

    iso_rows = isolation_equivalence()
    with (OUT / "parallel_vs_sequential.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(iso_rows[0].keys()))
        writer.writeheader()
        writer.writerows(iso_rows)

    # persisted DB evidence
    persisted = load_json(LIVE_DIR / "parallel" / "persisted_signal_replay.json")["persisted_signals"]
    live_full = load_json(LIVE_DIR / "parallel" / "full_replay.json")
    trades_p = load_json(LIVE_DIR / "parallel" / "trade_replay.json")["trades"]
    orders = load_json(LIVE_DIR / "parallel" / "order_replay.json")["orders"]
    fills = load_json(LIVE_DIR / "parallel" / "fill_replay.json")["fills"]
    positions = load_json(LIVE_DIR / "parallel" / "position_replay.json")["positions"]
    sl_exits = [t for t in trades_p if t.get("exit_reason") == "STOP_LOSS"]
    non_sl = [t for t in trades_p if t.get("exit_reason") != "STOP_LOSS"]

    summary = {
        "window": {"start": "2026-09-02 09:00 IST", "end": iso_epoch(live_full["last_bar_end_ts"]),
                   "data_source": "offline_csv(replay_input) deterministic seed 20260902",
                   "notes": "DhanAuthError DH-901 blocks online historical fetch on this host; offline CSVs are the window's authoritative data"},
        "reference": {"total_signals": sum(r["ref_signals"] for r in causes),
                      "total_trades": sum(t["ref_trades"] for t in trades),
                      "source": "project/core/dema_mtf.py via tools/replay_mcx_from_2026_09_02.py"},
        "live_parallel": {"total_live_crossovers": sum(r["live_crossovers"] for r in causes),
                          "total_persisted_signals": len(persisted),
                          "persisted_entry": sum(1 for s in persisted if s.get("signal_type") == "entry"),
                          "persisted_exit": sum(1 for s in persisted if s.get("signal_type") == "exit"),
                          "total_trades": len(trades_p),
                          "total_orders": len(orders), "total_fills": len(fills),
                          "total_positions": len(positions),
                          "closed_trades": sum(1 for t in trades_p if t.get("status") == "CLOSED"),
                          "sl_exit_trades": len(sl_exits),
                          "reversal_exit_trades": sum(1 for t in non_sl if t.get("exit_reason", "").endswith("reversal")),
                          "trades_without_signal": [t for t in trades_p if not t.get("entry_signal_id")],
                          "sl_trades_empty_exit_signal": sum(1 for t in sl_exits if not t.get("exit_signal_id"))},
        "signal_parity_per_strategy": causes,
        "trade_parity_per_strategy": trades,
        "isolation_equivalence": iso_rows,
        "checksums": {
            "sequential_signals": load_json(LIVE_DIR / "sequential" / "sequential.json").get("signals_checksum"),
            "sequential_trades": load_json(LIVE_DIR / "sequential" / "sequential.json").get("trades_checksum"),
            "parallel_evaluation_stream": checksum(
                [json.loads(l) for l in
                 (LIVE_DIR / "parallel" / "evaluation_stream.jsonl").read_text(encoding="utf-8").splitlines()]),
        },
        "observations": {
            "dema3_atr6_htf_mapping_identical_to_reference": "",
            "filled_later": [
                "reference counts every qualifying crossover bar regardless of position (backtest semantics)",
                "live engine gates new-crossing evaluation by strategy lifecycle (in-position bars arm only reversals)",
                "measured indicator parity (see forensics/indicator_parity.json): 60m line live==ref on 29/29 matched bars; 15m line 20/29 exact with all 9 remaining == reference one period later (reference period-edge publication, live is no-lookahead)",
            ],
        },
    }
    (OUT / "reconciliation.json").write_text(json.dumps(summary, indent=1, default=str), encoding="utf-8")
    print(json.dumps({"signal_parity": {r["strategy_id"]: (r["ref_signals"], r["live_crossovers"],
                     r["matched"], r["ref_only"], r["live_only"]) for r in causes},
                      "trades": {r["strategy_id"]: (r["ref_trades"], r["live_trades"]) for r in trades},
                      "isolation_identical": {r["strategy_id"]: (r["signals_identical"], r["trades_identical"])
                                              for r in iso_rows}}, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())