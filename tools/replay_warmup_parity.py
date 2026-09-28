"""WARMUP-VS-ROUTER PARITY FORENSIC (mission Part 7 / 31 / 34).

Compares, for every shared indicator stream (security_id, timeframe), the
bar-by-bar state produced by:

  A (router path)  — bars fed through the real engine (_on_bar_closed ->
                    NativeCandleRouter -> SharedNativeIndicatorEngine) up to a
                    checkpoint Tchk, i.e. exactly what a running process has.
  C (warmup path)  — a fresh process that restores nothing but sleeps the same
                    history through TradingEngine._warmup_from_rest (the restart
                    instant feed: everything up to Tchk).

This is the STATE the restart contract depends on: after a crash, warmup must
reconstruct the pre-crash stream state, else post-restart decisions diverge.

Output: replay_output/live_replay/restart/warmup_parity.json + printed summary.
Exit 0 iff every stream is bit-exact.
"""
from __future__ import annotations

import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import tools.replay_live_architecture as live

IST = timezone(timedelta(hours=5, minutes=30))
SIDS = live.SIDS
CHK_ISO = "2026-09-02T23:30:00+05:30"
CHK = datetime.fromisoformat(CHK_ISO).timestamp()
TF_MAP = {"5": "5m", "15": "15m", "60": "1h"}


def offline_fetch(bars_by_key, *, chk):
    def fetch(name: str, tf_id: str, from_date: date, to_date: date):
        key = (name, TF_MAP.get(str(tf_id), tf_id))
        rows = []
        for b in bars_by_key.get(key, []):
            ts = b["end_ts"] - live.TF_MIN[b["timeframe"]] * 60  # bar open
            rows.append((ts, b["open"], b["high"], b["low"], b["close"],
                         int(b["volume"])))
        out_rows = []
        for ts, o, h, l, c, v in rows:
            if ts > chk:
                continue
            d = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(IST).date()
            if from_date <= d <= to_date:
                out_rows.append((ts, o, h, l, c, v))
        out_rows.sort(key=lambda r: r[0])
        return out_rows

    return fetch


def capture_streams(engine) -> dict[str, list[dict]]:
    streams: dict[str, list[dict]] = {}
    keys = engine.indicator_engine.stats().get("stream_keys") or []
    for key in sorted(keys):
        sid_s, tf_s = key.split(":")
        stream = engine.indicator_engine.get(sid_s, tf_s)
        dedup = getattr(stream, "_dedup_count", None)
        streams[key] = {
            "end_times": list(stream._end_times),
            "values": list(stream._values),
            "dedup": dedup,
        }
    return streams


def compare(left: dict[str, dict],
            right: dict[str, dict]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for key in sorted(set(left) | set(right)):
        a = left.get(key, {"end_times": [], "values": []})
        b = right.get(key, {"end_times": [], "values": []})
        a_pairs = list(zip(a["end_times"], a["values"]))
        b_pairs = list(zip(b["end_times"], b["values"]))
        a_by_ts = {r[0]: r[1] for r in a_pairs}
        b_by_ts = {r[0]: r[1] for r in b_pairs}
        a_ts, b_ts = set(a_by_ts), set(b_by_ts)
        common = a_ts & b_ts
        diffs = sorted((ts for ts in common
                        if (a_by_ts[ts] is None) != (b_by_ts[ts] is None)
                        or (a_by_ts[ts] is not None
                            and b_by_ts[ts] is not None
                            and abs(a_by_ts[ts] - b_by_ts[ts]) > 1e-12)),
                       key=lambda ts: ts)
        out[key] = {
            "A_bars": len(a),
            "C_bars": len(b),
            "only_A": sorted(a_ts - b_ts),
            "only_C": sorted(b_ts - a_ts),
            "differing": len(diffs),
            "first_differences": [
                {"end_ts_iso": datetime.fromtimestamp(d, tz=IST).isoformat(),
                 "A": a_by_ts.get(d),
                 "C": b_by_ts.get(d)}
                for d in diffs[:5]
            ],
            "max_abs_diff": max(
                (abs((a_by_ts[ts] or 0) - (b_by_ts[ts] or 0)) for ts in common
                 if a_by_ts[ts] is not None and b_by_ts[ts] is not None),
                default=0.0),
            "identical": not diffs and not (a_ts ^ b_ts),
        }
    return out


def main() -> int:
    import tempfile
    from tools.replay_restart import strip_anchor_bars
    bars = strip_anchor_bars(live.load_candles(ROOT / "replay_input"))
    stream = live.chronological(bars)
    cutoff = next((i for i, b in enumerate(stream) if b["end_ts"] > CHK), len(stream))
    pre = stream[:cutoff]

    base = Path(tempfile.gettempdir()) / "opencode" / "warmup_parity_work"

    # A: router path (real engine, bars up to Tchk)
    engA, persA, _ = live.build_engine(live.fresh_workdir(base / "A"))
    outA: dict = {}
    try:
        live.run_replay(engA, persA, pre, outA)
        streams_A = capture_streams(engA)
        print(f"[A] streams: { {k: len(v['end_times']) for k, v in streams_A.items()} }")
        for key in sorted(streams_A):
            ev = streams_A[key]["end_times"]
            print(f"[A] {key}: first={datetime.fromtimestamp(ev[0], IST).isoformat()} "
                  f"last={datetime.fromtimestamp(ev[-1], IST).isoformat()} n={len(ev)}")
    finally:
        try:
            engA.stop()
        except Exception:
            pass
        try:
            persA.close()
        except Exception:
            pass

    # C: warmup path (fresh process, restart-instant feed)
    engC, persC, _ = live.build_engine(live.fresh_workdir(base / "C"))
    try:
        fetch = offline_fetch(bars, chk=CHK)
        rows = fetch("GOLDM", "5", date(2026, 8, 22), date(2026, 9, 4))
        print(f"[C] GOLDM/5 rows={len(rows)} first_ts="
              f"{datetime.fromtimestamp(rows[0][0], IST).isoformat()} last_ts="
              f"{datetime.fromtimestamp(rows[-1][0], IST).isoformat()}")
        engC.data_adapter.fetch_historical_candles = fetch
        engC._warmup_from_rest()
        streams_C = capture_streams(engC)
        print(f"[C] streams: { {k: len(v['end_times']) for k, v in streams_C.items()} }")
        for key in sorted(streams_C):
            ev = streams_C[key]["end_times"]
            print(f"[C] {key}: first={datetime.fromtimestamp(ev[0], IST).isoformat()} "
                  f"last={datetime.fromtimestamp(ev[-1], IST).isoformat()} "
                  f"n={len(ev)} dedup={streams_C[key]['dedup']}")
    finally:
        try:
            engC.stop()
        except Exception:
            pass
        try:
            persC.close()
        except Exception:
            pass

    result = compare(streams_A, streams_C)
    out_dir = ROOT / "replay_output" / "live_replay" / "restart"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "warmup_parity.json").write_text(
        json.dumps(result, indent=1, default=str), encoding="utf-8")

    all_ok = all(v["identical"] for v in result.values())
    print(json.dumps(result, indent=1, default=str))
    print(f"WARMUP PARITY: {'EXACT' if all_ok else 'DIVERGENT'}")
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())