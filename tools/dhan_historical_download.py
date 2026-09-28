"""Dhan REST historical market-data downloader (REAL data, immutable snapshot).

Per the historical-replay master directive:
  - Source: Dhan REST POST /charts/intraday (the SAME endpoint + client the
    production live system uses, via data.dhan.rest_client.DhanRESTClient).
  - Intervals: native "5", "15", "60" only - NEVER resampled.
  - Range: warmup range (config fetch_calendar_days before replay start) plus
    2026-09-02 09:00 IST -> latest closed candle.
  - Bounded, finite: 90-day max per Dhan call; we chunk by bounded windows
    and assert each chunk within budget.
  - Output: immutable, checksummed snapshot under replay_output/dhan_snapshot/
    - candles.json raw rows [[epoch_open, o, h, l, c, v], ...] per
      (instrument, interval) - identical shape to the REST client response.
    - SHA256 of every file in checksums.json, manifest in replay_manifest.json.
    - .IST.csv human-readable IST-labeled copies for audit (same data).
  - NEVER overwrites in place; failures leave prior snapshot intact.

Usage:
  python tools/dhan_historical_download.py            # default range
  python tools/dhan_historical_download.py --days 45  # from replay_start-days
"""
from __future__ import annotations

import argparse
import csv
import datetime
import hashlib
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))

INSTRUMENTS = {
    "GOLDM":   {"security_id": "569003", "exchange_segment": "MCX_COMM", "instrument": "FUTCOM"},
    "SILVERM": {"security_id": "483080", "exchange_segment": "MCX_COMM", "instrument": "FUTCOM"},
}
# Dhan chart interval id (native) -> label used by the engine.
INTERVAL_LABELS = {"5": "5m", "15": "15m", "60": "1h"}
SESSION_OPEN_MIN = 9 * 60          # 09:00 IST
SESSION_MINUTES = 870              # 09:00 -> 23:30
CHUNK_DAYS = 25                    # safely under Dhan's 90-day ceiling
REPLAY_START = datetime.datetime(2026, 9, 2, 9, 0, 0, tzinfo=IST)

OUT_DIR = ROOT / "replay_output" / "dhan_snapshot"
CHECKS = ROOT / "replay_output" / "checksums.json"
MANIFEST_PATH = ROOT / "replay_output" / "replay_manifest.json"


def _bucket_epochs(dt: datetime.datetime, interval_min: int) -> tuple[int, int]:
    """Return (bucket_start_epoch, bucket_end_epoch) for dt under the
    session-anchored grid used by the engine (09:00 IST anchor)."""
    day = datetime.datetime(dt.year, dt.month, dt.day, tzinfo=IST)
    day_start = day + datetime.timedelta(minutes=SESSION_OPEN_MIN)
    day_end = day_start + datetime.timedelta(minutes=SESSION_MINUTES)
    secs = max(0, int((dt - day_start).total_seconds()))
    start = int(day_start.timestamp()) + (secs // (interval_min * 60)) * interval_min * 60
    return start, start + interval_min * 60


def fetch_windows(client, security_id: str, exchange_segment: str,
                  instrument: str, interval: str, from_dt: datetime.datetime,
                  to_dt: datetime.datetime) -> list[list]:
    """Fetch [interval] candles between from_dt..to_dt in bounded chunks.

    Returns raw candle rows [[epoch_open, o, h, l, c, v], ...] sorted ascending.
    Every chunk is bounded; the total loop terminates at to_dt.
    """
    rows: list[list] = []
    cursor = from_dt
    while cursor < to_dt:
        chunk_end = min(cursor + datetime.timedelta(days=CHUNK_DAYS), to_dt)
        chunk = client.fetch_intraday(
            security_id, interval, cursor, chunk_end,
            exchange_segment, instrument,
        )
        if chunk:
            rows.extend(chunk)
        # Advance a full chunk regardless; the next chunk re-queries overlap-
        # free. Bounded by (to_dt - from_dt).days / CHUNK_DAYS iterations.
        cursor = chunk_end + datetime.timedelta(seconds=1)
    # Dedup (same epoch) and sort; preserve the API's sub-second grid.
    seen: set[float] = set()
    deduped: list[list] = []
    for r in rows:
        if r[0] not in seen:
            seen.add(r[0])
            deduped.append(r)
    deduped.sort(key=lambda r: r[0])
    return deduped


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def write_snapshot(rows_by_key: dict[tuple[str, str], list[list]],
                   from_dt: datetime.datetime, to_dt: datetime.datetime,
                   days: int, token_exp: str) -> tuple[Path, dict]:
    """Write immutable snapshot files + checksums + manifest. Returns (out_dir, checks)."""
    out = OUT_DIR
    out.mkdir(parents=True, exist_ok=True)
    checks: dict[str, str] = {}

    for (instrument, interval), rows in rows_by_key.items():
        label = INTERVAL_LABELS[interval]
        raw = out / f"{instrument}_{interval}.json"
        raw.write_text(json.dumps({"interval": interval,
                                   "instrument": instrument,
                                   "security_id": INSTRUMENTS[instrument]["security_id"],
                                   "candles": rows}, indent=1), encoding="utf-8")
        checks[str(raw.relative_to(out))] = sha256_file(raw)

        ist = out / f"{instrument}_{label}.IST.csv"
        row0 = rows[0][0] if rows else 0
        with ist.open("w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["datetime", "open", "high", "low", "close", "volume"])
            for r in rows:
                dt = datetime.datetime.fromtimestamp(r[0], IST)
                w.writerow([dt.strftime("%Y-%m-%dT%H:%M:%S%z"), r[1], r[2], r[3], r[4], int(r[5])])
        checks[str(ist.relative_to(out))] = sha256_file(ist)

    checks_path = CHECKS
    checks_path.write_text(json.dumps(dict(sorted(checks.items())), indent=2), encoding="utf-8")

    stats = {}
    for (instrument, interval), rows in rows_by_key.items():
        stats[f"{instrument}/{interval}"] = {
            "n_candles": len(rows),
            "first": None if not rows else datetime.datetime.fromtimestamp(rows[0][0], IST).isoformat(),
            "last": None if not rows else datetime.datetime.fromtimestamp(rows[-1][0], IST).isoformat(),
        }

    manifest = {
        "purpose": "finite deterministic historical replay (REAL Dhan REST data)",
        "source": "Dhan REST POST /charts/intraday (data/dhan/rest_client.py fetch_intraday)",
        "auth": {"client_id": os.environ.get("DHAN_CLIENT_ID", "UNSET"), "token_issue": token_exp},
        "intervals_native": {"5": "5m", "15": "15m", "60": "1h"},
        "resampling_used": False,
        "lookahead_used": False,
        "range": {
            "window_start_ist": from_dt.isoformat(),
            "window_end_ist": to_dt.isoformat(),
            "replay_start_ist": REPLAY_START.isoformat(),
            "fetch_back_days": days,
        },
        "instruments": INSTRUMENTS,
        "stats": stats,
        "checksums": checks,
        "files": {k: str((out / k).relative_to(ROOT)) for k in sorted(checks)},
    }
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return out, checks


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=30,
                    help="calendar days of history before REPLAY_START to fetch (warmup). Default 30 (>= config fetch_calendar_days 14 + margin).")
    ap.add_argument("--to", type=str, default=None,
                    help="optional explicit end 'YYYY-MM-DDTHH:MM:SS' (IST). Default: latest closed 5m candle.")
    args = ap.parse_args()

    import os
    import os
    from data.dhan.rest_client import DhanRESTClient
    token_file = ROOT / "data" / "dhan_token.json"
    client_id = os.environ.get("DHAN_CLIENT_ID") or "REPLACE_WITH_DHAN_CLIENT_ID"
    client = DhanRESTClient(token_file=str(token_file), client_id=client_id,
                            rate_per_sec=3.5, burst=3)

    from_dt = REPLAY_START - datetime.timedelta(days=args.days)

    if args.to:
        to_dt = datetime.datetime.fromisoformat(args.to).replace(tzinfo=IST)
    else:
        now_ist = datetime.datetime.now(IST)
        to_dt = now_ist
    # Floor 'to' to the last closed 5m bucket so the window is deterministic.
    b0, _ = _bucket_epochs(to_dt, 5)
    to_dt = datetime.datetime.fromtimestamp(b0, IST)

    print(f"window: {from_dt.isoformat()} .. {to_dt.isoformat()} (back {args.days}d)", flush=True)

    token_exp = client.load_token()[:20] + "..."

    rows_by_key: dict[tuple[str, str], list[list]] = {}
    for instrument, meta in INSTRUMENTS.items():
        for interval in ("5", "15", "60"):
            rows = fetch_windows(client, meta["security_id"], meta["exchange_segment"],
                                 meta["instrument"], interval, from_dt, to_dt)
            rows_by_key[(instrument, interval)] = rows
            print(f"  {instrument} {INTERVAL_LABELS[interval]}: {len(rows)} native candles "
                  f"({datetime.datetime.fromtimestamp(rows[0][0], IST).isoformat() if rows else 'none'} .. "
                  f"{datetime.datetime.fromtimestamp(rows[-1][0], IST).isoformat() if rows else 'none'})", flush=True)

    if not any(rows_by_key.values()):
        print("ERROR: no candles fetched. Token expired or network/auth failure.", flush=True)
        sys.exit(1)

    out, checks = write_snapshot(rows_by_key, from_dt, to_dt, args.days, token_exp)
    print(f"snapshot written: {out}", flush=True)
    print(f"  {len(checks)} files checksummed -> {CHECKS}", flush=True)
    print(f"  manifest: {MANIFEST_PATH}", flush=True)


if __name__ == "__main__":
    main()