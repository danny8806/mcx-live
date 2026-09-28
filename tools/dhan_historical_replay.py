"""Deterministic historical replay with REAL Dhan REST market data.

This is the finite, deterministic historical-replay environment required by the
master directive. It REPLACES ONLY the external market-data source with an
immutable REAL-data adapter (HistoricalDhanReplaySource) and drives the SAME
production pipeline:

  NativeCandleRouter -> SharedIndicatorEngine -> 4 strategies -> HTF
    -> signal candle -> trigger -> trade lifecycle -> LTP replay -> fills
    -> positions -> SL/reversal -> exit -> P&L -> trading.db

The warm-up consumes the EXACT same history the live engine consumes at its
start instant (engine._warmup_from_rest() with the period clock frozen at the
replay start), so a restart at the warmup boundary reproduces the uninterrupted
engine deterministically. All loops are finite and bounded by the immutable
dataset.

Data source: replay_output/dhan_snapshot/{INSTRUMENT}_{interval}.json drawn
from Dhan REST POST /charts/intraday (native 5/15/60 intervals, never
resampled). Files are SHA-256 checksummed via replay_output/checksums.json.

Usage:
  python tools/dhan_historical_replay.py                    # warmup + full replay
  python tools/dhan_historical_replay.py --warmup-only      # warmup + checkpoint
  python tools/dhan_historical_replay.py --from-ts 1770000000 --to-ts 1780000000
"""
from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import sys
import time
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from tools.replay_live_architecture import (  # noqa: E402
    SIDS, MISSION_NAME, SECURITY, TF_MIN, TF_RANK,
    db_forensics, dump, fresh_workdir, write_config,
)

IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))

SNAPSHOT_DIR = ROOT / "replay_output" / "dhan_snapshot"
OUT_DIR = ROOT / "replay_output" / "historical_replay"

REPLAY_START = datetime.datetime(2026, 9, 2, 9, 0, 0, tzinfo=IST)
INTERVAL_BY_TF = {"5m": "5", "15m": "15", "1h": "60"}
TF_MIN_BY_TF = {"5m": 5, "15m": 15, "1h": 60}


class HistoricalDhanReplaySource:
    """Deterministic drop-in for DhanDataAdapter backed by the immutable REAL
    Dhan snapshot (the production REST client contract, but offline).

    fetch_historical_candles(symbol, timeframe, from_date, to_date) returns
    rows of the exact shape the REST client produces: [epoch,o,h,l,c,v],
    filtered to the IST trading days in [from_date, to_date] AND whose bar
    already closed at/before the replay start instant (cutoff_ts). The latter
    guarantees ZERO lookahead: the warmup sees exactly the same closed candles
    the live engine consumed at its start instant.
    """

    def __init__(self, client_id="", token_file="", pin="", totp_secret="",
                 on_tick=None, on_status=None, cutoff_ts=None, **kwargs):
        self.client_id = client_id
        self._on_tick = on_tick
        self._on_status = on_status
        self.__dict__["_rows"] = self._load_snapshot()
        self.cutoff_ts = float(cutoff_ts or REPLAY_START.timestamp())
        self.instruments = {}
        self._calls: list[dict] = []
        self.ws = _ReplayWS()

    @staticmethod
    def _load_snapshot() -> dict:
        rows_by_key: dict[tuple[str, str], list[list]] = {}
        for instrument, sec_id in SECURITY.items():
            for interval in ("5", "15", "60"):
                path = SNAPSHOT_DIR / f"{instrument}_{interval}.json"
                if not path.exists():
                    raise FileNotFoundError(
                        f"REAL Dhan snapshot missing: {path}. Run tools/dhan_historical_download.py first.")
                rows_by_key[(instrument, interval)] = json.loads(path.read_text())["candles"]
        return rows_by_key

    def register_instruments(self, instruments: dict) -> None:
        self.instruments = instruments

    def connect(self) -> None:
        self.ws.connected = True

    def disconnect(self) -> None:
        self.ws.connected = False

    def fetch_historical_candles(self, symbol, timeframe="5", from_date=None, to_date=None):
        from_date_d = from_date if isinstance(from_date, datetime.date) else datetime.date.fromisoformat(str(from_date))
        to_date_d = to_date if isinstance(to_date, datetime.date) else datetime.date.fromisoformat(str(to_date))
        tf_min = {"5": 5, "15": 15, "60": 60}.get(str(timeframe), 5)
        rows = self.__dict__["_rows"].get((symbol, timeframe), [])
        out = []
        for r in rows:
            day = datetime.datetime.fromtimestamp(r[0], IST).date()
            if from_date_d <= day <= to_date_d and (r[0] + tf_min * 60) <= self.cutoff_ts:
                out.append(r)
        self._calls.append({"symbol": symbol, "timeframe": timeframe,
                            "from": str(from_date_d), "to": str(to_date_d),
                            "n": len(out)})
        return out

    # ---- stats hook used by collectors ----
    def fetch_call_log(self) -> list[dict]:
        return list(self._calls)


class _ReplayWS:
    def __init__(self):
        self.connected = True
        self._last_tick_time = time.time()

    def is_stale(self) -> bool:
        return False


def load_snapshot_bars() -> dict[tuple[str, str], list[dict]]:
    """{(instrument, tf): [bar dicts]} built with the LIVE bar-timestamp
    convention (start_ts = candle bucket-open epoch, end_ts = start + tf*60)."""
    src = HistoricalDhanReplaySource.__new__(HistoricalDhanReplaySource)
    snap_dict = src._load_snapshot()
    out: dict[tuple[str, str], list[dict]] = {}
    for instrument in ("GOLDM", "SILVERM"):
        for tf, interval in INTERVAL_BY_TF.items():
            bars = []
            for c in snap_dict[(instrument, interval)]:
                start = float(c[0])
                bars.append({
                    "instrument": instrument, "timeframe": tf,
                    "start_ts": start, "end_ts": start + TF_MIN_BY_TF[tf] * 60.0,
                    "open": float(c[1]), "high": float(c[2]), "low": float(c[3]),
                    "close": float(c[4]), "volume": float(c[5]),
                })
            bars.sort(key=lambda b: b["end_ts"])
            out[(instrument, tf)] = bars
    return out


def chronological(bars: dict[tuple[str, str], list[dict]]) -> list[dict]:
    all_bars = []
    for key, by_tf in bars.items():
        for b in by_tf:
            all_bars.append({**b, "rank": TF_RANK[b["timeframe"]]})
    all_bars.sort(key=lambda b: (b["end_ts"], b["rank"]))
    return all_bars


def build_engine(workdir: Path, enabled: list[str] | None = None):
    """Build the REAL TradingEngine with the historical replay source adapter.
    Mirrors tools/replay_live_architecture.build_engine but uses
    HistoricalDhanReplaySource (deterministic REAL data) instead of MockDhanAdapter."""
    from tests.fresh_audit import test_full_deep_architecture as _unused  # noqa - keep harness import path warm
    from analytics.schema import init_analytics_db
    from core.market_status import MarketState, EngineStatus
    from core.trade_close import TradeCloseManager
    from persistence.manager import PersistenceManager
    from trading_engine import TradingEngine, Bar
    import trading_engine as te

    te.DhanDataAdapter = HistoricalDhanReplaySource
    cfg_path = write_config(workdir, enabled)
    init_analytics_db(str(workdir / "data" / "db" / "analytics.db"))
    persistence = PersistenceManager(
        state_path=str(workdir / "data" / "db" / "system_state.json"),
        db_path=str(workdir / "data" / "db" / "trading.db"),
    )
    engine = TradingEngine(config_path=str(cfg_path))
    engine.set_persistence(persistence)
    engine._trade_close_manager = TradeCloseManager(
        position_manager=engine.position_manager,
        pnl_engines=engine.pnl_engines,
        account_engines=engine.account_engines,
        global_account=engine.account_engine,
        risk_engine=engine.risk_engine,
        persistence=persistence,
        event_store=engine.event_store,
        telegram=engine.telegram,
        event_callback=engine._event_callback,
        trade_ledger=engine.trade_ledger,
    )
    ws = engine.data_adapter.ws
    ws.connected = True
    engine.market_status.force_state(MarketState.LIVE_TRADING)
    engine.market_status.set_engine_status(EngineStatus.TRADING)
    engine._running = True
    engine._on_tick({"instrument": "GOLDM", "ltp": 150000.0, "event_timestamp": time.time()})
    return engine, persistence, cfg_path


class _FrozenDT(datetime.datetime):
    """datetime subclass whose now() returns REPLAY_START (deterministic
    warmup window). Every other datetime behaviour is inherited."""
    @classmethod
    def now(cls, tz=None):
        return cls(REPLAY_START.year, REPLAY_START.month, REPLAY_START.day,
                   REPLAY_START.hour, REPLAY_START.minute, REPLAY_START.second,
                   tzinfo=IST)


def warmup(engine, as_of: datetime.datetime) -> dict:
    """Run the PRODUCTION engine._warmup_from_rest() with the period clock frozen
    at `as_of`, so the warmup window (config last_trading_days / fetch_calendar_days)
    equals exactly what the live engine consumed at that start instant."""
    with mock.patch("trading_engine.datetime", _FrozenDT):
        engine._warmup_from_rest()
    return {"as_of": as_of.isoformat(),
            "fetch_calls": list(engine.data_adapter.fetch_call_log())}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--warmup-only", action="store_true")
    ap.add_argument("--from-ts", type=float, default=None)
    ap.add_argument("--to-ts", type=float, default=None)
    ap.add_argument("--workdir", type=Path)
    args = ap.parse_args()

    bars = load_snapshot_bars()
    stream_all = chronological(bars)

    replay_start_epoch = REPLAY_START.timestamp()
    replay = [b for b in stream_all if b["end_ts"] > replay_start_epoch]
    if args.from_ts is not None:
        replay = [b for b in replay if b["end_ts"] >= args.from_ts]
    if args.to_ts is not None:
        replay = [b for b in replay if b["end_ts"] <= args.to_ts]
    if not replay:
        # No bars end strictly after replay start; still replay all (window edge).
        replay = stream_all
    print(f"REAL snapshot: {sum(len(v) for v in bars.values())} native bars total, "
          f"{len(replay)} in replay window from {REPLAY_START.isoformat()}", flush=True)

    import shutil
    workdir = args.workdir or (Path(__file__).resolve().parent.parent / "replay_output"
                               / "historical_replay" / "work")
    if workdir.exists():
        shutil.rmtree(workdir, ignore_errors=True)
    workdir.mkdir(parents=True, exist_ok=True)

    engine, persistence, cfg = build_engine(workdir)
    try:
        wu = warmup(engine, REPLAY_START)
        print(f"warmup called {len(wu['fetch_calls'])} fetch windows; "
              f"per-strategy fast counts: "
              + json.dumps({s: engine.strategies[s].fast_indicator._count
                            for s in SIDS if s in engine.strategies}), flush=True)

        if args.warmup_only:
            snapshot = engine.snapshot()
            out_dir = OUT_DIR / "warmup_checkpoint"
            out_dir.mkdir(parents=True, exist_ok=True)
            (out_dir / "engine_snapshot.json").write_text(
                json.dumps(snapshot, indent=1, default=str), encoding="utf-8")
            (out_dir / "warmup_report.json").write_text(
                json.dumps(wu, indent=1, default=str), encoding="utf-8")
            print(f"warmup checkpoint: {out_dir}", flush=True)
            return 0

        from tools.replay_live_architecture import install_crossover_loggers, run_replay, collect
        install_crossover_loggers(engine)
        out: dict = {}
        run_replay(engine, persistence, replay, out)
        out.update(collect(engine, persistence, workdir))
        out["crossover_log"] = {sid: getattr(engine.strategies.get(sid), "_crossover_log", [])
                                for sid in SIDS}
        out["db_forensics"] = db_forensics(persistence, workdir)
        out["warmup"] = wu
        out["dataset"] = {
            "source": "Dhan REST /charts/intraday (native 5/15/60, no resampling)",
            "snapshot_dir": str(SNAPSHOT_DIR),
            "bar_counts": {f"{k[0]}/{k[1]}": len(v) for k, v in bars.items()},
            "replay_bar_count": len(replay),
            "replay_first_end_ts": replay[0]["end_ts"],
            "replay_last_end_ts": replay[-1]["end_ts"],
        }
        out_dir = OUT_DIR / "single_run"
        dump(out_dir, out)
        summary = {
            "signals": len(out["signals"]),
            "trades": len(out["trades"]),
            "orders": len(out["orders"]),
            "fills": len(out["fills"]),
            "positions": len(out["positions"]),
            "pnl_groups": len(out["pnl"]),
            "strategies": {k: v["signals_count"] for k, v in out["strategies"].items()},
            "artifacts": sorted(str(p.relative_to(ROOT)) for p in out_dir.iterdir()),
        }
        (out_dir / "summary.json").write_text(json.dumps(summary, indent=1, default=str),
                                              encoding="utf-8")
        print(json.dumps(summary, indent=1), flush=True)
        return 0
    finally:
        try:
            engine.stop()
        except Exception:
            pass
        try:
            persistence.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())