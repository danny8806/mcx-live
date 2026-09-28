"""Local ultra-low-latency benchmark (Phase 1b, BEFORE baseline).

Measures the critical execution-tier latencies on THIS machine with a fully
wired four-strategy engine (mock Dhan adapter, paper execution, LIVE_TRADING),
standalone OrderWatcher over StubLiveBroker, and the raw EventBus.

All timings use time.perf_counter_ns (monotonic, high resolution). Each bench
collects N_NANOS samples and reports mean / median / P50 / P95 / P99 / MAX.

BENCHES
  trigger_scan_*       strategy.on_tick no-op scans (pending armed / SL armed)
  trigger_fire_*       strategy.on_tick trigger fire  (entry / SL signal build)
  engine_tick_*        trading_engine._on_tick end-to-end (flat / armed / fire)
  process_signal_*     _process_signal entry order pipeline (paper synthetic)
  watcher_feed_market  order watcher feed_market per tick
  watcher_scan         order watcher scan() decision pass (PENDING_BUT_VALID)
  eventbus_tick        raw EventBus publish tick:GOLDM dispatch

Run from the repo root:  python tools/benchmark_latency.py
Outputs tools/bench_before.json (machine-readable) for the AFTER comparison.
"""
from __future__ import annotations

import json
import statistics
import sys
import time
from pathlib import Path
from typing import Any, Optional

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from analytics.schema import init_analytics_db  # noqa: E402
from core.market_status import EngineStatus, MarketState  # noqa: E402
from core.trade_close import TradeCloseManager  # noqa: E402
from events.bus import EventBus  # noqa: E402
from events.types import TickEvent  # noqa: E402
from execution.live.broker_client import StubLiveBroker  # noqa: E402
from execution.live.order_watcher import OrderWatcher  # noqa: E402
from persistence.manager import PersistenceManager  # noqa: E402
from strategies.instance import StrategyInstance  # noqa: E402
from strategies.types import PendingEntry, Signal, SignalType  # noqa: E402
from tests.fresh_audit import test_full_deep_architecture as harness  # noqa: E402
from tests.new_architecture._harness import write_config  # noqa: E402
from trading_engine import TradingEngine  # noqa: E402

N_WARM = 200
N_RUN = 2000

_ST = type("State", (object,), {"value": "SUBMITTED"})


def _pct(sorted_ns, p: float) -> float:
    idx = (len(sorted_ns) - 1) * p
    lo = int(idx)
    hi = min(lo + 1, len(sorted_ns) - 1)
    return sorted_ns[lo] + (sorted_ns[hi] - sorted_ns[lo]) * (idx - lo)


def _stats(samples_ns: list[int]) -> dict:
    s = sorted(samples_ns)
    n = len(s)
    return {
        "N": n,
        "mean_us": round(statistics.mean(samples_ns) / 1000.0, 2),
        "median_us": round(s[n // 2] / 1000.0, 2),
        "p50_us": round(_pct(s, 0.50) / 1000.0, 2),
        "p95_us": round(_pct(s, 0.95) / 1000.0, 2),
        "p99_us": round(_pct(s, 0.99) / 1000.0, 2),
        "max_us": round(max(s) / 1000.0, 2),
        "min_us": round(min(s) / 1000.0, 2),
    }


class Bench:
    def __init__(self, name: str):
        self.name = name
        self.samples: list[int] = []

    def sample(self, fn, *a, **k):
        t0 = time.perf_counter_ns()
        fn(*a, **k)
        self.samples.append(time.perf_counter_ns() - t0)

    def run(self, fn, *a, warm=N_WARM, n=N_RUN, **k):
        with self._supress_stdout():
            for _ in range(warm):
                fn(*a, **k)
        for _ in range(n):
            t0 = time.perf_counter_ns()
            fn(*a, **k)
            self.samples.append(time.perf_counter_ns() - t0)

    @staticmethod
    def _supress_stdout():
        import contextlib
        import io
        return contextlib.redirect_stdout(io.StringIO())

    def done(self) -> dict:
        return {"bench": self.name, **(_stats(self.samples))}


def build_engine():
    import tempfile

    import trading_engine as te

    te.DhanDataAdapter = harness.MockDhanAdapter
    tmp = Path(tempfile.mkdtemp(prefix="bench_"))
    cfg_path = write_config(tmp)
    db_path = str(tmp / "data" / "db" / "trading.db")
    init_analytics_db(str(tmp / "data" / "db" / "analytics.db"))
    persistence = PersistenceManager(
        state_path=str(tmp / "data" / "db" / "system_state.json"),
        db_path=db_path,
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
    ws._last_tick_time = time.time()
    engine.market_status.force_state(MarketState.LIVE_TRADING)
    engine.market_status.set_engine_status(EngineStatus.TRADING)
    engine._running = True
    engine._on_tick({"instrument": "GOLDM", "ltp": 78000.0, "event_timestamp": time.time()})
    return engine, persistence


def _arm_pending(strat: StrategyInstance, ltp: float = 78050.0) -> None:
    sig = Signal(
        SignalType.LONG, strat.instrument, strategy_id=strat.strategy_id,
        timestamp=time.time(), trigger_price=ltp, stop_price=ltp - 100.0,
        quantity=strat.quantity,
    )
    strat.pending_entry = PendingEntry(signal=sig, trigger_price=ltp, side="LONG")
    strat.just_entered = False
    strat.enabled = True


def _disarm_pending(strat: StrategyInstance) -> None:
    strat.pending_entry = None
    strat.just_entered = False
    strat.stop_price = None


def _arm_sl(strat: StrategyInstance, ltp: float = 78000.0) -> None:
    strat.position_side = "LONG"
    strat.stop_price = ltp - 50.0
    strat.stop_exit_submitted = False
    strat.just_entered = False
    strat.enabled = True


def _reset_strat(strat: StrategyInstance) -> None:
    strat.position_side = None
    strat.pending_entry = None
    strat.stop_price = None
    strat.just_entered = False
    strat.stop_exit_submitted = False


def bench_strategy(engine) -> list:
    gold = engine.strategies["gold_01"]
    gold.enabled = True
    out = []

    b1 = Bench("trigger_scan_pending")
    _arm_pending(gold, 78050.0)
    b1.run(gold.on_tick, 78000.0, 1000.0)
    out.append(b1.done())

    b2 = Bench("trigger_scan_sl")
    _arm_sl(gold, 78000.0)
    b2.run(gold.on_tick, 78000.0, 1000.0)
    out.append(b2.done())

    b3 = Bench("trigger_fire_entry")
    _disarm_pending(gold)
    b3.run(_fire_entry_once, gold)
    out.append(b3.done())

    b4 = Bench("trigger_fire_sl")
    _reset_strat(gold)
    b4.run(_fire_sl_once, gold)
    out.append(b4.done())

    _reset_strat(gold)
    return out


def _fire_entry_once(gold: StrategyInstance) -> None:
    _arm_pending(gold, 78050.0)
    gold.on_tick(78055.0, 1000.0)
    _disarm_pending(gold)


def _fire_sl_once(gold: StrategyInstance) -> None:
    _arm_sl(gold, 78000.0)
    gold.on_tick(77945.0, 1000.0)
    _reset_strat(gold)


def bench_engine(engine) -> list:
    out = []
    gold = engine.strategies["gold_01"]
    gold.enabled = True
    _reset_strat(gold)

    b1 = Bench("engine_tick_flat")
    b1.run(engine._on_tick, {"instrument": "GOLDM", "ltp": 78000.0,
                             "event_timestamp": time.time()})
    out.append(b1.done())

    b2 = Bench("engine_tick_armed_pending")
    _arm_pending(gold, 78050.0)
    b2.run(engine._on_tick, {"instrument": "GOLDM", "ltp": 78000.0,
                             "event_timestamp": time.time()})
    out.append(b2.done())
    _disarm_pending(gold)

    b3 = Bench("engine_tick_armed_sl")
    _arm_sl(gold, 78000.0)
    b3.run(engine._on_tick, {"instrument": "GOLDM", "ltp": 78000.0,
                             "event_timestamp": time.time()})
    out.append(b3.done())
    _reset_strat(gold)
    return out


def _fresh_signal(strategy_id: str, s_type: SignalType) -> Signal:
    return Signal(
        s_type, _INST[strategy_id], strategy_id=strategy_id,
        timestamp=time.time(), trigger_price=_PRICE[strategy_id],
        stop_price=_PRICE[strategy_id] - 100.0,
        quantity=1, signal_id=f"bench-{int(time.perf_counter_ns())}",
    )


_INST = {"gold_01": "GOLDM", "gold_02": "GOLDM",
         "silver_01": "SILVERM", "silver_02": "SILVERM"}
_PRICE = {"gold_01": 78000.0, "gold_02": 78000.0,
          "silver_01": 239000.0, "silver_02": 239000.0}


class _Alternator:
    """Runs an entry signal through _process_signal on alternating sides so
    every iteration exercises gating -> lifecycle -> order -> paper fill."""

    def __init__(self, engine, strategy_id: str):
        self.engine = engine
        self.strategy_id = strategy_id
        self.flip = 0

    def __call__(self):
        self.flip += 1
        side = SignalType.SHORT if self.flip % 2 else SignalType.LONG
        self.engine._process_signal(_fresh_signal(self.strategy_id, side))


def bench_process_signal(engine) -> list:
    out = []
    for sid in ("gold_01", "silver_01"):
        b = Bench(f"process_signal_entry_{sid}")
        alt = _Alternator(engine, sid)
        b.run(alt)
        out.append(b.done())
    return out


class _FakeEngine:
    _orders: dict = {}
    _current_prices: dict = {"GOLDM": 78040.0, "SILVERM": 239000.0}


class _FakeOrder:
    order_id = "ORDER-1"
    state = _ST()
    _broker_order_id = "BROKER-1"
    correlation_id = "MCX-bench"
    strategy_id = "gold_01"
    trade_id = "TRADE-1"
    entry_signal_id = "SIG-1"
    reversal_id = None
    order_role = "ENTRY"
    instrument = "GOLDM"
    side = "BUY"
    order_type = "LIMIT"
    createdAt = 0.0
    created_at = 0.0
    requested_price = 78050.0
    price = 78050.0
    quantity = 1
    planned_entry_price = 78050.0
    planned_sl = 77950.0


def _watcher_config() -> dict:
    return {
        "live": {
            "order_watcher": {
                "trigger_detection_enabled": True,
                "market_fallback_enabled": True,
                "market_fallback_timeout_ms": 30000.0,
                "max_reprices": 2,
                "reprice_interval_ms": 5000.0,
                "max_order_age_ms": 60000.0,
                "max_price_deviation_pct": 0.5,
                "max_order_ops_per_sec": 10,
                "stale_after_ws_ms": 15000.0,
                "reconcile_verify_interval_ms": 5000.0,
                "limit_skip_policy": {
                    "enabled": True, "max_age_ms": 15000.0,
                    "max_deviation_pct": 0.5, "trigger_crossed_unfilled_ms": 3000.0,
                    "cancel_check_ms": 2000.0,
                },
            },
        },
    }


def bench_watcher() -> list:
    out = []
    broker = StubLiveBroker(gate_enabled=True)
    watcher = OrderWatcher(
        engine=_FakeEngine(), broker=broker, config=_watcher_config(),
        clock=time.time, quote_fn=lambda inst: 78040.0,
    )
    watcher.register_from_order(_FakeOrder())

    b1 = Bench("watcher_feed_market")
    b1.run(watcher.feed_market, "GOLDM", 78040.0)
    out.append(b1.done())

    # Fresh record: classify PENDING_BUT_VALID -> WAIT (decision-only pass).
    rec = watcher._records["ORDER-1"]
    now = time.time()
    rec.submitted_at = now
    rec.last_market_check_at = now
    rec.last_rest_check_at = now
    rec.current_market_price = 78040.0
    rec.status = "SUBMITTED"

    b2 = Bench("watcher_scan_wait")
    b2.run(watcher.scan, now)
    out.append(b2.done())
    return out


def bench_eventbus() -> list:
    bus = EventBus()
    calls = {"n": 0}

    def _noop(_e):
        calls["n"] += 1

    for topic in ("tick:GOLDM", "tick:SILVERM"):
        bus.subscribe(topic, _noop)
        bus.subscribe(topic, _noop)
    for topic in ("candle:GOLDM:5m", "candle:GOLDM:15m", "candle:GOLDM:1h",
                  "candle:SILVERM:5m", "candle:SILVERM:15m", "candle:SILVERM:1h"):
        bus.subscribe(topic, _noop)

    event = TickEvent(instrument="GOLDM", ltp=78040.0,
                      timestamp=time.time(), volume=1.0)

    b = Bench("eventbus_tick_dispatch")
    b.run(bus.publish, "tick:GOLDM", event)
    return [b.done()]


def main(argv: list[str] | None = None) -> int:
    import argparse
    import logging
    logging.disable(logging.CRITICAL)

    parser = argparse.ArgumentParser(description="Latency benchmark tool")
    parser.add_argument("--after", action="store_true",
                        help="write report to tools/bench_after.json (AFTER phase)")
    args = parser.parse_args(argv)

    t0 = time.perf_counter()
    engine, persistence = build_engine()
    results: list[dict] = []
    try:
        results += bench_strategy(engine)
        results += bench_engine(engine)
        results += bench_process_signal(engine)
    finally:
        try:
            engine.stop()
        except Exception:
            pass
        try:
            persistence.close()
        except Exception:
            pass
    results += bench_watcher()
    results += bench_eventbus()

    dt = time.perf_counter() - t0
    phase = "AFTER" if args.after else "BEFORE"
    print(f"\n=== {phase} LATENCY PHASE ===  (machine: local)"
          f"  [{dt:.1f}s total]")
    print(f"{'bench':<28}{'N':>7}{'mean_us':>9}{'median_us':>10}"
          f"{'p50':>8}{'p95':>8}{'p99':>8}{'max':>9}")
    for r in results:
        print(f"{r['bench']:<28}{r['N']:>7}{r['mean_us']:>9.2f}"
              f"{r['median_us']:>10.2f}{r['p50_us']:>8.2f}"
              f"{r['p95_us']:>8.2f}{r['p99_us']:>8.2f}{r['max_us']:>9.2f}")

    report = {
        "phase": phase,
        "timestamp": time.time(),
        "runner": "tools/benchmark_latency.py",
        "host": "local",
        "benches": results,
    }
    out = REPO / "tools" / ("bench_after.json" if args.after else "bench_before.json")
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\nreport -> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())