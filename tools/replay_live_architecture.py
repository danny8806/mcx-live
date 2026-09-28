"""LIVE-ARCHITECTURE FOUR-STRATEGY REPLAY (mission Phase 34/35/65).

Drives the REAL production stack — TradingEngine, NativeCandleRouter, shared
SharedNativeIndicatorEngine, immutable snapshots, four independent
StrategyRuntime objects, paper broker transport, canonical trading.db — over a
deterministic historical window. The only substitution is the network layer
(the Dhan WS/REST is replaced by a mock adapter; historical candles are read
from offline CSVs because DhanAuthError DH-901 blocks live REST fetch on this
host).

Modes:
  --mode parallel   one engine, all four strategies live on a shared event
                    bus / shared indicator engine (the production topology)
  --mode sequential each strategy runs alone in its own engine over the same
                    data (proves isolation equivalence)

Outputs (per mode) are written under replay_output/live_replay/<mode>:
  full_replay.json, signal_replay.json, trade_replay.json, order_replay.json,
  fill_replay.json, position_replay.json, pnl_replay.json,
  strategy_replay.json, evaluation_stream.jsonl, checksums.json
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

IST = timezone(timedelta(hours=5, minutes=30))

SIDS = ["gold_01", "gold_02", "silver_01", "silver_02"]
MISSION_NAME = {
    "gold_01": "GOLDM_5M",
    "gold_02": "GOLDM_15M",
    "silver_01": "SILVERM_15M",
    "silver_02": "SILVERM_5M",
}
SECURITY = {"GOLDM": "569003", "SILVERM": "483080"}
TF_RANK = {"1h": 0, "15m": 1, "5m": 2}
TF_MIN = {"1h": 60, "15m": 15, "5m": 5}


# ═══════════════════════════════════════════════════════════════════════
# Offline historical data loading
# ═══════════════════════════════════════════════════════════════════════
def load_candles(csv_root: Path) -> dict[tuple[str, str], list[dict]]:
    """Return {(instrument, timeframe): [bar dicts]} sorted ascending."""
    out: dict[tuple[str, str], list[dict]] = {}
    for instrument in ("GOLDM", "SILVERM"):
        for tf, fname in (("1h", "1h"), ("15m", "15m"), ("5m", "5m")):
            path = csv_root / f"{instrument}_{fname}.csv"
            if not path.exists():
                alt = csv_root / f"{instrument}_60m.csv"
                if tf == "1h" and alt.exists():
                    path = alt
                else:
                    raise FileNotFoundError(path)
            bars = []
            with path.open(newline="", encoding="utf-8") as fh:
                for row in csv.DictReader(fh):
                    bars.append({
                        "instrument": instrument,
                        "timeframe": tf,
                        "end_ts": _to_epoch(row["datetime"]),
                        "open": float(row["open"]),
                        "high": float(row["high"]),
                        "low": float(row["low"]),
                        "close": float(row["close"]),
                        "volume": float(row.get("volume") or 0),
                    })
            bars.sort(key=lambda b: b["end_ts"])
            out[(instrument, tf)] = bars
    return out


def _to_epoch(dt_str: str) -> float:
    dt = datetime.fromisoformat(dt_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST)
    return dt.timestamp()


def chronological(bars: dict[tuple[str, str], list[dict]]) -> list[dict]:
    all_bars: list[dict] = []
    for key, by_tf in bars.items():
        for b in by_tf:
            all_bars.append({**b, "rank": TF_RANK[b["timeframe"]]})
    all_bars.sort(key=lambda b: (b["end_ts"], b["rank"]))
    return all_bars


# ═══════════════════════════════════════════════════════════════════════
# Engine construction (mirrors tests/new_architecture/conftest wiring)
# ═══════════════════════════════════════════════════════════════════════
def write_config(root: Path, enabled: list[str] | None = None) -> Path:
    import json as _json
    enabled = enabled or SIDS
    strategies_cfg = {
        "gold_01": {"instrument": "GOLDM", "quantity": 1, "capital": 500000,
                    "enabled": "gold_01" in enabled},
        "gold_02": {"instrument": "GOLDM", "quantity": 1, "capital": 500000,
                    "enabled": "gold_02" in enabled},
        "silver_01": {"instrument": "SILVERM", "quantity": 1, "capital": 500000,
                      "enabled": "silver_01" in enabled},
        "silver_02": {"instrument": "SILVERM", "quantity": 1, "capital": 500000,
                      "enabled": "silver_02" in enabled},
    }
    data = {
        "system": {"name": "ReversAll", "version": "1.0.0", "environment": "paper",
                   "log_level": "WARNING",
                   "db_path": str(root / "data" / "db" / "trading.db"),
                   "state_path": str(root / "data" / "db" / "system_state.json")},
        "dhan": {"client_id": "TEST", "access_token": "", "ws_url": "wss://fake",
                 "rest_base": "https://fake",
                 "token_file": str(root / "data" / "db" / "dhan_token.json"),
                 "pin": "", "totp_secret": ""},
        "instruments": {
            "GOLDM": {"symbol": "MCX:GOLDM202610", "security_id": "569003",
                      "exchange_segment": "MCX_COMM", "instrument": "FUTCOM",
                      "multiplier": 10.0, "tick_size": 1.0, "lot_size": 1,
                      "session_open": "09:00", "session_close": "23:30", "session_minutes": 870,
                      "margin_model": {"slope": 0.125, "intercept": 126930.0}},
            "SILVERM": {"symbol": "MCX:SILVERM202611", "security_id": "483080",
                        "exchange_segment": "MCX_COMM", "instrument": "FUTCOM",
                        "multiplier": 5.0, "tick_size": 1.0, "lot_size": 1,
                        "session_open": "09:00", "session_close": "23:30", "session_minutes": 870,
                        "margin_model": {"slope": 0.0625, "intercept": 142900.0}},
        },
        "indicators": {"dema_period": 3, "atr_period": 6, "atr_factor": 1.0},
        "strategies": strategies_cfg,
        "paper_execution": {"slippage_ticks": 0, "latency_ms": 0, "partial_fill_probability": 0},
        "charges": {
            "GOLDM": {"brokerage_per_side": 20.0, "stt_sell_pct": 0.01,
                      "exchange_pct": 0.0026, "sebi_pct": 0.0001,
                      "gst_pct": 18.0, "stamp_duty_pct": 0.0},
            "SILVERM": {"brokerage_per_side": 20.0, "stt_sell_pct": 0.01,
                        "exchange_pct": 0.0026, "sebi_pct": 0.0001,
                        "gst_pct": 18.0, "stamp_duty_pct": 0.0},
        },
        "risk": {"max_open_positions_per_strategy": 1, "max_open_positions_total": 8,
                 "max_daily_loss": 999999999.0, "max_drawdown_pct": 100.0,
                 "margin_per_trade_pct": 6.5, "kill_switch_enabled": False},
        "account": {"starting_capital": 2000000.0,
                    "starting_capital_per_strategy": 500000.0, "currency": "INR"},
        "telegram": {"bot_token": "", "chat_id": "", "enabled": False},
        "dashboard": {"api_key": ""},
        "execution_mode": "paper",
    }
    root.mkdir(parents=True, exist_ok=True)
    (root / "data" / "db").mkdir(parents=True, exist_ok=True)
    cfg_path = root / "settings.json"
    cfg_path.write_text(_json.dumps(data), encoding="utf-8")
    return cfg_path


def build_engine(workdir: Path, enabled: list[str] | None = None):
    from tests.fresh_audit import test_full_deep_architecture as harness
    from analytics.schema import init_analytics_db
    from core.market_status import MarketState, EngineStatus
    from core.trade_close import TradeCloseManager
    from persistence.manager import PersistenceManager
    from trading_engine import TradingEngine
    import trading_engine as te

    te.DhanDataAdapter = harness.MockDhanAdapter
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
    ws._last_tick_time = time.time()
    engine.market_status.force_state(MarketState.LIVE_TRADING)
    engine.market_status.set_engine_status(EngineStatus.TRADING)
    engine._running = True
    engine._on_tick({"instrument": "GOLDM", "ltp": 78000.0, "event_timestamp": time.time()})
    return engine, persistence, cfg_path


# ═══════════════════════════════════════════════════════════════════════
# Replay driver
# ═══════════════════════════════════════════════════════════════════════
def bar_obj(cls, b: dict):
    return cls(
        instrument=b["instrument"], timeframe=b["timeframe"],
        start_ts=b["end_ts"] - TF_MIN[b["timeframe"]] * 60,
        end_ts=b["end_ts"], open=b["open"], high=b["high"], low=b["low"],
        close=b["close"], volume=int(b["volume"]),
    )


def install_crossover_loggers(engine) -> None:
    """Log every crossover event (pending-entry arm + reversal arm) with the
    same bar-level context the reference compute_signals emits, so signal
    parity can be itemized bar-by-bar (Phase 39/40)."""
    def _make_wrapper(strategy):
        log = []
        strategy._crossover_log = log

        def wrap_creator(creator):
            def wrapped(side, close, high, low, timestamp, prev_high=None,
                        prev_low=None, htf_val=None, mid_val=None,
                        fast_dema_atr=None, **kws):
                log.append({
                    "timestamp": timestamp,
                    "side": side,
                    "close": close,
                    "high": high,
                    "low": low,
                    "prev_high": prev_high,
                    "prev_low": prev_low,
                    "htf_value": htf_val,
                    "mid_value": mid_val,
                    "fast_dema_atr": fast_dema_atr,
                })
                return creator(side, close, high, low, timestamp, prev_high,
                               prev_low, htf_val=htf_val, mid_val=mid_val,
                               fast_dema_atr=fast_dema_atr, **kws)
            return wrapped

        strategy._create_pending_signal = wrap_creator(strategy._create_pending_signal)
        strategy._create_reversal_signal = wrap_creator(strategy._create_reversal_signal)

    for sid in SIDS:
        strat = engine.strategies.get(sid)
        if strat is not None:
            _make_wrapper(strat)


def run_replay(engine, persistence, stream: list[dict], out: dict[str, Any]) -> None:
    from trading_engine import Bar
    clock_holder = {"ts": 0.0}
    engine.execution_engine._clock = lambda: clock_holder["ts"]
    evaluations = []
    for b in stream:
        clock_holder["ts"] = b["end_ts"]
        engine.execution_engine.update_price(b["instrument"], b["close"])
        if b["timeframe"] == "5m":
            _capture_evaluations(engine, b, evaluations)
        engine._on_bar_closed(bar_obj(Bar, b))
        engine._on_tick({"instrument": b["instrument"], "ltp": b["close"],
                         "event_timestamp": b["end_ts"]})
    out["evaluation_stream"] = evaluations
    out["last_bar_end_ts"] = stream[-1]["end_ts"]


def _capture_evaluations(engine, b: dict, out: list[dict]) -> None:
    from strategies.instance import Bar
    bar = Bar(instrument=b["instrument"], timeframe=b["timeframe"],
              start_ts=b["end_ts"] - 300.0, end_ts=b["end_ts"],
              open=b["open"], high=b["high"], low=b["low"], close=b["close"],
              volume=int(b["volume"]))
    for sid in SIDS:
        strat = engine.strategies.get(sid)
        if strat is None:
            continue
        mid = None
        slow = None
        try:
            mid = strat.mid_htf_state.get_mapped_value(bar).value
        except Exception:
            pass
        try:
            slow = strat.slow_htf_state.get_mapped_value(bar).value
        except Exception:
            pass
        out.append({
            "strategy_id": sid,
            "timestamp": b["end_ts"],
            "candle": [b["open"], b["high"], b["low"], b["close"], b["volume"]],
            "fast_value": strat.fast_indicator.value,
            "mid_htf_value": mid,
            "slow_htf_value": slow,
            "state": getattr(strat, "state", None).value
                    if getattr(strat, "state", None) is not None else None,
            "position_side": strat.position_side,
            "stop_price": strat.stop_price,
        })


def db_forensics(persistence, workdir: Path) -> dict[str, Any]:
    """Phase 26/27/28/60/61/69 — canonical trading.db integrity, lineage,
    cross-strategy, and orphan forensic scan on the isolated replay db."""
    out: dict[str, Any] = {}
    db = persistence._db

    def q(sql):
        try:
            return list(db.query(sql))
        except Exception:
            return []

    def dups(table, col):
        rows = q(f"SELECT {col}, COUNT(*) AS n FROM {table} GROUP BY {col} HAVING COUNT(*) > 1")
        return [dict(r) for r in rows]

    out["pragmas"] = {}
    try:
        out["pragmas"]["foreign_keys"] = [dict(r) for r in db.query("PRAGMA foreign_keys")]
    except Exception:
        pass
    out["duplicate_ids"] = {
        "trades": dups("trades", "trade_id"),
        "orders": dups("orders", "order_id"),
        "fills": dups("fills", "fill_id"),
        "positions": dups("positions", "position_id"),
        "pending_orders": dups("pending_orders", "pending_order_id"),
        "broker_mappings": dups("broker_order_mapping", "broker_order_id"),
    }
    out["trades_without_entry_signal"] = q(
        "SELECT trade_id, strategy_id FROM trades "
        "WHERE entry_signal_id IS NULL OR entry_signal_id = ''")
    out["position_equals_trade"] = q(
        "SELECT t.trade_id, p.position_id FROM trades t "
        "JOIN positions p ON p.position_id = t.trade_id")
    # orphans: trade-linked entities pointing at nonexistent trades
    out["orphans"] = {}
    for table, col in (("pending_orders", "trade_id"), ("orders", "trade_id"),
                       ("fills", "trade_id"), ("positions", "trade_id"),
                       ("trade_legs", "trade_id"), ("trade_signal_link", "trade_id"),
                       ("trade_events", "trade_id"), ("trade_snapshots", "trade_id")):
        out["orphans"][table] = q(
            f"SELECT DISTINCT {col} FROM {table} WHERE {col} NOT IN "
            "(SELECT trade_id FROM trades)")
    out["orphans"]["fills_without_order"] = q(
        "SELECT DISTINCT order_id FROM fills WHERE order_id NOT IN "
        "(SELECT order_id FROM orders)")
    # unknown broker ids (should only exist if quarantine wrote them)
    out["unknown_broker_orders"] = q(
        "SELECT DISTINCT broker_order_id, order_id, trade_id FROM broker_order_mapping "
        "WHERE order_id NOT IN (SELECT order_id FROM orders)")
    # cross-strategy contamination on every lineage edge
    out["cross_strategy"] = {
        "order_vs_trade": q(
            "SELECT o.order_id, o.strategy_id AS order_strategy, t.strategy_id AS trade_strategy "
            "FROM orders o JOIN trades t ON o.trade_id = t.trade_id "
            "WHERE o.strategy_id != t.strategy_id"),
        "fill_vs_trade": q(
            "SELECT f.fill_id, f.strategy_id AS fill_strategy, t.strategy_id AS trade_strategy "
            "FROM fills f JOIN trades t ON f.trade_id = t.trade_id "
            "WHERE f.strategy_id != t.strategy_id"),
        "fill_vs_order": q(
            "SELECT f.fill_id, f.strategy_id AS fill_strategy, o.strategy_id AS order_strategy "
            "FROM fills f JOIN orders o ON f.order_id = o.order_id "
            "WHERE f.strategy_id != o.strategy_id"),
        "position_vs_trade": q(
            "SELECT p.position_id, p.strategy_id AS pos_strategy, t.strategy_id AS trade_strategy "
            "FROM positions p JOIN trades t ON p.trade_id = t.trade_id "
            "WHERE p.strategy_id != t.strategy_id"),
    }
    # SL invariant: SL exits must not carry a fabricated exit signal
    out["sl_invariant_violations"] = q(
        "SELECT trade_id, strategy_id, exit_reason, exit_signal_id FROM trades "
        "WHERE exit_reason = 'STOP_LOSS' AND exit_signal_id IS NOT NULL AND exit_signal_id != ''")
    # reversal invariant: exit_signal_id of a reversal exit == entry_signal_id of the next trade
    rev = q("SELECT trade_id, strategy_id, exit_reason, exit_signal_id FROM trades "
            "WHERE exit_reason LIKE '%reversal%'")
    out["reversal_exits"] = [dict(r) for r in rev]
    # idempotency: duplicate idempotency_key on trade_events
    out["duplicate_trade_events"] = q(
        "SELECT idempotency_key, COUNT(*) AS n FROM trade_events "
        "GROUP BY idempotency_key HAVING COUNT(*) > 1")
    out["processed_fill_dups"] = dups("processed_fills", "fill_id")
    # analytics reconciliation
    try:
        out["analytics_vs_trades"] = q(
            "SELECT t.strategy_id, COUNT(DISTINCT t.trade_id) AS trades_db, "
            "COALESCE(SUM(t.net_pnl), 0) AS pnl_db, "
            "COUNT(DISTINCT a.trade_id) AS trades_analytics, "
            "COALESCE(SUM(a.net_pnl), 0) AS pnl_analytics, "
            "COUNT(DISTINCT t.trade_id) - COUNT(DISTINCT a.trade_id) AS diff_count, "
            "ROUND(COALESCE(SUM(t.net_pnl),0) - COALESCE(SUM(a.net_pnl),0), 4) AS diff_pnl "
            "FROM trades t LEFT JOIN trades_analytics a ON a.trade_id = t.trade_id "
            "GROUP BY t.strategy_id ORDER BY t.strategy_id")
    except Exception as e:
        out["analytics_error"] = str(e)
    out["counts"] = {t: len(q(f"SELECT * FROM {t}")) for t in
                     ("signals", "trades", "pending_orders", "orders", "fills",
                      "positions", "events", "trade_events", "quarantine_records")}
    return out


def collect(engine, persistence, workdir: Path) -> dict[str, Any]:
    out = {
        "signals": [],
        "trades": [],
        "orders": [],
        "fills": [],
        "positions": [],
        "pnl": [],
        "strategies": {},
        "indicator_streams": {},
        "dedup": {},
    }
    # signals from strategy audit trails
    for sid in SIDS:
        strat = engine.strategies.get(sid)
        if strat is None:
            continue
        for s in getattr(strat, "_signals", []):
            out["signals"].append({
                "strategy_id": sid,
                "signal_id": s.signal_id,
                "signal_type": s.signal_type.name,
                "instrument": s.instrument,
                "timestamp": s.timestamp,
                "trigger_price": s.trigger_price,
                "stop_price": s.stop_price,
                "pending": bool((s.metadata or {}).get("pending")),
                "htf_value": (s.metadata or {}).get("htf_value"),
                "mid_value": (s.metadata or {}).get("mid_value"),
                "fast_dema_atr": (s.metadata or {}).get("fast_dema_atr"),
                "trigger_level": (s.metadata or {}).get("trigger_level"),
            })
    # indicator stream snapshots
    stream_keys = [k for k in engine.indicator_engine.stats()["stream_keys"]]
    for key in sorted(stream_keys):
        sid_s, tf_s = key.split(":")
        stream = engine.indicator_engine.get(sid_s, tf_s)
        snap = stream.snapshot()
        out["indicator_streams"][key] = {
            "bar_count": snap.get("bar_count"),
            "dema": stream.dema_value,
            "atr": stream.atr_value,
            "dema_atr": stream.value,
            "dedup_count": snap.get("dedup_count", 0),
        }
        out["dedup"][key] = snap.get("dedup_count", 0)
    # strategies snapshot
    for sid in SIDS:
        strat = engine.strategies.get(sid)
        if strat is None:
            continue
        out["strategies"][sid] = {
            "bars_processed": strat._bars_processed,
            "state": strat.state.value if strat.state else None,
            "position_side": strat.position_side,
            "signals_count": len(getattr(strat, "_signals", [])),
        }
    # DB tables
    try:
        tables = {
            "trades": "SELECT * FROM trades",
            "orders": "SELECT * FROM orders",
            "fills": "SELECT * FROM fills",
            "positions": "SELECT * FROM positions",
            "pnl": ("SELECT strategy_id, trade_count, wins, losses, realized_net "
                    "FROM pnl_summary"),
        }
        for name, sql in tables.items():
            try:
                out[name] = [dict(r) for r in persistence._db.query(sql)]
            except Exception:
                out[name] = []
        try:
            out["persisted_signals"] = [dict(r) for r in
                                        persistence._db.query("SELECT * FROM signals")]
        except Exception:
            out["persisted_signals"] = []
    except Exception as e:
        out["db_error"] = str(e)
    return out


def normalize_record(rec: dict) -> dict:
    """Canonical deterministic projection for checksums (excludes wall-clock
    ids/timestamps that are intentionally nondeterministic)."""
    skip = {"signal_id", "trade_id", "order_id", "fill_id", "position_id",
            "created_at", "updated_at", "event_id", "id", "entry_order_id",
            "exit_order_id", "entry_fill_id", "exit_fill_id", "broker_order_id",
            "timestamp"}
    return {k: v for k, v in sorted(rec.items()) if k not in skip
            and not isinstance(v, (dict, list))}


def checksum(records: list[dict] | list[str]) -> str:
    text = json.dumps([normalize_record(r) if isinstance(r, dict) else r
                       for r in records], sort_keys=True, default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def dump(out_dir: Path, out: dict[str, Any]) -> dict[str, str]:
    out_dir.mkdir(parents=True, exist_ok=True)
    written = {}
    ev = out.pop("evaluation_stream", [])
    plans = {
        "full_replay.json": out,
        "signal_replay.json": {"signals": out["signals"]},
        "persisted_signal_replay.json": {"persisted_signals": out.get("persisted_signals", [])},
        "trade_replay.json": {"trades": out["trades"]},
        "order_replay.json": {"orders": out["orders"]},
        "fill_replay.json": {"fills": out["fills"]},
        "position_replay.json": {"positions": out["positions"]},
        "pnl_replay.json": {"pnl": out["pnl"], "strategies": out["strategies"]},
        "strategy_replay.json": {"strategies": out["strategies"]},
        "crossover_replay.json": {"crossover_log": out.get("crossover_log", {})},
        "db_integrity.json": out.get("db_forensics", {}),
    }
    for name, payload in plans.items():
        (out_dir / name).write_text(json.dumps(payload, indent=1, default=str), encoding="utf-8")
        written[name] = (out_dir / name).stat().st_size
    with (out_dir / "evaluation_stream.jsonl").open("w", encoding="utf-8") as fh:
        for row in ev:
            fh.write(json.dumps(row, default=str) + "\n")
    written["evaluation_stream.jsonl"] = (out_dir / "evaluation_stream.jsonl").stat().st_size
    # checksums
    sums = {
        "evaluation": checksum(ev),
        "signals": checksum(out["signals"]),
        "trades": checksum(out["trades"]),
        "orders": checksum(out["orders"]),
        "fills": checksum(out["fills"]),
        "positions": checksum(out["positions"]),
        "strategies": checksum([{"strategy_id": k, **v} for k, v in out["strategies"].items()]),
    }
    (out_dir / "checksums.json").write_text(json.dumps(sums, indent=1), encoding="utf-8")
    written["checksums.json"] = (out_dir / "checksums.json").stat().st_size
    return written


def fresh_workdir(path: Path) -> Path:
    import shutil
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)
    return path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv-root", type=Path, required=True)
    parser.add_argument("--mode", choices=["parallel", "sequential"], default="parallel")
    parser.add_argument("--workdir", type=Path)
    args = parser.parse_args()

    bars = load_candles(args.csv_root)
    stream = chronological(bars)
    out_dir = ROOT / "replay_output" / "live_replay" / args.mode
    import tempfile
    temp_root = Path(tempfile.gettempdir()) / "opencode" / "live_replay_work"
    base_work = args.workdir or (temp_root / args.mode)

    results: dict[str, Any] = {}
    if args.mode == "parallel":
        workdir = fresh_workdir(base_work / "parallel")
        engine, persistence, cfg = build_engine(workdir)
        try:
            install_crossover_loggers(engine)
            out: dict[str, Any] = {}
            run_replay(engine, persistence, stream, out)
            out.update(collect(engine, persistence, workdir))
            out["crossover_log"] = {sid: engine.strategies.get(sid)._crossover_log
                                    if engine.strategies.get(sid) is not None else []
                                    for sid in SIDS}
            out["db_forensics"] = db_forensics(persistence, workdir)
            results = dump(out_dir, out)
            print(json.dumps({
                "mode": args.mode,
                "signals": len(out["signals"]),
                "trades": len(out["trades"]),
                "orders": len(out["orders"]),
                "fills": len(out["fills"]),
                "positions": len(out["positions"]),
                "strategies": {k: v["signals_count"] for k, v in out["strategies"].items()},
                "artifacts": results,
            }, indent=1))
        finally:
            try:
                engine.stop()
            except Exception:
                pass
            try:
                persistence.close()
            except Exception:
                pass
    else:
        per: dict[str, dict] = {}
        for sid in SIDS:
            workdir = fresh_workdir(base_work / "sequential" / sid)
            engine, persistence, cfg = build_engine(workdir, [sid])
            try:
                install_crossover_loggers(engine)
                out = {}
                run_replay(engine, persistence, stream, out)
                out.update(collect(engine, persistence, workdir))
                per[sid] = {
                    "signals": [s for s in out["signals"] if s["strategy_id"] == sid],
                    "trades": [t for t in out["trades"] if t.get("strategy_id") == sid],
                    "strategies": out["strategies"].get(sid, {}),
                    "crossovers": engine.strategies.get(sid)._crossover_log,
                }
                out_dir.mkdir(parents=True, exist_ok=True)
                (out_dir / f"{sid}.json").write_text(
                    json.dumps(per[sid], indent=1, default=str), encoding="utf-8")
            finally:
                try:
                    engine.stop()
                except Exception:
                    pass
                try:
                    persistence.close()
                except Exception:
                    pass
        seq_out = {"sequential": per,
                   "signals_checksum": {sid: checksum(per[sid]["signals"])
                                        for sid in SIDS},
                   "trades_checksum": {sid: checksum(per[sid]["trades"])
                                       for sid in SIDS}}
        (out_dir / "sequential.json").write_text(
            json.dumps(seq_out, indent=1, default=str), encoding="utf-8")
        print(json.dumps({"mode": args.mode,
                          "signals": {sid: len(per[sid]["signals"]) for sid in SIDS},
                          "trades": {sid: len(per[sid]["trades"]) for sid in SIDS},
                          "artifacts": sorted(str(p.relative_to(ROOT))
                                              for p in out_dir.glob("*"))}, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())