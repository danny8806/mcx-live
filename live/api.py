"""LIVE application FastAPI backend.

Independent backend for the LIVE container: it reuses the same dashboard route
modules but initializes them against the LIVE app's own engine graph, event
bus and persistence (``live/data/db/live_trading.db``).  No DEMO/PAPER runtime
exists anywhere in this process — ``/api/paper/*`` correctly 404s here.
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import hmac
import ipaddress
import json
import logging
import os
import sys
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

_parent = str(Path(__file__).resolve().parent.parent)
if _parent not in sys.path:
    sys.path.insert(0, _parent)

from dashboard.event_bus import EventBus  # noqa: E402
from dashboard.ws_manager import ConnectionManager  # noqa: E402
from dashboard.routes import (  # noqa: E402
    overview, strategies, positions, orders, trades,
    pnl, market_data, risk, health,
    reconciliation, alerts, settings, audit_log, indicators,
    env_switch, broker_evidence, live_ops, reversals,
)
from analytics import routes as analytics_routes  # noqa: E402

logger = logging.getLogger("live.api")

ROUTE_MODULES = [
    analytics_routes,
    overview, strategies, positions, orders, trades, pnl, market_data,
    risk, health, reconciliation, alerts, settings, audit_log,
    indicators, env_switch, broker_evidence, live_ops, reversals,
]

_engine = None            # TradingEngine (live_only)
_bus = None               # UI-facing EventBus (dashboard.event_bus.EventBus)
_persistence = None       # LIVE PersistenceManager
_live_engine = None       # LiveEngine wrapper
_ws_manager = ConnectionManager()
_push_executor: Optional[concurrent.futures.ThreadPoolExecutor] = None
_events_executor: Optional[concurrent.futures.ThreadPoolExecutor] = None
_live_test_cycle_lock = threading.Lock()
_live_test_entry_signal_id: Optional[str] = None

_frontend_dist = Path(__file__).resolve().parent.parent / "dashboard-ui" / "dist"
_frontend_available = _frontend_dist.exists()


def _snapshot_sync():
    if not _engine:
        return None
    return _engine.snapshot("live")


def _enrich_strategies(snap):
    """Enrich LIVE strategy snapshots with P&L (same contract as /api/strategies)."""
    instruments = _engine.config.get("instruments", {})
    strategies_cfg = _engine.config.get("strategies", {})
    enriched = {}
    for name, strat_snap in snap.get("strategies", {}).items():
        cfg = strategies_cfg.get(name, {})
        inst = cfg.get("instrument", "")
        inst_cfg = instruments.get(inst, {})
        strat_snap = strategies._reconcile_open_position(name, dict(strat_snap))
        pnl_engine = _engine.pnl_engines.get(name)
        pnl_snap = pnl_engine.snapshot() if pnl_engine else {}
        val = lambda k, d=0: (pnl_snap.get(k, {}).get("value", d)
                              if isinstance(pnl_snap.get(k), dict) else pnl_snap.get(k, d))
        enriched[name] = {
            **strat_snap,
            "symbol": inst_cfg.get("symbol", strat_snap.get("symbol", inst)),
            "fast_timeframe": cfg.get("fast_timeframe", strat_snap.get("fast_timeframe", "")),
            "htf_timeframe": cfg.get("htf_timeframe", strat_snap.get("htf_timeframe", "")),
            "quantity": cfg.get("quantity", strat_snap.get("quantity", 1)),
            "enabled": bool(strat_snap.get("enabled", cfg.get("enabled", True))),
            "realized_net": val("realized_net"),
            "realized_gross": val("realized_gross"),
            "realized_charges": val("realized_charges"),
            "trade_count": val("trade_count"),
            "wins": val("wins"),
            "losses": val("losses"),
            "win_rate": val("win_rate", 0.0),
        }
    snap["strategies"] = enriched
    return snap


async def _periodic_save_state():
    while True:
        try:
            if _engine and _persistence:
                loop = asyncio.get_event_loop()
                state = await loop.run_in_executor(None, _snapshot_sync)
                if state:
                    await loop.run_in_executor(None, _persistence.save_state, state)
                    await loop.run_in_executor(None, _persistence.save_account_snapshot_from_state, state)
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[LiveSaveState] periodic save failed: {e}", file=sys.stderr, flush=True)
        await asyncio.sleep(60)


async def _push_updates():
    global _push_executor
    if _push_executor is None:
        _push_executor = concurrent.futures.ThreadPoolExecutor(max_workers=2, thread_name_prefix="live-push")
    while True:
        try:
            loop = asyncio.get_running_loop()
            snap = await loop.run_in_executor(_push_executor, _snapshot_sync)
            if snap:
                snap = await loop.run_in_executor(_push_executor, _enrich_strategies, snap)
                await _ws_manager.broadcast("engine_state", snap)
            await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("live push error: %s", e)
            await asyncio.sleep(2.0)


async def _push_events():
    global _events_executor
    if _events_executor is None:
        _events_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix="live-events")
    last_id = -1
    while True:
        try:
            loop = asyncio.get_running_loop()
            events = await loop.run_in_executor(_events_executor, _bus.get_recent, None, 50)
            new_events = [e for e in events if e["id"] > last_id]
            if new_events:
                await _ws_manager.broadcast("events", new_events)
                last_id = new_events[-1]["id"]
            await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            break
        except Exception:
            await asyncio.sleep(1.0)


def _on_engine_event(event_type: str, data: dict):
    if _bus is not None:
        try:
            _bus.publish(event_type, data)
        except Exception:
            pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _engine, _bus, _persistence, _live_engine
    if _live_engine is None:
        from live.engine import LiveEngine
        _live_engine = LiveEngine()
        _engine = _live_engine.engine
        _persistence = _live_engine.persistence
        try:
            _engine._event_callback = _on_engine_event
        except Exception:
            pass
    _live_engine.restore()
    if os.environ.get("LIVE_RECOVERY_ONLY") == "1":
        # Temporary operator recovery boot: restore owned lifecycle state and
        # expose the loopback-only recovery API without waiting for startup
        # warmup/reconciliation.  This mode must be removed after flattening.
        logger.critical("LIVE_RECOVERY_ONLY is set; normal engine startup is paused")
    else:
        _live_engine.start()
    for mod in ROUTE_MODULES:
        if mod is analytics_routes:
            db_path = str(_persistence.db_path) if _persistence is not None else "trading.db"
            strat_ids = list(_engine.strategies.keys()) if _engine is not None else None
            start_eq = _engine.config.get("account.starting_capital") if _engine is not None else None
            analytics_routes.init(db_path=db_path, strategy_ids=strat_ids, starting_equity=start_eq)
            continue
        kwargs = {}
        if _persistence is not None and "persistence" in mod.init.__code__.co_varnames:
            kwargs["persistence"] = _persistence
        mod.init(_engine, _bus, **kwargs)
    tasks = [
        asyncio.create_task(_push_updates()),
        asyncio.create_task(_push_events()),
        asyncio.create_task(_periodic_save_state()),
    ]
    print("[LiveAPI] LIVE backend started", file=sys.stderr, flush=True)
    yield
    for t in tasks:
        t.cancel()
    for t in tasks:
        try:
            await t
        except BaseException:
            pass
    if _live_engine is not None:
        try:
            _live_engine.stop()
        except Exception as e:
            print(f"[LiveAPI] shutdown error: {e}", file=sys.stderr, flush=True)


async def _handle_command(msg: dict, websocket: WebSocket):
    cmd = msg.get("command")
    params = msg.get("params", {})
    if not _engine:
        await websocket.send_text(json.dumps({"type": "error", "data": "Live engine not running"}))
        return
    result = {"command": cmd, "success": False, "data": None}
    try:
        if cmd == "pause_strategy":
            sid = params.get("strategy_id")
            if sid and sid in _engine.strategies:
                cr = _engine.control_strategy(sid, "pause")
                result["success"] = bool(cr.get("success"))
                result["data"] = cr.get("gate") if cr.get("success") else cr.get("error")
                if cr.get("success"):
                    _bus.publish("strategy_control", {"action": "pause", "strategy_id": sid})
        elif cmd == "resume_strategy":
            sid = params.get("strategy_id")
            if sid and sid in _engine.strategies:
                cr = _engine.control_strategy(sid, "resume")
                result["success"] = bool(cr.get("success", False))
                result["data"] = cr.get("gate") if cr.get("success") else cr.get("error")
                if cr.get("success"):
                    _bus.publish("strategy_control", {"action": "resume", "strategy_id": sid})
        elif cmd in ("start_strategy", "stop_strategy", "close_only_strategy",
                     "lock_strategy", "unlock_strategy"):
            sid = params.get("strategy_id")
            action = {"start_strategy": "start", "stop_strategy": "stop",
                      "close_only_strategy": "close_only",
                      "lock_strategy": "lock", "unlock_strategy": "start"}[cmd]
            if sid and sid in _engine.strategies:
                cr = _engine.control_strategy(sid, action)
                result["success"] = bool(cr.get("success", False))
                result["data"] = cr.get("gate") if cr.get("success") else cr.get("error")
                if cr.get("success"):
                    _bus.publish("strategy_control", {"action": action, "strategy_id": sid})
        elif cmd == "emergency_stop":
            for sid, _ in _engine.strategies.items():
                _engine.control_strategy(sid, "emergency_stop")
                _bus.publish("emergency_stop", {"strategy_id": sid})
            result["success"] = True
        elif cmd == "emergency_exit_all":
            result["data"] = _engine.emergency_exit_all("live")
            result["success"] = not bool(result["data"].get("errors"))
            _bus.publish("emergency_exit_all",
                         {"scope": "owned_lifecycles",
                          "submitted": len(result["data"].get("closed", [])),
                          "errors": len(result["data"].get("errors", []))})
        elif cmd == "get_snapshot":
            result["success"] = True
            result["data"] = _engine.snapshot("live")
        elif cmd == "get_trades":
            if _persistence:
                result["success"] = True
                result["data"] = _persistence.get_trades()
    except Exception as e:
        result["data"] = str(e)
    await websocket.send_text(json.dumps({"type": "command_result", "data": result}, default=str))


def create_live_app(live_engine=None) -> FastAPI:
    """Build the LIVE FastAPI app bound to a LiveEngine.

    ``live_engine`` is optional: when omitted, the lifespan auto-creates the
    LIVE engine — useful for the ``live.run`` container entry point.
    """
    global _engine, _bus, _persistence, _live_engine
    _live_engine = live_engine
    _bus = EventBus(max_events=50000)
    if _live_engine is not None:
        _engine = _live_engine.engine
        _persistence = _live_engine.persistence
        try:
            _engine._event_callback = _on_engine_event
        except Exception:
            pass

    app = FastAPI(title="GoldSilver LIVE App", version="1.1.0", lifespan=lifespan)
    _cors_origins = os.getenv("CORS_ORIGINS", "").split(",") if os.getenv("CORS_ORIGINS") else [
        "http://localhost:5173", "http://localhost:5174",
        "http://127.0.0.1:5173", "http://127.0.0.1:5174",
    ]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_cors_origins,
        allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
    )

    @app.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket):
        await websocket.accept()
        cid = f"live_client_{uuid.uuid4().hex[:8]}"
        _ws_manager.connect(cid, websocket, ["all"])
        try:
            while True:
                data = await websocket.receive_text()
                try:
                    msg = json.loads(data)
                    action = msg.get("action")
                    if action == "subscribe":
                        channels = msg.get("channels", ["all"])
                        _ws_manager.subscribe(cid, channels)
                    elif action == "ping":
                        await websocket.send_text(json.dumps({"type": "pong", "ts": time.time()}))
                    elif action == "command":
                        await _handle_command(msg, websocket)
                except json.JSONDecodeError:
                    pass
        except WebSocketDisconnect:
            _ws_manager.disconnect(cid)
        except Exception:
            try:
                _ws_manager.disconnect(cid)
            except Exception:
                pass

    @app.get("/api/health")
    async def api_health():
        return {
            "status": "ok",
            "engine": _engine is not None,
            "persistence": _persistence is not None,
            "live_only": bool(_engine is not None and getattr(_engine, "_live_only", False)),
            "ws_connections": _ws_manager.active_connections,
            "event_bus": _bus.get_stats() if _bus is not None else None,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    @app.post("/api/live/test-order-cycle")
    async def live_test_order_cycle(request: Request, body: dict):
        """Loopback-only, fail-closed canary hook for the real LIVE lifecycle.

        Disabled unless the operator explicitly enables
        ``live_test_order_cycle.enabled`` in live config and provides a
        one-time environment token. The test can arm a trigger only for the
        configured strategy/instrument/quantity. All trigger crossing must
        come from the real Dhan WebSocket; this is not a general order API.
        """
        if not _engine:
            raise HTTPException(status_code=503, detail="live engine unavailable")
        try:
            peer = ipaddress.ip_address(request.client.host if request.client else "")
        except ValueError:
            raise HTTPException(status_code=403, detail="loopback caller required")
        if not peer.is_loopback:
            raise HTTPException(status_code=403, detail="loopback caller required")
        test_cfg = _engine.config.get("live_test_order_cycle", {}) or {}
        if not test_cfg.get("enabled"):
            raise HTTPException(status_code=404, detail="test cycle is disabled")
        expected = os.environ.get("LIVE_TEST_SIGNAL_TOKEN", "")
        supplied = request.headers.get("x-live-test-token", "")
        if not expected or not hmac.compare_digest(expected, supplied):
            raise HTTPException(status_code=403, detail="test token required")

        action = str(body.get("action", "")).lower()
        env = _engine._env_for("live")
        strategy_id = str(test_cfg.get("strategy_id", ""))
        instrument = str(test_cfg.get("instrument", ""))
        strategy = env.strategies.get(strategy_id)
        if (not strategy or not instrument
                or strategy.instrument != instrument
                or int(strategy.quantity) != int(test_cfg.get("quantity", 0))):
            raise HTTPException(status_code=409, detail="configured test strategy mismatch")

        if action == "recover_reversal_exit":
            # Recover one broker-confirmed reversal exit that is already
            # persisted but was rejected before lifecycle booking. This route
            # never places an order; FillFlow revalidates the exact order,
            # fill, open DB owner and broker-flat state before applying it.
            reversals = _persistence.get_reversals(strategy_id, limit=100)
            reversal = next((r for r in reversals
                             if r.get("instrument") == instrument
                             and str(r.get("status", "")).upper() == "PENDING_EXIT"), None)
            if reversal is None:
                raise HTTPException(status_code=409, detail="no pending canary reversal exit")
            fills = [f for f in _persistence.get_fills()
                     if f.get("order_id") == reversal.get("old_exit_order_id")
                     and f.get("broker_fill_id")]
            if len(fills) != 1:
                raise HTTPException(status_code=409, detail={
                    "reason": "expected exactly one persisted broker fill for the reversal exit",
                    "fill_count": len(fills)})
            persisted_fill = fills[0]
            try:
                broker_rows = env.broker.positions()
                for row in broker_rows or []:
                    if row.get("instrument") == instrument and int(row.get("quantity") or 0):
                        raise HTTPException(status_code=409,
                                            detail="broker still reports a non-flat canary position")
            except HTTPException:
                raise
            except Exception as exc:
                raise HTTPException(status_code=503,
                                    detail=f"broker flat check unavailable: {exc}")
            from datetime import datetime
            from execution.models import Fill
            fill_time = persisted_fill.get("timestamp")
            if isinstance(fill_time, str):
                fill_time = datetime.fromisoformat(
                    fill_time.replace("Z", "+00:00")).timestamp()
            fill = Fill(
                fill_id=str(persisted_fill["fill_id"]),
                order_id=str(persisted_fill["order_id"]),
                instrument=str(persisted_fill["instrument"]),
                side=str(persisted_fill["side"]).upper(),
                quantity=int(persisted_fill["quantity"]),
                price=float(persisted_fill["price"]),
                timestamp=float(fill_time),
                strategy_id=str(persisted_fill["strategy_id"]),
                multiplier=float(persisted_fill.get("multiplier") or 1.0),
                trade_id=persisted_fill.get("trade_id"),
                lifecycle_id=persisted_fill.get("lifecycle_id"),
                position_id=persisted_fill.get("position_id"),
                position_generation=persisted_fill.get("position_generation"),
            )
            fill.broker_fill_id = persisted_fill["broker_fill_id"]
            fill.broker_order_id = persisted_fill.get("broker_order_id")
            fill.broker_trade_id = persisted_fill.get("broker_trade_id")
            fill.cumulative_filled_quantity = persisted_fill.get(
                "cumulative_filled_quantity")
            if not _engine._is_replayable_stale_exit(env, fill):
                order = env.execution_engine.get_order(fill.order_id)
                state_obj = getattr(order, "state", None) if order else None
                owner_id = ((getattr(order, "parent_position_id", None)
                             or getattr(order, "position_id", None))
                            if order else None) or reversal.get("old_position_id")
                raise HTTPException(status_code=409, detail={
                    "reason": "strict stale-exit ownership validation failed",
                    "order_found": order is not None,
                    "order_state": str(getattr(state_obj, "value", state_obj)),
                    "order_role": getattr(order, "order_role", None),
                    "owner_position_id": owner_id,
                    "db_open_owner_found": any(
                        p.get("position_id") == owner_id
                        and p.get("exit_order_id") == fill.order_id
                        for p in _persistence.get_open_positions(strategy_id)),
                    "persisted_fill_found": bool(_persistence.fill_by_broker_fill_id(
                        fill.broker_fill_id)),
                    "broker_position_count": len(env.broker.positions() or []),
                })
            _engine._handle_fill(fill, reversal.get("signal_id"),
                                 is_exit=True, env_name="live")
            closed_trade = next((t for t in _persistence.get_trades(strategy_id)
                                 if t.get("trade_id") == reversal.get("old_trade_id")), None)
            updated_reversal = next((r for r in _persistence.get_reversals(
                strategy_id, limit=100)
                if r.get("reversal_id") == reversal.get("reversal_id")), None)
            if (not closed_trade
                    or str(closed_trade.get("status", "")).upper() != "CLOSED"
                    or not updated_reversal
                    or str(updated_reversal.get("status", "")).upper() == "PENDING_EXIT"):
                raise HTTPException(status_code=409,
                                    detail="fill recovery was not applied; lifecycle remains unresolved")
            return {
                "replayed": True,
                "order_id": fill.order_id,
                "broker_fill_id": fill.broker_fill_id,
                "trade_id": reversal.get("old_trade_id"),
                "reversal": updated_reversal,
                "broker_flat_verified": True,
                "note": "persisted exit fill routed through normal FillFlow; no order sent",
            }

        # Conformance mode is intentionally WebSocket-driven.  These older
        # helper actions used to fabricate an adverse LTP or call on_tick()
        # directly; that is useful for unit tests, but cannot prove the live
        # feed/trigger contract required by the controlled live validation.
        if action in {"fire_reversal_exit", "fire_reversal_entry", "fire_test_stop"}:
            raise HTTPException(
                status_code=409,
                detail="synthetic trigger firing is disabled; wait for a real Dhan WebSocket tick",
            )

        if action == "entry":
            global _live_test_entry_signal_id
            with _live_test_cycle_lock:
                if _live_test_entry_signal_id:
                    raise HTTPException(status_code=409, detail="one-shot test already started")
                # A canary entry is permitted only when every other strategy is
                # prevented from entering, even if an operator forgot to pause it.
                gates = getattr(_engine, "_strategy_gates", {}) or {}
                for sid, other in env.strategies.items():
                    if sid == strategy_id:
                        continue
                    gate = gates.get(sid)
                    if getattr(other, "enabled", True) and (gate is None or gate.entries_allowed):
                        raise HTTPException(status_code=409, detail=f"other strategy {sid} can enter")
                gate = _engine._gate_for(strategy_id)
                if not getattr(env, "gate_enabled", False) or not gate.entries_allowed:
                    raise HTTPException(status_code=409, detail="test strategy live entry gate is closed")
                if any(p.is_open for p in env.position_manager.get_positions_by_instrument(instrument)):
                    raise HTTPException(status_code=409, detail="test instrument already has a local position")
                health = getattr(_engine, "market_data_health", None)
                if health is None or not health.is_healthy(instrument):
                    raise HTTPException(status_code=409, detail="test instrument feed is not fresh")
                execution = env.execution_engine
                with execution._price_lock:
                    observed = float(execution._current_prices.get(instrument, 0.0) or 0.0)
                if observed <= 0:
                    raise HTTPException(status_code=409, detail="no live tick price for test instrument")
                # Candle context is taken from the production REST adapter.
                # Never synthesize OHLC from the live WebSocket tick stream.
                try:
                    candle_state = await asyncio.wait_for(
                        asyncio.to_thread(
                            env.data_adapter.fetch_candle_state, instrument, "5"),
                        timeout=12.0,
                    )
                except Exception as exc:
                    raise HTTPException(
                        status_code=503,
                        detail=f"recent closed REST candle unavailable: {exc}",
                    )
                candles = (candle_state or {}).get("closed") or []
                if not candles:
                    raise HTTPException(status_code=409,
                                        detail="no completed REST candle for the test signal")
                candle = candles[-1]
                if len(candle) < 5:
                    raise HTTPException(status_code=503,
                                        detail="REST candle row is incomplete")
                candle_ts = float(candle[0])
                candle_open, candle_high, candle_low, candle_close = map(
                    float, candle[1:5])
                now = time.time()
                if candle_ts + 300.0 > now or now - (candle_ts + 300.0) > 900.0:
                    raise HTTPException(status_code=409,
                                        detail="REST candle is not recently completed")
                if (min(candle_open, candle_high, candle_low, candle_close) <= 0
                        or candle_high < max(candle_open, candle_close, candle_low)
                        or candle_low > min(candle_open, candle_close, candle_high)):
                    raise HTTPException(status_code=503,
                                        detail="REST candle OHLC validation failed")

                from strategies.types import PendingEntry, Signal, SignalType, StrategyState
                tick = float(_engine.config.instrument(instrument).get("tick_size", 1.0) or 1.0)
                quantity = int(test_cfg["quantity"])
                side = str(body.get("side", "LONG")).upper()
                if side not in ("LONG", "SHORT"):
                    raise HTTPException(status_code=422, detail="side must be LONG or SHORT")
                trigger_price = (observed + tick if side == "LONG"
                                 else max(tick, observed - tick))
                stop_price = candle_low if side == "LONG" else candle_high
                if ((side == "LONG" and stop_price >= trigger_price)
                        or (side == "SHORT" and stop_price <= trigger_price)):
                    raise HTTPException(
                        status_code=409,
                        detail="latest REST candle stop level is invalid for the current trigger",
                    )
                test_run_id = str(body.get("test_run_id") or uuid.uuid4())
                generation = int(getattr(strategy, "_trigger_generation", 0)) + 1
                strategy._trigger_generation = generation
                signal = Signal(
                    signal_type=SignalType(side), instrument=instrument,
                    strategy_id=strategy_id, timestamp=time.time(),
                    # The controlled signal stops at the signal/trigger
                    # boundary.  A real Dhan WebSocket tick must cross this
                    # one-tick-away level before production SignalFlow can
                    # create any order.
                    trigger_price=trigger_price,
                    stop_price=stop_price,
                    quantity=quantity,
                    side=side,
                    metadata={
                        "pending": True, "triggered": False,
                        "trigger_state": "ARMED",
                        "trigger_source": "market_websocket_ltp",
                        "trigger_generation": generation,
                        "test_cycle": True,
                        "test_mode": True,
                        "test_run_id": test_run_id,
                        "signal_candle_timestamp": candle_ts,
                        "signal_candle_open": candle_open,
                        "signal_candle_high": candle_high,
                        "signal_candle_low": candle_low,
                        "signal_candle_close": candle_close,
                        "candle_source": "dhan_rest",
                    },
                )
                broker_flat, flat_detail = _engine._broker_flat_for_entry(env, signal)
                if not broker_flat:
                    raise HTTPException(status_code=409,
                                        detail={"broker_flat_required": flat_detail})
                strategy.pending_entry = PendingEntry(
                    signal=signal, trigger_price=signal.trigger_price,
                    side=side, created_at=time.time(), status="pending")
                strategy.state = (StrategyState.PENDING_LONG if side == "LONG"
                                  else StrategyState.PENDING_SHORT)
                _live_test_entry_signal_id = signal.signal_id
                _engine._process_signal(signal, "live")
                pending_row = _engine._live_pending_row(env, signal)
                if (not pending_row
                        or str(pending_row.get("status", "")).lower() != "armed"):
                    strategy._cancel_trigger(strategy.pending_entry)
                    strategy.pending_entry = None
                    strategy.state = StrategyState.FLAT
                    _live_test_entry_signal_id = None
                    raise HTTPException(
                        status_code=409,
                        detail="production signal flow did not persist and arm the test trigger",
                    )
                return {
                    "accepted_by_app": True,
                    "signal_id": signal.signal_id,
                    "strategy_id": strategy_id,
                    "instrument": instrument,
                    "quantity": quantity,
                    "reference_ltp": observed,
                    "trigger_price": signal.trigger_price,
                    "signal_candle_timestamp": candle_ts,
                    "signal_candle": {
                        "open": candle_open,
                        "high": candle_high,
                        "low": candle_low,
                        "close": candle_close,
                        "source": "dhan_rest",
                    },
                    "trigger_state": "ARMED",
                    "trigger_source": "market_websocket_ltp",
                    "stop_price": signal.stop_price,
                    "order_created": False,
                    "note": "controlled signal is persisted as ARMED; only a real Dhan WebSocket crossing may enter production order flow",
                }

        if action == "opposite_signal":
            # Exercise the strategy's normal reversal construction.  The
            # reversal exit is armed first and its opposite entry remains
            # waiting for a confirmed flat broker book.
            with _live_test_cycle_lock:
                if not _live_test_entry_signal_id:
                    raise HTTPException(status_code=409, detail="test entry has not been started")
                positions = [p for p in env.position_manager.get_positions_by_strategy(strategy_id)
                             if p.instrument == instrument and p.is_open]
                if len(positions) != 1:
                    raise HTTPException(status_code=409, detail="exactly one open canary position required")
                position = positions[0]
                if int(position.quantity) != 1 or int(test_cfg["quantity"]) != 1:
                    raise HTTPException(status_code=409, detail="reversal test is restricted to quantity one")
                if strategy.just_entered:
                    raise HTTPException(status_code=409, detail="wait for the next completed candle after entry")
                health = getattr(_engine, "market_data_health", None)
                if health is None or not health.is_healthy(instrument):
                    raise HTTPException(status_code=409, detail="test instrument feed is not fresh")
                execution = env.execution_engine
                with execution._price_lock:
                    observed = float(execution._current_prices.get(instrument, 0.0) or 0.0)
                tick = float(_engine.config.instrument(instrument).get("tick_size", 1.0) or 1.0)
                side = "SHORT" if position.is_long else "LONG"
                signal = strategy._create_reversal_signal(
                    side, observed, observed + tick, max(tick, observed - tick),
                    time.time(), prev_high=observed + tick,
                    prev_low=max(tick, observed - tick), open_=observed)
                (signal.metadata or {})["test_cycle"] = True
                if strategy.pending_entry is not None:
                    (strategy.pending_entry.signal.metadata or {})["test_cycle"] = True
                _engine._bind_signal_position(signal, strategy, "live")
                _engine._process_signal(signal, "live")
                pending = strategy.pending_exit_trigger
                return {
                    "accepted_by_app": pending is not None,
                    "reversal_exit_signal_id": signal.signal_id,
                    "reversal_side": side,
                    "quantity": 1,
                    "exit_trigger": pending.trigger_price if pending else None,
                    "opposite_entry_trigger": (strategy.pending_entry.trigger_price
                                                if strategy.pending_entry else None),
                    "position_id": position.position_id,
                    "note": "strategy reversal armed; no opposite entry is sent before broker-confirmed flat",
                }

        if action == "fire_reversal_exit":
            with _live_test_cycle_lock:
                if not _live_test_entry_signal_id:
                    raise HTTPException(status_code=409, detail="test entry has not been started")
                pending = strategy.pending_exit_trigger
                if pending is None or pending.status != "pending":
                    raise HTTPException(status_code=409, detail="armed reversal exit required")
                if strategy.just_entered:
                    raise HTTPException(status_code=409, detail="wait for the next completed candle after entry")
                tick = float(_engine.config.instrument(instrument).get("tick_size", 1.0) or 1.0)
                forced_ltp = pending.trigger_price - tick if pending.side == "SHORT" else pending.trigger_price + tick
                signal = strategy.on_tick(forced_ltp, time.time())
                if signal is None:
                    raise HTTPException(status_code=409, detail="strategy reversal trigger did not fire")
                _engine._bind_signal_position(signal, strategy, "live")
                _engine._process_signal(signal, "live")
                return {"fired": True, "signal_id": signal.signal_id,
                        "forced_ltp": forced_ltp, "trigger_price": pending.trigger_price,
                        "note": "trigger passed through StrategyInstance.on_tick and standard exit lifecycle"}

        if action == "restore_reversal_entry":
            # Restart-safe re-arm for the exact durable opposite breakout. The
            # original signal and pending-order rows remain authoritative; this
            # only rebuilds StrategyInstance's in-memory trigger after a prior
            # close/restart cleared it. No broker order is sent here.
            reversals = _persistence.get_reversals(strategy_id, limit=100)
            reversal = next((r for r in reversals
                             if r.get("instrument") == instrument
                             and str(r.get("status", "")).upper() == "EXIT_FILLED"), None)
            if reversal is None:
                raise HTTPException(status_code=409, detail="reversal exit must be filled first")
            if any(p.is_open for p in env.position_manager.get_positions_by_instrument(instrument)):
                raise HTTPException(status_code=409, detail="local position must be flat")
            try:
                broker_rows = env.broker.positions()
                if any(row.get("instrument") == instrument
                       and int(row.get("quantity") or 0) != 0
                       for row in broker_rows or []):
                    raise HTTPException(status_code=409, detail="broker position must be flat")
            except HTTPException:
                raise
            except Exception as exc:
                raise HTTPException(status_code=503,
                                    detail=f"broker flat check unavailable: {exc}")
            parent_signal = _persistence.get_signal(reversal.get("signal_id"))
            reversal_side = str((parent_signal or {}).get("side", "")).upper()
            if reversal_side not in ("LONG", "SHORT"):
                raise HTTPException(status_code=409,
                                    detail="durable reversal signal side is unavailable")
            pending = next((p for p in _persistence.get_pending_orders(
                status="armed", execution_mode="LIVE")
                if p.get("strategy_id") == strategy_id
                and p.get("instrument") == instrument
                and str(p.get("side", "")).upper() == reversal_side), None)
            if pending is None:
                raise HTTPException(status_code=409, detail="durable opposite breakout is not armed")
            signal_row = _persistence.get_signal(pending.get("signal_id"))
            if signal_row is None:
                raise HTTPException(status_code=409, detail="durable opposite signal is missing")
            generation = int(pending.get("trigger_generation") or
                             strategy._trigger_generation or 1)
            side = str(pending.get("side", "")).upper()
            from strategies.types import PendingEntry, Signal, SignalType, StrategyState
            signal = Signal(
                signal_type=SignalType(side), instrument=instrument,
                strategy_id=strategy_id,
                timestamp=float(signal_row.get("signal_timestamp") or time.time()),
                trigger_price=float(pending.get("trigger_price") or 0),
                stop_price=float(signal_row.get("stop_price") or 0),
                quantity=1,
                metadata={
                    "pending": True, "triggered": False,
                    "trigger_state": "ARMED", "trigger_generation": generation,
                    "trigger_source": "market_websocket_ltp",
                    "is_reversal": True, "is_reversal_entry": True,
                    "reversal_parent_signal_id": reversal.get("signal_id"),
                    "test_cycle": True,
                },
            )
            signal.signal_id = str(pending.get("signal_id"))
            strategy._trigger_generation = max(strategy._trigger_generation, generation)
            strategy.pending_entry = PendingEntry(
                signal=signal, trigger_price=float(pending["trigger_price"]),
                side=side, status="pending",
                created_at=float(pending.get("signal_timestamp") or time.time()))
            strategy.state = (StrategyState.PENDING_LONG if side == "LONG"
                              else StrategyState.PENDING_SHORT)
            strategy.stop_price = signal.stop_price
            return {"restored": True, "signal_id": signal.signal_id,
                    "side": side, "quantity": 1,
                    "trigger_price": signal.trigger_price,
                    "stop_price": signal.stop_price,
                    "reversal_id": reversal.get("reversal_id"),
                    "note": "durable test reversal trigger re-armed in strategy memory; no order sent"}

        if action == "fire_reversal_entry":
            pen = strategy.pending_entry
            with _live_test_cycle_lock:
                cycle_survived_restart = bool(
                    pen is not None
                    and (pen.signal.metadata or {}).get("test_cycle"))
                if not _live_test_entry_signal_id and not cycle_survived_restart:
                    raise HTTPException(status_code=409, detail="test entry has not been started")
                if any(p.is_open for p in env.position_manager.get_positions_by_instrument(instrument)):
                    raise HTTPException(status_code=409, detail="old position is not locally flat")
                if pen is None or pen.status != "pending" or not (pen.signal.metadata or {}).get("is_reversal_entry"):
                    raise HTTPException(status_code=409, detail="armed opposite entry required")
                flat, detail = _engine._broker_flat_for_entry(env, pen.signal)
                if not flat:
                    raise HTTPException(status_code=409, detail={"broker_flat_required": detail})
                if strategy.just_entered:
                    raise HTTPException(status_code=409, detail="wait for the next completed candle before reversal entry")
                tick = float(_engine.config.instrument(instrument).get("tick_size", 1.0) or 1.0)
                with execution._price_lock:
                    observed = float(execution._current_prices.get(instrument, 0.0) or 0.0)
                if observed <= 0:
                    raise HTTPException(status_code=409, detail="no live tick price for opposite entry")
                forced_ltp = (max(observed, pen.trigger_price + tick)
                              if pen.side == "LONG"
                              else max(tick, min(observed, pen.trigger_price - tick)))
                signal = strategy.on_tick(forced_ltp, time.time())
                if signal is None:
                    raise HTTPException(status_code=409, detail="strategy opposite-entry trigger did not fire")
                _engine._process_signal(signal, "live")
                return {"fired": True, "signal_id": signal.signal_id,
                        "side": pen.side, "quantity": 1,
                        "forced_ltp": forced_ltp, "trigger_price": pen.trigger_price,
                        "note": "broker-flat checked; entry passed through standard live signal flow"}

        if action == "status":
            with _live_test_cycle_lock:
                signal_id = _live_test_entry_signal_id
            if not signal_id:
                raise HTTPException(status_code=409, detail="test entry has not been started")
            execution = env.execution_engine
            anchor = next((o for o in execution._orders.values()
                          if (getattr(o, "parent_signal_id", None) == signal_id
                              or getattr(o, "entry_signal_id", None) == signal_id)), None)
            start_at = getattr(anchor, "created_at", float("inf"))
            orders = [o.to_dict() if hasattr(o, "to_dict") else str(o)
                      for o in execution._orders.values()
                      if (getattr(o, "strategy_id", None) == strategy_id
                          and getattr(o, "instrument", None) == instrument
                          and float(getattr(o, "created_at", 0) or 0) >= start_at)]
            positions = [{
                "position_id": p.position_id, "trade_id": p.trade_id,
                "quantity": p.quantity, "is_open": p.is_open,
                "sl_state": p.sl_state, "stop_price": p.stop_price,
            } for p in env.position_manager.get_positions_by_strategy(strategy_id)
              if p.instrument == instrument]
            return {"signal_id": signal_id, "orders": orders, "positions": positions}

        if action == "fire_test_stop":
            with _live_test_cycle_lock:
                signal_id = _live_test_entry_signal_id
            recovery_boot = os.environ.get("LIVE_RECOVERY_ONLY") == "1"
            position = next((p for p in env.position_manager.get_positions_by_strategy(strategy_id)
                             if p.instrument == instrument and p.is_open), None)
            if position is None:
                raise HTTPException(status_code=409, detail="open canary position required")
            owned_test_position = any(
                r.get("new_position_id") == position.position_id
                and r.get("strategy_id") == strategy_id
                and r.get("instrument") == instrument
                and str(r.get("status", "")).upper() == "COMPLETE"
                for r in _persistence.get_reversals(strategy_id, limit=100))
            if not signal_id and not recovery_boot and not owned_test_position:
                raise HTTPException(status_code=409, detail="test entry has not been started")
            # A tick may already have fired the SL while its normal submit
            # path stalled before reaching Dhan. For this isolated test, also
            # allow a lifecycle-owned emergency flatten after restart when
            # the saved position is still open. Never duplicate a broker-backed
            # exit, and cancel only a provably local CREATED exit intent.
            execution = env.execution_engine
            exits = [o for o in execution._orders.values()
                     if (getattr(o, "parent_position_id", None) == position.position_id
                         or (recovery_boot
                             and getattr(o, "trade_id", None) == position.trade_id
                             and getattr(o, "strategy_id", None) == strategy_id
                             and getattr(o, "instrument", None) == instrument
                             and str(getattr(o, "side", "")).upper()
                                 == ("SELL" if position.is_long else "BUY")))
                     and str(getattr(o, "order_role", "")).upper()
                     in ("EXIT", "REVERSAL_EXIT", "EMERGENCY_EXIT")
                     and str(getattr(getattr(o, "state", None), "value",
                                     getattr(o, "state", ""))).lower()
                     in ("created", "submitted", "acknowledged", "partially_filled")]
            if any(getattr(o, "_broker_order_id", None)
                   or str(getattr(getattr(o, "state", None), "value",
                                  getattr(o, "state", ""))).lower() != "created"
                   for o in exits):
                raise HTTPException(status_code=409,
                                    detail="broker-backed canary exit is active; refusing duplicate flatten")
            if len(exits) > 1:
                raise HTTPException(status_code=409,
                                    detail="multiple local canary exits require reconciliation")
            stale = exits[0] if exits else None
            if stale is not None:
                if not execution.cancel_order(stale.order_id):
                    raise HTTPException(status_code=409,
                                        detail="unsubmitted canary exit could not be retired")
            if str(getattr(position, "sl_state", "")).upper() == "EXITING":
                _engine._release_sl_after_failed_exit(
                    env, position, reason="test_recovery_unsubmitted_exit")
            if not recovery_boot:
                # Test the ordinary position-owned local SL signal and normal
                # exit order/fill lifecycle, with a single adverse test tick.
                if int(position.quantity) != 1 or int(test_cfg["quantity"]) != 1:
                    raise HTTPException(status_code=409, detail="stop test is restricted to quantity one")
                if position.stop_price is None or float(position.stop_price) <= 0:
                    raise HTTPException(status_code=409, detail="position has no armed stop price")
                adverse_ltp = (float(position.stop_price) - 1.0 if position.is_long
                               else float(position.stop_price) + 1.0)
                sl_signal = _engine._evaluate_position_sl(
                    env, position, adverse_ltp, env_name="live")
                return {
                    "sl_triggered": sl_signal is not None,
                    "signal_id": getattr(sl_signal, "signal_id", None),
                    "forced_ltp": adverse_ltp,
                    "stop_price": position.stop_price,
                    "position_closed": not position.is_open,
                    "note": "normal position-owned SL monitor and direct exit lifecycle",
                }

            result = _engine.emergency_exit_all("live", instrument=instrument)
            # In recovery-only mode the standard poller is intentionally not
            # running.  Poll just the newly submitted canary exit through the
            # ordinary transport/fill router so the same lifecycle can close.
            if os.environ.get("LIVE_RECOVERY_ONLY") == "1":
                try:
                    from core.trade_close import TradeCloseManager
                    if env.trade_close_manager is None:
                        close_manager = TradeCloseManager(
                            position_manager=env.position_manager,
                            pnl_engines=env.pnl_engines,
                            account_engines=env.account_engines,
                            global_account=env.account_engine,
                            risk_engine=env.risk_engine,
                            persistence=env.persistence,
                            event_store=env.event_store,
                            telegram=_engine.telegram,
                            event_callback=_engine._event_callback,
                            trade_ledger=env.trade_ledger,
                        )
                        env.trade_close_manager = close_manager
                        _engine._trade_close_manager = close_manager
                    for _ in range(8):
                        statuses = env.broker.order_statuses() or {}
                        fills = env.execution_engine.apply_broker_statuses(statuses)
                        for fill in fills:
                            order = env.execution_engine.get_order(fill.order_id)
                            if order is not None and env.persistence is not None:
                                try:
                                    from types import SimpleNamespace
                                    _engine._persist_order(
                                        order,
                                        SimpleNamespace(
                                            signal_id=(getattr(order, "parent_signal_id", None)
                                                       or getattr(order, "entry_signal_id", None)),
                                            trigger_price=getattr(order, "price", 0.0),
                                        ),
                                        env_name="live",
                                    )
                                except Exception:
                                    logger.exception(
                                        "canary recovery order persistence failed: %s",
                                        order.order_id,
                                    )
                            env.broker_router.route_fill(
                                fill,
                                lambda f, sid, is_exit: _engine._handle_fill(
                                    f, sid, is_exit=is_exit, env_name="live"),
                                entry_signal_id=getattr(fill, "entry_signal_id", None),
                                is_exit=True,
                            )
                        if not position.is_open:
                            break
                        time.sleep(1.0)
                except Exception as exc:
                    logger.exception("canary recovery fill poll failed: %s", exc)
            return {
                "recovered_unsubmitted_exit_id": stale.order_id if stale else None,
                "emergency_exit": result,
                "position_closed": not position.is_open,
                "note": "lifecycle-owned emergency flatten requested for the single canary position",
            }

        raise HTTPException(status_code=400, detail="unsupported canary action")

    for r in ROUTE_MODULES:
        app.include_router(r.router)

    # Container health/readiness/metrics (unauthenticated by design)
    from services.safety import bind_health_endpoints
    bind_health_endpoints(
        app,
        service="live-mcx",
        version="1.1.0",
        checks={"engine": lambda: _engine is not None,
                "persistence": lambda: _persistence is not None,
                "live_only": lambda: bool(_engine is not None and getattr(_engine, "_live_only", False))},
        start_wall_clock=time.monotonic(),
    )

    if _frontend_available:
        app.mount("/assets", StaticFiles(directory=str(_frontend_dist / "assets")), name="live-static-assets")
        from dashboard.frontend import configured_index_response as _configured_index_response

        _LIVE_API_BASE = os.getenv("APP_API_BASE", "")
        _LIVE_WS_BASE = os.getenv("APP_WS_BASE", "")

        @app.get("/{full_path:path}", include_in_schema=False)
        async def serve_frontend(full_path: str):
            if full_path.startswith("api/") or full_path.startswith("ws"):
                raise HTTPException(status_code=404)
            file_path = _frontend_dist / full_path
            if file_path.is_file():
                return FileResponse(str(file_path))
            return _configured_index_response(_frontend_dist, _LIVE_API_BASE, _LIVE_WS_BASE)

    return app
