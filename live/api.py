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
        one-time environment token. The test can submit only the configured
        strategy/instrument/quantity and can force only that position's own
        local stop monitor. It is not a general order endpoint.
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

                from strategies.types import Signal, SignalType
                tick = float(_engine.config.instrument(instrument).get("tick_size", 1.0) or 1.0)
                quantity = int(test_cfg["quantity"])
                generation = int(getattr(strategy, "_trigger_generation", 0)) + 1
                strategy._trigger_generation = generation
                signal = Signal(
                    signal_type=SignalType.LONG, instrument=instrument,
                    strategy_id=strategy_id, timestamp=time.time(),
                    trigger_price=observed, stop_price=max(tick, observed - tick),
                    quantity=quantity,
                    metadata={
                        "triggered": True, "trigger_state": "FIRED",
                        "trigger_source": "loopback_live_canary",
                        "trigger_ltp": observed + tick,
                        "trigger_generation": generation,
                        "test_cycle": True,
                    },
                )
                broker_flat, flat_detail = _engine._broker_flat_for_entry(env, signal)
                if not broker_flat:
                    raise HTTPException(status_code=409,
                                        detail={"broker_flat_required": flat_detail})
                strategy._last_fired_trigger_signal_id = signal.signal_id
                strategy._fired_trigger_signal_ids[signal.signal_id] = None
                _live_test_entry_signal_id = signal.signal_id
                _engine._process_signal(signal, "live")
                order = next((o for o in reversed(list(execution._orders.values()))
                              if getattr(o, "parent_signal_id", None) == signal.signal_id
                              or getattr(o, "entry_signal_id", None) == signal.signal_id), None)
                return {
                    "accepted_by_app": order is not None,
                    "signal_id": signal.signal_id,
                    "strategy_id": strategy_id,
                    "instrument": instrument,
                    "quantity": quantity,
                    "reference_ltp": observed,
                    "limit_cap": observed + tick,
                    "stop_price": signal.stop_price,
                    "order": order.to_dict() if order and hasattr(order, "to_dict") else str(order),
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

        if action == "fire_reversal_entry":
            with _live_test_cycle_lock:
                if not _live_test_entry_signal_id:
                    raise HTTPException(status_code=409, detail="test entry has not been started")
                if any(p.is_open for p in env.position_manager.get_positions_by_instrument(instrument)):
                    raise HTTPException(status_code=409, detail="old position is not locally flat")
                pen = strategy.pending_entry
                if pen is None or pen.status != "pending" or not (pen.signal.metadata or {}).get("is_reversal_entry"):
                    raise HTTPException(status_code=409, detail="armed opposite entry required")
                flat, detail = _engine._broker_flat_for_entry(env, pen.signal)
                if not flat:
                    raise HTTPException(status_code=409, detail={"broker_flat_required": detail})
                if strategy.just_entered:
                    raise HTTPException(status_code=409, detail="wait for the next completed candle before reversal entry")
                tick = float(_engine.config.instrument(instrument).get("tick_size", 1.0) or 1.0)
                forced_ltp = pen.trigger_price + tick if pen.side == "LONG" else max(tick, pen.trigger_price - tick)
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
            orders = [o.to_dict() if hasattr(o, "to_dict") else str(o)
                      for o in execution._orders.values()
                      if (getattr(o, "parent_signal_id", None) == signal_id
                          or getattr(o, "entry_signal_id", None) == signal_id
                          or (getattr(o, "trade_id", None) == getattr(strategy, "current_trade_id", None)
                              and getattr(o, "order_role", "") in ("EXIT", "EMERGENCY_EXIT")))]
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
            if not signal_id and not recovery_boot:
                raise HTTPException(status_code=409, detail="test entry has not been started")
            position = next((p for p in env.position_manager.get_positions_by_strategy(strategy_id)
                             if p.instrument == instrument and p.is_open), None)
            if position is None:
                raise HTTPException(status_code=409, detail="open canary position required")
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
