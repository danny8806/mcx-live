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
_broker_position_adoption_lock = threading.Lock()

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


def _adopt_broker_position(body: dict) -> dict:
    """Import an exact filled Dhan order as a locally tracked position."""
    from execution.models import Fill, Order, OrderState
    from strategies.intent import entry_levels
    from strategies.types import Signal, SignalType, freeze_signal_context

    if not _engine or not _persistence:
        raise HTTPException(status_code=503, detail="live engine unavailable")
    if not _broker_position_adoption_lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="position adoption already running")
    try:
        strategy_id = str(body.get("strategy_id") or "")
        broker_order_id = str(body.get("broker_order_id") or "")
        try:
            candle_timestamp = float(body.get("signal_candle_timestamp"))
        except (TypeError, ValueError):
            raise HTTPException(status_code=422,
                                detail="signal_candle_timestamp is required")
        env = _engine.live
        strategy_cfg = (_engine.config.get("strategies", {}) or {}).get(strategy_id)
        strategy = (getattr(env, "strategies", {}) or {}).get(strategy_id)
        runtime = env.runtimes.require(strategy_id)
        if (not strategy_cfg or not strategy or strategy_cfg.get("instrument") != "SILVERM"
                or not bool(strategy_cfg.get("enabled"))
                or str(strategy_cfg.get("fast_timeframe", "")).lower() != "15m"):
            raise HTTPException(status_code=409,
                                detail="selected strategy is not an enabled SILVERM 15m strategy")
        if not broker_order_id:
            raise HTTPException(status_code=422, detail="broker_order_id is required")

        # Repeated requests are idempotent and return the already-imported row.
        for existing in (getattr(env.execution_engine, "_orders", {}) or {}).values():
            if str(getattr(existing, "_broker_order_id", "")) == broker_order_id:
                pos = next((p for p in runtime.position_manager.get_positions_by_strategy(
                    strategy_id) if p.instrument == "SILVERM" and p.is_open), None)
                if pos is None:
                    raise HTTPException(status_code=409,
                                        detail="imported order exists without its open position")
                return {"adopted": True, "already_adopted": True,
                        "broker_order_id": broker_order_id,
                        "position": pos.snapshot()}

        broker = env.broker
        day_orders_fn = getattr(broker, "day_order_book", None)
        tradebook_fn = getattr(broker, "tradebook", None)
        if not callable(day_orders_fn) or not callable(tradebook_fn):
            raise HTTPException(status_code=503,
                                detail="Dhan orderbook/tradebook reconciliation unavailable")
        day_orders = day_orders_fn() or []
        broker_order = next((o for o in day_orders
                             if str(o.get("broker_order_id") or "") == broker_order_id), None)
        if (broker_order is None or broker_order.get("status") != "filled"
                or str(broker_order.get("side") or "").upper() != "BUY"
                or int(broker_order.get("quantity") or 0) != 1
                or int(broker_order.get("filled_quantity") or 0) != 1
                or str(broker_order.get("security_id") or "") != "483080"):
            raise HTTPException(status_code=409,
                                detail="Dhan order is not a filled SILVERM BUY for quantity 1")

        trades = tradebook_fn() or []
        order_trades = [t for t in trades
                        if str(t.get("orderId") or t.get("order_id") or "")
                        == broker_order_id
                        and str(t.get("securityId") or t.get("security_id") or "")
                        == "483080"]
        if len(order_trades) != 1:
            raise HTTPException(status_code=409,
                                detail="expected one exact Dhan exchange fill for this order")
        broker_trade = order_trades[0]
        broker_fill_id = str(broker_trade.get("exchangeTradeId")
                             or broker_trade.get("tradeId") or "")
        fill_price = float(broker_trade.get("tradedPrice") or 0.0)
        fill_qty = int(broker_trade.get("tradedQuantity") or 0)
        if (not broker_fill_id or fill_price <= 0 or fill_qty != 1
                or str(broker_trade.get("transactionType") or "").upper() != "BUY"):
            raise HTTPException(status_code=409,
                                detail="Dhan tradebook fill identity/side/quantity is invalid")
        if _persistence.fill_by_broker_fill_id(broker_fill_id):
            raise HTTPException(status_code=409,
                                detail="Dhan exchange fill is already in the local ledger")

        # Confirm that this is still the instrument's complete open net and
        # that no more recent fill superseded the selected manual entry.
        broker_rows = broker.positions() or []
        nets = {(str(r.get("side") or "").upper(), abs(int(r.get("quantity") or 0)))
                for r in broker_rows if r.get("instrument") == "SILVERM"
                and int(r.get("quantity") or 0) != 0}
        if nets != {("LONG", 1)}:
            raise HTTPException(status_code=409,
                                detail=f"current Dhan SILVERM net does not equal this 1-lot LONG: {sorted(nets)}")
        all_silver_trades = [t for t in trades
                             if str(t.get("securityId") or t.get("security_id") or "")
                             == "483080"]
        if not all_silver_trades:
            raise HTTPException(status_code=409, detail="Dhan SILVERM tradebook is empty")
        latest_trade = max(all_silver_trades,
                           key=lambda t: str(t.get("exchangeTime") or t.get("createTime") or ""))
        latest_order_id = str(latest_trade.get("orderId") or latest_trade.get("order_id") or "")
        if latest_order_id != broker_order_id:
            raise HTTPException(status_code=409,
                                detail="a later SILVERM tradebook fill exists; refusing stale adoption")
        signed_tradebook_qty = sum(
            (1 if str(t.get("transactionType") or "").upper() == "BUY" else -1)
            * int(t.get("tradedQuantity") or 0) for t in all_silver_trades)
        if signed_tradebook_qty != 1:
            raise HTTPException(status_code=409,
                                detail="Dhan tradebook net does not prove a single remaining long")
        for rt in env.runtimes.all():
            sid = rt.strategy_id
            if any(p.instrument == "SILVERM" and p.is_open
                   for p in rt.position_manager.get_positions_by_strategy(sid)):
                raise HTTPException(status_code=409,
                                    detail="a local SILVERM position already exists")

        adapter = getattr(env, "data_adapter", None)
        if adapter is None:
            raise HTTPException(status_code=503, detail="REST candle adapter unavailable")
        candle_state = adapter.fetch_candle_state("SILVERM", "15") or {}
        closed = candle_state.get("closed") or []
        if len(closed) < 2:
            raise HTTPException(status_code=409,
                                detail="two completed 15m candles are required for the structural stop")
        candle = next((c for c in closed if float(c[0]) == candle_timestamp), None)
        if candle is None:
            raise HTTPException(status_code=409,
                                detail="requested SILVERM 15m signal candle is not present in completed candles")
        idx = closed.index(candle)
        if idx == 0:
            history_fn = getattr(adapter, "fetch_historical_candles", None)
            if callable(history_fn):
                try:
                    signal_day = datetime.fromtimestamp(
                        candle_timestamp, timezone.utc).date()
                    today = datetime.now(timezone.utc).date()
                    history = history_fn("SILVERM", "15", signal_day, today) or []
                    # Historical REST is the full-day source of truth. When a
                    # short-window response disagrees for the same timestamp,
                    # let the historical row win (it carries the complete
                    # finalized OHLC for the signal candle).
                    by_start = {float(c[0]): c for c in [*closed, *history]}
                    closed = sorted(by_start.values(), key=lambda c: float(c[0]))
                    candle = next((c for c in closed
                                   if float(c[0]) == candle_timestamp), None)
                    idx = closed.index(candle) if candle is not None else 0
                except Exception as exc:
                    raise HTTPException(
                        status_code=503,
                        detail=f"historical candle check for the structural stop failed: {exc}")
            if candle is None or idx == 0:
                raise HTTPException(status_code=409,
                                    detail="prior candle is unavailable for stop calculation")
        candle_end = candle_timestamp + 900.0
        fill_time_text = (broker_trade.get("exchangeTime")
                          or broker_trade.get("createTime") or "")
        try:
            from zoneinfo import ZoneInfo
            fill_time = datetime.strptime(
                str(fill_time_text), "%Y-%m-%d %H:%M:%S").replace(
                    tzinfo=ZoneInfo("Asia/Kolkata")).timestamp()
        except (TypeError, ValueError):
            raise HTTPException(status_code=409,
                                detail="Dhan fill timestamp cannot be verified")
        if not (candle_end <= fill_time <= candle_end + 900.0):
            raise HTTPException(status_code=409,
                                detail="Dhan fill is outside the selected candle's trigger window")

        o, h, low, close = map(float, candle[1:5])
        prev = closed[idx - 1]
        trigger, stop = entry_levels("LONG", h, low,
                                     float(prev[2]), float(prev[3]))
        if stop <= 0 or trigger <= stop:
            raise HTTPException(status_code=409,
                                detail="signal-candle trigger/stop levels are invalid")
        forming = candle_state.get("forming") or []
        live_price = float(forming[4]) if len(forming) > 4 else 0.0
        if live_price <= stop:
            raise HTTPException(status_code=409,
                                detail="current broker candle is at/below the long stop; adoption halted")

        from strategies.types import SignalExecutionContext
        signal = Signal(
            signal_type=SignalType.LONG, instrument="SILVERM",
            strategy_id=strategy_id, timestamp=candle_timestamp,
            trigger_price=trigger, stop_price=stop, quantity=1, side="LONG",
            metadata={
                "executed": True, "entry_price": fill_price,
                "external_position_import": True,
                "external_position_source": "dhan_tradebook_operator_reconcile",
                "broker_order_id": broker_order_id,
                "broker_fill_id": broker_fill_id,
                "signal_reason": "operator_attributed_to_latest_closed_15m_candle",
                "signal_candle_start": candle_timestamp,
                "signal_candle_open": o, "signal_candle_high": h,
                "signal_candle_low": low, "signal_candle_close": close,
                "trigger_level": trigger,
            })
        freeze_signal_context(signal, timestamp=candle_timestamp, open_=o,
                              high=h, low=low, close=close)

        execution = env.execution_engine
        local_order_id = f"IMPORT-{broker_order_id}"
        order = Order(
            order_id=local_order_id, strategy_id=strategy_id,
            instrument="SILVERM", side="BUY", quantity=1,
            order_type=str(broker_order.get("order_type") or "LIMIT").upper(),
            price=fill_price, planned_entry_price=trigger, planned_sl=stop,
            planned_order_type="EXTERNAL_DHAN_FILL", order_role="ENTRY",
            state=OrderState.FILLED, filled_quantity=1,
            average_fill_price=fill_price, created_at=fill_time,
            updated_at=fill_time, reason="operator_imported_existing_dhan_position",
            multiplier=float(_engine.config.instrument("SILVERM").get("multiplier", 1.0)),
            entry_signal_id=signal.signal_id, trade_id="",
            lifecycle_id="", position_generation=0)
        order._broker_order_id = broker_order_id
        order.parent_signal_id = signal.signal_id
        order.correlation_id = None  # Dhan's manual-order correlation is NA.

        _engine._persist_signal(signal, "EXTERNAL_POSITION_IMPORT", env_name="live")
        lifecycle = runtime.lifecycle
        trade = lifecycle.create_trade_from_signal(
            signal, strategy_id=strategy_id, strategy_name=strategy_id,
            instrument="SILVERM", quantity=1,
            multiplier=order.multiplier,
            signal_reason="operator_imported_existing_dhan_position",
            entry_reason="external_dhan_position_import")
        if trade is None:
            raise HTTPException(status_code=409,
                                detail="lifecycle rejected external position trade")
        order.trade_id = trade.trade_id
        order.lifecycle_id = trade.trade_id
        order.position_generation = runtime.position_manager.allocate_generation(
            strategy_id, "SILVERM")
        order.planned_sl = stop
        if not lifecycle.register_order(trade.trade_id, local_order_id, role="ENTRY"):
            raise HTTPException(status_code=409,
                                detail="lifecycle rejected imported broker order")
        with execution._lock:
            if local_order_id in execution._orders:
                raise HTTPException(status_code=409,
                                    detail="imported local order id already exists")
            execution._orders[local_order_id] = order
        _engine._persist_order(order, signal, env_name="live")

        fill = Fill(
            fill_id=f"DHAN-{broker_fill_id}", order_id=local_order_id,
            instrument="SILVERM", side="BUY", quantity=1,
            price=fill_price, timestamp=fill_time,
            strategy_id=strategy_id, multiplier=order.multiplier,
            entry_signal_id=signal.signal_id, trade_id=trade.trade_id,
            lifecycle_id=trade.trade_id,
            position_generation=order.position_generation)
        fill.broker_order_id = broker_order_id
        fill.broker_fill_id = broker_fill_id
        fill.broker_trade_id = broker_fill_id
        fill.cumulative_filled_quantity = 1
        _engine._handle_fill(fill, signal.signal_id, env_name="live")

        position = next((p for p in runtime.position_manager.get_positions_by_strategy(
            strategy_id) if p.instrument == "SILVERM" and p.is_open), None)
        if position is None or str(position.sl_state).upper() != "ARMED":
            raise HTTPException(status_code=500,
                                detail="broker fill imported but local position/SL did not arm")
        order.position_id = position.position_id
        order.parent_position_id = position.position_id
        order.position_generation = position.position_generation
        _engine._persist_order(order, signal, env_name="live")
        _persistence.save_state(_engine.snapshot("live"))
        return {
            "adopted": True, "already_adopted": False,
            "source": "Dhan tradebook and current broker net",
            "broker_order_id": broker_order_id,
            "broker_fill_id": broker_fill_id,
            "position": position.snapshot(),
            "signal_candle": {"timestamp": candle_timestamp,
                              "open": o, "high": h, "low": low,
                              "close": close},
            "trigger_price": trigger, "stop_price": stop,
            "broker_order_sent": False,
        }
    finally:
        _broker_position_adoption_lock.release()


def _adopt_filled_reversal_short(body: dict) -> dict:
    """Book an exact, already-filled manual SHORT onto its fired reversal.

    This is deliberately narrower than general position adoption: it only
    accepts the exact pending reversal signal whose old exit is broker-filled,
    verifies the matching new SELL fill and current broker net, then routes
    that existing fill through the ordinary FillFlow so its local SL is armed.
    It never sends or changes a broker order.
    """
    from zoneinfo import ZoneInfo
    from execution.models import Fill, Order, OrderState
    from strategies.intent import entry_levels
    from strategies.types import Signal, SignalType, freeze_signal_context

    if not _engine or not _persistence:
        raise HTTPException(status_code=503, detail="live engine unavailable")
    if not _broker_position_adoption_lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="position adoption already running")
    try:
        strategy_id = str(body.get("strategy_id") or "")
        broker_order_id = str(body.get("broker_order_id") or "")
        signal_id = str(body.get("signal_id") or "")
        if not all((strategy_id, broker_order_id, signal_id)):
            raise HTTPException(status_code=422,
                                detail="strategy_id, broker_order_id, and signal_id are required")
        env = _engine.live
        strategy_cfg = (_engine.config.get("strategies", {}) or {}).get(strategy_id)
        strategy = (getattr(env, "strategies", {}) or {}).get(strategy_id)
        runtime = env.runtimes.require(strategy_id)
        if (not strategy_cfg or not strategy or strategy_cfg.get("instrument") != "SILVERM"
                or not bool(strategy_cfg.get("enabled"))
                or str(strategy_cfg.get("fast_timeframe", "")).lower() != "15m"
                or int(strategy_cfg.get("quantity", 0)) != 1):
            raise HTTPException(status_code=409,
                                detail="selected strategy is not an enabled SILVERM 15m quantity-1 strategy")

        # Resolve the exact durable signal/trade/reversal triplet. A free-form
        # stop or an unrelated manually placed position is never adopted here.
        saved_signal = _persistence.get_signal(signal_id)
        if not saved_signal or saved_signal.get("strategy_id") != strategy_id:
            raise HTTPException(status_code=409, detail="durable reversal entry signal was not found")
        metadata = json.loads(saved_signal.get("signal_metadata") or "{}")
        parent_signal_id = str(metadata.get("reversal_parent_signal_id") or "")
        if (saved_signal.get("instrument") != "SILVERM"
                or not metadata.get("is_reversal_entry")
                or not metadata.get("entry_after_confirmed_reversal_exit")
                or not parent_signal_id):
            raise HTTPException(status_code=409,
                                detail="signal is not the exact fired SHORT reversal entry")
        reversal = _persistence.get_reversal_by_signal_id(parent_signal_id)
        parent_signal = _persistence.get_signal(parent_signal_id)
        if (not reversal or reversal.get("strategy_id") != strategy_id
                or reversal.get("instrument") != "SILVERM"
                or not parent_signal
                or str(parent_signal.get("side", "")).upper() != "SHORT"
                or (saved_signal.get("side")
                    and str(saved_signal.get("side")).upper() != "SHORT")
                or str(reversal.get("status", "")).upper() != "EXIT_FILLED"
                or str(reversal.get("old_exit_broker_status", "")).upper() != "FILLED"
                or reversal.get("new_entry_order_id")):
            raise HTTPException(status_code=409,
                                detail="paired reversal is not awaiting its first entry fill")
        trade = runtime.lifecycle.resolve_trade_from_signal(signal_id)
        trade_status = str(getattr(getattr(trade, "status", None), "value",
                                    getattr(trade, "status", ""))).upper()
        if (trade is None or trade.strategy_id != strategy_id
                or trade.instrument != "SILVERM"
                or str(getattr(trade, "entry_signal_id", "")) != signal_id
                or str(getattr(trade, "entry_side", "") or "").upper() not in ("", "SHORT")
                or trade_status not in ("PENDING", "OPEN")):
            raise HTTPException(status_code=409,
                                detail="existing pending reversal trade is missing or settled")
        if int(saved_signal.get("quantity") or 0) != 1 or int(trade.quantity) != 1:
            raise HTTPException(status_code=409, detail="reversal quantity is not exactly one")

        # Dhan must prove this exact SELL was fully filled and is still the
        # current instrument net. The duplicate strategy-labelled net rows
        # returned by Dhan are collapsed before comparison.
        broker = env.broker
        day_orders_fn, tradebook_fn = getattr(broker, "day_order_book", None), getattr(broker, "tradebook", None)
        if not callable(day_orders_fn) or not callable(tradebook_fn):
            raise HTTPException(status_code=503, detail="Dhan orderbook/tradebook unavailable")
        day_orders = day_orders_fn() or []
        broker_order = next((o for o in day_orders
                             if str(o.get("broker_order_id") or "") == broker_order_id), None)
        if (not broker_order or str(broker_order.get("status", "")).lower() != "filled"
                or str(broker_order.get("side", "")).upper() != "SELL"
                or int(broker_order.get("quantity") or 0) != 1
                or int(broker_order.get("filled_quantity") or 0) != 1
                or str(broker_order.get("security_id") or "") != "483080"):
            raise HTTPException(status_code=409,
                                detail="Dhan order is not the exact filled SILVERM SELL quantity 1")
        trades = tradebook_fn() or []
        order_trades = [t for t in trades
                        if str(t.get("orderId") or t.get("order_id") or "") == broker_order_id
                        and str(t.get("securityId") or t.get("security_id") or "") == "483080"]
        if len(order_trades) != 1:
            raise HTTPException(status_code=409,
                                detail="expected one exact Dhan exchange fill for the manual SELL")
        broker_trade = order_trades[0]
        broker_fill_id = str(broker_trade.get("exchangeTradeId")
                             or broker_trade.get("tradeId") or "")
        fill_price = float(broker_trade.get("tradedPrice") or 0.0)
        fill_qty = int(broker_trade.get("tradedQuantity") or 0)
        if (not broker_fill_id or fill_price <= 0 or fill_qty != 1
                or str(broker_trade.get("transactionType") or "").upper() != "SELL"):
            raise HTTPException(status_code=409, detail="Dhan SELL fill identity is invalid")
        reversal_entry_trigger = float(
            metadata.get("reversal_entry_trigger_level")
            or saved_signal.get("trigger_price") or 0)
        if reversal_entry_trigger <= 0 or fill_price > reversal_entry_trigger:
            raise HTTPException(status_code=409,
                                detail="manual SELL fill does not confirm beyond the fired SHORT trigger")
        if _persistence.fill_by_broker_fill_id(broker_fill_id):
            raise HTTPException(status_code=409, detail="manual SELL fill is already in local ledger")
        broker_rows = broker.positions() or []
        nets = {(str(r.get("side") or "").upper(), abs(int(r.get("quantity") or 0)))
                for r in broker_rows if r.get("instrument") == "SILVERM"
                and int(r.get("quantity") or 0) != 0}
        if nets != {("SHORT", 1)}:
            raise HTTPException(status_code=409,
                                detail=f"current Dhan SILVERM net is not exactly SHORT 1: {sorted(nets)}")
        silver_trades = [t for t in trades
                         if str(t.get("securityId") or t.get("security_id") or "") == "483080"]
        latest = max(silver_trades,
                     key=lambda t: str(t.get("exchangeTime") or t.get("createTime") or ""),
                     default=None)
        if not latest or str(latest.get("orderId") or latest.get("order_id") or "") != broker_order_id:
            raise HTTPException(status_code=409,
                                detail="a later SILVERM tradebook fill superseded this manual SELL")
        signed_net = sum(
            (-1 if str(t.get("transactionType") or "").upper() == "SELL" else 1)
            * int(t.get("tradedQuantity") or 0) for t in silver_trades)
        if signed_net != -1:
            raise HTTPException(status_code=409,
                                detail=f"Dhan SILVERM tradebook net is not SHORT 1: {signed_net}")

        # Attribute the manual fill only to the existing reversal after the
        # old exit confirmation and within its immediate follow-up window.
        try:
            fill_time = datetime.strptime(
                str(broker_trade.get("exchangeTime") or broker_trade.get("createTime") or ""),
                "%Y-%m-%d %H:%M:%S").replace(tzinfo=ZoneInfo("Asia/Kolkata")).timestamp()
            exit_verified = datetime.fromisoformat(
                str(reversal.get("exit_verified_at")).replace("Z", "+00:00")).timestamp()
        except (TypeError, ValueError):
            raise HTTPException(status_code=409, detail="Dhan fill/reversal timestamps cannot be verified")
        if not (exit_verified <= fill_time <= exit_verified + 900):
            raise HTTPException(status_code=409,
                                detail="manual SELL is outside the paired reversal follow-up window")

        candle_ts = float(saved_signal.get("candle_timestamp") or 0)
        candle_state = env.data_adapter.fetch_candle_state("SILVERM", "15") or {}
        closed = candle_state.get("closed") or []
        candle = next((c for c in closed if float(c[0]) == candle_ts), None)
        idx = closed.index(candle) if candle is not None else -1
        if candle is None or idx == 0:
            history_fn = getattr(env.data_adapter, "fetch_historical_candles", None)
            if callable(history_fn):
                try:
                    signal_day = datetime.fromtimestamp(
                        candle_ts, timezone.utc).date()
                    today = datetime.now(timezone.utc).date()
                    history = history_fn("SILVERM", "15", signal_day, today) or []
                    by_start = {float(c[0]): c for c in [*closed, *history]}
                    closed = sorted(by_start.values(), key=lambda c: float(c[0]))
                    candle = next((c for c in closed if float(c[0]) == candle_ts), None)
                    idx = closed.index(candle) if candle is not None else -1
                except Exception as exc:
                    raise HTTPException(status_code=503,
                                        detail=f"historical reversal candle fetch failed: {exc}")
        if candle is None:
            raise HTTPException(status_code=409,
                                detail="the reversal signal candle is unavailable from Dhan REST history")
        if idx == 0:
            raise HTTPException(status_code=409,
                                detail="prior candle is unavailable to verify the reversal stop")
        o, high, low, close = map(float, candle[1:5])
        prev = closed[idx - 1]
        trigger, stop = entry_levels("SHORT", high, low,
                                     float(prev[2]), float(prev[3]))
        stored_stop = float(saved_signal.get("stop_price")
                            or getattr(trade, "stop_loss_price", 0) or 0)
        if stop != stored_stop or stop <= fill_price:
            raise HTTPException(status_code=409,
                                detail=f"stored reversal stop {stored_stop} does not match candle stop {stop}")
        # Some restored PENDING TradeContext rows predate full signal-field
        # hydration. Reconstitute only blank fields from the exact persisted
        # signal before booking its broker-confirmed fill.
        trade.entry_side = "SHORT"
        trade.entry_trigger_price = float(saved_signal.get("trigger_price") or trigger)
        trade.stop_loss_price = stop
        trade.quantity = 1
        trade.multiplier = float(_engine.config.instrument("SILVERM").get("multiplier", 1.0))
        trade.signal_candle_open = o
        trade.signal_candle_high = high
        trade.signal_candle_low = low
        trade.signal_candle_close = close
        trade.signal_htf_value = float(saved_signal.get("htf_value") or 0.0)
        trade.signal_mid_value = float(saved_signal.get("mid_value") or 0.0)
        trade.signal_fast_dema = float(saved_signal.get("fast_dema") or 0.0)
        trade.signal_fast_atr = float(saved_signal.get("fast_atr") or 0.0)
        if not runtime.lifecycle.persist_trade(trade):
            raise HTTPException(status_code=500,
                                detail="could not restore pending reversal trade context")
        ltp = float((candle_state.get("forming") or [0, 0, 0, 0, 0])[4] or 0)
        if ltp <= 0:
            raise HTTPException(status_code=503, detail="live SILVERM price unavailable")
        if ltp >= stop:
            raise HTTPException(status_code=409,
                                detail="SHORT stop is already crossed; refusing to mark it safely armed")
        for rt in env.runtimes.all():
            if any(p.instrument == "SILVERM" and p.is_open
                   for p in rt.position_manager.get_positions_by_strategy(rt.strategy_id)):
                raise HTTPException(status_code=409,
                                    detail="a local SILVERM position already exists")

        signal = Signal(
            signal_type=SignalType.SHORT, instrument="SILVERM",
            strategy_id=strategy_id, timestamp=candle_ts,
            trigger_price=float(saved_signal.get("trigger_price") or trigger),
            stop_price=stop, quantity=1, side="SHORT", metadata=metadata,
        )
        signal.signal_id = signal_id
        signal.lifecycle_id = trade.trade_id
        signal.metadata = dict(metadata)
        signal.position_generation = runtime.position_manager.allocate_generation(
            strategy_id, "SILVERM")
        freeze_signal_context(signal, timestamp=candle_ts, open_=o,
                              high=high, low=low, close=close)

        local_order_id = f"IMPORT-{broker_order_id}"
        for existing in (getattr(env.execution_engine, "_orders", {}) or {}).values():
            if str(getattr(existing, "_broker_order_id", "")) == broker_order_id:
                raise HTTPException(status_code=409,
                                    detail="Dhan order already has a local execution record")
        order = Order(
            order_id=local_order_id, strategy_id=strategy_id,
            instrument="SILVERM", side="SELL", quantity=1,
            order_type=str(broker_order.get("order_type") or "LIMIT").upper(),
            price=fill_price, planned_entry_price=float(saved_signal.get("trigger_price") or trigger),
            planned_sl=stop, planned_order_type="EXTERNAL_DHAN_FILL",
            order_role="REVERSAL_ENTRY", state=OrderState.FILLED,
            filled_quantity=1, average_fill_price=fill_price,
            created_at=fill_time, updated_at=fill_time,
            reason="operator_imported_existing_reversal_sell",
            multiplier=float(_engine.config.instrument("SILVERM").get("multiplier", 1.0)),
            entry_signal_id=signal_id, trade_id=trade.trade_id,
            lifecycle_id=trade.trade_id, parent_signal_id=signal_id,
            position_generation=signal.position_generation,
            reversal_parent_signal_id=parent_signal_id,
            trigger_state="FIRED", trigger_generation=metadata.get("trigger_generation"),
            trigger_source="broker_confirmed_reversal_flat",
        )
        order._broker_order_id = broker_order_id
        order.correlation_id = None
        if not runtime.lifecycle.register_order(trade.trade_id, local_order_id,
                                                role="REVERSAL_ENTRY"):
            raise HTTPException(status_code=409, detail="lifecycle rejected the imported reversal order")
        with env.execution_engine._lock:
            if local_order_id in env.execution_engine._orders:
                raise HTTPException(status_code=409, detail="imported order id already exists")
            env.execution_engine._orders[local_order_id] = order
        _engine._persist_order(order, signal, env_name="live")
        _engine._update_reversal_entry_created(env, parent_signal_id, trade, order)

        fill = Fill(
            fill_id=f"DHAN-{broker_fill_id}", order_id=local_order_id,
            instrument="SILVERM", side="SELL", quantity=1,
            price=fill_price, timestamp=fill_time, strategy_id=strategy_id,
            multiplier=order.multiplier, entry_signal_id=signal_id,
            trade_id=trade.trade_id, lifecycle_id=trade.trade_id,
            position_generation=order.position_generation,
        )
        fill.broker_order_id = broker_order_id
        fill.broker_fill_id = broker_fill_id
        fill.broker_trade_id = broker_fill_id
        fill.cumulative_filled_quantity = 1
        _engine._handle_fill(fill, signal_id, env_name="live")
        position = next((p for p in runtime.position_manager.get_positions_by_strategy(
            strategy_id) if p.instrument == "SILVERM" and p.is_open), None)
        if position is None or str(position.sl_state).upper() != "ARMED":
            raise HTTPException(status_code=500,
                                detail="manual fill booked but local SHORT stop did not arm")
        order.position_id = position.position_id
        order.parent_position_id = position.position_id
        order.position_generation = position.position_generation
        _engine._persist_order(order, signal, env_name="live")
        _persistence.save_state(_engine.snapshot("live"))
        return {
            "adopted": True, "broker_order_sent": False,
            "broker_order_id": broker_order_id, "broker_fill_id": broker_fill_id,
            "signal_id": signal_id, "reversal_parent_signal_id": parent_signal_id,
            "position": position.snapshot(), "stop_price": stop,
            "monitor": "local_position_owned_sl_armed",
        }
    finally:
        _broker_position_adoption_lock.release()


def _correct_imported_position_stop(body: dict) -> dict:
    """Recompute one imported open position's local stop from exact Dhan bars.

    This is loopback-only at the route and requires the existing broker fill,
    open net, local position, and candle timestamp to agree. It never sends a
    broker order.
    """
    from strategies.intent import entry_levels
    if not _engine or not _persistence:
        raise HTTPException(status_code=503, detail="live engine unavailable")
    if not _broker_position_adoption_lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="position reconciliation already running")
    try:
        strategy_id = str(body.get("strategy_id") or "")
        broker_order_id = str(body.get("broker_order_id") or "")
        try:
            candle_ts = float(body.get("signal_candle_timestamp"))
        except (TypeError, ValueError):
            raise HTTPException(status_code=422, detail="signal_candle_timestamp is required")
        env = _engine.live
        runtime = env.runtimes.require(strategy_id)
        order = next((o for o in (getattr(env.execution_engine, "_orders", {}) or {}).values()
                      if str(getattr(o, "_broker_order_id", "")) == broker_order_id), None)
        position = next((p for p in runtime.position_manager.get_positions_by_strategy(strategy_id)
                         if p.instrument == "SILVERM" and p.is_open), None)
        position_side = str(getattr(position.side, "value", position.side)).upper() if position else ""
        if not order or not position or position_side != "LONG" or int(position.quantity) != 1:
            raise HTTPException(status_code=409, detail="exact imported open SILVERM LONG position was not found")

        broker_order = next((o for o in (env.broker.day_order_book() or [])
                             if str(o.get("broker_order_id") or "") == broker_order_id), None)
        if (not broker_order or broker_order.get("status") != "filled"
                or int(broker_order.get("filled_quantity") or 0) != 1):
            raise HTTPException(status_code=409, detail="Dhan no longer confirms the imported filled order")
        fills = [t for t in (env.broker.tradebook() or [])
                 if str(t.get("orderId") or t.get("order_id") or "") == broker_order_id
                 and str(t.get("securityId") or t.get("security_id") or "") == "483080"]
        if len(fills) != 1 or str(fills[0].get("exchangeTradeId") or fills[0].get("tradeId") or "") != "240073750":
            raise HTTPException(status_code=409, detail="Dhan tradebook fill no longer matches the imported position")
        nets = {(str(r.get("side") or "").upper(), abs(int(r.get("quantity") or 0)))
                for r in (env.broker.positions() or [])
                if r.get("instrument") == "SILVERM" and int(r.get("quantity") or 0)}
        if nets != {("LONG", 1)}:
            raise HTTPException(status_code=409, detail="current Dhan SILVERM net is not exactly LONG 1")

        adapter = env.data_adapter
        history_fn = getattr(adapter, "fetch_historical_candles", None)
        if not callable(history_fn):
            raise HTTPException(status_code=503, detail="Dhan historical candles unavailable")
        signal_day = datetime.fromtimestamp(candle_ts, timezone.utc).date()
        candles = history_fn("SILVERM", "15", signal_day,
                             datetime.now(timezone.utc).date()) or []
        candles = sorted((c for c in candles if float(c[0]) + 900 <= time.time()),
                         key=lambda c: float(c[0]))
        idx = next((i for i, c in enumerate(candles) if float(c[0]) == candle_ts), None)
        if idx is None or idx == 0:
            raise HTTPException(status_code=409, detail="signal candle or its preceding candle is unavailable")
        candle, previous = candles[idx], candles[idx - 1]
        open_, high, low, close = map(float, candle[1:5])
        trigger, stop = entry_levels("LONG", high, low,
                                     float(previous[2]), float(previous[3]))
        if low != 228100.0 or stop != 228100.0:
            raise HTTPException(status_code=409, detail={
                "reason": "Dhan candle does not confirm the requested 228100 stop",
                "signal_candle": {"timestamp": candle_ts, "open": open_, "high": high,
                                   "low": low, "close": close},
                "computed_stop": stop})
        ltp_fn = getattr(adapter, "get_live_ltp", None)
        live_ltp = ltp_fn("SILVERM") if callable(ltp_fn) else None
        ltp = float(live_ltp or getattr(position, "current_mark", 0) or 0)
        if ltp <= stop:
            raise HTTPException(status_code=409, detail="current SILVERM price is at/below corrected stop; stop correction halted")

        position.stop_price = stop
        position.sl_state = "ARMED"
        position.sl_trigger_price = None
        if hasattr(order, "planned_sl"):
            order.planned_sl = stop
        strategy = env.strategies.get(strategy_id)
        if strategy is not None:
            strategy.stop_price = stop
            strategy.position_side = "LONG"
        monitor = _engine._sl_monitor(env)
        if str(monitor.arm(position).value) != "ARMED":
            raise HTTPException(status_code=500, detail="corrected stop did not arm in the local monitor")
        _engine._persist_position(position, env_name="live")
        _engine._persist_order(order, None, env_name="live")
        _persistence.save_signal({
            "signal_id": order.entry_signal_id, "strategy_id": strategy_id,
            "instrument": "SILVERM", "side": "LONG", "signal_type": "ENTRY_LONG",
            "timestamp": position.entry_timestamp, "trigger_price": trigger,
            "stop_price": stop, "quantity": 1, "candle_timestamp": candle_ts,
            "open": open_, "high": high, "low": low, "close": close,
            "candle_data": {"timestamp": candle_ts, "open": open_, "high": high,
                            "low": low, "close": close},
            "signal_metadata": {"external_position_import": True,
                                "broker_order_id": broker_order_id,
                                "signal_candle_start": candle_ts,
                                "signal_candle_high": high,
                                "signal_candle_low": low,
                                "trigger_level": trigger},
        })
        _persistence.save_state(_engine.snapshot("live"))
        return {"corrected": True, "broker_order_sent": False,
                "broker_order_id": broker_order_id, "position_id": position.position_id,
                "signal_candle": {"timestamp": candle_ts, "open": open_, "high": high,
                                   "low": low, "close": close},
                "trigger_price": trigger, "stop_price": stop,
                "monitor_state": monitor.state_of(position.position_id).value}
    finally:
        _broker_position_adoption_lock.release()


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

    @app.post("/api/live/reconcile/adopt-broker-position")
    async def adopt_broker_position(request: Request, body: dict):
        """Import one operator-identified, already-filled Dhan entry locally.

        This never submits, modifies, or cancels a broker order. It is
        loopback-only and refuses adoption unless Dhan's filled order, its
        exchange trade, the current net position, the selected completed
        candle, and the strategy/instrument/quantity all agree. The strategy's
        existing candle rule supplies the stop; no stop is guessed.
        """
        try:
            peer = ipaddress.ip_address(
                request.client.host if request.client else "")
        except ValueError:
            raise HTTPException(status_code=403, detail="loopback caller required")
        if not peer.is_loopback:
            raise HTTPException(status_code=403, detail="loopback caller required")
        if str(body.get("side", "")).upper() == "SHORT":
            return await asyncio.to_thread(_adopt_filled_reversal_short, body)
        return await asyncio.to_thread(_adopt_broker_position, body)

    @app.post("/api/live/reconcile/correct-imported-position-stop")
    async def correct_imported_position_stop(request: Request, body: dict):
        """Correct an imported position's local stop from Dhan's exact candle."""
        try:
            peer = ipaddress.ip_address(
                request.client.host if request.client else "")
        except ValueError:
            raise HTTPException(status_code=403, detail="loopback caller required")
        if not peer.is_loopback:
            raise HTTPException(status_code=403, detail="loopback caller required")
        return await asyncio.to_thread(_correct_imported_position_stop, body)

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
                    registry = getattr(env, "pending_triggers", None)
                    if registry is not None:
                        registry.sync_strategy(strategy)
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
            registry = getattr(env, "pending_triggers", None)
            if registry is not None:
                registry.sync_strategy(strategy)
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
