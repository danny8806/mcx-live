"""LIVE application FastAPI backend.

Independent backend for the LIVE container: it reuses the same dashboard route
modules but initializes them against the LIVE app's own engine graph, event
bus and persistence (``live/data/db/live_trading.db``).  No DEMO/PAPER runtime
exists anywhere in this process — ``/api/paper/*`` correctly 404s here.
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import json
import logging
import os
import sys
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
    env_switch, broker_evidence, reversals,
)
from analytics import routes as analytics_routes  # noqa: E402
from live._forensic import router as forensic_router  # noqa: E402

logger = logging.getLogger("live.api")

ROUTE_MODULES = [
    analytics_routes,
    overview, strategies, positions, orders, trades, pnl, market_data,
    risk, health, reconciliation, alerts, settings, audit_log,
    indicators, env_switch, broker_evidence, reversals,
]

_engine = None            # TradingEngine (live_only)
_bus = None               # UI-facing EventBus (dashboard.event_bus.EventBus)
_persistence = None       # LIVE PersistenceManager
_live_engine = None       # LiveEngine wrapper
_ws_manager = ConnectionManager()
_push_executor: Optional[concurrent.futures.ThreadPoolExecutor] = None
_events_executor: Optional[concurrent.futures.ThreadPoolExecutor] = None

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
            _bus.publish("emergency_exit_all", {
                "scope": "owned_position_lifecycles"})
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

    app.include_router(forensic_router)
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
