"""System health routes."""
from __future__ import annotations
import asyncio
import time
from typing import Optional
from fastapi import APIRouter
router = APIRouter()
_engine = None
_bus = None
_ws_manager = None

def init(engine, event_bus, ws_manager=None):
    global _engine, _bus, _ws_manager
    _engine = engine
    _bus = event_bus
    _ws_manager = ws_manager

def _health_sync():
    """Synchronous health check to run in thread pool."""
    if not _engine:
        return {"error": "Engine not initialized"}
    try:
        health = _engine.health.snapshot()
        components = health.get("components", {})
        result = []
        for name, comp in components.items():
            result.append({
                "name": name,
                "status": comp.get("status", "unknown"),
                "last_heartbeat": comp.get("last_update"),
                "errors": comp.get("error_count", 0),
                "uptime": comp.get("uptime", 0),
            })
        ws_connected = False
        try:
            ws_connected = _engine.data_adapter.connected
        except Exception:
            pass
        result.append({"name": "dhan_ws", "status": "healthy" if ws_connected else "disconnected"})
        result.append({"name": "dashboard_api", "status": "healthy"})
        if _ws_manager:
            ws_stats = _ws_manager.get_stats()
            result.append({"name": "websocket", "status": "healthy", "connections": ws_stats.get("active_connections", 0)})

        market_status = {}
        safe_mode = {}
        try:
            market_status = _engine.market_status.snapshot()
        except Exception:
            pass
        try:
            safe_mode = _engine.safe_mode.get_status()
        except Exception:
            pass

        return {
            "components": result,
            "overall": health.get("overall_status", "unknown"),
            "market_status": market_status.get("market_state", "unknown"),
            "engine_status": market_status.get("engine_status", "unknown"),
            "data_status": market_status.get("data_status", "unknown"),
            "safe_mode": safe_mode,
            "timestamp": time.time(),
        }
    except Exception as e:
        return {"error": str(e)}

@router.get("/api/health/system")
def get_system_health():
    return _health_sync()
