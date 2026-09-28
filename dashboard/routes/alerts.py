"""Alerts routes - alert center."""
from __future__ import annotations
import asyncio
import time
from typing import Optional
from fastapi import APIRouter
router = APIRouter()
_event_bus = None

_SEVERITY_MAP = {
    "risk_alert": "critical",
    "order_rejected": "warning",
    "kill_switch": "critical",
    "engine_error": "critical",
    "risk_rejected": "warning",
    "error": "critical",
    "trade_closed": "info",
    "signal": "info",
    "fill": "info",
    "order": "info",
    "position_opened": "info",
    "position_closed": "info",
}

def init(engine, event_bus):
    global _event_bus
    _event_bus = event_bus

def _get_alerts_sync(event_type: Optional[str] = None, limit: int = 100):
    if not _event_bus:
        return {"alerts": [], "count": 0}
    try:
        events = _event_bus.get_recent(event_type=event_type, limit=limit)
        alerts = []
        for e in events:
            evt_type = e.get("event_type", "unknown")
            severity = _SEVERITY_MAP.get(evt_type, "info")
            alerts.append({
                "id": e.get("id"),
                "timestamp": e.get("timestamp"),
                "type": evt_type,
                "data": e.get("data", {}),
                "severity": severity,
            })
        return {"alerts": alerts, "count": len(alerts)}
    except Exception as e:
        return {"error": str(e)}

@router.get("/api/alerts")
async def get_alerts(event_type: Optional[str] = None, limit: int = 100):
    return await asyncio.to_thread(_get_alerts_sync, event_type, limit)
