"""Audit log routes."""
from __future__ import annotations
import asyncio
import time
from typing import Optional
from fastapi import APIRouter
router = APIRouter()
_event_bus = None

def init(engine, event_bus):
    global _event_bus
    _event_bus = event_bus

def _get_audit_log_sync(limit: int = 200, event_type: Optional[str] = None):
    if not _event_bus:
        return {"entries": [], "count": 0}
    try:
        events = _event_bus.get_recent(event_type=event_type, limit=limit)
        entries = []
        for e in events:
            entries.append({
                "id": e.get("id"),
                "timestamp": e.get("timestamp"),
                "event_type": e.get("event_type"),
                "data": e.get("data", {}),
            })
        entries.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
        return {"entries": entries, "count": len(entries)}
    except Exception as e:
        return {"error": str(e)}

@router.get("/api/audit")
async def get_audit_log(limit: int = 200, event_type: Optional[str] = None):
    return await asyncio.to_thread(_get_audit_log_sync, limit, event_type)
