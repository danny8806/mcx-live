"""Replay routes - historical replay monitoring."""
from __future__ import annotations
import time
from typing import Optional
from fastapi import APIRouter
router = APIRouter()
_engine = None
_bus = None

def init(engine, event_bus):
    global _engine, _bus
    _engine = engine
    _bus = event_bus

@router.get("/api/replay/status")
async def get_replay_status():
    return {"status": "idle", "timestamp": time.time()}

@router.post("/api/replay/start")
async def start_replay(body: dict):
    return {"status": "not_implemented", "message": "Replay requires historical data file"}

@router.post("/api/replay/stop")
async def stop_replay():
    return {"status": "stopped"}
