"""Positions routes - open/closed positions, position details."""
from __future__ import annotations
import asyncio
import time
from typing import Any, Optional
from fastapi import APIRouter

from dashboard.envs import resolve as _resolve_env

router = APIRouter()
_engine = None
_bus = None


def init(engine, event_bus):
    global _engine, _bus
    _engine = engine
    _bus = event_bus


def _env_for(env=None):
    return _resolve_env(_engine, env)


def _list_positions_sync(status: Optional[str] = "open", instrument: Optional[str] = None,
                         strategy_id: Optional[str] = None, env=None):
    if not _engine:
        return {"error": "Engine not initialized"}
    try:
        env = _env_for(env)
        pm = env.position_manager
        status = (status or "open").lower()
        if status == "closed":
            raw = [p.snapshot() for p in pm.closed_positions]
        elif status == "all":
            raw = [p.snapshot() for p in pm.closed_positions] + [
                v for v in pm.snapshot().get("open_positions", {}).values()
            ]
        else:
            raw = list(pm.snapshot().get("open_positions", {}).values())
        result = []
        for pos in raw:
            if instrument and pos.get("instrument", "").upper() != instrument.upper():
                continue
            if strategy_id and pos.get("strategy_id") != strategy_id:
                continue
            result.append(pos)
        return {"execution_mode": getattr(env, "mode", "PAPER"),
                "positions": result, "count": len(result)}
    except Exception as e:
        return {"error": str(e)}


@router.get("/api/positions")
async def list_positions(status: Optional[str] = "open", instrument: Optional[str] = None,
                         strategy_id: Optional[str] = None):
    return await asyncio.to_thread(_list_positions_sync, status, instrument, strategy_id)


@router.get("/api/{env}/positions")
async def list_positions_env(env: str, status: Optional[str] = "open",
                             instrument: Optional[str] = None,
                             strategy_id: Optional[str] = None):
    if _resolve_env(_engine, env) is None:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"environment '{env}' not found")
    return await asyncio.to_thread(_list_positions_sync, status, instrument, strategy_id, env)


def _get_position_sync(position_id: str, env=None):
    if not _engine:
        return {"error": "Engine not initialized"}
    try:
        env = _env_for(env)
        pos = env.position_manager.get_position(position_id)
        if not pos:
            return {"error": f"Position {position_id} not found"}
        return pos.snapshot()
    except Exception as e:
        return {"error": str(e)}


@router.get("/api/positions/{position_id}")
async def get_position(position_id: str):
    return await asyncio.to_thread(_get_position_sync, position_id)


@router.get("/api/{env}/positions/{position_id}")
async def get_position_env(env: str, position_id: str):
    if _resolve_env(_engine, env) is None:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"environment '{env}' not found")
    return await asyncio.to_thread(_get_position_sync, position_id, env)


def _get_position_pnl_sync(position_id: str, env=None):
    if not _engine:
        return {"error": "Engine not initialized"}
    try:
        env = _env_for(env)
        pos = env.position_manager.get_position(position_id)
        if not pos:
            return {"error": f"Position {position_id} not found"}
        inst = pos.instrument
        strategy_id = pos.strategy_id
        pnl_eng = env.pnl_engines.get(strategy_id)
        snap = pos.snapshot()
        return {
            "position": snap,
            "realized_pnl": snap.get("realized_pnl", 0),
            "unrealized_pnl": snap.get("unrealized_pnl", 0),
            "mark_price": snap.get("current_mark"),
            "entry_price": snap.get("average_entry"),
            "quantity": snap.get("quantity", 0),
            "multiplier": snap.get("multiplier", 1.0),
            "margin": snap.get("margin", 0),
        }
    except Exception as e:
        return {"error": str(e)}


@router.get("/api/positions/{position_id}/pnl")
async def get_position_pnl(position_id: str):
    return await asyncio.to_thread(_get_position_pnl_sync, position_id)


@router.get("/api/{env}/positions/{position_id}/pnl")
async def get_position_pnl_env(env: str, position_id: str):
    if _resolve_env(_engine, env) is None:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"environment '{env}' not found")
    return await asyncio.to_thread(_get_position_pnl_sync, position_id, env)
