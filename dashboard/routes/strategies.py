"""Strategy API for the GOLDM and SILVERM 15-minute SuperTrend runtime."""
from __future__ import annotations

import asyncio
import time
from typing import Optional

from fastapi import APIRouter, HTTPException

from dashboard.envs import resolve as _resolve_env

router = APIRouter()
_engine = None
_bus = None


def init(engine, event_bus):
    global _engine, _bus
    _engine, _bus = engine, event_bus


def _env_for(env=None):
    return _resolve_env(_engine, env)


def _position_snapshot(env, strategy_id: str) -> list[dict]:
    manager = getattr(env, "position_manager", None) if env else None
    try:
        positions = manager.get_positions_by_strategy(strategy_id)
    except Exception:
        return []
    result = []
    for position in positions:
        if getattr(position, "is_open", False):
            row = position.snapshot()
            row["entry_price"] = row.get("average_entry")
            result.append(row)
    return result


def _reconcile_open_position(strategy_id: str, snap: dict, env=None) -> dict:
    """Make the pushed strategy state agree with its authoritative position book.

    The live WebSocket snapshot path uses this helper before enriching strategy
    rows. The strategy object can lag the position manager during restoration
    or a broker fill; in that case the open position is the source of truth for
    the dashboard's displayed side/state.
    """
    target_env = _env_for(env)
    manager = getattr(target_env, "position_manager", None) if target_env else None
    if manager is None:
        return dict(snap)
    try:
        positions = manager.get_positions_by_strategy(strategy_id) or []
    except Exception:
        return dict(snap)
    open_positions = [p for p in positions if getattr(p, "is_open", False)]
    if len(open_positions) != 1:
        return dict(snap)
    position = open_positions[0]
    result = dict(snap)
    result["state"] = "long_position" if position.is_long else "short_position"
    result["position_side"] = "LONG" if position.is_long else "SHORT"
    result["position"] = position.snapshot()
    return result


def _strategy_rows(instrument: Optional[str] = None, status: Optional[str] = None,
                   env_name=None):
    if _engine is None:
        return {"error": "Engine not initialized"}
    env = _env_for(env_name)
    strategies = getattr(env, "strategies", {}) if env else {}
    pnl_engines = getattr(env, "pnl_engines", {}) if env else {}
    result = []
    for strategy_id, strategy in strategies.items():
        if getattr(strategy, "strategy_type", None) != "supertrend":
            continue
        if instrument and strategy.instrument.upper() != instrument.upper():
            continue
        snapshot = strategy.snapshot()
        positions = _position_snapshot(env, strategy_id)
        position = next(iter(positions), None)
        state = ("long_position" if position and position.get("is_long") else
                 "short_position" if position else snapshot.get("state", "unknown"))
        if status and state != status:
            continue
        pnl_engine = pnl_engines.get(strategy_id)
        pnl = pnl_engine.snapshot() if pnl_engine else {}
        cfg = _engine.config.strategy(strategy_id) or {}
        result.append({
            "strategy_id": strategy_id,
            "instrument": strategy.instrument,
            "strategy_type": "supertrend",
            "timeframe": "15m",
            "signal_timeframe": "15m",
            "supertrend": strategy.supertrend.snapshot(),
            "quantity": int(cfg.get("quantity", strategy.quantity)),
            "enabled": bool(snapshot.get("enabled", cfg.get("enabled", True))),
            "state": state,
            "position_side": ("LONG" if position and position.get("is_long") else
                              "SHORT" if position else snapshot.get("position_side")),
            "position": position,
            "pending_entry": snapshot.get("pending_entry"),
            "bars_processed": snapshot.get("bars_processed", 0),
            "trade_count": pnl.get("trade_count", 0),
            "wins": pnl.get("wins", 0),
            "losses": pnl.get("losses", 0),
            "win_rate": pnl.get("win_rate", 0),
            "realized_net": pnl.get("realized_net", 0),
            "realized_gross": pnl.get("realized_gross", 0),
            "realized_charges": pnl.get("realized_charges", 0),
        })
    return {"strategies": result, "count": len(result), "timeframe": "15m"}


@router.get("/api/strategies")
async def list_strategies(instrument: Optional[str] = None, status: Optional[str] = None):
    return await asyncio.to_thread(_strategy_rows, instrument, status)


@router.get("/api/{env}/strategies")
async def list_strategies_env(env: str, instrument: Optional[str] = None,
                              status: Optional[str] = None):
    if _resolve_env(_engine, env) is None:
        raise HTTPException(status_code=404, detail=f"environment '{env}' not found")
    return await asyncio.to_thread(_strategy_rows, instrument, status, env)


def _strategy_detail(strategy_id: str, env_name=None):
    env = _env_for(env_name)
    strategies = getattr(env, "strategies", {}) if env else {}
    strategy = strategies.get(strategy_id)
    if strategy is None or getattr(strategy, "strategy_type", None) != "supertrend":
        return {"error": f"Strategy {strategy_id} not found"}
    snapshot = strategy.snapshot()
    cfg = _engine.config.strategy(strategy_id) or {}
    pnl_engine = (getattr(env, "pnl_engines", {}) or {}).get(strategy_id)
    positions = _position_snapshot(env, strategy_id)
    return {
        "strategy_id": strategy_id,
        "execution_mode": getattr(env, "mode", "LIVE"),
        "gates": _engine.strategy_gate(strategy_id),
        "configuration": {
            "instrument": strategy.instrument,
            "strategy_type": "supertrend",
            "timeframe": "15m",
            "quantity": int(cfg.get("quantity", strategy.quantity)),
            "enabled": bool(cfg.get("enabled", True)),
            "supertrend": strategy.supertrend.snapshot(),
            "entry": "market after completed 15m signal candle close",
            "separate_stop_loss": False,
        },
        "current_state": {
            "state": snapshot.get("state"),
            "position_side": snapshot.get("position_side"),
            "pending_entry": snapshot.get("pending_entry"),
            "bars_processed": snapshot.get("bars_processed", 0),
        },
        "performance": pnl_engine.snapshot() if pnl_engine else {},
        "positions": positions,
        "snapshot": snapshot,
    }


@router.get("/api/strategies/{strategy_id}")
async def get_strategy(strategy_id: str):
    return await asyncio.to_thread(_strategy_detail, strategy_id)


@router.get("/api/{env}/strategies/{strategy_id}")
async def get_strategy_env(env: str, strategy_id: str):
    if _resolve_env(_engine, env) is None:
        raise HTTPException(status_code=404, detail=f"environment '{env}' not found")
    return await asyncio.to_thread(_strategy_detail, strategy_id, env)


def _control_strategy_sync(strategy_id: str, action: str):
    if _engine is None or strategy_id not in _engine.strategies:
        return {"success": False, "error": f"Strategy {strategy_id} not found"}
    if getattr(_engine.strategies[strategy_id], "strategy_type", None) != "supertrend":
        return {"success": False, "error": "Only SuperTrend strategies are active"}
    result = _engine.control_strategy(strategy_id, action)
    if result.get("success") and _bus is not None:
        _bus.publish("strategy_control", {
            "strategy_id": strategy_id, "action": result.get("action"),
            "gate": result.get("gate"), "timestamp": time.time(),
        })
    return result


@router.post("/api/strategies/{strategy_id}/control")
async def control_strategy(strategy_id: str, body: dict):
    return await asyncio.to_thread(_control_strategy_sync, strategy_id,
                                   body.get("action", ""))


@router.post("/api/{env}/strategies/{strategy_id}/control")
async def control_strategy_env(env: str, strategy_id: str, body: dict):
    if _resolve_env(_engine, env) is None:
        raise HTTPException(status_code=404, detail=f"environment '{env}' not found")
    return await asyncio.to_thread(_control_strategy_sync, strategy_id,
                                   body.get("action", ""))


def _strategy_parameters(strategy_id: str, env_name=None):
    env = _env_for(env_name)
    strategy = (getattr(env, "strategies", {}) or {}).get(strategy_id) if env else None
    if strategy is None or getattr(strategy, "strategy_type", None) != "supertrend":
        return {"error": f"Strategy {strategy_id} not found"}
    cfg = _engine.config.strategy(strategy_id) or {}
    instrument = strategy.instrument
    return {
        "strategy": {
            "instrument": instrument,
            "timeframe": "15m",
            "quantity": int(cfg.get("quantity", strategy.quantity)),
            "enabled": bool(cfg.get("enabled", True)),
            "supertrend": strategy.supertrend.snapshot(),
        },
        "instrument_config": _engine.config.instrument(instrument),
        "execution": {"entry": "market after completed signal candle close",
                      "reversal": "exit, confirm flat, then opposite market entry",
                      "separate_stop_loss": False},
        "risk": _engine.config.get("risk", {}),
        "charges": _engine.config.get("charges", {}).get(instrument, {}),
        "source": "active runtime configuration",
        "execution_mode": getattr(env, "mode", "LIVE"),
    }


@router.get("/api/strategies/{strategy_id}/parameters")
async def get_strategy_parameters(strategy_id: str):
    return await asyncio.to_thread(_strategy_parameters, strategy_id)


@router.get("/api/{env}/strategies/{strategy_id}/parameters")
async def get_strategy_parameters_env(env: str, strategy_id: str):
    if _resolve_env(_engine, env) is None:
        raise HTTPException(status_code=404, detail=f"environment '{env}' not found")
    return await asyncio.to_thread(_strategy_parameters, strategy_id, env)
