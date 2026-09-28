"""Env switch routes — the LIVE / PAPER switcher plus env-scoped read endpoints.

The dual-env architecture keeps TWO execution books in one process.  The legacy
bare ``/api/...`` endpoints are the PAPER book (backward compatible); this
router (plus the ``/api/{env}/...`` aliases living in the sibling route modules)
exposes every book read side-by-side:

  * ``GET /api/envs``                    — switcher cards (one per environment)
  * ``GET /api/live|paper/account``      — env-scoped account + funds
  * ``GET /api/live|paper/orders``       — env-scoped order book (memory + DB)
  * ``GET /api/live|paper/fills``        — env-scoped fills (memory + DB)
  * ``GET /api/live|paper/health``       — env-scoped runtime health

Overview / strategies / positions env-scoped routes are defined on the sibling
modules (overview.py, strategies.py, positions.py) so they share the exact same
serializers as the legacy endpoints.
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException

from dashboard.envs import available, resolve as _resolve_env, summary

router = APIRouter()

_engine = None
_bus = None


def init(engine, event_bus):
    global _engine, _bus
    _engine = engine
    _bus = event_bus


def _require_env(env: str):
    if _resolve_env(_engine, env) is None:
        raise HTTPException(status_code=404, detail=f"environment '{env}' not found")
    return env


@router.get("/api/envs")
async def list_envs():
    if _engine is None:
        return {"error": "Engine not initialized", "environments": []}
    names = available(_engine)
    return {
        "execution_mode": getattr(_env_default(), "mode", "PAPER"),
        "environments": [summary(_engine, name) for name in names if summary(_engine, name)],
        "count": len(names),
    }


def _env_default():
    return _resolve_env(_engine, "paper")


# ── account ───────────────────────────────────────────────────────────

def _account_sync(env: str):
    env_obj = _resolve_env(_engine, env)
    account = env_obj.account_engine.snapshot()
    positions = env_obj.position_manager.snapshot().get("open_positions", {})
    risk = None
    try:
        risk = env_obj.risk_engine.snapshot()
    except Exception:
        pass
    # LIVE exposes the REAL Dhan account numbers (poller cache or fresh call)
    # alongside the lifecycle book so the dashboard can show broker truth.
    broker_account = None
    if getattr(env_obj, "is_live", False):
        poller = getattr(env_obj, "poller", None)
        if poller is not None:
            try:
                cached = getattr(poller, "_last_account", {}) or {}
            except Exception:
                cached = {}
            if isinstance(cached, dict) and cached.get("equity"):
                broker_account = cached
        if not broker_account:
            broker = getattr(env_obj, "broker", None)
            if broker is not None and hasattr(broker, "account_status"):
                try:
                    broker_account = broker.account_status() or {}
                except Exception:
                    broker_account = None
    return {
        "execution_mode": env_obj.mode,
        "environment": env_obj.name,
        "account": account,
        "broker_account": broker_account,
        "open_positions_count": len(positions),
        "kill_switch": bool((risk or {}).get("kill_switch_active", False)) if risk else False,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/api/{env}/account")
async def get_account(env: str):
    _require_env(env)
    return await asyncio.to_thread(_account_sync, env)


# ── orders / fills (env-scoped, memory + that env's DB) ───────────────

def _ts_to_epoch(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value:
        text = value.replace("Z", "+00:00") if value.endswith("Z") else value
        try:
            dt = datetime.fromisoformat(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except Exception:
            try:
                return time.mktime(time.strptime(value.split(".")[0], "%Y-%m-%dT%H:%M:%S"))
            except Exception:
                return 0.0
    return 0.0


def _orders_sync(env: str, strategy: Optional[str], instrument: Optional[str]):
    env_obj = _resolve_env(_engine, env)
    execution = env_obj.execution_engine
    orders_dict = getattr(execution, "_orders", {}) or {}
    db_rows = []
    persistence = getattr(env_obj, "persistence", None)
    if persistence is not None and hasattr(persistence, "get_orders"):
        try:
            for row in persistence.get_orders():
                db_rows.append({
                    "order_id": row.get("order_id"),
                    "strategy_id": row.get("strategy_id"),
                    "instrument": row.get("instrument"),
                    "side": row.get("side"),
                    "quantity": row.get("quantity"),
                    "order_type": row.get("order_type"),
                    "price": row.get("price"),
                    "state": row.get("state"),
                    "filled_quantity": row.get("filled_quantity"),
                    "average_fill_price": row.get("average_fill_price"),
                    "created_at": _ts_to_epoch(row.get("created_at")),
                    "updated_at": _ts_to_epoch(row.get("updated_at")),
                    "reason": None,
                })
        except Exception:
            db_rows = []

    def _keep(o):
        if strategy and o.get("strategy_id") != strategy:
            return False
        if instrument and o.get("instrument") != instrument.upper():
            return False
        return True

    result = []
    for order in orders_dict.values():
        o = {
            "order_id": order.order_id,
            "strategy_id": order.strategy_id,
            "instrument": order.instrument,
            "side": order.side,
            "quantity": order.quantity,
            "order_type": order.order_type,
            "price": order.price,
            "state": order.state.value if hasattr(order.state, "value") else str(order.state),
            "filled_quantity": order.filled_quantity,
            "average_fill_price": order.average_fill_price,
            "created_at": order.created_at,
            "updated_at": order.updated_at,
            "reason": order.reason,
        }
        if _keep(o):
            result.append(o)
    seen = {o["order_id"] for o in result}
    for row in db_rows:
        if row.get("order_id") in seen:
            continue
        if _keep(row):
            result.append(row)
    result.sort(key=lambda x: x.get("created_at") or 0, reverse=True)
    return {"execution_mode": env_obj.mode, "orders": result, "count": len(result)}


@router.get("/api/{env}/orders")
async def get_orders(env: str, strategy: Optional[str] = None, instrument: Optional[str] = None):
    _require_env(env)
    return await asyncio.to_thread(_orders_sync, env, strategy, instrument)


def _fills_sync(env: str, strategy: Optional[str], instrument: Optional[str]):
    env_obj = _resolve_env(_engine, env)
    execution = env_obj.execution_engine
    db_rows = []
    persistence = getattr(env_obj, "persistence", None)
    if persistence is not None and hasattr(persistence, "get_fills"):
        try:
            for row in persistence.get_fills():
                db_rows.append({
                    "fill_id": row.get("fill_id"),
                    "order_id": row.get("order_id"),
                    "instrument": row.get("instrument"),
                    "side": row.get("side"),
                    "quantity": row.get("quantity"),
                    "price": row.get("price"),
                    "timestamp": _ts_to_epoch(row.get("timestamp")),
                    "strategy_id": row.get("strategy_id"),
                    "multiplier": None,
                })
        except Exception:
            db_rows = []

    def _keep(f):
        if strategy and f.get("strategy_id") != strategy:
            return False
        if instrument and f.get("instrument") != instrument.upper():
            return False
        return True

    result = []
    try:
        fills = execution.get_fills(strategy_id=strategy, instrument=instrument)
    except Exception:
        fills = []
    for f in fills:
        result.append({
            "fill_id": f.fill_id,
            "order_id": f.order_id,
            "instrument": f.instrument,
            "side": f.side,
            "quantity": f.quantity,
            "price": f.price,
            "timestamp": f.timestamp,
            "strategy_id": f.strategy_id,
            "multiplier": f.multiplier,
        })
    seen = {f["fill_id"] for f in result}
    for row in db_rows:
        if row.get("fill_id") in seen:
            continue
        if _keep(row):
            result.append(row)
    result.sort(key=lambda x: (x.get("timestamp") or 0), reverse=True)
    return {"execution_mode": env_obj.mode, "fills": result, "count": len(result)}


@router.get("/api/{env}/fills")
async def get_fills(env: str, strategy: Optional[str] = None, instrument: Optional[str] = None):
    _require_env(env)
    return await asyncio.to_thread(_fills_sync, env, strategy, instrument)


# ── health ────────────────────────────────────────────────────────────

def _health_sync(env: str):
    env_obj = _resolve_env(_engine, env)
    broker = getattr(env_obj, "broker", None)
    stats = {}
    if _bus is not None:
        try:
            stats = _bus.get_stats()
        except Exception:
            stats = {}
    return {
        "status": "ok",
        "environment": env_obj.name,
        "execution_mode": env_obj.mode,
        "is_live": bool(getattr(env_obj, "is_live", env_obj.name != "paper")),
        "gate_enabled": bool(getattr(env_obj, "gate_enabled", False)),
        "master_gate": bool(_engine.config.get("live.live_trading_enabled", False)
                            if _engine and _engine.config else False),
        "broker": type(broker).__name__ if broker is not None else None,
        "engine_running": bool(getattr(_engine, "_running", False)),
        "strategies": len(getattr(env_obj, "strategies", {}) or {}),
        "open_positions": len(getattr(env_obj.position_manager, "open_positions", [])),
        "event_bus": stats,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/api/{env}/health")
async def get_health(env: str):
    _require_env(env)
    return await asyncio.to_thread(_health_sync, env)