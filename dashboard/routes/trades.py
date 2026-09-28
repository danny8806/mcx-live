"""Trades routes - tradebook, trade details.

Serves canonical lifecycle data from TradeLifecycleManager as the single
source of truth. Falls back to persistence DB when lifecycle is empty.
"""
from __future__ import annotations
import asyncio
import time
from typing import Optional
from fastapi import APIRouter
router = APIRouter()
_engine = None
_bus = None
_persistence = None

def init(engine, event_bus, persistence=None):
    global _engine, _bus, _persistence
    _engine = engine
    _bus = event_bus
    _persistence = persistence

def _env_for(env=None):
    from dashboard.envs import resolve as _resolve_env
    return _resolve_env(_engine, env)


def _list_trades_sync(strategy: Optional[str] = None, instrument: Optional[str] = None,
                      env=None):
    try:
        env = _env_for(env)
        # Primary: canonical lifecycle (per-strategy runtimes of that env)
        if env is not None and getattr(env, "runtimes", None):
            trades = []
            runtimes = env.runtimes.all() if hasattr(env.runtimes, "all") else (
                env.runtimes.values() if isinstance(env.runtimes, dict) else [])
            for rt in runtimes:
                try:
                    trades.extend(rt.lifecycle.get_trades_for_api(
                        strategy_id=strategy, instrument=instrument))
                except Exception:
                    pass
            if trades:
                return {"execution_mode": getattr(env, "mode", "PAPER"),
                        "trades": trades, "count": len(trades), "source": "lifecycle"}

        # Fallback: persistence DB (scoped to the env mode)
        persistence = (getattr(env, "persistence", None) if env is not None
                       else _persistence)
        if persistence:
            trades = persistence.get_trades(strategy_id=strategy)
            if instrument:
                trades = [t for t in trades if t.get("instrument") == instrument.upper()]
            return {"execution_mode": getattr(env, "mode", "PAPER") if env else None,
                    "trades": trades, "count": len(trades), "source": "persistence"}

        if not _engine:
            return {"error": "Engine not initialized"}
        fills = (_engine.execution_engine.get_fills() if env is None
                 else env.execution_engine.get_fills())
        result = []
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
            })
        return {"trades": result, "count": len(result), "source": "fills"}
    except Exception as e:
        return {"error": str(e)}


@router.get("/api/trades")
async def list_trades(strategy: Optional[str] = None, instrument: Optional[str] = None):
    return await asyncio.to_thread(_list_trades_sync, strategy, instrument)


@router.get("/api/{env}/trades")
async def list_trades_env(env: str, strategy: Optional[str] = None,
                          instrument: Optional[str] = None):
    return await asyncio.to_thread(_list_trades_sync, strategy, instrument, env)

def _get_trade_sync(trade_id: str):
    try:
        # Primary: canonical lifecycle (read-only aggregate over runtimes)
        if _engine and hasattr(_engine, "get_trade"):
            trade = _engine.get_trade(trade_id)
            if trade:
                return trade.snapshot()

        # Fallback: persistence DB
        if _persistence:
            trades = _persistence.get_trades()
            for t in trades:
                if t.get("trade_id") == trade_id:
                    return t
        return {"error": f"Trade {trade_id} not found"}
    except Exception as e:
        return {"error": str(e)}

def _lifecycle_orphan_scan_sync():
    """Run comprehensive orphan scan across all per-strategy lifecycles."""
    if not _engine or not hasattr(_engine, "orphan_scan"):
        return {"error": "Engine lifecycle not initialized"}
    try:
        return _engine.orphan_scan()
    except Exception as e:
        return {"error": str(e)}

@router.get("/api/trades/orphan-scan")
async def lifecycle_orphan_scan():
    return await asyncio.to_thread(_lifecycle_orphan_scan_sync)

def _lifecycle_reconcile_sync():
    """Run lifecycle reconciliation across all per-strategy lifecycles."""
    if not _engine or not hasattr(_engine, "reconcile_trades"):
        return {"error": "Engine lifecycle not initialized"}
    try:
        return _engine.reconcile_trades()
    except Exception as e:
        return {"error": str(e)}

@router.get("/api/trades/lifecycle-reconcile")
async def lifecycle_reconciliation():
    return await asyncio.to_thread(_lifecycle_reconcile_sync)

@router.get("/api/trades/{trade_id}")
async def get_trade(trade_id: str):
    return await asyncio.to_thread(_get_trade_sync, trade_id)
