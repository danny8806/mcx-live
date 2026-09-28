"""Risk routes - portfolio risk, per-strategy risk, limits."""
from __future__ import annotations
import asyncio
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

def _get_risk_sync():
    if not _engine:
        return {"error": "Engine not initialized"}
    try:
        risk = _engine.risk_engine.snapshot()
        account = _engine.account_engine.snapshot()
        positions = _engine.position_manager.snapshot()
        open_pos = positions.get("open_positions", {})
        risk_config = _engine.config.get("risk", {})
        equity = account.get("equity", 0) or 0
        used_margin = account.get("used_margin", 0) or 0
        return {
            "kill_switch_active": risk.get("kill_switch_active", False),
            "daily_pnl": risk.get("daily_pnl", 0),
            "peak_equity": risk.get("peak_equity", 0),
            "open_positions": len(open_pos),
            "max_positions_per_strategy": risk_config.get("max_open_positions_per_strategy", 1),
            "max_positions_total": risk_config.get("max_positions_total", 8),
            "max_daily_loss": risk_config.get("max_daily_loss", 50000),
            "max_drawdown_pct": risk_config.get("max_drawdown_pct", 5),
            "equity": equity,
            "used_margin": used_margin,
            "available_margin": equity - used_margin,
            "margin_utilization": (used_margin / equity * 100) if equity > 0 else 0,
            "daily_loss_remaining": risk_config.get("max_daily_loss", 50000) + risk.get("daily_pnl", 0),
            "risk_config": risk_config,
            "timestamp": time.time(),
        }
    except Exception as e:
        return {"error": str(e)}

@router.get("/api/risk")
async def get_risk():
    return await asyncio.to_thread(_get_risk_sync)
