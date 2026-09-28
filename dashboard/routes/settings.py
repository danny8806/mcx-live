"""Settings routes."""
from __future__ import annotations
import asyncio
import time
from fastapi import APIRouter
router = APIRouter()
_engine = None

def init(engine, event_bus):
    global _engine
    _engine = engine

def _mask_cid(cid) -> str:
    cid = str(cid or "")
    if len(cid) <= 4:
        return cid
    return f"{cid[:3]}{'*' * (len(cid) - 5)}{cid[-2:]}"

def _get_settings_sync():
    if not _engine:
        return {"error": "Engine not initialized"}
    tg_stats = {}
    try:
        tg_stats = _engine.telegram.get_stats()
    except Exception:
        tg_stats = {"enabled": False, "queue_size": 0, "total_sent": 0, "total_failed": 0}
    return {
        "system": _engine.config.get("system", {}),
        "dhan": {"client_id": _mask_cid(_engine.config.get("dhan.client_id", "")), "ws_url": _engine.config.get("dhan.ws_url", "")},
        "instruments": _engine.config.get("instruments", {}),
        "strategies": _engine.config.get("strategies", {}),
        "indicators": _engine.config.get("indicators", {}),
        "risk": _engine.config.get("risk", {}),
        "account": _engine.config.get("account", {}),
        "paper_execution": _engine.config.get("paper_execution", {}),
        "telegram": {
            "bot_token": "***" if _engine.config.get("telegram.bot_token", "") else "",
            "chat_id": _engine.config.get("telegram.chat_id", ""),
            "enabled": _engine.config.get("telegram.enabled", False),
            "stats": tg_stats,
        },
        "timestamp": time.time(),
    }

@router.get("/api/settings")
async def get_settings():
    return await asyncio.to_thread(_get_settings_sync)

def _refresh_settings_sync():
    if not _engine:
        return {"error": "Engine not initialized"}
    applied = []
    warnings = []
    try:
        _engine.config.load()
        for name, strat in _engine.strategies.items():
            cfg = _engine.config.strategy(name)
            if not cfg:
                continue
            open_positions = _engine.position_manager.get_positions_by_strategy(name)
            has_open = any(pos.is_open for pos in open_positions)
            changed = {}
            if "quantity" in cfg and cfg.get("quantity") != strat.quantity:
                if has_open:
                    warnings.append(f"{name}: quantity change deferred while position open")
                else:
                    strat.quantity = int(cfg["quantity"])
                    changed["quantity"] = strat.quantity
            if "enabled" in cfg and bool(cfg.get("enabled")) != strat.enabled:
                strat.enabled = bool(cfg.get("enabled"))
                changed["enabled"] = strat.enabled
            if cfg.get("fast_timeframe") != strat.fast_timeframe:
                warnings.append(f"{name}: fast_timeframe change requires engine restart")
            if cfg.get("htf_timeframe") != strat.htf_timeframe:
                warnings.append(f"{name}: htf_timeframe change requires engine restart")
            if changed:
                applied.append({"strategy_id": name, "changed": changed})
        _engine.notify_settings_refreshed()
    except Exception as e:
        return {"error": str(e)}
    return {"status": "refreshed", "applied": applied, "warnings": warnings, "timestamp": time.time()}

@router.post("/api/settings/refresh")
async def refresh_settings():
    return await asyncio.to_thread(_refresh_settings_sync)
