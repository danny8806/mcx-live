"""15-minute SuperTrend values for the two production strategy profiles."""
from __future__ import annotations

import asyncio
import time
from dataclasses import asdict, is_dataclass

from fastapi import APIRouter, HTTPException

router = APIRouter()
_engine = None


def init(engine, event_bus):
    global _engine
    _engine = engine


def _serialize(strategy_id: str, strategy) -> dict:
    indicator = getattr(strategy, "supertrend", None)
    if indicator is None:
        return {}
    snapshot = indicator.snapshot()
    value = snapshot.get("value")
    if is_dataclass(value):
        value = asdict(value)
    return {
        **snapshot,
        "value": value,
        "strategy_id": strategy_id,
        "instrument": strategy.instrument,
        "timeframe": "15m",
    }


def _get_all_indicators_sync():
    if _engine is None:
        return {"error": "Engine not initialized"}
    values = {
        f"{strategy_id}_supertrend": _serialize(strategy_id, strategy)
        for strategy_id, strategy in (_engine.strategies or {}).items()
        if getattr(strategy, "strategy_type", None) == "supertrend"
    }
    return {"indicators": values, "count": len(values),
            "timeframe": "15m", "timestamp": time.time()}


@router.get("/api/indicators")
async def get_all_indicators():
    return await asyncio.to_thread(_get_all_indicators_sync)


def _get_instrument_indicators_sync(instrument: str):
    if _engine is None:
        return {"error": "Engine not initialized"}
    inst = instrument.upper()
    strategies = _engine.strategies or {}
    active_instruments = {
        str(getattr(strategy, "instrument", "") or "").upper()
        for strategy in strategies.values()
        if getattr(strategy, "strategy_type", None) == "supertrend"
    }
    if inst not in active_instruments:
        raise HTTPException(
            status_code=404,
            detail=f"Instrument {inst} not found in the active strategy runtime",
        )
    values = {
        f"{strategy_id}_supertrend": _serialize(strategy_id, strategy)
        for strategy_id, strategy in strategies.items()
        if (getattr(strategy, "strategy_type", None) == "supertrend"
            and strategy.instrument.upper() == inst)
    }
    return {"instrument": inst, "indicators": values,
            "timeframe": "15m", "timestamp": time.time()}


@router.get("/api/indicators/{instrument}")
async def get_instrument_indicators(instrument: str):
    return await asyncio.to_thread(_get_instrument_indicators_sync, instrument)
