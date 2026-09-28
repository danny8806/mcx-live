"""Market data routes - live prices, data health, tick stats."""
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

def _get_market_data_sync():
    if not _engine:
        return {"error": "Engine not initialized"}
    try:
        prices = _engine.execution_engine._current_prices
        instruments = _engine.config.get("instruments", {})
        inst_ticks = {}
        try:
            adapter = getattr(_engine, "data_adapter", None)
            stats_obj = getattr(adapter, "stats", {}) or {}
            if isinstance(stats_obj, dict):
                inst_ticks = stats_obj.get("instrument_ticks", {}) or {}
        except Exception:
            inst_ticks = {}
        data = {}
        for name, cfg in instruments.items():
            ltp = prices.get(name, 0.0)
            data[name] = {
                "ltp": ltp,
                "spread": 0.0,
                "tick_count": inst_ticks.get(name, 0),
                "timestamp": time.time(),
            }
        ws_connected = False
        try:
            ws_connected = _engine.data_adapter.connected
        except Exception:
            pass
        return {
            "instruments": data,
            "ws_connected": ws_connected,
            "adapter_stats": _engine.data_adapter.stats if hasattr(_engine.data_adapter, "stats") else {},
            "timestamp": time.time(),
        }
    except Exception as e:
        return {"error": str(e)}

@router.get("/api/market-data")
async def get_market_data():
    return await asyncio.to_thread(_get_market_data_sync)

def _get_instrument_data_sync(instrument: str):
    if not _engine:
        return {"error": "Engine not initialized"}
    try:
        inst = instrument.upper()
        prices = _engine.execution_engine._current_prices
        ltp = prices.get(inst, 0.0)
        cfg = _engine.config.instrument(inst)
        bars = {}
        for tf in ["5m", "15m", "1h"]:
            try:
                fetcher = _engine.candle_fetcher
                bars[tf] = {
                    "forming": None,
                    "closed": None,
                }
            except Exception:
                bars[tf] = {"forming": None, "closed": None}
        return {
            "instrument": inst,
            "ltp": ltp,
            "config": cfg,
            "bars": bars,
            "timestamp": time.time(),
        }
    except Exception as e:
        return {"error": str(e)}

@router.get("/api/market-data/{instrument}")
async def get_instrument_data(instrument: str):
    return await asyncio.to_thread(_get_instrument_data_sync, instrument)
