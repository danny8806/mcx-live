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
        ws_connected = False
        try:
            ws_connected = bool(_engine.data_adapter.connected)
        except Exception:
            pass
        adapter = getattr(_engine, "data_adapter", None)
        health = getattr(_engine, "market_data_health", None)
        data = {}
        for name, cfg in instruments.items():
            ltp = prices.get(name, 0.0)
            quote = {}
            if adapter is not None:
                lock = getattr(adapter, "_ltp_lock", None)
                cache = getattr(adapter, "_live_ltp", None)
                if lock is not None and isinstance(cache, dict):
                    with lock:
                        quote = dict(cache.get(name) or {})
            received_at = quote.get("receive_timestamp")
            tick_age = max(0.0, time.time() - float(received_at)) if received_at else None
            try:
                feed_healthy = bool(health.is_healthy(name)) if health is not None else (
                    bool(ws_connected) and tick_age is not None and tick_age <= 60)
            except Exception:
                feed_healthy = False
            data[name] = {
                "ltp": ltp,
                "spread": None,
                "tick_count": inst_ticks.get(name, 0),
                "timestamp": received_at,
                "receive_timestamp": received_at,
                "event_timestamp": quote.get("timestamp"),
                "tick_age_seconds": tick_age,
                "feed_healthy": feed_healthy,
            }
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
