"""Strategy routes - strategy list, details, parameters, enable/disable."""
from __future__ import annotations
import asyncio
import time
from typing import Any, Optional
from fastapi import APIRouter, Query

from dashboard.envs import resolve as _resolve_env

router = APIRouter()
_engine = None
_bus = None
_persistence = None


def init(engine, event_bus):
    global _engine, _bus
    _engine = engine
    _bus = event_bus


def _env_for(env=None):
    """Resolve the target environment (defaults to PAPER)."""
    return _resolve_env(_engine, env)


def _with_flat_indicators(ind_snap: dict) -> dict:
    """Flatten the persistence-shaped DEMA/ATR snapshot into the field names the
    UI reads (dema_value / atr_value) while KEEPING the raw nested snapshot
    (dema/atr dicts) intact for backward compatibility.

    For shared-stream indicators the snapshot has no persistence-shaped
    dema/atr dicts, so any values the serializer already resolved are kept.
    """
    out = dict(ind_snap)
    dema = ind_snap.get("dema") or {}
    atr = ind_snap.get("atr") or {}
    dema_val = None
    if dema.get("ema1") is not None and dema.get("ema2") is not None:
        dema_val = 2 * dema["ema1"] - dema["ema2"]
    if dema_val is None:
        dema_val = out.get("dema_value")
    out["dema_value"] = dema_val
    atr_val = atr.get("atr")
    if atr_val is None:
        atr_val = out.get("atr_value")
    out["atr_value"] = atr_val
    return out


def _snapshot_view(obj) -> dict:
    """Pull value / dema / atr / prev from the latest IndicatorSnapshot, whether
    exposed on the object itself, its shared stream, or a raw dict."""
    snap = None
    if isinstance(obj, dict):
        snap = obj.get("latest_snapshot")
    else:
        snap = getattr(obj, "latest_snapshot", None)
        if snap is None:
            stream = getattr(obj, "_stream", None)
            if stream is not None:
                snap = getattr(stream, "latest_snapshot", None)
    if snap is None:
        return {}
    if isinstance(snap, dict):
        return {
            "value": snap.get("dema_atr"),
            "dema_value": snap.get("dema"),
            "atr_value": snap.get("atr"),
            "prev_output": snap.get("previous_dema_atr"),
        }
    return {
        "value": getattr(snap, "dema_atr", None),
        "dema_value": getattr(snap, "dema", None),
        "atr_value": getattr(snap, "atr", None),
        "prev_output": getattr(snap, "previous_dema_atr", None),
    }


def _flat_indicator(ind) -> dict:
    """Serialize either a raw DEMAATR (dict snapshot) or a
    StrategyIndicatorView / IndicatorStream (flat property surface)."""
    if ind is None:
        return {}
    raw = ind.snapshot() if hasattr(ind, "snapshot") else None
    if isinstance(raw, dict):
        out = dict(raw)
        out.update(_with_flat_indicators(raw))
        if raw.get("value") is not None and out.get("value") is None:
            out["value"] = raw["value"]
        if out.get("count") is None:
            out["count"] = raw.get("indicator_count")
        if out.get("initialized") is None:
            out["initialized"] = raw.get("indicator_initialized")
    else:
        out = {}
        for attr in ("value", "dema_value", "atr_value", "_count", "initialized"):
            try:
                out[attr.lstrip("_")] = getattr(ind, attr)
            except Exception:
                out[attr.lstrip("_")] = None
    for key, val in _snapshot_view(ind).items():
        if val is not None:
            out[key] = val
    return out


def _strategy_htf_state(strategy_id: str, strat) -> dict:
    """HTF state lives per-strategy since the per-strategy runtime refactor;
    expose it (plus a flattened slow-indicator snapshot) instead of the removed
    aggregate htf engine."""
    hts = {}
    try:
        hts = dict(strat.slow_htf_state.snapshot())
    except Exception:
        return {}
    hts["strategy_id"] = strategy_id
    slow_ind = getattr(strat, "slow_indicator", None)
    if slow_ind is not None:
        slow_flat = _flat_indicator(slow_ind)
        if slow_flat:
            hts["indicator"] = slow_flat
    return hts


def _reconcile_open_position(strategy_id: str, snap: dict, env=None) -> dict:
    """Reconcile the visible strategy state against the open-position truth so
    the Strategy Matrix / Positions panel can never disagree.

    The position manager is the authoritative source for open positions.  A
    strategy object's own ``position_side``/``state`` can lag or fail to be set
    (e.g. after a crash-restart restore that backs the position manager but not
    the strategy object), which made the Strategy Matrix show FLAT/None for
    strategies that hold an open position.  Derive the reported state from the
    open position when present so every consumer sees consistent data.
    ``env`` scopes the reconciliation to one execution environment (PAPER by
    default) so the live book and the paper book never contaminate each other.
    """
    env = _env_for(env)
    pm = getattr(env, "position_manager", None) if env is not None else None
    if pm is None:
        if _engine is None or _engine.position_manager is None:
            return snap
        pm = _engine.position_manager
    out = dict(snap)
    try:
        positions = pm.get_positions_by_strategy(strategy_id)
    except Exception:
        return out
    open_pos = next((p for p in positions if getattr(p, "is_open", False)), None)
    if open_pos is not None and open_pos.is_open:
        if open_pos.is_long:
            out["state"] = out.get("state") or "flat"
            out["state"] = "long_position" if out["state"] not in (
                "long_position", "short_position") else out["state"]
            out["position_side"] = "LONG"
        else:
            out["state"] = "short_position" if out.get("state") not in (
                "long_position", "short_position") else out["state"]
            out["position_side"] = "SHORT"
        stop = getattr(open_pos, "stop_price", None)
        if stop is not None:
            out["stop_price"] = stop
    return out


def _enrich_pending_entry(pe: dict, persistence=None) -> dict:
    """Fill missing pending-entry candle/indicator fields from the signals DB.

    After a restart the in-memory pending_entry is reconstructed from a bare
    snapshot that lacks the full signal candle context.  The signals table
    (UPSERT at arming time) has the authoritative OHLC + DEMA/ATR values.
    We look up by signal_id and populate any null UI keys (belt-and-suspenders).
    """
    if pe is None or not isinstance(pe, dict):
        return pe
    if persistence is None:
        persistence = getattr(_engine, "_persistence", None)
    if persistence is None:
        return pe
    signal_id = pe.get("signal_id")
    if not signal_id:
        return pe
    # Fields already present (non-null) are kept — only fill what's missing.
    missing = any(pe.get(k) is None for k in (
        "signal_candle_start", "signal_candle_open", "signal_candle_high",
        "signal_candle_low", "signal_candle_close",
        "signal_htf_dema_atr", "signal_mid_dema_atr", "signal_fast_dema_atr",
    ))
    if not missing:
        return pe
    try:
        sig = persistence.get_signal(signal_id)
    except Exception:
        return pe
    if sig is None:
        return pe
    if pe.get("signal_candle_start") is None and sig.get("candle_timestamp"):
        pe["signal_candle_start"] = sig["candle_timestamp"]
    if pe.get("signal_candle_open") is None and sig.get("open") is not None:
        pe["signal_candle_open"] = sig["open"]
    if pe.get("signal_candle_high") is None and sig.get("high") is not None:
        pe["signal_candle_high"] = sig["high"]
    if pe.get("signal_candle_low") is None and sig.get("low") is not None:
        pe["signal_candle_low"] = sig["low"]
    if pe.get("signal_candle_close") is None and sig.get("close") is not None:
        pe["signal_candle_close"] = sig["close"]
    if pe.get("signal_htf_dema_atr") is None and sig.get("htf_value") is not None:
        pe["signal_htf_dema_atr"] = sig["htf_value"]
    if pe.get("signal_mid_dema_atr") is None and sig.get("mid_value") is not None:
        pe["signal_mid_dema_atr"] = sig["mid_value"]
    if pe.get("signal_fast_dema_atr") is None and sig.get("fast_dema") is not None:
        pe["signal_fast_dema_atr"] = sig["fast_dema"]
    return pe


def _list_strategies_sync(instrument: Optional[str] = None, status: Optional[str] = None,
                          env=None):
    if not _engine:
        return {"error": "Engine not initialized"}
    try:
        env = _env_for(env)
        strategies = getattr(env, "strategies", None) if env is not None else None
        if strategies is None:
            strategies = _engine.strategies
        pnl_engines = getattr(env, "pnl_engines", None) if env is not None else None
        if pnl_engines is None:
            pnl_engines = _engine.pnl_engines
        result = []
        for name, strat in strategies.items():
            snap = _reconcile_open_position(name, strat.snapshot(), env)
            inst = snap.get("instrument", "")
            state = snap.get("state", "unknown")
            if instrument and inst != instrument.upper():
                continue
            if status and state != status:
                continue
            pnl_eng = pnl_engines.get(name)
            pnl_snap = pnl_eng.snapshot() if pnl_eng else {}
            cfg = _engine.config.strategy(name)
            result.append({
                "strategy_id": name,
                "instrument": inst,
                "fast_timeframe": cfg.get("fast_timeframe", strat.fast_timeframe),
                "htf_timeframe": cfg.get("htf_timeframe", strat.htf_timeframe),
                "quantity": cfg.get("quantity", strat.quantity),
                "enabled": snap.get("enabled", cfg.get("enabled", True)),
                "state": state,
                "position_side": snap.get("position_side"),
                "stop_price": snap.get("stop_price"),
                "pending_entry": _enrich_pending_entry(snap.get("pending_entry")),
                "bars_processed": snap.get("bars_processed", 0),
                "trade_count": pnl_snap.get("trade_count", 0),
                "wins": pnl_snap.get("wins", 0),
                "losses": pnl_snap.get("losses", 0),
                "win_rate": pnl_snap.get("win_rate", 0),
                "realized_net": pnl_snap.get("realized_net", 0),
                "realized_gross": pnl_snap.get("realized_gross", 0),
                "realized_charges": pnl_snap.get("realized_charges", 0),
            })
        return {"strategies": result, "count": len(result)}
    except Exception as e:
        return {"error": str(e)}


@router.get("/api/strategies")
async def list_strategies(instrument: Optional[str] = None, status: Optional[str] = None):
    return await asyncio.to_thread(_list_strategies_sync, instrument, status)

# Env-scoped alias: /api/{env}/strategies
@router.get("/api/{env}/strategies")
async def list_strategies_env(env: str, instrument: Optional[str] = None,
                              status: Optional[str] = None):
    if _resolve_env(_engine, env) is None:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"environment '{env}' not found")
    return await asyncio.to_thread(_list_strategies_sync, instrument, status, env)


def _get_strategy_sync(strategy_id: str, env=None):
    env = _env_for(env)
    strategies = getattr(env, "strategies", None) if env is not None else None
    if strategies is None:
        strategies = _engine.strategies if _engine else {}
    if strategies and strategy_id not in strategies:
        return {"error": f"Strategy {strategy_id} not found"}
    try:
        strat = strategies[strategy_id]
        snap = _reconcile_open_position(strategy_id, strat.snapshot(), env)
        inst = snap.get("instrument", "")
        pnl_engines = getattr(env, "pnl_engines", None) if env is not None else (_engine.pnl_engines if _engine else {})
        pnl_eng = pnl_engines.get(strategy_id)
        pnl_snap = pnl_eng.snapshot() if pnl_eng else {}
        cfg = _engine.config.strategy(strategy_id)

        fast_key = f"{strategy_id}_fast"
        fast_ind = _engine.indicators.get(fast_key)
        ind_snap = _flat_indicator(fast_ind)

        htf_state = _strategy_htf_state(strategy_id, strat)

        position_manager = getattr(env, "position_manager", None) if env is not None else (_engine.position_manager if _engine else None)
        positions = []
        if position_manager is not None:
            try:
                positions = position_manager.get_positions_by_strategy(strategy_id)
            except Exception:
                positions = []
        pos_list = []
        for p in positions:
            if not p.is_open:
                continue
            psnap = p.snapshot()
            psnap["entry_price"] = psnap.get("average_entry")
            pos_list.append(psnap)

        return {
            "strategy_id": strategy_id,
            "execution_mode": getattr(env, "mode", "PAPER") if env is not None else "PAPER",
            "gates": (_engine.strategy_gate(strategy_id)
                      if _engine is not None else {}),
            "configuration": {
                "instrument": inst,
                "fast_timeframe": cfg.get("fast_timeframe", strat.fast_timeframe),
                "htf_timeframe": cfg.get("htf_timeframe", strat.htf_timeframe),
                "quantity": cfg.get("quantity", strat.quantity),
                "enabled": snap.get("enabled", cfg.get("enabled", True)),
                "dema_period": _engine.config.get("indicators.dema_period", 3),
                "atr_period": _engine.config.get("indicators.atr_period", 6),
                "atr_factor": _engine.config.get("indicators.atr_factor", 1.0),
                "starting_capital": _engine.config.get("account.starting_capital", 0),
            },
            "current_state": {
                "state": snap.get("state"),
                "position_side": snap.get("position_side"),
                "stop_price": snap.get("stop_price"),
                "pending_entry": _enrich_pending_entry(snap.get("pending_entry")),
                "bars_processed": snap.get("bars_processed", 0),
            },
            "indicators": _with_flat_indicators(ind_snap),
            "htf": htf_state,
            "performance": pnl_snap,
            "positions": pos_list,
            "snapshot": snap,
        }
    except Exception as e:
        return {"error": str(e)}


@router.get("/api/strategies/{strategy_id}")
async def get_strategy(strategy_id: str):
    return await asyncio.to_thread(_get_strategy_sync, strategy_id)


@router.get("/api/{env}/strategies/{strategy_id}")
async def get_strategy_env(env: str, strategy_id: str):
    if _resolve_env(_engine, env) is None:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"environment '{env}' not found")
    return await asyncio.to_thread(_get_strategy_sync, strategy_id, env)


def _control_strategy_sync(strategy_id: str, action: str):
    if not _engine or strategy_id not in _engine.strategies:
        return {"error": f"Strategy {strategy_id} not found"}
    try:
        # ALL control actions funnel through the engine's per-strategy gate
        # state machine so REST, WS and future interfaces share one truth
        # (spec §4/§5).  pause/resume map to the engine's own semantics.
        result = _engine.control_strategy(strategy_id, action)
        if not result.get("success"):
            return result
        if _bus:
            _bus.publish("strategy_control", {
                "strategy_id": strategy_id, "action": result.get("action"),
                "gate": result.get("gate"), "timestamp": time.time(),
            })
        return result
    except Exception as e:
        return {"error": str(e)}


@router.post("/api/strategies/{strategy_id}/control")
async def control_strategy(strategy_id: str, body: dict):
    action = body.get("action", "")
    return await asyncio.to_thread(_control_strategy_sync, strategy_id, action)


@router.post("/api/{env}/strategies/{strategy_id}/control")
async def control_strategy_env(env: str, strategy_id: str, body: dict):
    action = body.get("action", "")
    if env not in ("paper", "live"):
        return {"success": False,
                "error": f"Unknown environment {env!r} (expected paper or live)"}
    return await asyncio.to_thread(_control_strategy_sync, strategy_id, action)


def _get_strategy_parameters_sync(strategy_id: str, env=None):
    env = _env_for(env)
    strategies = getattr(env, "strategies", None) if env is not None else (_engine.strategies if _engine else {})
    if not strategies or strategy_id not in strategies:
        return {"error": f"Strategy {strategy_id} not found"}
    try:
        strat = strategies[strategy_id]
        inst = strat.instrument
        cfg = _engine.config.strategy(strategy_id)
        inst_cfg = _engine.config.instrument(inst)
        indicators = _engine.config.get("indicators", {})
        paper = _engine.config.get("paper_execution", {})
        risk = _engine.config.get("risk", {})
        charges = _engine.config.get("charges", {}).get(inst, {})

        return {
            "strategy": {
                "fast_timeframe": cfg.get("fast_timeframe", strat.fast_timeframe),
                "htf_timeframe": cfg.get("htf_timeframe", strat.htf_timeframe),
                "quantity": cfg.get("quantity", strat.quantity),
                "enabled": cfg.get("enabled", True),
            },
            "instrument": inst_cfg,
            "indicators": indicators,
            "execution": paper,
            "risk": risk,
            "charges": charges,
            "source": "settings.json",
            "execution_mode": getattr(env, "mode", "PAPER") if env is not None else "PAPER",
        }
    except Exception as e:
        return {"error": str(e)}


@router.get("/api/strategies/{strategy_id}/parameters")
async def get_strategy_parameters(strategy_id: str):
    return await asyncio.to_thread(_get_strategy_parameters_sync, strategy_id)


@router.get("/api/{env}/strategies/{strategy_id}/parameters")
async def get_strategy_parameters_env(env: str, strategy_id: str):
    if _resolve_env(_engine, env) is None:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"environment '{env}' not found")
    return await asyncio.to_thread(_get_strategy_parameters_sync, strategy_id, env)
