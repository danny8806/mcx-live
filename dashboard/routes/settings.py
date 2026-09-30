"""Read and safely update the running engine's settings."""
from __future__ import annotations

import asyncio
import json
import os
import tempfile
import threading
import time
from pathlib import Path

from fastapi import APIRouter, HTTPException

router = APIRouter()
_engine = None
_config_path: Path | None = None
_resolved_config_path: Path | None = None
_persistence = None
_settings_lock = threading.RLock()

_GATE_FIELDS = {
    "live_gate", "entry_enabled", "exit_enabled", "reversal_enabled",
    "sl_enabled", "close_only",
}
_ALLOWED_FIELDS = _GATE_FIELDS | {"quantity"}
_LIVE_GATES = {"ON", "OFF", "CLOSE_ONLY", "EMERGENCY_STOP", "LOCKED"}


def init(engine, event_bus, *, config_path=None, resolved_config_path=None,
         persistence=None):
    global _engine, _config_path, _resolved_config_path, _persistence
    _engine = engine
    _config_path = Path(config_path).resolve() if config_path else None
    _resolved_config_path = (Path(resolved_config_path).resolve()
                             if resolved_config_path else None)
    _persistence = persistence


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
        tg_stats = {"enabled": False, "queue_size": 0,
                    "total_sent": 0, "total_failed": 0}
    strategies = _engine.config.get("strategies", {})
    return {
        "system": _engine.config.get("system", {}),
        "dhan": {"client_id": _mask_cid(_engine.config.get("dhan.client_id", "")),
                 "ws_url": _engine.config.get("dhan.ws_url", "")},
        "instruments": _engine.config.get("instruments", {}),
        "strategies": strategies,
        "strategy_gates": {sid: _engine.strategy_gate(sid)
                           for sid in strategies},
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


def _atomic_json_write(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp",
                                     dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
            json.dump(data, stream, indent=2, ensure_ascii=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_name, path)
    finally:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass


def _validate_update(body: dict) -> dict:
    if not isinstance(body, dict) or not body:
        raise HTTPException(status_code=422, detail="settings body must be a non-empty object")
    unknown = sorted(set(body) - _ALLOWED_FIELDS)
    if unknown:
        raise HTTPException(status_code=422,
                            detail=f"unsupported setting field(s): {', '.join(unknown)}")

    update = dict(body)
    if "quantity" in update:
        value = update["quantity"]
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise HTTPException(status_code=422,
                                detail="quantity must be a positive whole number")
    if "live_gate" in update:
        update["live_gate"] = str(update["live_gate"]).upper()
        if update["live_gate"] not in _LIVE_GATES:
            raise HTTPException(status_code=422,
                                detail=f"live_gate must be one of {sorted(_LIVE_GATES)}")
    for key in _GATE_FIELDS - {"live_gate"}:
        if key in update and not isinstance(update[key], bool):
            raise HTTPException(status_code=422, detail=f"{key} must be true or false")
    return update


def _has_open_position(strategy_id: str) -> bool:
    pm = getattr(_engine, "position_manager", None)
    try:
        if pm is None:
            return True  # unknown position state must never be treated as flat
        return any(p.is_open for p in pm.get_positions_by_strategy(strategy_id))
    except Exception:
        return True  # fail closed when broker/local position state cannot be read


def _has_active_orders(strategy_id: str) -> bool:
    execution = getattr(_engine, "execution_engine", None)
    try:
        order_map = getattr(execution, "_orders", None)
        if not isinstance(order_map, dict):
            return True  # unknown order state must block quantity edits
        orders = order_map.values()
        return any(
            getattr(order, "strategy_id", None) == strategy_id
            and str(getattr(getattr(order, "state", None), "value",
                            getattr(order, "state", ""))).lower()
                in {"created", "submitted", "acknowledged", "partially_filled"}
            for order in orders
        )
    except Exception:
        return True  # fail closed for quantity edits when order state is unknown


def _save_strategy_settings_sync(strategy_id: str, body: dict):
    if not _engine:
        raise HTTPException(status_code=503, detail="Engine not initialized")
    if _config_path is None or _resolved_config_path is None:
        raise HTTPException(status_code=503,
                            detail="active LIVE configuration paths are unavailable")

    update = _validate_update(body)
    strategies = _engine.config.get("strategies", {}) or {}
    if strategy_id not in strategies or strategy_id not in _engine.strategies:
        raise HTTPException(status_code=404, detail="strategy not found")

    strategy = _engine.strategies[strategy_id]
    is_open = _has_open_position(strategy_id)
    if "quantity" in update and update["quantity"] != int(strategy.quantity):
        if is_open or strategy.pending_entry is not None or _has_active_orders(strategy_id):
            raise HTTPException(
                status_code=409,
                detail="quantity can only change while the strategy is flat with no pending orders",
            )
    gate = _engine.strategy_gate(strategy_id)
    next_gate = {**gate, **{k: v for k, v in update.items() if k in _GATE_FIELDS}}
    if is_open and (not next_gate["exit_enabled"] or not next_gate["sl_enabled"]
                    or next_gate["live_gate"] == "LOCKED"):
        raise HTTPException(
            status_code=409,
            detail="an open position requires exits and stop-loss enabled; LOCKED is unavailable",
        )

    with _settings_lock:
        try:
            raw = json.loads(_config_path.read_text(encoding="utf-8"))
            raw_strategy = raw.get("strategies", {}).get(strategy_id)
            if not isinstance(raw_strategy, dict):
                raise HTTPException(status_code=404,
                                    detail="strategy missing from source configuration")
            resolved = json.loads(_resolved_config_path.read_text(encoding="utf-8"))
            resolved_strategy = resolved.get("strategies", {}).get(strategy_id)
            if not isinstance(resolved_strategy, dict):
                raise HTTPException(status_code=503,
                                    detail="strategy missing from resolved configuration")
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=503,
                                detail=f"could not read active config: {exc}") from exc

        raw_strategy.update(update)
        resolved_strategy.update(update)
        try:
            original_raw = _config_path.read_bytes()
            original_resolved = _resolved_config_path.read_bytes()
            _atomic_json_write(_config_path, raw)
            _atomic_json_write(_resolved_config_path, resolved)
        except Exception as exc:
            # Keep the source and runtime config copies aligned if the second
            # replacement fails. Restore only files whose writes completed.
            try:
                if "original_raw" in locals():
                    _atomic_json_write(_config_path, json.loads(original_raw))
                if "original_resolved" in locals():
                    _atomic_json_write(_resolved_config_path, json.loads(original_resolved))
            except Exception:
                # The original write error remains the actionable failure;
                # never apply a partially persisted setting to the engine.
                pass
            raise HTTPException(status_code=500,
                                detail=f"could not persist settings: {exc}") from exc

        # The engine and REST read paths share Config's live resolved dictionary.
        config_data = getattr(_engine.config, "_config", None)
        if isinstance(config_data, dict):
            config_data["strategies"][strategy_id].update(update)
        if "quantity" in update:
            strategy.quantity = update["quantity"]
        gate_update = {k: v for k, v in update.items() if k in _GATE_FIELDS}
        if gate_update:
            _engine.set_strategy_gate(strategy_id, **gate_update)
        if _persistence is not None:
            try:
                _persistence.save_state(_engine.snapshot("live"))
            except Exception:
                # The config file is authoritative across restart; surface the
                # state snapshot issue without rolling back the applied config.
                pass
        notify = getattr(_engine, "notify_settings_refreshed", None)
        if callable(notify):
            notify()

    return {
        "status": "saved",
        "strategy_id": strategy_id,
        "quantity": strategy.quantity,
        "gate": _engine.strategy_gate(strategy_id),
        "timestamp": time.time(),
    }


@router.put("/api/settings/strategies/{strategy_id}")
async def save_strategy_settings(strategy_id: str, body: dict):
    return await asyncio.to_thread(_save_strategy_settings_sync, strategy_id, body)


def _refresh_settings_sync():
    if not _engine:
        return {"error": "Engine not initialized"}
    if _resolved_config_path is None:
        return {"error": "active LIVE configuration path is unavailable"}
    applied = []
    warnings = []
    try:
        _engine.config.load(str(_resolved_config_path))
        for name, strat in _engine.strategies.items():
            cfg = _engine.config.strategy(name)
            if not cfg:
                continue
            if int(cfg.get("quantity", strat.quantity)) != strat.quantity:
                if (_has_open_position(name) or strat.pending_entry is not None
                        or _has_active_orders(name)):
                    warnings.append(f"{name}: quantity change deferred while a trade/order is active")
                else:
                    strat.quantity = int(cfg["quantity"])
                    applied.append({"strategy_id": name,
                                    "changed": {"quantity": strat.quantity}})
            if "enabled" in cfg and bool(cfg.get("enabled")) != strat.enabled:
                strat.enabled = bool(cfg.get("enabled"))
                applied.append({"strategy_id": name,
                                "changed": {"enabled": strat.enabled}})
            if cfg.get("fast_timeframe") != strat.fast_timeframe:
                warnings.append(f"{name}: fast_timeframe change requires engine restart")
            if cfg.get("htf_timeframe") != strat.htf_timeframe:
                warnings.append(f"{name}: htf_timeframe change requires engine restart")
        _engine.notify_settings_refreshed()
    except Exception as e:
        return {"error": str(e)}
    return {"status": "refreshed", "applied": applied,
            "warnings": warnings, "timestamp": time.time()}


@router.post("/api/settings/refresh")
async def refresh_settings():
    return await asyncio.to_thread(_refresh_settings_sync)
