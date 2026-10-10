"""Read and safely update the running engine's settings."""
from __future__ import annotations

import asyncio
from contextlib import nullcontext
import json
import math
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
_event_bus = None
_settings_lock = threading.RLock()

_GATE_FIELDS = {
    "live_gate", "entry_enabled", "exit_enabled", "reversal_enabled",
    "close_only",
}
_ALLOWED_FIELDS = _GATE_FIELDS | {"quantity", "supertrend"}
_LIVE_GATES = {"ON", "OFF", "CLOSE_ONLY", "EMERGENCY_STOP", "LOCKED"}
_SUPERTREND_FIELDS = {"atr_period", "multiplier", "source", "atr_method"}
_SUPERTREND_SOURCES = {"open", "high", "low", "close", "hl2", "hlc3", "ohlc4"}
_SUPERTREND_ATR_METHODS = {"sma_tr", "wilder"}
_MAX_LIVE_ORDER_QUANTITY = 100
_MAX_SUPERTREND_ATR_PERIOD = 500


def init(engine, event_bus, *, config_path=None, resolved_config_path=None,
         persistence=None):
    global _engine, _event_bus, _config_path, _resolved_config_path, _persistence
    _engine = engine
    _event_bus = event_bus
    _config_path = Path(config_path).resolve() if config_path else None
    _resolved_config_path = (Path(resolved_config_path).resolve()
                             if resolved_config_path else None)
    _persistence = persistence


def _publish_settings_change(data: dict) -> None:
    """Publish successful operator changes to dashboard audit and Telegram.

    Audit/notification failures must never roll back a setting that has already
    been atomically persisted and applied to the running engine.
    """
    try:
        if _event_bus is not None:
            _event_bus.publish("settings_changed", data)
    except Exception:
        pass
    try:
        router = getattr(_engine, "telegram", None)
        notify = getattr(router, "on_configuration_change", None)
        if callable(notify):
            notify(data)
    except Exception:
        pass


def _mask_cid(cid) -> str:
    cid = str(cid or "")
    if len(cid) <= 4:
        return cid
    return f"{cid[:3]}{'*' * (len(cid) - 5)}{cid[-2:]}"


def _live_environment():
    envs = getattr(_engine, "_envs", {}) or {}
    env = envs.get("live") if isinstance(envs, dict) else None
    return env or getattr(_engine, "live", None)


def _get_settings_sync():
    if not _engine:
        return {"error": "Engine not initialized"}
    tg_stats = {}
    try:
        tg_stats = _engine.telegram.get_stats()
    except Exception:
        tg_stats = {"enabled": False, "queue_size": 0,
                    "total_sent": 0, "total_failed": 0}
    strategies = _engine.config.get("strategies", {}) or {}
    live_cfg = _engine.config.get("live", {}) or {}
    live_env = _live_environment()
    runtime_policy = _engine.config.get("strategy_runtime", {}) or {}
    if runtime_policy.get("exclusive"):
        allowed_ids = set((runtime_policy.get("allowlist", {}) or {}).keys())
        strategies = {sid: cfg for sid, cfg in strategies.items()
                      if sid in allowed_ids}
        active_instruments = {cfg.get("instrument") for cfg in strategies.values()}
        instruments = {name: cfg for name, cfg in
                       (_engine.config.get("instruments", {}) or {}).items()
                       if name in active_instruments}
        indicator_settings = {
            "mode": "strategy_owned",
            "strategy": "SuperTrend",
            "timeframe": "15m",
            "higher_timeframe": False,
        }
    else:
        instruments = _engine.config.get("instruments", {})
        indicator_settings = _engine.config.get("indicators", {})
    return {
        "system": _engine.config.get("system", {}),
        "live_master_gate": {
            "gate": (getattr(live_env, "gate_state", None)
                     or live_cfg.get("gate", "OFF")),
            "live_trading_enabled": bool(
                live_cfg.get("live_trading_enabled", False)),
            "broker_gate_enabled": bool(getattr(
                getattr(live_env, "broker", None), "gate_enabled", False)),
        },
        "dhan": {"client_id": _mask_cid(_engine.config.get("dhan.client_id", "")),
                 "ws_url": _engine.config.get("dhan.ws_url", "")},
        "instruments": instruments,
        "strategies": strategies,
        "strategy_gates": {sid: _engine.strategy_gate(sid)
                           for sid in strategies},
        "indicators": indicator_settings,
        "strategy_runtime": {
            "exclusive": bool(runtime_policy.get("exclusive")),
            "active_strategy_ids": sorted(strategies),
            "only_completed_candles": "15m",
        },
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


def _local_live_gate_blocker(env) -> str | None:
    """Check in-process and durable state while the engine lock is held."""
    manager = getattr(env, "position_manager", None)
    if manager is None:
        return "LIVE position state is unavailable; master gate was not changed"
    try:
        if any(getattr(position, "is_open", False)
               for position in manager.open_positions):
            return "Close all live positions before disabling the master gate"
    except Exception:
        return "LIVE position state could not be verified; master gate was not changed"
    for strategy in (getattr(env, "strategies", {}) or {}).values():
        if (getattr(strategy, "pending_entry", None) is not None
                or getattr(strategy, "pending_exit_trigger", None) is not None):
            return "Resolve pending strategy triggers before disabling the master gate"

    execution = getattr(env, "execution_engine", None)
    orders = getattr(execution, "_orders", None)
    if not isinstance(orders, dict):
        return "LIVE order state is unavailable; master gate was not changed"
    terminal = {"filled", "rejected", "canceled", "cancelled", "expired"}
    for order in orders.values():
        state = getattr(order, "state", None)
        state = str(getattr(state, "value", state) or "unknown").lower()
        if state not in terminal:
            return "Resolve active or unknown orders before disabling the master gate"

    persistence = getattr(env, "persistence", None)
    if persistence is not None:
        try:
            rows = persistence.get_pending_orders(execution_mode="LIVE")
        except Exception:
            return "Durable LIVE order state could not be verified; master gate was not changed"
        active_pending = {"pending", "armed", "entry_sent", "waiting_for_flat"}
        if any(str(row.get("status", "unknown")).lower() in active_pending
               for row in rows):
            return "A durable LIVE trigger or order is still active"
    return None


def _live_gate_blocker(env) -> str | None:
    """Fail closed before disabling the broker gate around exposure/order state."""
    blocker = _local_live_gate_blocker(env)
    if blocker:
        return blocker
    broker = getattr(env, "broker", None)
    positions_method = getattr(broker, "positions", None)
    if not callable(positions_method):
        return "Broker position lookup is unavailable; master gate was not changed"
    try:
        if positions_method():
            return "Dhan reports an open position; use per-strategy CLOSE ONLY instead"
    except Exception:
        return "Dhan position lookup failed; master gate was not changed"
    return None


def _save_live_gate_sync(body: dict):
    if not _engine:
        raise HTTPException(status_code=503, detail="Engine not initialized")
    if _config_path is None or _resolved_config_path is None:
        raise HTTPException(status_code=503,
                            detail="active LIVE configuration paths are unavailable")
    if not isinstance(body, dict) or set(body) != {"gate"}:
        raise HTTPException(status_code=422,
                            detail="body must contain only gate: ON or OFF")
    gate = str(body.get("gate", "")).upper()
    if gate not in {"ON", "OFF"}:
        raise HTTPException(status_code=422, detail="gate must be ON or OFF")

    live_env = _live_environment()
    if live_env is None or not getattr(live_env, "is_live", False):
        raise HTTPException(status_code=503, detail="LIVE environment is unavailable")
    live_cfg = _engine.config.get("live", {}) or {}
    if gate == "ON" and not bool(live_cfg.get("live_trading_enabled", False)):
        raise HTTPException(status_code=409,
                            detail="live_trading_enabled is false in the active configuration")
    if gate == "OFF":
        blocker = _live_gate_blocker(live_env)
        if blocker:
            raise HTTPException(status_code=409, detail=blocker)

    state_lock = getattr(_engine, "_lock", None) or nullcontext()
    with _settings_lock, state_lock:
        # Recheck local ownership after acquiring the engine lock so a signal
        # or fill cannot race the gate transition.
        if gate == "OFF":
            blocker = _local_live_gate_blocker(live_env)
            if blocker:
                raise HTTPException(status_code=409, detail=blocker)
        previous_gate = (getattr(live_env, "gate_state", None)
                         or live_cfg.get("gate", "OFF"))
        try:
            raw = json.loads(_config_path.read_text(encoding="utf-8"))
            resolved = json.loads(_resolved_config_path.read_text(encoding="utf-8"))
            if not isinstance(raw.get("live"), dict) or not isinstance(
                    resolved.get("live"), dict):
                raise ValueError("live configuration section is missing")
            raw["live"]["gate"] = gate
            resolved["live"]["gate"] = gate
            original_raw = _config_path.read_bytes()
            original_resolved = _resolved_config_path.read_bytes()
            _atomic_json_write(_config_path, raw)
            _atomic_json_write(_resolved_config_path, resolved)
        except Exception as exc:
            try:
                if "original_raw" in locals():
                    _atomic_json_write(_config_path, json.loads(original_raw))
                if "original_resolved" in locals():
                    _atomic_json_write(_resolved_config_path,
                                       json.loads(original_resolved))
            except Exception:
                pass
            raise HTTPException(status_code=500,
                                detail=f"could not persist LIVE gate: {exc}") from exc

        config_data = getattr(_engine.config, "_config", None)
        if isinstance(config_data, dict):
            config_data.setdefault("live", {})["gate"] = gate
        live_env.gate_state = gate
        live_env.gate_enabled = gate == "ON"
        broker = getattr(live_env, "broker", None)
        if broker is not None and hasattr(broker, "gate_enabled"):
            broker.gate_enabled = gate == "ON"
        execution_broker = getattr(
            getattr(live_env, "execution_engine", None), "broker", None)
        if execution_broker is not None and hasattr(execution_broker,
                                                    "gate_enabled"):
            execution_broker.gate_enabled = gate == "ON"
        if _persistence is not None:
            try:
                _persistence.save_state(_engine.snapshot("live"))
            except Exception:
                pass
    if previous_gate != gate:
        _publish_settings_change({
            "scope": "live master gate",
            "changes": {"gate": {"from": previous_gate, "to": gate}},
            "timestamp": time.time(),
        })
    return {"status": "saved", "gate": gate,
            "live_trading_enabled": bool(live_cfg.get("live_trading_enabled")),
            "broker_gate_enabled": bool(getattr(broker, "gate_enabled", False)),
            "timestamp": time.time()}


@router.put("/api/settings/live-gate")
async def save_live_gate(body: dict):
    return await asyncio.to_thread(_save_live_gate_sync, body)


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
    if "supertrend" in update:
        params = update["supertrend"]
        if not isinstance(params, dict) or not params:
            raise HTTPException(status_code=422,
                                detail="supertrend must be a non-empty object")
        unknown_params = sorted(set(params) - _SUPERTREND_FIELDS)
        if unknown_params:
            raise HTTPException(status_code=422,
                                detail="unsupported SuperTrend field(s): "
                                       + ", ".join(unknown_params))
        params = dict(params)
        if "atr_period" in params:
            value = params["atr_period"]
            if (isinstance(value, bool) or not isinstance(value, int)
                    or value < 1 or value > _MAX_SUPERTREND_ATR_PERIOD):
                raise HTTPException(status_code=422,
                                    detail=f"SuperTrend ATR period must be between 1 and {_MAX_SUPERTREND_ATR_PERIOD}")
        if "multiplier" in params:
            value = params["multiplier"]
            if (isinstance(value, bool) or not isinstance(value, (int, float))
                    or not math.isfinite(float(value)) or value <= 0):
                raise HTTPException(status_code=422,
                                    detail="SuperTrend multiplier must be positive")
        if "source" in params:
            params["source"] = str(params["source"]).lower()
            if params["source"] not in _SUPERTREND_SOURCES:
                raise HTTPException(status_code=422,
                                    detail=f"SuperTrend source must be one of {sorted(_SUPERTREND_SOURCES)}")
        if "atr_method" in params:
            params["atr_method"] = str(params["atr_method"]).lower()
            if params["atr_method"] not in _SUPERTREND_ATR_METHODS:
                raise HTTPException(status_code=422,
                                    detail=f"SuperTrend ATR method must be one of {sorted(_SUPERTREND_ATR_METHODS)}")
        update["supertrend"] = params
    if "quantity" in update:
        value = update["quantity"]
        if (isinstance(value, bool) or not isinstance(value, int)
                or value < 1 or value > _MAX_LIVE_ORDER_QUANTITY):
            raise HTTPException(status_code=422,
                                detail=f"quantity must be a whole number between 1 and {_MAX_LIVE_ORDER_QUANTITY}")
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
    if "supertrend" in update:
        if getattr(strategy, "strategy_type", None) != "supertrend":
            raise HTTPException(status_code=409,
                                detail="SuperTrend parameters apply only to a SuperTrend strategy")
        if (is_open or strategy.pending_entry is not None
                or getattr(strategy, "pending_exit_trigger", None) is not None
                or _has_active_orders(strategy_id)):
            raise HTTPException(status_code=409,
                                detail="SuperTrend parameters can change only while flat with no pending trigger or order")
    if "quantity" in update and update["quantity"] != int(strategy.quantity):
        if is_open or strategy.pending_entry is not None or _has_active_orders(strategy_id):
            raise HTTPException(
                status_code=409,
                detail="quantity can only change while the strategy is flat with no pending orders",
            )
    gate = _engine.strategy_gate(strategy_id)
    next_gate = {**gate, **{k: v for k, v in update.items() if k in _GATE_FIELDS}}
    if is_open and (not next_gate["exit_enabled"]
                    or next_gate["live_gate"] == "LOCKED"):
        raise HTTPException(
            status_code=409,
            detail="an open position requires exits enabled; LOCKED is unavailable",
        )

    state_lock = getattr(_engine, "_lock", None) or nullcontext()
    with _settings_lock, state_lock:
        # Recheck against the event-processing lock immediately before saving
        # and applying; a candle cannot arm a trigger between validation and
        # indicator reconfiguration.
        if "supertrend" in update and (
                _has_open_position(strategy_id)
                or strategy.pending_entry is not None
                or getattr(strategy, "pending_exit_trigger", None) is not None
                or _has_active_orders(strategy_id)):
            raise HTTPException(status_code=409,
                                detail="SuperTrend parameters can change only while flat with no pending trigger or order")
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

        if "supertrend" in update:
            update["supertrend"] = {
                **(raw_strategy.get("supertrend") or {}),
                **update["supertrend"],
            }
        previous_values = {}
        for key, value in update.items():
            old_value = raw_strategy.get(key)
            if key == "supertrend":
                old_params = raw_strategy.get("supertrend") or {}
                previous_values[key] = {
                    name: {"from": old_params.get(name), "to": new_value}
                    for name, new_value in value.items()
                    if old_params.get(name) != new_value
                }
            elif old_value != value:
                previous_values[key] = {"from": old_value, "to": value}
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
        if "supertrend" in update:
            current = dict(_engine.config.get("strategies", {})[strategy_id]
                           .get("supertrend") or {})
            current.update(update["supertrend"])
            strategy.supertrend.configure(
                atr_period=current.get("atr_period", strategy.supertrend.atr_period),
                multiplier=current.get("multiplier", strategy.supertrend.multiplier),
                source=current.get("source", strategy.supertrend.source_name),
                atr_method=current.get("atr_method", strategy.supertrend.atr_method),
            )
            config_data = getattr(_engine.config, "_config", None)
            if isinstance(config_data, dict):
                config_data["strategies"][strategy_id]["supertrend"] = current
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

    changes = {}
    for key, values in previous_values.items():
        if key == "supertrend":
            changes.update({f"supertrend.{name}": pair
                            for name, pair in values.items()})
        else:
            changes[key] = values
    if changes:
        _publish_settings_change({
            "scope": "strategy",
            "strategy_id": strategy_id,
            "changes": changes,
            "timestamp": time.time(),
        })

    return {
        "status": "saved",
        "strategy_id": strategy_id,
        "quantity": strategy.quantity,
        "gate": _engine.strategy_gate(strategy_id),
        "supertrend": (strategy.supertrend.snapshot()
                       if getattr(strategy, "strategy_type", None) == "supertrend"
                       else None),
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
            if (cfg.get("strategy_type") != "supertrend"
                    or cfg.get("signal_timeframe") != "15m"):
                warnings.append(
                    f"{name}: runtime requires the configured 15m SuperTrend profile")
        _engine.notify_settings_refreshed()
    except Exception as e:
        return {"error": str(e)}
    return {"status": "refreshed", "applied": applied,
            "warnings": warnings, "timestamp": time.time()}


@router.post("/api/settings/refresh")
async def refresh_settings():
    return await asyncio.to_thread(_refresh_settings_sync)
