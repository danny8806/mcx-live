"""LIVE / PAPER environment resolution for the dashboard's env switch.

The engine runs both execution environments in one process (Phase 1/2 dual-env
architecture).  The dashboard exposes every read endpoint twice — `/api/paper/...`
and `/api/live/...` — so the operator can compare the two books side by side.
The legacy bare `/api/...` endpoints keep meaning "paper" (backward compatible).

This module is the single place the routes resolve an env name (or an already
resolved :class:`~core.environments.Environment`) to the right execution
context, plus the per-env summary cards shown by the env switcher UI.
"""
from __future__ import annotations

from typing import Any, Optional


def resolve(engine: Any, target: Optional[str] = None) -> Any:
    """Return the :class:`Environment` for a name (default 'paper').

    ``target`` may be an Environment (passed through) or a name such as
    ``"live"`` / ``"paper"`` / ``"system"``.  Falls back gracefully for legacy
    single-env engines built without the ``_envs`` registry.
    """
    if engine is None:
        return None
    if target is not None and not isinstance(target, str):
        return target
    name = target or "paper"
    envs = getattr(engine, "_envs", None)
    # Strict name check for engines that use the _envs registry: an unknown
    # name must NOT silently resolve to the paper fallback (env endpoints 404).
    if envs is not None and name not in envs:
        alias = getattr(engine, name, None)
        if alias is not None and getattr(alias, "mode", None) is not None:
            return alias
        # Fallback: if "paper" was requested but doesn't exist, try "live"
        # (single-env live-only engine serves live on the legacy bare /api/ path).
        if target is None and "live" in envs:
            return envs["live"]
        return None
    if hasattr(engine, "_env_for"):
        try:
            return engine._env_for(name)
        except Exception:
            pass
    if envs and name in envs:
        return envs[name]
    alias = getattr(engine, name, None)
    if alias is not None and getattr(alias, "mode", None) is not None:
        return alias
    return None


def available(engine: Any) -> list[str]:
    """Enabled environment names in boot order (paper first)."""
    if engine is None:
        return []
    names = getattr(engine, "environments", None)
    if names:
        out = list(names)
        if "paper" in out:
            out.remove("paper")
            out.insert(0, "paper")
        return out
    envs = getattr(engine, "_envs", None)
    if envs:
        out = [n for n in envs if n in ("paper", "live", "system")]
        if "paper" in out:
            out.remove("paper")
            out.insert(0, "paper")
        return out
    return ["paper"]


def summary(engine: Any, name: str) -> dict:
    """One env-switcher card: mode, broker, gate, counts and live book value."""
    env = resolve(engine, name)
    if env is None:
        return {"name": name, "error": "environment not found"}
    open_positions = []
    try:
        open_positions = list(getattr(env.position_manager, "open_positions", []) or [])
    except Exception:
        pass
    orders = []
    try:
        co = getattr(env.execution_engine, "_orders", {}) or {}
        orders = list(co.values())
    except Exception:
        pass
    account = {}
    try:
        account = env.account_engine.snapshot()
    except Exception:
        pass
    broker = getattr(env, "broker", None)
    db_path = None
    try:
        db_path = env.persistence.db_path if env.persistence is not None else None
    except Exception:
        pass
    return {
        "name": name,
        "mode": getattr(env, "mode", "PAPER"),
        "is_live": bool(getattr(env, "is_live", name != "paper")),
        "gate_enabled": bool(getattr(env, "gate_enabled", False)),
        "broker": type(broker).__name__ if broker is not None else None,
        "db_path": str(db_path) if db_path else None,
        "strategy_count": len(getattr(env, "strategies", {}) or {}),
        "open_positions": len(open_positions),
        "open_orders": len(orders),
        "equity": float(account.get("equity", 0.0) or 0.0),
        "used_margin": float(account.get("used_margin", 0.0) or 0.0),
        "available_margin": float(account.get("available_margin", 0.0) or 0.0),
    }