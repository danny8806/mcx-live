"""Overview routes - portfolio summary, gold/silver panels."""
from __future__ import annotations
import asyncio
import time
from typing import Any, Optional
from fastapi import APIRouter

from dashboard.envs import resolve as _resolve_env

router = APIRouter()

_engine = None
_bus = None


def init(engine, event_bus):
    global _engine, _bus
    _engine = engine
    _bus = event_bus


def _env_for(env=None):
    """Resolve the target execution environment (defaults to PAPER)."""
    return _resolve_env(_engine, env)


def _ts():
    return time.time()


def _watch_failure_states(env):
    """Surface the ORDER WATCHER's observable failure states for the dashboard:

    PENDING ENTRY AGE  - a LIVE entry order resting at the broker past the
                         configured max age (missed / aging limit).
    UNPROTECTED POSITION - an open position with no placed/verified protective
                         SL (broker SL missing -> local stop is the safety net).
    PENDING EXIT       - an EXIT / REVERSAL_EXIT / EMERGENCY_EXIT order still
                         resting at the broker.
    UNKNOWN BROKER STATE - a tracked order the broker has not confirmed via
                         REST (status unknown / never rest_verified).
    STRATEGY LOCKED    - an entry LOCKed behind an in-flight higher-priority
                         leg (exit/SL/reversal).
    """
    states = []
    watcher = getattr(env, "order_watcher", None)
    now = time.time()
    if watcher is not None:
        try:
            snap = watcher.snapshot() or {}
            age_limit_ms = float(watcher._tick_cfg.get("max_order_age_ms", 60000) or 60000)
            for rec in (snap.get("orders") or []):
                oid = rec.get("internal_order_id")
                role = str(rec.get("order_role") or "").upper()
                status = str(rec.get("status") or "").upper()
                classification = rec.get("classification") or ""
                rest_verified = bool(rec.get("rest_verified"))
                if status in ("FILLED", "REJECTED", "CANCELLED", "CANCELED", "EXPIRED"):
                    continue
                if role and role not in ("ENTRY",):
                    if "EXIT" in role or role == "STOP_LOSS":
                        states.append({
                            "state": "PENDING EXIT",
                            "order_id": oid,
                            "strategy_id": rec.get("strategy_id"),
                            "instrument": rec.get("instrument"),
                            "role": role,
                            "detail": f"resting at broker ({status})"})
                    continue
                # ENTRY observables
                submitted = float(rec.get("submitted_at") or 0)
                age_ms = (now - submitted) * 1000.0 if submitted else 0.0
                if age_ms > age_limit_ms:
                    states.append({
                        "state": "PENDING ENTRY AGE",
                        "order_id": oid,
                        "strategy_id": rec.get("strategy_id"),
                        "instrument": rec.get("instrument"),
                        "role": role,
                        "detail": f"resting {age_ms/1000.0:.1f}s > {age_limit_ms/1000.0:.0f}s",
                        "classification": classification})
                if not rest_verified and status not in ("FILLED",):
                    states.append({
                        "state": "UNKNOWN BROKER STATE",
                        "order_id": oid,
                        "strategy_id": rec.get("strategy_id"),
                        "instrument": rec.get("instrument"),
                        "role": role,
                        "detail": f"no REST confirmation for {status}"})
                if rec.get("decision") == "LOCK":
                    states.append({
                        "state": "STRATEGY LOCKED",
                        "order_id": oid,
                        "strategy_id": rec.get("strategy_id"),
                        "instrument": rec.get("instrument"),
                        "role": role,
                        "detail": "entry locked behind a higher-priority leg"})
        except Exception as exc:
            states.append({"state": "WATCHER_UNAVAILABLE", "detail": str(exc)})
    # SL coverage from the position book.  There is no broker-side protective
    # stop: "armed" means THIS PROCESS is watching position.stop_price, and it
    # is only ever shown for an open position.
    try:
        pm = getattr(env, "position_manager", None)
        if pm is not None:
            snap = pm.snapshot() or {}
            for pos in (snap.get("open_positions") or {}).values():
                if not (pos or {}).get("is_open", False):
                    continue
                sl_state = (pos or {}).get("sl_state") or "NONE"
                if sl_state != "ARMED":
                    states.append({
                        "state": "POSITION WITHOUT ACTIVE LOCAL SL",
                        "position_id": pos.get("position_id"),
                        "strategy_id": pos.get("strategy_id"),
                        "instrument": pos.get("instrument"),
                        "stop_price": pos.get("stop_price"),
                        "detail": (f"sl_state={sl_state} "
                                   f"(local position-owned stop monitor)")})
    except Exception:
        pass
    return states


def _safe(val, default=0.0):
    return float(val) if val is not None else default


def _active_order_count(execution_engine, snapshot: dict) -> int:
    """Count in-flight orders, not all retained order history."""
    orders = snapshot.get("orders") if isinstance(snapshot, dict) else None
    if isinstance(orders, dict):
        rows = list(orders.values())
    elif isinstance(orders, (list, tuple)):
        rows = list(orders)
    else:
        retained = getattr(execution_engine, "_orders", {}) or {}
        rows = list(retained.values()) if isinstance(retained, dict) else []

    active_states = {"created", "submitted", "acknowledged", "partially_filled"}
    count = 0
    for order in rows:
        state = order.get("state") if isinstance(order, dict) else getattr(order, "state", None)
        state = getattr(state, "value", state)
        if str(state or "").strip().lower().replace(" ", "_") in active_states:
            count += 1
    return count


def _get_overview_sync(env=None):
    if not _engine:
        return {"error": "Engine not initialized"}
    try:
        env = _env_for(env)
        account = env.account_engine.snapshot()
        positions = env.position_manager.snapshot()
        open_pos = positions.get("open_positions", {})
        orders_snap = env.execution_engine.snapshot()
        risk_snap = env.risk_engine.snapshot()
        strategies_snap = {name: s.snapshot() for name, s in env.strategies.items()}
        book_equity = _safe(account.get("equity", 0))
        book_starting = _safe(account.get("starting_capital", 0))

        equity = book_equity
        starting = book_starting

        # LIVE headlines must show the REAL Dhan account (poller cache or a
        # fresh broker call), never the configured starting capital. The
        # lifecycle book stays available as book_equity/book_starting_capital.
        equity_source = "engine"
        broker_account = None
        if getattr(env, "is_live", False):
            try:
                poller = getattr(env, "poller", None)
                cached = getattr(poller, "_last_account", {}) or {}
                if isinstance(cached, dict) and cached.get("equity"):
                    broker_account = cached
                if not broker_account:
                    broker = getattr(env, "broker", None)
                    if broker is not None and hasattr(broker, "account_status"):
                        broker_account = broker.account_status() or {}
            except Exception:
                broker_account = None
            if isinstance(broker_account, dict) and broker_account.get("equity"):
                equity = _safe(broker_account.get("equity"))
                equity_source = "dhan"
            else:
                broker_account = None

        used_margin = _safe(account.get("used_margin", 0))
        available_margin = equity - used_margin
        realized = sum(
            pnl.snapshot().get("realized_net", 0) for pnl in env.pnl_engines.values()
        )
        unrealized = sum(
            pos.get("unrealized_pnl", 0) for pos in open_pos.values()
        )
        if equity_source == "dhan":
            realized = _safe(broker_account.get("realized_pnl", realized))
            unrealized = _safe(broker_account.get("unrealized_pnl", unrealized))
            used_margin = _safe(broker_account.get("used_margin", used_margin))
            available_margin = _safe(broker_account.get("available_margin", available_margin))
            starting = equity

        net_pnl = equity - starting
        if equity_source == "dhan":
            # The Dhan account is not a 1.2M starting-capital book; net P&L is
            # the broker's own realized+unrealized total.
            net_pnl = _safe(broker_account.get("realized_pnl", 0)) + _safe(broker_account.get("unrealized_pnl", 0))

        # Coerce non-finite daily_pnl to 0 so a NaN/inf value can't poison the API.
        _daily = risk_snap.get("daily_pnl", 0)
        if isinstance(_daily, float) and (_daily != _daily or abs(_daily) == float("inf")):
            _daily = 0.0
        return {
            "execution_mode": getattr(env, "mode", "PAPER"),
            "equity_source": equity_source,
            "total_equity": {"value": equity, "timestamp": _ts()},
            "starting_capital": {"value": starting, "timestamp": _ts()},
            "today_pnl": {"value": _daily, "timestamp": _ts()},
            "total_net_pnl": {"value": net_pnl, "timestamp": _ts()},
            "realized_pnl": {"value": realized, "timestamp": _ts()},
            "unrealized_pnl": {"value": unrealized, "timestamp": _ts()},
            "margin_used": {"value": used_margin, "timestamp": _ts()},
            "available_margin": {"value": available_margin, "timestamp": _ts()},
            "book_equity": {"value": book_equity, "timestamp": _ts()},
            "book_starting_capital": {"value": book_starting, "timestamp": _ts()},
            "open_positions_count": {"value": len(open_pos), "timestamp": _ts()},
            "active_orders_count": {"value": _active_order_count(env.execution_engine, orders_snap), "timestamp": _ts()},
            "active_strategies_count": {"value": len(strategies_snap), "timestamp": _ts()},
            "kill_switch": {"value": risk_snap.get("kill_switch_active", False), "timestamp": _ts()},
            "failure_states": _watch_failure_states(env),
            "strategies": strategies_snap,
            "positions": open_pos,
            "account": account,
        }
    except Exception as e:
        return {"error": str(e), "timestamp": _ts()}


@router.get("/api/overview")
async def get_overview():
    return await asyncio.to_thread(_get_overview_sync)


@router.get("/api/{env}/overview")
async def get_overview_env(env: str):
    if _resolve_env(_engine, env) is None:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"environment '{env}' not found")
    return await asyncio.to_thread(_get_overview_sync, env)


def _get_instrument_overview_sync(instrument: str):
    if not _engine:
        return {"error": "Engine not initialized"}
    try:
        e = _engine
        instrument_upper = instrument.upper()

        prices = e.execution_engine._current_prices
        ltp = prices.get(instrument_upper, 0.0)

        strat_summaries = []
        for name, strat in e.strategies.items():
            if strat.instrument != instrument_upper:
                continue
            snap = strat.snapshot()
            pnl_eng = e.pnl_engines.get(name)
            pnl_snap = pnl_eng.snapshot() if pnl_eng else {}
            strat_summaries.append({
                "strategy_id": name,
                "status": snap.get("state", "unknown"),
                "fast_timeframe": getattr(strat, "fast_timeframe", ""),
                "htf_timeframe": getattr(strat, "htf_timeframe", ""),
                "position_side": snap.get("position_side"),
                "stop_price": snap.get("stop_price"),
                "bars_processed": snap.get("bars_processed", 0),
                "pending_entry": snap.get("pending_entry"),
                "trades": pnl_snap.get("trade_count", 0),
                "win_rate": pnl_snap.get("win_rate", 0),
                "net_pnl": pnl_snap.get("realized_net", 0),
            })

        return {
            "instrument": instrument_upper,
            "ltp": ltp,
            "spread": 0.0,
            "strategies": strat_summaries,
            "timestamp": _ts(),
        }
    except Exception as e:
        return {"error": str(e), "timestamp": _ts()}


@router.get("/api/overview/{instrument}")
async def get_instrument_overview(instrument: str):
    return await asyncio.to_thread(_get_instrument_overview_sync, instrument)
