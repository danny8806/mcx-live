"""Reversals routes — the reversal-chain dashboard view.

Each row is a durable reversal lifecycle record: the OLD-position exit
(REVERSAL_EXIT, ORDER_A) and the NEW opposite entry (REVERSAL_ENTRY,
ORDER_B) with the SAME reversal trigger price.  The chain render is NEVER
marked complete until the old position is provably flat AND the new entry
fill is broker-confirmed AND the new protective SL is placed.
"""
from __future__ import annotations
import asyncio
from typing import Optional
from fastapi import APIRouter
router = APIRouter()
_engine = None
_bus = None
_persistence = None

def init(engine, event_bus, persistence=None):
    global _engine, _bus, _persistence
    _engine = engine
    _bus = event_bus
    _persistence = persistence

def _env_for(env=None):
    from dashboard.envs import resolve as _resolve_env
    return _resolve_env(_engine, env)


def _chain_payload(rev: dict, signal: Optional[dict] = None) -> dict:
    """Render the reversal chain from one durable reversal record.

    statuses (display only): REVERSAL SIGNAL -> TRIGGERED -> OLD EXIT
    (order/status) -> OLD FLAT -> NEW ENTRY (order/status) -> NEW POSITION
    -> NEW SL.  The persistent ``status`` field is the gate: the record goes
    PENDING_EXIT -> EXIT_FILLED -> ENTRY_SUBMITTED -> COMPLETE.

    The optional ``signal`` row (persistence.get_signal by signal_id)
    enriches the payload with the signal-candle fields (security_id,
    signal_timestamp, side); strategy_name is the canonical strategy_id used
    across every read model — there is no separate display-name concept.
    """
    status = rev.get("status") or "PENDING_EXIT"
    old_exit_status = rev.get("old_exit_broker_status") or "SUBMITTED"
    new_entry_status = rev.get("new_entry_broker_status") or "PENDING"
    old_sl = rev.get("old_sl_order_id")
    new_sl = rev.get("new_sl_order_id")
    old_flat = bool(rev.get("exit_verified_at"))
    entry_confirmed = bool(rev.get("entry_fill_confirmed_at"))
    signal = signal or {}
    old_sl_cancelled_ok = bool(old_flat)
    chain = [
        {"step": "REVERSAL SIGNAL", "value": rev.get("signal_id"),
         "ok": bool(rev.get("signal_id"))},
        {"step": "TRIGGER", "value": rev.get("reversal_trigger_price"),
         "ok": rev.get("reversal_trigger_price") is not None},
        {"step": "OLD EXIT", "value": rev.get("old_exit_order_id"),
         "detail": old_exit_status,
         "ok": old_exit_status == "FILLED"},
        {"step": "OLD SL CANCEL",
         "value": old_sl,
         "detail": (rev.get("old_sl_state") or "cancelled")
         if old_flat else "pending",
         "ok": old_sl_cancelled_ok},
        {"step": "OLD SL VERIFIED",
         "value": None,
         "detail": "verified" if old_flat else "pending",
         "ok": old_sl_cancelled_ok},
        {"step": "OLD POSITION FLAT",
         "value": rev.get("old_position_id"),
         "detail": "flat" if old_flat else "open",
         "ok": bool(old_flat)},
        {"step": "NEW ENTRY",
         "value": rev.get("new_entry_order_id"),
         "detail": new_entry_status,
         "ok": new_entry_status == "FILLED"},
        {"step": "NEW FILL",
         "value": rev.get("new_entry_fill_price"),
         "detail": ("qty %s" % rev.get("new_entry_filled_quantity"))
         if rev.get("new_entry_filled_quantity") else "",
         "ok": new_entry_status == "FILLED" and bool(rev.get("entry_fill_confirmed_at"))},
        {"step": "NEW POSITION",
         "value": rev.get("new_position_id"),
         "detail": ("qty %s" % rev.get("new_entry_filled_quantity"))
         if rev.get("new_entry_filled_quantity") else "",
         "ok": bool(rev.get("new_position_id"))},
        {"step": "NEW SL", "value": new_sl,
         "detail": rev.get("new_sl_state") or "",
         "ok": bool(new_sl) and rev.get("new_sl_state") == "placed"},
    ]
    complete = (
        status == "COMPLETE"
        and old_flat
        and entry_confirmed
        and bool(new_sl)
    )
    return {
        "reversal_id": rev.get("reversal_id"),
        "signal_id": rev.get("signal_id"),
        "strategy_id": rev.get("strategy_id"),
        "strategy_name": rev.get("strategy_id"),
        "instrument": rev.get("instrument"),
        "security_id": signal.get("security_id") if signal.get("security_id") not in (None, "None") else None,
        "signal_timestamp": signal.get("signal_timestamp"),
        "side": signal.get("side"),
        "trigger_price": rev.get("reversal_trigger_price"),
        "old_trade_id": rev.get("old_trade_id"),
        "old_position_id": rev.get("old_position_id"),
        "old_exit_order_id": rev.get("old_exit_order_id"),
        "old_broker_order_id": rev.get("old_broker_order_id"),
        "old_exit_status": old_exit_status,
        "old_exit_fill_price": rev.get("old_exit_fill_price"),
        "old_exit_filled_quantity": rev.get("old_exit_filled_quantity"),
        "old_sl_order_id": old_sl,
        "old_sl_status": rev.get("old_sl_state"),
        "new_trade_id": rev.get("new_trade_id"),
        "new_position_id": rev.get("new_position_id"),
        "new_entry_order_id": rev.get("new_entry_order_id"),
        "new_broker_order_id": rev.get("new_broker_order_id"),
        "new_entry_status": new_entry_status,
        "new_entry_fill_price": rev.get("new_entry_fill_price"),
        "new_entry_filled_quantity": rev.get("new_entry_filled_quantity"),
        "new_sl_order_id": new_sl,
        "new_sl_status": rev.get("new_sl_state"),
        "exit_verified_at": rev.get("exit_verified_at"),
        "entry_fill_confirmed_at": rev.get("entry_fill_confirmed_at"),
        "fallback_used": bool(rev.get("fallback_used")),
        "fallback_status": rev.get("fallback_status"),
        "status": status,
        "complete": complete,
        "chain": chain,
        "created_at": rev.get("created_at"),
        "updated_at": rev.get("updated_at"),
    }


def _signal_row(persistence, signal_id) -> dict:
    """Best-effort enrichment from the durable signals row for a signal id."""
    if persistence is None or not signal_id:
        return {}
    try:
        row = persistence.get_signal(signal_id)
        return row if row else {}
    except Exception:
        return {}


def _list_reversals_sync(strategy: Optional[str] = None,
                         env=None) -> dict:
    try:
        env = _env_for(env)
        persistence = (getattr(env, "persistence", None) if env is not None
                       else _persistence)
        if persistence is None:
            return {"error": "Persistence not initialized"}
        rows = persistence.get_reversals(strategy_id=strategy, limit=200)
        signals_cache = {}
        def _sig(signal_id):
            if signal_id not in signals_cache:
                signals_cache[signal_id] = _signal_row(persistence, signal_id)
            return signals_cache[signal_id]
        return {
            "execution_mode": getattr(env, "mode", "LIVE") if env else None,
            "reversals": [_chain_payload(r, _sig(r.get("signal_id"))) for r in rows],
            "count": len(rows),
        }
    except Exception as e:
        return {"error": str(e)}


@router.get("/api/reversals")
async def list_reversals(strategy: Optional[str] = None):
    return await asyncio.to_thread(_list_reversals_sync, strategy)


@router.get("/api/{env}/reversals")
async def list_reversals_env(env: str, strategy: Optional[str] = None):
    return await asyncio.to_thread(_list_reversals_sync, strategy, env)


def _get_reversal_sync(reversal_id: str) -> dict:
    try:
        persistence = _persistence
        env = _env_for(None)
        if env is not None:
            persistence = getattr(env, "persistence", None) or _persistence
        if persistence is None:
            return {"error": "Persistence not initialized"}
        rev = persistence.get_reversal(reversal_id)
        if rev is None:
            return {"error": f"Reversal {reversal_id} not found"}
        return _chain_payload(rev, _signal_row(persistence, rev.get("signal_id")))
    except Exception as e:
        return {"error": str(e)}


@router.get("/api/reversals/{reversal_id}")
async def get_reversal(reversal_id: str):
    return await asyncio.to_thread(_get_reversal_sync, reversal_id)