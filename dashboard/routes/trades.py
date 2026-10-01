"""Trades routes - tradebook, trade details.

Serves canonical lifecycle data from TradeLifecycleManager as the single
source of truth. Falls back to persistence DB when lifecycle is empty.
"""
from __future__ import annotations
import asyncio
import time
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


def _merge_trade_rows(lifecycle_rows: list[dict], persisted_rows: list[dict]) -> list[dict]:
    """Merge runtime and durable history instead of hiding one source.

    Persistence supplies history across restarts; the runtime lifecycle can
    have newer fields for trades updated since startup. Prefer non-empty
    runtime values on duplicates while retaining durable-only records.
    """
    merged: dict[str, dict] = {}
    unkeyed: list[dict] = []
    for row in persisted_rows or []:
        item = dict(row or {})
        key = str(item.get("trade_id") or "")
        (merged.setdefault(key, item) if key else unkeyed.append(item))
    for row in lifecycle_rows or []:
        item = dict(row or {})
        key = str(item.get("trade_id") or "")
        if not key:
            unkeyed.append(item)
            continue
        if key not in merged:
            merged[key] = item
            continue
        for field, value in item.items():
            if value is not None and value != "":
                merged[key][field] = value
    rows = list(merged.values()) + unkeyed
    rows.sort(key=lambda row: str(row.get("created_at") or ""), reverse=True)
    return rows


def _reconcile_history_from_fills(trade: dict, fills: list[dict], fee_model=None) -> dict:
    """Return a history row repaired from complete, attributable fills.

    This is a read-time repair only. It never writes historical records. We
    only replace P&L when one entry order and one exit order form a complete
    round trip, which avoids guessing about scale-ins or partial exits.
    """
    result = dict(trade or {})
    known_side = str(result.get("entry_side") or result.get("side") or "").upper()
    if known_side in ("BUY", "LONG"):
        result.update({"side": "LONG", "entry_side": "LONG",
                       "entry_action": "BUY"})
    elif known_side in ("SELL", "SHORT"):
        result.update({"side": "SHORT", "entry_side": "SHORT",
                       "entry_action": "SELL"})
    tid = str(result.get("trade_id") or "")
    rows = [dict(fill) for fill in (fills or [])
            if str(fill.get("trade_id") or "") == tid
            and int(fill.get("quantity") or 0) > 0]
    if not tid or not rows or str(result.get("status") or "").upper() != "CLOSED":
        return result
    rows.sort(key=lambda fill: str(fill.get("timestamp") or ""))

    entry_id = str(result.get("entry_fill_id") or "")
    entry_fill = next((f for f in rows if entry_id and str(f.get("fill_id")) == entry_id), None)
    if entry_fill is None:
        entry_fill = rows[0]
    entry_action = str(entry_fill.get("side") or "").upper()
    if entry_action not in ("BUY", "SELL"):
        return result
    side = "LONG" if entry_action == "BUY" else "SHORT"
    result.update({"side": side, "entry_side": side,
                   "entry_action": entry_action,
                   "exit_action": "SELL" if side == "LONG" else "BUY"})
    entry_rows = [f for f in rows if str(f.get("side") or "").upper() == entry_action]
    exit_action = "SELL" if entry_action == "BUY" else "BUY"
    exit_rows = [f for f in rows if str(f.get("side") or "").upper() == exit_action]
    entry_orders = {str(f.get("order_id") or "") for f in entry_rows}
    exit_orders = {str(f.get("order_id") or "") for f in exit_rows}
    entry_qty = sum(int(f.get("quantity") or 0) for f in entry_rows)
    exit_qty = sum(int(f.get("quantity") or 0) for f in exit_rows)
    multiplier = float(result.get("multiplier") or 0)
    if (not multiplier or entry_qty <= 0 or entry_qty != exit_qty
            or len(entry_orders) != 1 or len(exit_orders) != 1
            or "" in entry_orders or "" in exit_orders):
        return result

    entry_avg = sum(float(f.get("price") or 0) * int(f.get("quantity") or 0)
                    for f in entry_rows) / entry_qty
    exit_avg = sum(float(f.get("price") or 0) * int(f.get("quantity") or 0)
                   for f in exit_rows) / exit_qty
    gross = ((exit_avg - entry_avg) if side == "LONG" else
             (entry_avg - exit_avg)) * entry_qty * multiplier
    result.update({
        "side": side,
        "entry_price": entry_avg,
        "exit_price": exit_avg,
        "quantity": entry_qty,
        "gross_pnl": round(gross, 2),
        "pnl_reconciled_from_fills": True,
        "pnl_reconciliation_status": "MATCHED",
        "pnl_basis": "broker-attributed persisted fills",
    })
    # Point the trade at the order that actually generated its closing fill;
    # retain the original linkage for audit if it was a cancelled limit parent.
    actual_exit_order = next(iter(exit_orders))
    if result.get("exit_order_id") and str(result["exit_order_id"]) != actual_exit_order:
        result["parent_exit_order_id"] = result["exit_order_id"]
    result["exit_order_id"] = actual_exit_order
    if fee_model is not None:
        try:
            fees = fee_model.calculate(entry_avg, exit_avg, entry_qty,
                                       multiplier, side=side).total
            result["charges"] = fees
            result["net_pnl"] = round(gross - fees, 2)
            result["realized_pnl"] = result["net_pnl"]
            result["charges_basis"] = "system_fee_model_estimate"
        except Exception:
            result["net_pnl"] = round(gross - float(result.get("charges") or 0), 2)
    else:
        result["net_pnl"] = round(gross - float(result.get("charges") or 0), 2)
    result["realized_pnl"] = result["net_pnl"]
    return result


def _list_trades_sync(strategy: Optional[str] = None, instrument: Optional[str] = None,
                      env=None):
    try:
        env = _env_for(env)
        # Runtime state is fresher for trades touched this process; persistence
        # is required for complete history. Fetch both and merge by trade_id.
        lifecycle_rows = []
        if env is not None and getattr(env, "runtimes", None):
            runtimes = env.runtimes.all() if hasattr(env.runtimes, "all") else (
                env.runtimes.values() if isinstance(env.runtimes, dict) else [])
            for rt in runtimes:
                try:
                    lifecycle_rows.extend(rt.lifecycle.get_trades_for_api(
                        strategy_id=strategy, instrument=instrument))
                except Exception:
                    pass
        if strategy:
            lifecycle_rows = [t for t in lifecycle_rows
                              if t.get("strategy_id") == strategy]
        if instrument:
            lifecycle_rows = [t for t in lifecycle_rows
                              if str(t.get("instrument") or "").upper()
                              == instrument.upper()]

        persistence = (getattr(env, "persistence", None) if env is not None
                       else _persistence)
        persisted_rows = []
        if persistence:
            persisted_rows = persistence.get_trades(strategy_id=strategy)
            if instrument:
                persisted_rows = [t for t in persisted_rows
                                  if str(t.get("instrument") or "").upper()
                                  == instrument.upper()]
            merged = _merge_trade_rows(lifecycle_rows, persisted_rows)
            try:
                all_fills = persistence.get_fills()
            except Exception:
                all_fills = []
            for index, trade in enumerate(merged):
                pnl_engine = (getattr(env, "pnl_engines", {}) or {}).get(
                    trade.get("strategy_id")) if env is not None else None
                merged[index] = _reconcile_history_from_fills(
                    trade, all_fills,
                    getattr(pnl_engine, "fee_model", None))
            return {"execution_mode": getattr(env, "mode", "PAPER") if env else None,
                    "trades": merged, "count": len(merged),
                    "source": "merged" if lifecycle_rows else "persistence"}

        if lifecycle_rows:
            return {"execution_mode": getattr(env, "mode", "PAPER"),
                    "trades": _merge_trade_rows(lifecycle_rows, []),
                    "count": len(lifecycle_rows), "source": "lifecycle"}

        if not _engine:
            return {"error": "Engine not initialized"}
        fills = (_engine.execution_engine.get_fills() if env is None
                 else env.execution_engine.get_fills())
        result = []
        for f in fills:
            result.append({
                "fill_id": f.fill_id,
                "order_id": f.order_id,
                "instrument": f.instrument,
                "side": f.side,
                "quantity": f.quantity,
                "price": f.price,
                "timestamp": f.timestamp,
                "strategy_id": f.strategy_id,
            })
        return {"trades": result, "count": len(result), "source": "fills"}
    except Exception as e:
        return {"error": str(e)}


@router.get("/api/trades")
async def list_trades(strategy: Optional[str] = None, instrument: Optional[str] = None):
    return await asyncio.to_thread(_list_trades_sync, strategy, instrument)


@router.get("/api/{env}/trades")
async def list_trades_env(env: str, strategy: Optional[str] = None,
                          instrument: Optional[str] = None):
    return await asyncio.to_thread(_list_trades_sync, strategy, instrument, env)

def _get_trade_sync(trade_id: str):
    try:
        # Primary: canonical lifecycle (read-only aggregate over runtimes)
        if _engine and hasattr(_engine, "get_trade"):
            trade = _engine.get_trade(trade_id)
            if trade:
                return trade.snapshot()

        # Fallback: persistence DB
        if _persistence:
            trades = _persistence.get_trades()
            for t in trades:
                if t.get("trade_id") == trade_id:
                    return t
        return {"error": f"Trade {trade_id} not found"}
    except Exception as e:
        return {"error": str(e)}

def _lifecycle_orphan_scan_sync():
    """Run comprehensive orphan scan across all per-strategy lifecycles."""
    if not _engine or not hasattr(_engine, "orphan_scan"):
        return {"error": "Engine lifecycle not initialized"}
    try:
        return _engine.orphan_scan()
    except Exception as e:
        return {"error": str(e)}

@router.get("/api/trades/orphan-scan")
async def lifecycle_orphan_scan():
    return await asyncio.to_thread(_lifecycle_orphan_scan_sync)

def _lifecycle_reconcile_sync():
    """Run lifecycle reconciliation across all per-strategy lifecycles."""
    if not _engine or not hasattr(_engine, "reconcile_trades"):
        return {"error": "Engine lifecycle not initialized"}
    try:
        return _engine.reconcile_trades()
    except Exception as e:
        return {"error": str(e)}

@router.get("/api/trades/lifecycle-reconcile")
async def lifecycle_reconciliation():
    return await asyncio.to_thread(_lifecycle_reconcile_sync)

@router.get("/api/trades/{trade_id}")
async def get_trade(trade_id: str):
    return await asyncio.to_thread(_get_trade_sync, trade_id)
