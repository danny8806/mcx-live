"""Orders routes."""
from __future__ import annotations
import asyncio
import time
from datetime import datetime, timezone
from typing import Optional
from fastapi import APIRouter
router = APIRouter()
_engine = None
_bus = None

def init(engine, event_bus):
    global _engine, _bus
    _engine = engine
    _bus = event_bus

def _ts_to_epoch(value):
    """Normalize DB timestamps (ISO strings) to epoch seconds.

    The paper broker stores epoch floats but trading.db stores ISO strings;
    both must reach the frontend as epoch so formatTimestamp stays correct.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value:
        text = value.replace("Z", "+00:00") if value.endswith("Z") else value
        try:
            dt = datetime.fromisoformat(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except Exception:
            try:
                return time.mktime(time.strptime(value.split(".")[0], "%Y-%m-%dT%H:%M:%S"))
            except Exception:
                return 0.0
    return 0.0

def _db_order_rows() -> list[dict]:
    """Load canonical order rows for the merge view (memory + DB history)."""
    try:
        persistence = getattr(_engine, "_persistence", None) if _engine else None
        if persistence is None or not hasattr(persistence, "get_orders"):
            return []
        out = []
        for row in persistence.get_orders():
            out.append({
                "order_id": row.get("order_id"),
                "strategy_id": row.get("strategy_id"),
                "instrument": row.get("instrument"),
                "side": row.get("side"),
                "quantity": row.get("quantity"),
                "order_type": row.get("order_type"),
                "price": row.get("price"),
                "planned_entry_price": row.get("planned_entry_price"),
                "trigger_price": row.get("trigger_price"),
                "broker_order_id": row.get("broker_order_id"),
                "order_role": row.get("order_role"),
                "state": row.get("state"),
                "filled_quantity": row.get("filled_quantity"),
                "average_fill_price": row.get("average_fill_price"),
                "created_at": _ts_to_epoch(row.get("created_at")),
                "updated_at": _ts_to_epoch(row.get("updated_at")),
                "reason": None,
            })
        return out
    except Exception:
        return []

def _db_fill_rows() -> list[dict]:
    """Load canonical fill rows for the merge view (memory + DB history).

    ### OBSERVATION (orders/fills restore)
    Orders and fills lived only in the paper broker's memory, so after any
    restart /api/orders and /api/fills returned empty lists even though the
    rows were safe in trading.db. Merging memory (live, authoritative state)
    with the DB rows (durable history) makes the two pages truthful across
    restarts without reconstructing broker objects.
    """
    try:
        persistence = getattr(_engine, "_persistence", None) if _engine else None
        if persistence is None or not hasattr(persistence, "get_fills"):
            return []
        out = []
        for row in persistence.get_fills():
            out.append({
                "fill_id": row.get("fill_id"),
                "order_id": row.get("order_id"),
                "instrument": row.get("instrument"),
                "side": row.get("side"),
                "quantity": row.get("quantity"),
                "price": row.get("price"),
                "timestamp": _ts_to_epoch(row.get("timestamp")),
                "strategy_id": row.get("strategy_id"),
                "multiplier": None,
            })
        return out
    except Exception:
        return []

def _list_orders_sync(strategy: Optional[str] = None, instrument: Optional[str] = None):
    if not _engine:
        return {"error": "Engine not initialized"}
    try:
        orders_dict = _engine.execution_engine._orders
        result = []
        for oid, order in orders_dict.items():
            o = {
                "order_id": order.order_id,
                "strategy_id": order.strategy_id,
                "instrument": order.instrument,
                "side": order.side,
                "quantity": order.quantity,
                "order_type": order.order_type,
                "price": order.price,
                "state": order.state.value if hasattr(order.state, "value") else str(order.state),
                "filled_quantity": order.filled_quantity,
                "average_fill_price": order.average_fill_price,
                "created_at": order.created_at,
                "updated_at": order.updated_at,
                "reason": order.reason,
            }
            if strategy and order.strategy_id != strategy:
                continue
            if instrument and order.instrument != instrument.upper():
                continue
            result.append(o)
        seen_ids = {o["order_id"]: o for o in result}
        for row in _db_order_rows():
            existing = seen_ids.get(row.get("order_id"))
            if existing is not None:
                # Runtime status is authoritative; SQLite retains the price
                # plan and Dhan identity that a restored memory order can lack.
                for field in ("price", "planned_entry_price", "trigger_price",
                              "broker_order_id", "order_role"):
                    if existing.get(field) is None:
                        existing[field] = row.get(field)
                continue
            if strategy and row.get("strategy_id") != strategy:
                continue
            if instrument and row.get("instrument") != instrument.upper():
                continue
            result.append(row)
        result.sort(key=lambda x: x.get("created_at") or 0, reverse=True)
        return {"orders": result, "count": len(result)}
    except Exception as e:
        return {"error": str(e)}

@router.get("/api/orders")
async def list_orders(strategy: Optional[str] = None, instrument: Optional[str] = None):
    return await asyncio.to_thread(_list_orders_sync, strategy, instrument)

def _get_order_sync(order_id: str):
    if not _engine:
        return {"error": "Engine not initialized"}
    try:
        order = _engine.execution_engine.get_order(order_id)
        if not order:
            for row in _db_order_rows():
                if row.get("order_id") == order_id:
                    return row
            return {"error": f"Order {order_id} not found"}
        return {
            "order_id": order.order_id,
            "strategy_id": order.strategy_id,
            "instrument": order.instrument,
            "side": order.side,
            "quantity": order.quantity,
            "order_type": order.order_type,
            "price": order.price,
            "state": order.state.value if hasattr(order.state, "value") else str(order.state),
            "filled_quantity": order.filled_quantity,
            "average_fill_price": order.average_fill_price,
            "fill_ids": order.fill_ids,
            "created_at": order.created_at,
            "updated_at": order.updated_at,
            "reason": order.reason,
        }
    except Exception as e:
        return {"error": str(e)}

@router.get("/api/orders/{order_id}")
async def get_order(order_id: str):
    return await asyncio.to_thread(_get_order_sync, order_id)

def _list_fills_sync(strategy: Optional[str] = None, instrument: Optional[str] = None):
    if not _engine:
        return {"error": "Engine not initialized"}
    try:
        fills = _engine.execution_engine.get_fills(strategy_id=strategy, instrument=instrument)
        result = []
        for f in fills:
            result.append({
                "fill_id": f.fill_id,
                "order_id": f.order_id,
                "instrument": f.instrument,
                "side": f.side,
                "quantity": f.quantity,
                "price": f.price,
                # Runtime fill timestamps may be ISO strings after broker
                # recovery, while paper/runtime fills commonly use epochs.
                # Normalize before sorting the merged memory+DB history.
                "timestamp": _ts_to_epoch(f.timestamp),
                "strategy_id": f.strategy_id,
                "multiplier": f.multiplier,
            })
        seen_ids = {x["fill_id"] for x in result}
        for row in _db_fill_rows():
            if row.get("fill_id") in seen_ids:
                continue
            if strategy and row.get("strategy_id") != strategy:
                continue
            if instrument and row.get("instrument") != instrument.upper():
                continue
            result.append(row)
        result.sort(key=lambda x: (x.get("timestamp") or 0), reverse=True)
        return {"fills": result, "count": len(result)}
    except Exception as e:
        return {"error": str(e)}

@router.get("/api/fills")
async def list_fills(strategy: Optional[str] = None, instrument: Optional[str] = None):
    return await asyncio.to_thread(_list_fills_sync, strategy, instrument)
