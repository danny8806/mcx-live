"""Broker Evidence + Alert Ledger dashboard routes (spec §AK / §AI / §AH).

Read-only endpoints over the two durable observability stores:

* ``broker_api_events`` — the COMPLETE sanitized Dhan request/response audit
  (every order/cancel/modify/status/funds/positions/tradebook wire call).
* ``alert_events`` — the canonical alert/notification ledger with the Telegram
  delivery outcome, supporting the Alert Center filters of spec §AI.

Both are instantiated lazily against the resolved LIVE database path (the same
path the engine uses), so the route works identically in the full dashboard
app and the LIVE container without touching either store's write path.
"""
from __future__ import annotations

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


def _db_path() -> Optional[str]:
    """Resolved LIVE-db path (fall back to the active engine db)."""
    if not _engine:
        return None
    try:
        from config import Config
        return Config.resolve_path(_engine.config.get(
            "system", {}).get("live_db_path", "live/data/db/live_trading.db"))
    except Exception:
        return None


def _audit_store():
    try:
        from persistence.broker_audit import BrokerAuditStore
        path = _db_path()
        if path:
            return BrokerAuditStore(db_path=path)
    except Exception:
        pass
    return None


def _alert_ledger():
    try:
        from persistence.alert_ledger import AlertEventLedger
        path = _db_path()
        if path:
            return AlertEventLedger(db_path=path)
    except Exception:
        pass
    return None


def _paged(payload: list, limit: int, offset: int) -> dict:
    return {
        "items": payload,
        "count": len(payload),
        "limit": limit,
        "offset": offset,
        "timestamp": time.time(),
    }


@router.get("/api/broker-events")
async def get_broker_events(action: Optional[str] = None,
                            broker_order_id: Optional[str] = None,
                            correlation_id: Optional[str] = None,
                            limit: int = 200, offset: int = 0):
    store = _audit_store()
    if store is None:
        return {"error": "Broker audit store unavailable",
                "detail": "No LIVE database path resolved"}
    return await _paged_async(
        lambda: store.query(action=action, broker_order_id=broker_order_id,
                            correlation_id=correlation_id,
                            limit=min(max(limit, 1), 1000),
                            offset=max(offset, 0)),
        store, action=action, broker_order_id=broker_order_id, limit=limit,
        offset=offset)


@router.get("/api/broker-events/actions")
async def get_broker_event_actions():
    store = _audit_store()
    if store is None:
        return {"actions": []}
    try:
        rows = await _paged_async(
            lambda: store._db.query(
                "SELECT DISTINCT action FROM broker_api_events "
                "ORDER BY action"),
            store)
        return {"actions": [r["action"] for r in rows.get("items", [])]}
    except Exception as e:
        return {"error": str(e), "actions": []}


@router.get("/api/broker-events/{event_id}")
async def get_broker_event(event_id: str):
    store = _audit_store()
    if store is None:
        return {"error": "Broker audit store unavailable"}
    row = await _paged_async(lambda: store.get(event_id), store)
    if row.get("items"):
        return row["items"][0]
    return {"error": "event not found"}


@router.get("/api/alert-ledger/stats")
async def get_alert_ledger_stats():
    ledger = _alert_ledger()
    if ledger is None:
        return {"error": "Alert ledger unavailable", "stats": None}
    return await _paged_async(lambda: ledger.stats(), ledger)


@router.get("/api/alert-ledger")
async def get_alert_ledger(event_type: Optional[str] = None,
                           telegram_status: Optional[str] = None,
                           strategy_id: Optional[str] = None,
                           trade_id: Optional[str] = None,
                           broker_order_id: Optional[str] = None,
                           correlation_id: Optional[str] = None,
                           since: Optional[float] = None,
                           until: Optional[float] = None,
                           limit: int = 200, offset: int = 0):
    ledger = _alert_ledger()
    if ledger is None:
        return {"error": "Alert ledger unavailable",
                "detail": "No LIVE database path resolved"}
    return await _paged_async(
        lambda: ledger.query(event_type=event_type,
                             telegram_status=telegram_status,
                             strategy_id=strategy_id, trade_id=trade_id,
                             broker_order_id=broker_order_id,
                             correlation_id=correlation_id,
                             since=since, until=until,
                             limit=min(max(limit, 1), 1000),
                             offset=max(offset, 0)),
        ledger)


@router.get("/api/alert-ledger/{event_id}")
async def get_alert_event(event_id: str):
    ledger = _alert_ledger()
    if ledger is None:
        return {"error": "Alert ledger unavailable"}
    row = await _paged_async(lambda: ledger.get(event_id), ledger)
    if row.get("items"):
        return row["items"][0]
    return {"error": "event not found"}


async def _paged_async(fn, store, **meta) -> dict:
    import asyncio
    try:
        items = await asyncio.to_thread(fn)
    except Exception as e:
        return {"error": str(e), **meta}
    if isinstance(items, list):
        return {"items": items, "count": len(items), **meta}
    if isinstance(items, dict) and {"total", "sent"} <= set(items):
        return {"stats": items, **meta}
    return {"items": [items], "count": 1, **meta}