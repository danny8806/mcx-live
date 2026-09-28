"""Durable alert event ledger (spec Appendix §AG).

The ledger persists ONE logical row per configured state transition with the
full identity lineage (strategy / signal / execution intent / trade / local
order / broker order / correlation / security / price) plus the Telegram
delivery outcome (``telegram_status``).  Telegram is only a presentation
channel: this table is what allows complete forensic reconstruction with no
network access and survives restarts without re-sending old broker orders.
"""
from __future__ import annotations

import logging
import threading
import time
import uuid
from typing import Any, Optional

from analytics.sanitize import payload_snapshot
from persistence.database import Database

log = logging.getLogger(__name__)

EVENT_CATEGORIES = [
    "SIGNAL", "ORDER", "FILL", "POSITION", "SL", "EXIT", "REVERSAL",
    "RECONCILIATION", "WEBSOCKET", "LTP", "RISK", "SYSTEM", "TELEGRAM",
    "ERROR", "CRITICAL",
]

TELEGRAM_PENDING = "QUEUED"


class AlertEventLedger:
    """Append-only alert ledger with one row per logical alert."""

    def __init__(self, db_path: str = "trading.db", execution_mode: str = "LIVE"):
        self._db = Database(db_path)
        self._lock = threading.Lock()
        self.execution_mode = str(execution_mode).upper()
        self._enabled = True

    # ── write ────────────────────────────────────────────────────────────

    def record(
        self,
        event_type: str,
        payload: Any = None,
        *,
        event_source: str = "LOCAL",
        event_timestamp: Optional[float] = None,
        received_timestamp: Optional[float] = None,
        strategy_id: Optional[str] = None,
        signal_id: Optional[str] = None,
        execution_intent_id: Optional[str] = None,
        trade_id: Optional[str] = None,
        local_order_id: Optional[str] = None,
        broker_order_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        security_id: Optional[str] = None,
        side: Optional[str] = None,
        quantity: Optional[int] = None,
        price: Optional[float] = None,
        trigger_price: Optional[float] = None,
        status_before: Optional[str] = None,
        status_after: Optional[str] = None,
        telegram_status: str = TELEGRAM_PENDING,
        error: Optional[str] = None,
        execution_intent: Optional[str] = None,
    ) -> str:
        """Persist one alert event; returns its immutable event id.

        Never raises: audit persistence must never break the alert hot path.
        """
        if not self._enabled:
            return ""
        event_id = str(uuid.uuid4())
        now = time.time()
        fields = {
            "event_id": event_id,
            "event_type": str(event_type or "SYSTEM").upper(),
            "event_source": str(event_source or "LOCAL").upper(),
            "event_timestamp": event_timestamp if event_timestamp is not None else now,
            "received_timestamp": received_timestamp if received_timestamp is not None else now,
            "processed_timestamp": None,
            "strategy_id": strategy_id,
            "signal_id": signal_id,
            "execution_intent_id": execution_intent_id or execution_intent,
            "trade_id": trade_id,
            "local_order_id": local_order_id,
            "broker_order_id": broker_order_id,
            "correlation_id": correlation_id,
            "security_id": security_id,
            "side": str(side or "").upper() or None,
            "quantity": _coerce_int(quantity),
            "price": _coerce_float(price),
            "trigger_price": _coerce_float(trigger_price),
            "status_before": status_before,
            "status_after": status_after,
            "payload_sanitized": payload_snapshot(payload),
            "processing_status": "NEW",
            "error": error,
            "telegram_status": telegram_status,
            "execution_mode": self.execution_mode,
        }
        try:
            with self._lock, self._db.transaction() as conn:
                cols = ", ".join(fields.keys())
                placeholders = ", ".join("?" * len(fields))
                conn.execute(
                    f"INSERT INTO alert_events ({cols}) VALUES ({placeholders})",
                    tuple(fields.values()),
                )
        except Exception as e:  # ledger must never break the alert path
            log.error("[AlertLedger] record failed (%s): %s", event_type, e)
        return event_id

    def mark_telegram(self, event_id: str, telegram_status: str,
                      error: Optional[str] = None) -> None:
        self._update(event_id, telegram_status=telegram_status, error=error)

    def mark_processed(self, event_id: str, processing_status: str,
                       error: Optional[str] = None) -> None:
        self._update(event_id, processing_status=processing_status, error=error)

    def _update(self, event_id: str, **fields) -> None:
        if not self._enabled or not event_id:
            return
        fields["processed_timestamp"] = time.time()
        sets = ", ".join(f"{k} = ?" for k in fields)
        try:
            with self._lock, self._db.transaction() as conn:
                conn.execute(
                    f"UPDATE alert_events SET {sets} WHERE event_id = ?",
                    (*fields.values(), event_id),
                )
        except Exception as e:
            log.error("[AlertLedger] update failed (%s): %s", event_id, e)

    def disable(self) -> None:
        self._enabled = False

    # ── read (dashboard alert center / export) ───────────────────────────

    def query(self, event_type: Optional[str] = None,
              event_source: Optional[str] = None,
              strategy_id: Optional[str] = None,
              trade_id: Optional[str] = None,
              broker_order_id: Optional[str] = None,
              correlation_id: Optional[str] = None,
              telegram_status: Optional[str] = None,
              since: Optional[float] = None,
              until: Optional[float] = None,
              limit: int = 200, offset: int = 0) -> list[dict]:
        clauses = []
        params: list = []
        if event_type:
            clauses.append("event_type = ?")
            params.append(str(event_type).upper())
        if event_source:
            clauses.append("event_source = ?")
            params.append(str(event_source).upper())
        if strategy_id:
            clauses.append("strategy_id = ?")
            params.append(strategy_id)
        if trade_id:
            clauses.append("trade_id = ?")
            params.append(trade_id)
        if broker_order_id:
            clauses.append("broker_order_id = ?")
            params.append(str(broker_order_id))
        if correlation_id:
            clauses.append("correlation_id = ?")
            params.append(str(correlation_id))
        if telegram_status:
            clauses.append("telegram_status = ?")
            params.append(str(telegram_status).upper())
        if since is not None:
            clauses.append("event_timestamp >= ?")
            params.append(since)
        if until is not None:
            clauses.append("event_timestamp <= ?")
            params.append(until)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(limit)
        params.append(offset)
        return self._db.query(
            f"SELECT * FROM alert_events{where} "
            "ORDER BY event_timestamp DESC LIMIT ? OFFSET ?", tuple(params))

    def get(self, event_id: str) -> Optional[dict]:
        return self._db.query_one(
            "SELECT * FROM alert_events WHERE event_id = ?", (event_id,))

    def stats(self) -> dict:
        total = int(self._db.scalar("SELECT COUNT(*) FROM alert_events"))
        failed = int(self._db.scalar(
            "SELECT COUNT(*) FROM alert_events WHERE telegram_status = 'FAILED'"))
        sent = int(self._db.scalar(
            "SELECT COUNT(*) FROM alert_events WHERE telegram_status = 'SENT'"))
        queued = int(self._db.scalar(
            "SELECT COUNT(*) FROM alert_events WHERE telegram_status = 'QUEUED'"))
        return {"total": total, "sent": sent, "failed": failed, "queued": queued}


def _coerce_int(value: Any) -> Optional[int]:
    try:
        if value in (None, ""):
            return None
        return int(value)
    except Exception:
        return None


def _coerce_float(value: Any) -> Optional[float]:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None