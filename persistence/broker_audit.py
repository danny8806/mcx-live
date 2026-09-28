"""Durable COMPLETE broker API request/response storage (spec Appendix §L).

:class:`BrokerAuditStore` persists, for every material Dhan wire call, the
sanitized request payload and response/error plus the extracted broker-native
identity fields (broker order id, exchange order id, correlation id, status,
price, validity, segment ...).  The table is the canonical "Broker Evidence"
source for the dashboard (spec §AK) and the forensic export (spec §AN).

Rules enforced here:

* credentials are NEVER stored — request/response payloads pass through
  :func:`analytics.sanitize.sanitize_json` before they touch the database;
* the store never raises into the trading hot path: a persistence hiccup is
  logged and swallowed so broker execution never depends on audit success;
* append-only: rows are never mutated.
"""
from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from typing import Any, Optional

from analytics.sanitize import sanitize_json
from persistence.database import Database

log = logging.getLogger(__name__)


def _pick(row: dict, *keys) -> Any:
    """First non-None value among the named keys (camel/snake tolerated)."""
    for key in keys:
        val = row.get(key)
        if val is not None:
            return val
    return None


def _path_order_id(endpoint: Optional[str]) -> Optional[str]:
    """Best-effort broker order id from a resource path like ``/orders/ABC``."""
    try:
        import re
        m = re.search(r"/orders/([^/?]+)", (endpoint or ""))
        return m.group(1) if m else None
    except Exception:
        return None


class BrokerAuditStore:
    """Append-only broker API evidence ledger backed by the canonical DB."""

    def __init__(self, db_path: str = "trading.db", execution_mode: str = "LIVE"):
        self._db = Database(db_path)
        self._lock = threading.Lock()
        self.execution_mode = str(execution_mode).upper()
        self._enabled = True

    # ── write ────────────────────────────────────────────────────────────

    def record(
        self,
        action: str,
        endpoint: str,
        http_method: str = "POST",
        request_payload: Any = None,
        response: Any = None,
        http_status: Optional[int] = None,
        error: Optional[BaseException] = None,
        request_timestamp: Optional[float] = None,
        response_timestamp: Optional[float] = None,
        correlation_id: Optional[str] = None,
        **extra,
    ) -> str:
        """Persist one broker request/response pair; returns the event id.

        Never raises: a DB failure is logged and the caller's flow continues
        (broker execution must not depend on audit persistence).
        """
        if not self._enabled:
            return ""
        req_ts = request_timestamp if request_timestamp is not None else time.time()
        resp_ts = response_timestamp if response_timestamp is not None else time.time()
        event_id = str(uuid.uuid4())

        body = {}
        err_type = err_code = err_msg = None
        if isinstance(response, dict):
            body = response
        elif error is not None:
            body = getattr(error, "body", None)
            if isinstance(body, str):
                try:
                    body = json.loads(body)
                except Exception:
                    body = None
            err_type = getattr(error, "dhan_error_type", None) or None
            err_code = getattr(error, "error_code", None) or None
            err_msg = str(error)

        fields = {
            "event_id": event_id,
            "action": str(action or "").upper(),
            "endpoint": endpoint or "",
            "http_method": str(http_method or "").upper(),
            "request_timestamp": req_ts,
            "response_timestamp": resp_ts,
            "http_status": http_status,
            "request_payload": sanitize_json(request_payload),
            "response_payload": sanitize_json(body),
            "error_type": _pick(body, "errorType", "omsErrorType", "error_type")
                           or err_type,
            "error_code": (_pick(body, "errorCode", "omsErrorCode", "error_code")
                           or err_code),
            "error_message": (_pick(body, "errorMessage", "omsErrorDescription",
                                    "reason", "error_message") or err_msg),
            "broker_order_id": str(_pick(body, "orderId", "order_id")
                                   or _path_order_id(endpoint) or ""),
            "exchange_order_id": str(_pick(body, "exchangeOrderId",
                                           "exchange_order_id") or ""),
            "correlation_id": correlation_id or str(
                _pick(body, "correlationId", "correlation_id") or ""),
            "order_status": (_pick(body, "orderStatus", "order_status")
                             or _pick(body, "status") or ""),
            "order_type": (_pick(body, "orderType", "order_type") or ""),
            "transaction_type": (_pick(body, "transactionType",
                                       "transaction_type") or ""),
            "security_id": str(_pick(body, "securityId", "security_id") or ""),
            "quantity": _to_int(_pick(body, "quantity", "tradedQuantity",
                                      "filledQty", "filled_quantity")),
            "remaining_quantity": _to_int(_pick(body, "remainingQuantity",
                                                "remaining_quantity")),
            "price": _to_float(_pick(body, "price", "limitPrice", "limit_price")),
            "trigger_price": _to_float(_pick(body, "triggerPrice",
                                             "trigger_price")),
            "validity": (_pick(body, "validity") or ""),
            "product_type": (_pick(body, "productType", "product_type") or ""),
            "exchange_segment": (_pick(body, "exchangeSegment",
                                       "exchange_segment") or ""),
            "execution_mode": self.execution_mode,
        }
        fields.update({k: v for k, v in extra.items()
                       if k in fields or k in ("signal_id", "trade_id",
                                               "local_order_id", "instrument")})
        try:
            with self._lock, self._db.transaction() as conn:
                cols = ", ".join(fields.keys())
                placeholders = ", ".join("?" * len(fields))
                conn.execute(
                    f"INSERT INTO broker_api_events ({cols}) VALUES ({placeholders})",
                    tuple(fields.values()),
                )
        except Exception as e:  # audit must never break trading
            log.error("[BrokerAudit] record failed (%s %s): %s",
                      action, endpoint, e)
        return event_id

    def disable(self) -> None:
        self._enabled = False

    # ── read (dashboard / forensic) ──────────────────────────────────────

    def query(self, action: Optional[str] = None,
              broker_order_id: Optional[str] = None,
              correlation_id: Optional[str] = None,
              limit: int = 200, offset: int = 0) -> list[dict]:
        clauses = []
        params: list = []
        if action:
            clauses.append("action = ?")
            params.append(str(action).upper())
        if broker_order_id:
            clauses.append("broker_order_id = ?")
            params.append(str(broker_order_id))
        if correlation_id:
            clauses.append("correlation_id = ?")
            params.append(str(correlation_id))
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(limit)
        params.append(offset)
        return self._db.query(
            f"SELECT * FROM broker_api_events{where} "
            "ORDER BY request_timestamp DESC LIMIT ? OFFSET ?", tuple(params))

    def get(self, event_id: str) -> Optional[dict]:
        return self._db.query_one(
            "SELECT * FROM broker_api_events WHERE event_id = ?", (event_id,))

    def for_broker_order(self, broker_order_id: str) -> list[dict]:
        return self._db.query(
            "SELECT * FROM broker_api_events WHERE broker_order_id = ? "
            "ORDER BY request_timestamp", (broker_order_id,))

    def count(self, action: Optional[str] = None,
              broker_order_id: Optional[str] = None) -> int:
        clauses = []
        params: list = []
        if action:
            clauses.append("action = ?")
            params.append(str(action).upper())
        if broker_order_id:
            clauses.append("broker_order_id = ?")
            params.append(str(broker_order_id))
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        return int(self._db.scalar(
            f"SELECT COUNT(*) FROM broker_api_events{where}", tuple(params)))


def _to_int(value: Any) -> Optional[int]:
    try:
        if value in (None, ""):
            return None
        return int(value)
    except Exception:
        return None


def _to_float(value: Any) -> Optional[float]:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None