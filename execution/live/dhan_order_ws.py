"""Phase 7 — Dhan v2 Order-Update WebSocket bridge (accelerator feed).

Dhan pushes order lifecycle events over ``wss://api-order-update.dhan.co``::

    {"LoginReq":{"MsgCode":42,"ClientId":"<clientId>","Token":"<JWT>"},"UserType":"SELF"}
    {"Type":"order_alert","Data":{...}}

Contract facts this bridge is built around (see ``DHAN_V2_CONTRACT.md`` §3.2):

  * Only ``Source == "P"`` (API-originated) alerts are emitted — manual / book
    orders are never reacted to.
  * After a reconnect Dhan does NOT replay history, so the WS is an
    ACCELERATOR only; ``GET /orders/{id}`` (REST poll) stays the reconciliation
    source of truth.  This bridge therefore never mints fills — fill ids and
    fill prices always come from the REST poll.  It only surfaces the freshest
    order status (typically REJECTED / CANCELLED / TRADED flags) into the
    transport book so the next poll reflects it immediately.

Two testable pieces:

  * :func:`parse_order_alert` — pure frame -> normalized status record.
  * :class:`DhanOrderUpdateFeed` — a websocket-client compatible loop
    (login handshake, reconnect + token reload, stale watchdog, stats) whose
    ``on_record`` callback receives normalized records.  A fake socket can be
    injected for tests; no network is required.
"""
from __future__ import annotations

import json
import threading
import time
from typing import Any, Callable, Optional

import websocket

from execution.live.dhan_transport import _normalize_status

_ACCELERATOR_FIELD_ALIASES = {
    "order_no": ("OrderNo", "orderNo", "orderId", "order_id"),
    "status": ("Status", "OrderStatus", "orderStatus"),
    "traded_qty": ("TradedQty", "tradedQty", "tradedQuantity"),
    "traded_price": ("TradedPrice", "tradedPrice"),
    "avg_price": ("AvgTradedPrice", "averageTradedPrice", "averageFillPrice"),
    "remaining_qty": ("RemainingQuantity", "remainingQuantity"),
    "correlation_id": ("CorrelationId", "correlationId"),
    "reason": ("ReasonDescription", "reason"),
    "source": ("Source", "source"),
    "order_type": ("OrderType", "orderType"),
}


def _first(data: dict, *names) -> Any:
    for n in names:
        if n in data and data[n] is not None:
            return data[n]
    return None


def login_message(client_id: str, token: str) -> dict:
    """The exact Dhan auth handshake for the order-update socket (MsgCode 42)."""
    return {
        "LoginReq": {
            "MsgCode": 42,
            "ClientId": str(client_id),
            "Token": str(token),
        },
        "UserType": "SELF",
    }


def parse_order_alert(frame: Any) -> Optional[dict]:
    """Parse one order-update frame into a normalized status record.

    Returns None for non-order-alert / malformed / non-API-source frames.
    The record mirrors the REST ``_record_for`` status shape minus fills::

        {"broker_order_id", "status", "raw_status", "filled_quantity",
         "average_fill_price", "correlation_id", "reason"}
    """
    if isinstance(frame, str):
        try:
            frame = json.loads(frame)
        except (ValueError, TypeError):
            return None
    if not isinstance(frame, dict):
        return None
    if frame.get("Type") not in ("order_alert", "orderAlert"):
        return None
    data = frame.get("Data") or {}
    if not isinstance(data, dict):
        return None

    source = str(_first(data, "Source", "source") or "").upper()
    if source and source != "P":
        # Only API-originated orders are this runtime's concern.  Ignoring
        # manual/book orders keeps the accelerator from reacting to orders we
        # never placed (their OrderNo is not mappable anyway).
        return None

    bid = str(_first(data, "OrderNo", "orderNo", "orderId", "order_id") or "")
    if not bid:
        return None
    raw_status = str(_first(data, "Status", "OrderStatus", "orderStatus")
                     or "PENDING").upper()
    traded_qty = int(_first(data, "TradedQty", "tradedQty", "tradedQuantity") or 0)
    avg_price = 0.0
    try:
        avg_price = float(
            _first(data, "AvgTradedPrice", "averageTradedPrice", "averageFillPrice") or 0.0)
    except (TypeError, ValueError):
        avg_price = 0.0

    return {
        "broker_order_id": bid,
        "status": _normalize_status(raw_status),
        "raw_status": raw_status,
        "filled_quantity": traded_qty,
        "average_fill_price": avg_price,
        "remaining_quantity": int(_first(
            data, "RemainingQuantity", "remainingQuantity") or 0),
        "correlation_id": _first(data, "CorrelationId", "correlationId"),
        "reason": _first(data, "ReasonDescription", "reason"),
        "order_type": _first(data, "OrderType", "orderType"),
    }


class DhanOrderUpdateFeed:
    """Order-update WS client: login (MsgCode 42) + reconnect + feed/stats.

    Mirrors the LTP client's lifecycle (:class:`data.dhan.websocket_client`)
    but for the JSON order-alert socket.  ``socket_factory`` can be injected
    (tests use a fake) and must expose ``on_open/on_message/on_error/on_close``
    setters plus ``send`` and ``run_forever`` — i.e. ``websocket.WebSocketApp``.
    """

    def __init__(
        self,
        client_id: str,
        token_loader: Optional[Callable[[], Optional[str]]] = None,
        on_record: Optional[Callable[[dict], None]] = None,
        on_status: Optional[Callable[[str], None]] = None,
        url: str = "wss://api-order-update.dhan.co",
        heartbeat_interval: float = 10.0,
        reconnect_delay: float = 5.0,
        token: str = "",
        stale_threshold: float = 90.0,
        socket_factory: Optional[Callable] = None,
    ):
        self.client_id = str(client_id)
        self.token = token
        self.token_loader = token_loader
        self.on_record = on_record
        self.on_status = on_status
        self.url = url
        self.heartbeat_interval = heartbeat_interval
        self.reconnect_delay = reconnect_delay
        self.stale_threshold = stale_threshold

        self._ws: Optional[Any] = None
        self._thread: Optional[threading.Thread] = None
        self._watchdog: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._connected = False
        self._connecting = False
        self._last_message_time = 0.0
        self._last_activity_time = 0.0
        self._stats = {"recv": 0, "alerts": 0, "parse_err": 0, "reconnects": 0}
        self._factory = socket_factory or websocket.WebSocketApp

    # ── lifecycle ─────────────────────────────────────────────────────

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def stats(self) -> dict:
        return dict(self._stats)

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True,
                                        name="dhan-order-ws")
        self._thread.start()
        self._watchdog = threading.Thread(
            target=self._watchdog_loop, daemon=True, name="dhan-order-ws-watchdog")
        self._watchdog.start()

    def stop(self, timeout: float = 3.0) -> None:
        self._stop.set()
        if self._ws is not None:
            try:
                self._ws.close()
            except Exception:
                pass
        if self._watchdog is not None:
            self._watchdog.join(timeout=timeout)
            self._watchdog = None
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=timeout)
        self._thread = None

    # ── loops ─────────────────────────────────────────────────────────

    def _watchdog_loop(self) -> None:
        while not self._stop.is_set():
            self._stop.wait(15.0)
            self._watchdog_once()

    def _watchdog_once(self) -> None:
        if self._stop.is_set():
            return
        if self._connected and self.is_stale():
            if self._ws is not None:
                try:
                    self._ws.close()
                except Exception:
                    pass
            if self.on_status:
                self.on_status("stale_reconnect")

    def _run_loop(self) -> None:
        delay = self.reconnect_delay
        max_delay = self.reconnect_delay * 3
        while not self._stop.is_set():
            if self.token_loader:
                try:
                    fresh = self.token_loader()
                    if fresh:
                        self.token = fresh
                except Exception:
                    pass
            try:
                self._connect_once()
                delay = self.reconnect_delay
            except Exception:
                pass
            if self._stop.is_set():
                break
            self._stop.wait(delay)
            delay = min(delay * 2, max_delay)

    def _connect_once(self) -> None:
        self._connecting = True
        self._connected = False
        self._ws = self._factory(
            self.url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
            on_pong=self._on_pong,
        )
        try:
            self._ws.run_forever(ping_interval=self.heartbeat_interval)
        finally:
            self._connecting = False
            self._connected = False
        if self.on_status:
            self.on_status("disconnected")

    # ── ws callbacks ──────────────────────────────────────────────────

    def _on_open(self, ws: Any) -> None:
        self._connected = True
        self._last_activity_time = self._last_message_time = time.time()
        try:
            ws.send(json.dumps(login_message(self.client_id, self.token)))
        except Exception as e:  # pragma: no cover - socket-layer edge
            self._stats["parse_err"] += 1
            if self.on_status:
                self.on_status(f"login_send_error:{e}")
            return
        if self.on_status:
            self.on_status("connected")

    def _on_message(self, ws: Any, message: Any) -> None:
        if not isinstance(message, str):
            return  # binary frames are not order alerts
        self._stats["recv"] += 1
        self._last_activity_time = self._last_message_time = time.time()
        try:
            record = parse_order_alert(message)
        except Exception as e:
            self._stats["parse_err"] += 1
            if self.on_status:
                self.on_status(f"parse_error:{e}")
            return
        if record is None:
            return
        self._stats["alerts"] += 1
        if self.on_record is not None:
            try:
                self.on_record(record)
            except Exception as e:  # pragma: no cover - defensive
                self._stats["parse_err"] += 1
                if self.on_status:
                    self.on_status(f"record_handler_error:{e}")

    def _on_error(self, ws: Any, error: Exception) -> None:
        if self.on_status:
            self.on_status(f"error:{error}")

    def _on_pong(self, ws: Any, message: Any) -> None:
        """Treat a valid protocol pong as transport activity, not an order."""
        self._last_activity_time = time.time()

    def _on_close(self, ws: Any, close_code: int, close_msg: str) -> None:
        if self.on_status:
            self.on_status(f"closed:{close_code}:{close_msg}")

    # ── diagnostics ───────────────────────────────────────────────────

    def is_stale(self) -> bool:
        if not self._connected:
            return True
        return (time.time() - self._last_activity_time) > self.stale_threshold
