"""Telegram alert router - routes trading events to Telegram messages.

The router is the ONLY channel between trading events and Telegram.  Every
material event is first written to the durable alert ledger (spec §AG) — the
canonical chronological view — and then enqueued to Telegram.  Telegram is a
presentation channel only:

* the ledger row is created regardless of the enabled flag / deliverability,
  so a design-time suppression never loses the event history;
* the delivery outcome (QUEUED -> SENT/FAILED) is persisted back via the
  client's post-delivery callback, giving the dashboard a live delivery view
  (spec §AH) with no dependency of trading on Telegram.
"""
from __future__ import annotations

import logging
import queue
import threading
import time
from datetime import datetime
from html import escape
from typing import Any, Optional
from zoneinfo import ZoneInfo

from notifications.telegram_client import TelegramClient
from notifications.telegram_formatter import (
    format_new_trade, format_trade_exit, format_risk_alert, format_error_alert, format_daily_summary,
    format_signal_alert, format_startup_alert, format_shutdown_alert, format_order_lifecycle,
    format_reversal_complete,
)

logger = logging.getLogger(__name__)

# Ultra-low-latency: alert ledger writes + text formatting are moved off the
# execution-critical path onto a bounded single writer (see _run_emit).  The
# alert ledger is an AUDIT/presentation channel only — order safety (dup
# protection, position checks, fill reconciliation) and the durable trade/
# signal/event stores persist synchronously and are untouched by this.
_EMIT_STOP = object()
_EMIT_QUEUE_MAX = 512

# Only durable, operator-actionable transitions are bridged from the engine
# event stream. High-frequency market data and transient wait/poll events are
# intentionally excluded so Telegram remains useful during a live session.
_ENGINE_ALERT_CATEGORIES = {
    "trigger_crossed": "ORDER",
    "order_submission_outcome": "ORDER",
    "order_rejected": "ORDER",
    "order_created": "ORDER",
    "pending_order_created": "ORDER",
    "pending_order_armed": "ORDER",
    "pending_order_entry_sent": "ORDER",
    "pending_order_blocked": "ORDER",
    "entry_order_terminal_without_fill": "ORDER",
    "stale_lifecycle_trigger_rejected": "ORDER",
    "entry_market_fallback": "ORDER",
    "entry_cancel": "ORDER",
    "entry_reprice": "ORDER",
    "market_fallback_partial_exit_cancelled": "ORDER",
    "position_augmented": "POSITION",
    "position_partially_exited": "POSITION",
    "reversal_entry_triggered_after_flat": "REVERSAL",
    "reversal_flat_gate_blocked": "REVERSAL",
    "reversal_entry_park_persist_failed": "REVERSAL",
    "reversal_entry_journal_failed": "REVERSAL",
    "reversal_entry_waiting_for_entry_gate": "REVERSAL",
    "broker_orphan_position": "RECONCILIATION",
    "broker_position_reconciliation_error": "RECONCILIATION",
    "reconciliation_mismatch": "RECONCILIATION",
    "reconciliation_snapshot_discarded": "RECONCILIATION",
    "position_close_persistence_failed": "CRITICAL",
    "position_close_persistence_recovered": "RECONCILIATION",
    "trade_close_persistence_failed": "CRITICAL",
    "pending_trigger_cleanup_failed": "ERROR",
    "startup_restore_failed": "CRITICAL",
    "quarantine_event": "CRITICAL",
    "safe_mode_entered": "CRITICAL",
    "strategy_emergency_stop": "RISK",
    "strategy_control": "CONFIGURATION_CHANGED",
    "strategy_runtime_policy_blocked": "RISK",
    "strategy_gate_blocked": "RISK",
    "stale_signal_candle_skipped": "RISK",
    "contract_rollover_applied_at_boot": "SYSTEM",
    "contract_rollover_cancel_resting": "SYSTEM",
    "contract_rollover_force_close": "CRITICAL",
    "contract_rollover_switched": "SYSTEM",
    "market_websocket_status_changed": "WEBSOCKET",
    "order_websocket_status_changed": "WEBSOCKET",
    "MARKET_FALLBACK_PARTIAL_FILL": "ORDER",
    "MARKET_FALLBACK_TERMINAL_WITHOUT_FULL_FILL": "CRITICAL",
    "ENTRY_ORDER_CANCELLED_OR_REJECTED": "ORDER",
    "ENTRY_REPRICE_FAILED": "ORDER",
    "PARTIAL_REMAINDER_ABANDONED": "RISK",
    "ENTRY_MARKET_FALLBACK_FAILED": "CRITICAL",
    "MARKET_FALLBACK_WAITING_FOR_BROKER_CONFIRMATION": "ORDER",
}

# These events already have a richer, dedicated alert path. Bridging them a
# second time would generate duplicate Telegram messages and ledger rows.
_ENGINE_ALERT_EXCLUSIONS = {
    "signal_created", "position_opened", "trade_closed",
    "margin_breach_entry", "contract_rollover_alert", "engine_started",
    "engine_stopped", "strategy_gate_changed", "settings_refreshed",
    "engine_restored",
}


class TelegramRouter:
    def __init__(self, client: Optional[TelegramClient] = None,
                 ledger: Optional[object] = None):
        self.client = client or TelegramClient()
        self._ledger = ledger
        self._emit_queue = queue.Queue(maxsize=_EMIT_QUEUE_MAX)
        self._emit_worker: Optional[threading.Thread] = None
        self._emit_worker_lock = threading.Lock()
        cb = getattr(self.client, "set_delivery_callback", None)
        if callable(cb):
            cb(self._on_delivery)
        try:
            from config import Config
            self._enabled = Config.get("telegram.enabled", True)
        except Exception:
            self._enabled = True

    # ── lifecycle ────────────────────────────────────────────────────────

    def start(self) -> None:
        self.client.start()

    def stop(self) -> None:
        """Drain buffered alerts, stop the emit worker, then the client."""
        if self._emit_worker is not None:
            try:
                self._emit_queue.put(_EMIT_STOP)
                self._emit_queue.join()
            except Exception:  # Telegram must never break shutdown
                logger.error("[Telegram] emit worker drain failed: %s", self._emit_worker)
            self._emit_worker = None
        self.client.stop()

    # ── async emit worker ────────────────────────────────────────────────

    def _ensure_emit_worker(self) -> None:
        if self._emit_worker is None:
            with self._emit_worker_lock:
                if self._emit_worker is None:
                    self._emit_worker = threading.Thread(
                        target=self._emit_worker_loop, name="telegram-emit",
                        daemon=True)
                    self._emit_worker.start()

    def _emit_worker_loop(self) -> None:
        while True:
            item = self._emit_queue.get()
            try:
                if item is _EMIT_STOP:
                    break
                self._run_emit(*item)
            except BaseException as e:  # worker must never die silently
                logger.error("[Telegram] emit worker failed: %s", e)
            finally:
                self._emit_queue.task_done()

    # ── ledger helpers ───────────────────────────────────────────────────

    @staticmethod
    def _extract(data: dict, *keys) -> Any:
        """Pull the first present key from a flat (or nested) alert payload."""
        if not isinstance(data, dict):
            return None
        for key in keys:
            val = data.get(key)
            if val is not None:
                return val
            for inner_key, inner in data.items():
                if isinstance(inner, dict) and inner.get(key) is not None:
                    return inner.get(key)
        return None

    def _record(self, event_type: str, data: dict, text: str,
                source: str = "LOCAL", status_before: Optional[str] = None,
                status_after: Optional[str] = None,
                telegram_status: str = "QUEUED") -> str:
        """Persist one alert ledger row when a ledger is configured."""
        if self._ledger is None:
            return ""
        try:
            return self._ledger.record(
                event_type=event_type,
                event_source=source or "LOCAL",
                payload={"message": (text or "")[:2000]},
                event_timestamp=time.time(),
                strategy_id=self._extract(data, "strategy_id"),
                signal_id=self._extract(data, "signal_id"),
                execution_intent_id=self._extract(
                    data, "execution_intent_id", "execution_intent"),
                trade_id=self._extract(data, "trade_id"),
                local_order_id=self._extract(data, "local_order_id", "order_id"),
                broker_order_id=self._extract(data, "broker_order_id"),
                correlation_id=self._extract(data, "correlation_id"),
                security_id=self._extract(data, "security_id") or self._extract(
                    data, "symbol", "instrument"),
                side=self._extract(data, "side"),
                quantity=self._extract(data, "quantity"),
                price=self._extract(data, "price", "fill_price",
                                    "average_fill_price"),
                trigger_price=self._extract(data, "trigger_price",
                                            "triggerPrice"),
                status_before=status_before or self._extract(
                    data, "status_before", "old_status"),
                status_after=status_after or self._extract(
                    data, "status_after", "new_status", "status"),
                telegram_status=telegram_status,
                error=self._extract(data, "error", "error_message"),
            )
        except Exception as e:
            logger.error("TelegramRouter ledger record failed: %s", e)
            return ""

    def _finalize(self, event_id: str, ok: bool, error: Optional[str] = None) -> None:
        if self._ledger is None or not event_id:
            return
        try:
            self._ledger.mark_telegram(
                event_id, "SENT" if ok else "FAILED", error=error)
        except Exception as e:
            logger.error("TelegramRouter ledger finalize failed: %s", e)

    def _on_delivery(self, meta: dict, ok: bool, error: Optional[str] = None) -> None:
        event_id = (meta or {}).get("event_id")
        if event_id:
            self._finalize(event_id, ok, error)

    def _emit(self, event_type: str, data: dict, text: str,
              source: str = "LOCAL", silent: bool = False,
              sync: bool = False, status_before: Optional[str] = None,
              status_after: Optional[str] = None,
              async_fast_path: bool = False) -> None:
        """Record the event then dispatch to Telegram (never raising).

        Non-``sync`` emits are deferred to a bounded single-writer worker so
        the alert ledger write + formatting never block the execution-critical
        path.  If the worker cannot keep up the queue overflows and the emit
        falls back to inline (never dropped).
        """
        if async_fast_path and not sync:
            item = (event_type, data, text, source, silent, False,
                    status_before, status_after)
            self._ensure_emit_worker()
            try:
                self._emit_queue.put_nowait(item)
            except queue.Full:
                self._run_emit(*item)
            return
        self._run_emit(event_type, data, text, source=source,
                       silent=silent, sync=sync,
                       status_before=status_before,
                       status_after=status_after)

    def _run_emit(self, event_type: str, data: dict, text: str,
                  source: str = "LOCAL", silent: bool = False,
                  sync: bool = False, status_before: Optional[str] = None,
                  status_after: Optional[str] = None) -> None:
        """Record the event then dispatch to Telegram (never raising)."""
        telegram_status = "SKIPPED_DISABLED" if not self._enabled else "QUEUED"
        event_id = self._record(
            event_type, data, text, source=source,
            status_before=status_before, status_after=status_after,
            telegram_status=telegram_status)
        if not self._enabled:
            return
        try:
            if sync:
                ok = bool(self.client.send_sync(text, silent=silent))
                error = None if ok else getattr(
                    self.client, "last_error", lambda: None)() or "send_failed"
                if event_id:
                    self._finalize(event_id, ok, error)
            else:
                ok = bool(self.client.send(
                    text, silent=silent,
                    meta={"event_id": event_id, "event_type": event_type}))
                error = None if ok else "queue_full_or_error"
                # Queue acceptance is not delivery. The Telegram client's
                # post-send callback owns QUEUED -> SENT/FAILED transitions.
                if event_id and not ok:
                    self._finalize(event_id, False, error)
        except Exception as e:  # Telegram must never break trading
            logger.error("[Telegram] send failed: %s", e)
            self._finalize(event_id, False, str(e))

    # ── event handlers ───────────────────────────────────────────────────

    def on_fill(self, fill: dict, strategy: dict, account: dict) -> None:
        text = format_new_trade(fill, strategy, account)
        self._emit("FILL", {**(fill or {}), **(strategy or {}), **(account or {})},
                   text, source="LOCAL")

    def on_signal(self, signal_data: dict) -> None:
        """Send a signal-candle alert naming the signal candle and the candle
        the trade was placed on, as a separate message from the fill alert."""
        text = format_signal_alert(signal_data)
        # Signal alerts fire on the order submission path — defer the ledger
        # write + presentation entirely so they never block the trade.
        self._emit("SIGNAL", signal_data or {}, text, source="LOCAL",
                   async_fast_path=True)

    def on_trade_close(self, close_data: dict) -> None:
        text = format_trade_exit(close_data)
        self._emit("EXIT", close_data or {}, text, source="LOCAL")

    def on_reversal_complete(self, reversal_data: dict) -> None:
        """Notify after the old exit and new entry fill are broker-confirmed."""
        text = format_reversal_complete(reversal_data or {})
        self._emit("REVERSAL", reversal_data or {}, text, source="LOCAL")

    def on_risk_alert(self, alert_data: dict) -> None:
        text = format_risk_alert(alert_data)
        severity = str(alert_data.get("severity", "") or "").upper()
        category = "CRITICAL" if severity == "CRITICAL" else "RISK"
        self._emit(category, alert_data or {}, text, source="LOCAL")

    def on_error(self, error_data: dict) -> None:
        text = format_error_alert(error_data)
        self._emit("ERROR", error_data or {}, text, source="LOCAL", silent=True)

    def on_lifecycle(self, data: dict) -> None:
        """Route order lifecycle events (ENTRY_SENT, TRIGGER_CROSSED,
        MARKET_FALLBACK, SL_*, REVERSAL, etc.) to Telegram."""
        text = format_order_lifecycle(data or {})
        event_type = (data or {}).get("event_type", "LIFECYCLE")
        self._emit(event_type, data or {}, text, source="LOCAL")

    def on_engine_event(self, event_type: str, data: dict) -> None:
        """Send selected engine state transitions through the alert ledger.

        This deliberately does not forward every event: ticks, candles, and
        repeated broker-wait polls are not useful as Telegram alerts. Events
        with dedicated signal/fill/exit/risk handlers are excluded to prevent
        duplicate notifications.
        """
        event_type = str(event_type or "").strip()
        if not event_type or event_type in _ENGINE_ALERT_EXCLUSIONS:
            return
        category = _ENGINE_ALERT_CATEGORIES.get(event_type)
        if not category:
            return
        source_data = data if isinstance(data, dict) else {}
        details = source_data.get("details")
        details = details if isinstance(details, dict) else {}

        # Keep messages informative without dumping raw broker responses,
        # credentials, or arbitrary nested payloads into Telegram.
        fields = (
            ("component", "Component"), ("status", "Status"),
            ("detail", "Detail"),
            ("strategy_id", "Strategy"), ("instrument", "Instrument"),
            ("side", "Side"), ("order_side", "Side"),
            ("quantity", "Quantity"), ("trigger_price", "Trigger"),
            ("price", "Price"), ("new_status", "Status"),
            ("reason", "Reason"),
            ("error", "Error"), ("broker_order_id", "Broker order"),
            ("order_id", "Order"), ("signal_id", "Signal"),
        )
        lines = [f"📣 <b>{escape(event_type.replace('_', ' ').upper())}</b>"]
        seen = set()
        for key, label in fields:
            value = source_data.get(key)
            if value is None or value == "":
                value = details.get(key)
            if value is None or value == "" or label in seen:
                continue
            seen.add(label)
            lines.append(f"<b>{label}:</b> {escape(str(value))}")
        lines.append(f"<b>Timestamp:</b> {datetime.now(ZoneInfo('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S IST')}")
        payload = {**source_data, "engine_event_type": event_type}
        self._emit(category, payload, "\n".join(lines), source="ENGINE",
                   async_fast_path=True)

    def on_configuration_change(self, data: dict) -> None:
        """Audit an operator-applied settings change in the ledger and Telegram."""
        data = data or {}
        changes = data.get("changes") or {}
        lines = ["⚙️ <b>LIVE SETTINGS CHANGED</b>"]
        if data.get("strategy_id"):
            lines.append(f"<b>Strategy:</b> {escape(str(data['strategy_id']))}")
        if data.get("scope"):
            lines.append(f"<b>Scope:</b> {escape(str(data['scope']))}")
        for name, values in changes.items():
            if not isinstance(values, dict):
                continue
            old = escape(str(values.get("from", "—")))
            new = escape(str(values.get("to", "—")))
            lines.append(f"<b>{escape(str(name))}:</b> {old} → {new}")
        timestamp = datetime.now(ZoneInfo("Asia/Kolkata")).strftime(
            "%Y-%m-%d %H:%M:%S IST")
        lines.append(f"<b>Timestamp:</b> {timestamp}")
        self._emit("CONFIGURATION_CHANGED", data, "\n".join(lines),
                   source="DASHBOARD")

    def on_startup(self, data: dict) -> None:
        text = format_startup_alert(data)
        self._emit("SYSTEM", data or {}, text, source="LOCAL", sync=True)

    def on_shutdown(self, data: dict) -> None:
        text = format_shutdown_alert(data)
        self._emit("SYSTEM", data or {}, text, source="LOCAL", sync=True)

    def send_daily_summary(self, account: dict, pnl_data: dict, risk: dict) -> None:
        text = format_daily_summary(account, pnl_data, risk)
        self._emit("SYSTEM", {**(account or {}), **(pnl_data or {})}, text,
                   source="LOCAL")

    def enable(self) -> None:
        self._enabled = True

    def disable(self) -> None:
        self._enabled = False

    def get_stats(self) -> dict[str, Any]:
        return {
            "enabled": self._enabled,
            "ledger": bool(self._ledger),
            **self.client.get_stats(),
        }

    def send_sync(self, text: str) -> bool:
        """Send message synchronously (bypasses queue)."""
        return self.client.send_sync(text)
