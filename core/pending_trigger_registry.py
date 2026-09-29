"""Fast in-memory index of active strategy triggers and durable LIVE rows.

SQLite is the recovery journal, not the WebSocket hot-path lookup. This index
is rebuilt before the feed starts and kept in sync with strategy trigger state.
"""
from __future__ import annotations

import threading
from typing import Any


class PendingTriggerRegistry:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._entry: dict[str, Any] = {}
        self._exit: dict[str, Any] = {}
        self._live_rows: dict[str, dict] = {}

    @staticmethod
    def _signal_id(trigger) -> str | None:
        return getattr(getattr(trigger, "signal", None), "signal_id", None)

    @staticmethod
    def _active(trigger) -> bool:
        return trigger is not None and getattr(trigger, "status", None) in (
            "pending", "waiting_for_flat")

    def sync_strategy(self, strategy) -> None:
        """Mirror both trigger slots for a strategy."""
        sid = strategy.strategy_id
        with self._lock:
            for slot, trigger in ((self._entry, strategy.pending_entry),
                                  (self._exit, strategy.pending_exit_trigger)):
                if self._active(trigger):
                    slot[sid] = trigger
                else:
                    slot.pop(sid, None)

    def entry_for(self, strategy_id: str):
        with self._lock:
            return self._entry.get(strategy_id)

    def exit_for(self, strategy_id: str):
        with self._lock:
            return self._exit.get(strategy_id)

    def remove_signal(self, signal_id: str) -> None:
        with self._lock:
            for slot in (self._entry, self._exit):
                for sid, trigger in list(slot.items()):
                    if self._signal_id(trigger) == signal_id:
                        slot.pop(sid, None)
            self._live_rows.pop(signal_id, None)

    def cache_live_row(self, row: dict) -> None:
        signal_id = row.get("signal_id") or row.get("pending_order_id")
        if signal_id:
            with self._lock:
                self._live_rows[str(signal_id)] = dict(row)

    def live_row(self, signal_id: str) -> dict | None:
        with self._lock:
            row = self._live_rows.get(str(signal_id))
            return dict(row) if row is not None else None

    def update_live_row(self, signal_id: str, updates: dict) -> None:
        with self._lock:
            row = self._live_rows.get(str(signal_id))
            if row is not None:
                row.update(updates)

    def expire_live_row(self, signal_id: str, status: str, reason: str) -> None:
        with self._lock:
            row = self._live_rows.get(str(signal_id))
            if row is not None:
                row.update(status=status, expired_reason=reason)

