"""Event bus for distributing candle/tick events to subscribed strategies.

The event bus is a synchronous dispatcher. Events are published and
delivered to all matching subscribers in the same thread. This ensures
deterministic ordering and avoids concurrency issues.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Callable

log = logging.getLogger(__name__)


class EventBus:
    """Synchronous publish/subscribe event bus.

    Subscribers register for event topics using colon-separated keys:
        "candle:GOLDM:5m"    — specific instrument + timeframe
        "candle:GOLDM:*"     — all timeframes for an instrument
        "candle:*:*"         — all candle events (wildcard)
        "tick:GOLDM"         — LTP ticks for an instrument

    Wildcard subscribers receive ALL matching events.
    Specific subscribers receive ONLY exact matches.
    Both are notified when a specific event is published.
    """

    def __init__(self):
        self._subscribers: dict[str, list[Callable]] = defaultdict(list)
        self._event_count: int = 0
        # Ultra-low-latency: keep a separate ordered index of wildcard topics so
        # the per-publish scan only walks patterns that can actually match.
        self._wildcards: list[str] = []
        self._wildcard_set: set[str] = set()

    def subscribe(self, topic: str, callback: Callable) -> None:
        """Register a callback for a topic.

        Topics use colon-separated keys. Wildcards (*) match any segment.
        """
        if callback not in self._subscribers[topic]:
            self._subscribers[topic].append(callback)
        if "*" in topic and topic not in self._wildcard_set:
            self._wildcard_set.add(topic)
            self._wildcards.append(topic)

    def unsubscribe(self, topic: str, callback: Callable) -> None:
        """Remove a callback from a topic."""
        subs = self._subscribers.get(topic, [])
        if callback in subs:
            subs.remove(callback)
        if not subs and topic in self._wildcard_set:
            self._wildcard_set.discard(topic)
            try:
                self._wildcards.remove(topic)
            except ValueError:
                pass

    def publish(self, topic: str, event: Any) -> None:
        """Publish an event to all matching subscribers.

        Delivery order:
        1. Exact topic subscribers
        2. Wildcard segment subscribers (left to right)
        """
        self._event_count += 1

        # Exact match
        for cb in self._subscribers.get(topic, []):
            try:
                cb(event)
            except Exception:
                log.exception("EventBus: subscriber error on topic=%s", topic)

        # Wildcard matching: only walk registered wildcard patterns.
        if not self._wildcards:
            return
        parts = topic.split(":")
        for t in self._wildcards:
            if t == topic or not self._matches(t, parts):
                continue
            for cb in self._subscribers.get(t, []):
                try:
                    cb(event)
                except Exception:
                    log.exception("EventBus: wildcard subscriber error on topic=%s", t)

    @staticmethod
    def _matches(pattern: str, actual_parts: list[str]) -> bool:
        """Check if a pattern matches actual topic parts.

        Pattern "candle:GOLDM:*" matches "candle:GOLDM:5m".
        Pattern "candle:*:*" matches "candle:GOLDM:5m".
        """
        pattern_parts = pattern.split(":")
        if len(pattern_parts) != len(actual_parts):
            return False
        for pp, ap in zip(pattern_parts, actual_parts):
            if pp != "*" and pp != ap:
                return False
        return True

    @property
    def subscriber_count(self) -> int:
        """Total number of registered subscriptions."""
        return sum(len(cbs) for cbs in self._subscribers.values())

    @property
    def event_count(self) -> int:
        """Total events published since creation."""
        return self._event_count

    def snapshot(self) -> dict:
        """Return diagnostic snapshot."""
        return {
            "topics": len(self._subscribers),
            "subscribers": self.subscriber_count,
            "events_published": self._event_count,
        }
