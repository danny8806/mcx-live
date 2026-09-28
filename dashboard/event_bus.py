"""In-process event bus for dashboard real-time updates."""
from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


@dataclass
class EventBusEvent:
    __slots__ = ("id", "timestamp", "event_type", "data")

    def __init__(self, event_id: int, event_type: str, data: dict[str, Any]):
        self.id = event_id
        self.timestamp = time.time()
        self.event_type = event_type
        self.data = data

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.id, "timestamp": self.timestamp, "event_type": self.event_type, "data": self.data}


class EventBusStats:
    def __init__(self) -> None:
        self._counts: Dict[str, int] = {}
        self._lock = threading.Lock()

    def increment(self, event_type: str) -> None:
        with self._lock:
            self._counts[event_type] = self._counts.get(event_type, 0) + 1

    def get_counts(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._counts)

    def reset(self) -> None:
        with self._lock:
            self._counts.clear()


class EventBus:
    def __init__(self, max_events: int = 10000) -> None:
        self._events: deque[EventBusEvent] = deque(maxlen=max_events)
        self._subscribers: Dict[str, List[Callable]] = {}
        self._wildcard_subscribers: List[Callable] = []
        self._lock = threading.Lock()
        self._next_id = 0
        self._stats = EventBusStats()

    def publish(self, event_type: str, data: dict[str, Any]) -> EventBusEvent:
        with self._lock:
            event = EventBusEvent(self._next_id, event_type, data)
            self._next_id += 1
            self._events.append(event)
        self._stats.increment(event_type)

        subscribers = []
        with self._lock:
            subscribers = list(self._subscribers.get(event_type, []))
            wildcards = list(self._wildcard_subscribers)

        for callback in subscribers + wildcards:
            try:
                callback(event)
            except Exception:
                pass
        return event

    def subscribe(self, event_type: str, callback: Callable[[EventBusEvent], None]) -> None:
        with self._lock:
            if event_type not in self._subscribers:
                self._subscribers[event_type] = []
            self._subscribers[event_type].append(callback)

    def subscribe_all(self, callback: Callable[[EventBusEvent], None]) -> None:
        with self._lock:
            self._wildcard_subscribers.append(callback)

    def unsubscribe(self, event_type: str, callback: Callable) -> None:
        with self._lock:
            if event_type in self._subscribers:
                self._subscribers[event_type] = [
                    cb for cb in self._subscribers[event_type] if cb != callback
                ]

    def get_recent(self, event_type: Optional[str] = None, limit: int = 100) -> List[dict[str, Any]]:
        with self._lock:
            events = list(self._events)
        if event_type:
            events = [e for e in events if e.event_type == event_type]
        return [e.to_dict() for e in events[-limit:]]

    def get_stats(self) -> dict[str, Any]:
        return {
            "total_events": len(self._events),
            "counts": self._stats.get_counts(),
            "subscribers": {
                k: len(v) for k, v in self._subscribers.items()
            },
            "wildcard_subscribers": len(self._wildcard_subscribers),
        }

    def clear(self) -> None:
        with self._lock:
            self._events.clear()
            self._stats.reset()
