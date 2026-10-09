"""Market-data freshness tracking used by the 15-minute strategy gate."""

from __future__ import annotations

import logging
import threading
import time
from typing import Callable, Optional

log = logging.getLogger("trading_engine")


class MarketDataHealthMonitor:
    """Tracks per-instrument tick freshness for the live environments.

    ``stale_after`` is the number of seconds without a tick after which the
    feed is considered unusable.  It is intentionally generous relative to the
    instrument's candle timeframe: a healthy feed produces ticks far more often
    than it produces candles.
    """

    def __init__(self, clock: Optional[Callable[[], float]] = None,
                 stale_after: float = 90.0) -> None:
        self._clock = clock or time.time
        self._stale_after = float(stale_after)
        self._lock = threading.RLock()
        self._last_tick: dict[str, float] = {}
        self._unhealthy_since: dict[str, float] = {}
        # Instruments we have never seen a tick for must not be treated as
        # healthy just because they are absent from the map.
        self._ever_seen: set[str] = set()

    @property
    def stale_after(self) -> float:
        return self._stale_after

    def record_tick(self, instrument: str, timestamp: Optional[float] = None) -> None:
        """Mark ``instrument`` as freshly fed."""
        if not instrument:
            return
        now = float(timestamp) if timestamp is not None else self._clock()
        with self._lock:
            self._last_tick[instrument] = now
            self._ever_seen.add(instrument)
            self._unhealthy_since.pop(instrument, None)

    def last_tick(self, instrument: str) -> Optional[float]:
        with self._lock:
            return self._last_tick.get(instrument)

    def age(self, instrument: str) -> Optional[float]:
        """Seconds since the last tick, or None if never seen."""
        with self._lock:
            ts = self._last_tick.get(instrument)
        if ts is None:
            return None
        return max(0.0, self._clock() - ts)

    def is_healthy(self, instrument: str) -> bool:
        """True when this instrument's feed is fresh enough to trade on.

        An instrument we have never seen a tick for is NOT healthy.
        """
        with self._lock:
            if instrument in self._unhealthy_since:
                return False
        age = self.age(instrument)
        return age is not None and age <= self._stale_after

    def unhealthy_since(self, instrument: str) -> Optional[float]:
        with self._lock:
            if instrument in self._unhealthy_since:
                return self._unhealthy_since[instrument]
            if self.is_healthy(instrument):
                return None
            # Lazily open the window so a caller polling after the fact can
            # still learn when the feed went bad.
            now = self._clock()
            self._unhealthy_since[instrument] = now
            return now

    def mark_unhealthy(self, instrument: str) -> None:
        """Force an instrument unhealthy (e.g. a feed-level disconnect)."""
        if not instrument:
            return
        with self._lock:
            self._unhealthy_since.setdefault(instrument, self._clock())

    def status(self) -> dict:
        with self._lock:
            instruments = sorted(self._ever_seen | set(self._last_tick))
        out = {}
        for instrument in instruments:
            age = self.age(instrument)
            out[instrument] = {
                "healthy": self.is_healthy(instrument),
                "age_seconds": age,
                "stale_after": self._stale_after,
            }
        return out
