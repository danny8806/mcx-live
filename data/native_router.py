"""NativeCandleRouter — first-class native candle distribution router (§7).

A single choke point between the Dhan REST candle source (CandleFetcher) or
replay path and the EventBus. Every native candle enters here once, is
validated (complete), de-duplicated by its canonical identity, checked for
out-of-order delivery, and only then forwarded to the NativeCandleDistributor
which publishes to every subscribed strategy.

Candle identity (mission §7):
    (security_id, timeframe, candle_end_ts)

Duplicate candles (same identity) are published once.
Out-of-order candles (an end_ts older than the last accepted on the same
stream) are detected and dropped — they must never be published after a newer
bar already advanced the shared indicator streams.
Incomplete candles (is_complete=False) are never published as completed.
"""
from __future__ import annotations

import logging
import threading
from typing import Callable, Optional

from core.timeframe_engine import Bar

log = logging.getLogger(__name__)


class NativeCandleRouter:
    """Receives native candles exactly once, guards and forwards them.

    The router is a pure distribution guard: it does NOT calculate indicators
    and does NOT own strategy state. It forwards accepted bars to the
    distributor callback (normally NativeCandleDistributor.on_candle_closed).
    """

    def __init__(
        self,
        distributor: Optional[Callable[[Bar], None]] = None,
        instruments: Optional[dict] = None,
    ):
        self._distributor = distributor
        # instruments config: {"GOLDM": {"security_id": "569003"}, ...}
        self._instruments = instruments or {}
        self._lock = threading.RLock()
        # (security_id, timeframe) -> last accepted candle_end_ts
        self._last_end_ts: dict[tuple, float] = {}
        self._published_count: int = 0
        self._dedup_count: int = 0
        self._out_of_order_count: int = 0
        self._incomplete_rejected_count: int = 0

    # ── identity ────────────────────────────────────────────────────────

    def security_id_for(self, instrument: str) -> Optional[str]:
        """Resolve canonical security_id for an instrument (mission §7)."""
        inst_cfg = self._instruments.get(instrument) or {}
        return inst_cfg.get("security_id") or None

    def _identity(self, bar: Bar) -> tuple:
        sec_id = self.security_id_for(bar.instrument) or bar.instrument
        return (sec_id, bar.timeframe, float(bar.end_ts))

    # ── entry points ────────────────────────────────────────────────────

    def on_candle(self, bar: Bar, is_complete: bool = True) -> bool:
        """Validate + dedupe + forward one native candle.

        Args:
            bar: native candle (Bar with instrument, timeframe, end_ts).
            is_complete: False indicates a still-forming/incomplete candle,
                which is NEVER published as a completed candle.

        Returns:
            True if the candle was forwarded to the distributor, False if it
            was rejected (incomplete) or dropped (duplicate / out-of-order).
        """
        if bar is None:
            return False

        # §7 — incomplete candles must not be published as completed candles.
        if not is_complete:
            with self._lock:
                self._incomplete_rejected_count += 1
            log.error("[CandleRouter] rejecting incomplete candle: %s", bar)
            return False

        identity = self._identity(bar)
        stream_key = (identity[0], identity[1])
        end_ts = identity[2]

        with self._lock:
            last = self._last_end_ts.get(stream_key)

            # §7 — duplicate candles are deduplicated (published once).
            if last is not None and end_ts == last:
                self._dedup_count += 1
                log.debug("[CandleRouter] duplicate candle dropped: %s", identity)
                return False

            # §7 — out-of-order candles are detected (and not published after
            # a newer bar already advanced the streams).
            if last is not None and end_ts < last:
                self._out_of_order_count += 1
                log.error(
                    "[CandleRouter] out-of-order candle dropped: %s "
                    "(last_accepted=%s)", identity, last)
                return False

            self._last_end_ts[stream_key] = end_ts
            self._published_count += 1

        if self._distributor is not None:
            self._distributor(bar)
        return True

    def on_candle_closed(self, bar: Bar) -> bool:
        """Alias for on_candle() — drop-in for the CandleFetcher callback.

        A bar reaching the router from the fetcher is by construction a fully
        formed candle (the fetcher only emits aggregated complete windows).
        """
        return self.on_candle(bar, is_complete=True)

    # ── diagnostics ─────────────────────────────────────────────────────

    def stats(self) -> dict:
        with self._lock:
            return {
                "published": self._published_count,
                "deduplicated": self._dedup_count,
                "out_of_order": self._out_of_order_count,
                "incomplete_rejected": self._incomplete_rejected_count,
                "streams": len(self._last_end_ts),
            }

    @property
    def candle_count(self) -> int:
        return self._published_count

    def reset(self) -> None:
        with self._lock:
            self._last_end_ts.clear()
            self._published_count = 0
            self._dedup_count = 0
            self._out_of_order_count = 0
            self._incomplete_rejected_count = 0