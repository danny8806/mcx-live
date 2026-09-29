"""Native candle distributor — converts raw bars to CandleEvents and publishes."""
from __future__ import annotations

import logging
from typing import Optional

from events.types import CandleEvent
from events.bus import EventBus
from core.timeframe_engine import Bar

log = logging.getLogger(__name__)


class NativeCandleDistributor:
    """Receives raw bars from CandleFetcher, creates CandleEvents, publishes to EventBus.

    This is the bridge between the polling CandleFetcher and the event-driven
    strategy layer. It does NOT calculate indicators or own strategy state.

    Topic routing:
        candle:GOLDM:5m     — specific instrument + timeframe
        candle:GOLDM:*      — all timeframes for GOLDM
        candle:*:5m         — all instruments for 5m
        candle:*:*          — global candle wildcard
    """

    def __init__(self, event_bus: EventBus):
        self.event_bus = event_bus
        self._candle_count: int = 0

    def on_candle_closed(self, bar: Bar) -> None:
        """Convert a Bar to CandleEvent and publish to EventBus.

        Called by CandleFetcher via the on_candle_closed callback.
        """
        event = CandleEvent(
            instrument=bar.instrument,
            timeframe=bar.timeframe,
            start_ts=bar.start_ts,
            end_ts=bar.end_ts,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=float(bar.volume),
            is_closed=True,
            source="rest",
        )

        self._candle_count += 1

        # Publish to specific topic: candle:GOLDM:5m
        specific = f"candle:{bar.instrument}:{bar.timeframe}"
        self.event_bus.publish(specific, event)

        # Publish to instrument wildcard: candle:GOLDM:*
        instrument_wc = f"candle:{bar.instrument}:*"
        self.event_bus.publish(instrument_wc, event)

    @property
    def candle_count(self) -> int:
        return self._candle_count
