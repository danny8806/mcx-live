"""Timeframe engine for bar aggregation and closed bar detection."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

IST = timezone(timedelta(hours=5, minutes=30))
from enum import Enum
from typing import Any, Callable, Optional


class BarState(Enum):
    FORMING = "forming"
    CLOSED = "closed"
    PROCESSED = "processed"


@dataclass
class Bar:
    instrument: str
    timeframe: str
    start_ts: float
    end_ts: float
    open: float
    high: float
    low: float
    close: float
    volume: int = 0
    state: BarState = BarState.FORMING
    source: str = "websocket"
    tick_count: int = 0

    @property
    def mid_ts(self) -> float:
        return (self.start_ts + self.end_ts) / 2

    @property
    def is_forming(self) -> bool:
        return self.state == BarState.FORMING

    @property
    def is_closed(self) -> bool:
        return self.state == BarState.CLOSED

    @property
    def is_processed(self) -> bool:
        return self.state == BarState.PROCESSED


TIMEFRAMES = {
    "1m": 1,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1h": 60,
}


class BarAggregator:
    """Aggregates ticks into bars for a single instrument and timeframe."""

    def __init__(
        self,
        instrument: str,
        timeframe: str,
        session_open: str = "09:00",
        session_minutes: int = 870,
        on_bar_closed: Optional[Callable[[Bar], None]] = None,
    ):
        self.instrument = instrument
        self.timeframe = timeframe
        self.session_open = session_open
        self.session_minutes = session_minutes
        self.on_bar_closed = on_bar_closed

        self._minutes = TIMEFRAMES.get(timeframe, 5)
        self._current_bar: Optional[Bar] = None
        self._last_closed_bar: Optional[Bar] = None
        self._processed_bars: list[Bar] = []

        hour, minute = map(int, session_open.split(":"))
        self._session_open_hour = hour
        self._session_open_minute = minute

    def update(
        self,
        ltp: float,
        volume_delta: int,
        timestamp: float,
    ) -> Optional[Bar]:
        """Update with new tick and return closed bar if any."""
        ist_dt = datetime.fromtimestamp(timestamp, tz=IST)

        bucket_minute = (ist_dt.minute // self._minutes) * self._minutes
        bucket_start_ist = ist_dt.replace(minute=bucket_minute, second=0, microsecond=0)
        bucket_end_ist = bucket_start_ist + timedelta(minutes=self._minutes)

        bucket_start_epoch = bucket_start_ist.timestamp()
        bucket_end_epoch = bucket_end_ist.timestamp()

        closed_bar = None

        if self._current_bar is not None:
            if bucket_start_epoch > self._current_bar.start_ts:
                self._current_bar.state = BarState.CLOSED
                self._current_bar.end_ts = bucket_start_epoch
                closed_bar = self._current_bar
                self._last_closed_bar = closed_bar
                if self.on_bar_closed:
                    self.on_bar_closed(closed_bar)

        if (self._current_bar is not None
                and bucket_start_epoch == self._current_bar.start_ts):
            self._current_bar.high = max(self._current_bar.high, ltp)
            self._current_bar.low = min(self._current_bar.low, ltp)
            self._current_bar.close = ltp
            self._current_bar.volume += volume_delta
            self._current_bar.tick_count += 1
        else:
            self._current_bar = Bar(
                instrument=self.instrument,
                timeframe=self.timeframe,
                start_ts=bucket_start_epoch,
                end_ts=bucket_end_epoch,
                open=ltp,
                high=ltp,
                low=ltp,
                close=ltp,
                volume=volume_delta,
                state=BarState.FORMING,
                tick_count=1,
            )

        return closed_bar

    def close_current_bar(self) -> Optional[Bar]:
        """Force close the current forming bar."""
        if self._current_bar is not None and self._current_bar.is_forming:
            self._current_bar.state = BarState.CLOSED
            bar = self._current_bar
            self._last_closed_bar = bar
            self._current_bar = None
            if self.on_bar_closed:
                self.on_bar_closed(bar)
            return bar
        return None

    def mark_processed(self, bar: Bar) -> None:
        """Mark a closed bar as processed by strategy."""
        if bar.is_closed:
            bar.state = BarState.PROCESSED
            self._processed_bars.append(bar)

    @property
    def current_bar(self) -> Optional[Bar]:
        return self._current_bar

    @property
    def last_closed_bar(self) -> Optional[Bar]:
        return self._last_closed_bar

    @property
    def has_pending_bar(self) -> bool:
        return self._last_closed_bar is not None and self._last_closed_bar.is_closed



