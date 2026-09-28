"""Per-strategy HTF state — tracks latest completed HTF bar and its DEMA-ATR value.

Each strategy instance owns its own HTFState objects. No sharing.
"""
from __future__ import annotations

import bisect
import logging
from typing import Optional

from core.timeframe_engine import Bar
from htf.confirmation import HTFMappedValue
from indicators.dema_atr import DEMAATR

log = logging.getLogger(__name__)


class HTFState:
    """Tracks the latest completed HTF bar and its DEMA-ATR value for ONE strategy.

    Each strategy instance owns its own HTFState for each required HTF.
    This replaces the shared BacktestStyleHTFEngine with per-strategy isolation.

    The mapping logic is IDENTICAL to BacktestStyleHTFEngine._map_htf_to_fast():
        bisect_right(end_times, fast_bar.end_ts) - 1
    """

    def __init__(self, instrument: str, timeframe: str, dema_period: int = 3,
                 atr_period: int = 6, atr_factor: float = 1.0):
        self.instrument = instrument
        self.timeframe = timeframe
        self.indicator = DEMAATR(dema_period, atr_period, atr_factor)

        # Parallel arrays — same as BacktestStyleHTFEngine
        self._end_times: list[float] = []
        self._values: list[Optional[float]] = []

        # Latest confirmed values
        self.last_value: Optional[float] = None
        self.prev_value: Optional[float] = None

    def update(self, bar: Bar) -> None:
        """Feed a closed HTF bar. Updates indicator + stores for mapping.

        Called when a native HTF candle closes.
        Each strategy calls this on its own HTFState instance.
        """
        self.indicator.update(bar.open, bar.high, bar.low, bar.close)
        value = self.indicator.value

        self._end_times.append(bar.end_ts)
        self._values.append(value)

        if value is not None:
            self.prev_value = self.last_value
            self.last_value = value

    def get_mapped_value(self, fast_bar: Bar) -> HTFMappedValue:
        """Map own HTF DEMA-ATR to a fast bar using bisect (EXACT backtest logic).

        This is the same algorithm as BacktestStyleHTFEngine._map_htf_to_fast()
        but operates on this strategy's own data.
        """
        if not self._end_times:
            return HTFMappedValue(None, None, False, None)

        target_close = fast_bar.end_ts

        # EXACT backtest logic: bisect_right(end_times, target_close) - 1
        idx = bisect.bisect_right(self._end_times, target_close) - 1

        if idx < 0:
            return HTFMappedValue(None, None, False, None)

        htf_value = self._values[idx]

        # Only return confirmed values (non-None = after warmup)
        if htf_value is None:
            return HTFMappedValue(None, None, False, None)

        prev_htf_value = self._values[idx - 1] if idx > 0 else None

        return HTFMappedValue(
            htf_value=htf_value,
            prev_htf_value=prev_htf_value,
            htf_confirmed=True,
            htf_source_timestamp=self._end_times[idx],
        )

    def get_latest_value(self) -> Optional[float]:
        """Get the most recent confirmed DEMA-ATR value."""
        return self.last_value

    def get_prev_value(self) -> Optional[float]:
        """Get the previous confirmed DEMA-ATR value."""
        return self.prev_value

    def reset(self) -> None:
        """Clear all state. Used before warmup backfill."""
        self._end_times.clear()
        self._values.clear()
        self.last_value = None
        self.prev_value = None
        self.indicator.reset()

    def bar_count(self) -> int:
        """Number of HTF bars fed to this state."""
        return len(self._end_times)

    def snapshot(self) -> dict:
        """Return diagnostic snapshot."""
        return {
            "instrument": self.instrument,
            "timeframe": self.timeframe,
            "bar_count": self.bar_count(),
            "last_value": self.last_value,
            "prev_value": self.prev_value,
            "indicator_count": self.indicator._count,
            "indicator_initialized": self.indicator.initialized,
        }
