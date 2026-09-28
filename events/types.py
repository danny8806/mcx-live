"""Immutable event types for the event-driven architecture."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class CandleEvent:
    """Immutable native candle event. Published when a candle closes.

    This is the primary data unit flowing through the event bus.
    Strategies receive these and update their own state.
    """
    instrument: str          # "GOLDM", "SILVERM"
    timeframe: str           # "5m", "15m", "1h"
    start_ts: float          # candle start (epoch seconds, IST-based)
    end_ts: float            # candle end (epoch seconds)
    open: float
    high: float
    low: float
    close: float
    volume: float
    is_closed: bool = True   # always True for published events
    source: str = "rest"     # "rest" | "websocket" | "replay"

    @property
    def security_id(self) -> str:
        """Security ID lookup from instrument name."""
        return _SECURITY_IDS.get(self.instrument, "")


@dataclass(frozen=True)
class TickEvent:
    """Immutable LTP tick event. Published on every WebSocket tick."""
    instrument: str          # "GOLDM", "SILVERM"
    ltp: float
    timestamp: float         # epoch seconds
    volume: float = 0.0


# Instrument → security_id mapping
_SECURITY_IDS = {
    "GOLDM": "569003",
    "SILVERM": "483080",
}
