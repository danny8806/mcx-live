"""Core engine package."""
from .timeframe_engine import Bar, BarState, TIMEFRAMES
from .risk_engine import RiskEngine
from .fill_dedup import FillDeduplicator

# TradeCloseManager imported lazily to avoid a heavy import chain at package
# load (it pulls execution/live modules); not a circular-dependency guard.
def _lazy_trade_close():
    from .trade_close import TradeCloseManager
    return TradeCloseManager

__all__ = [
    "Bar",
    "BarState",
    "TIMEFRAMES",
    "RiskEngine",
    "FillDeduplicator",
]
