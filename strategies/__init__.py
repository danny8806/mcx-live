"""Strategies package."""
from .base_dema_strategy import BaseDEMAStrategy, StrategyState, SignalType, Signal
from .gold import (
    GoldStrategy01,
    GoldStrategy02,
    GoldStrategy03,
    GoldStrategy04,
)
from .silver import (
    SilverStrategy01,
    SilverStrategy02,
    SilverStrategy03,
    SilverStrategy04,
)

__all__ = [
    "BaseDEMAStrategy",
    "StrategyState",
    "SignalType",
    "Signal",
    "GoldStrategy01",
    "GoldStrategy02",
    "GoldStrategy03",
    "GoldStrategy04",
    "SilverStrategy01",
    "SilverStrategy02",
    "SilverStrategy03",
    "SilverStrategy04",
]
