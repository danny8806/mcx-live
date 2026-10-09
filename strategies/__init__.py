"""Strategy API for the two supported 15-minute SuperTrend profiles."""
from __future__ import annotations

from importlib import import_module

from .types import Signal, SignalType, StrategyState

_LAZY_EXPORTS = {
    "create_gold_15m": (".gold", "create_gold_15m"),
    "create_silver_15m": (".silver", "create_silver_15m"),
}

__all__ = [
    "Signal", "SignalType", "StrategyState", *_LAZY_EXPORTS.keys(),
]


def __getattr__(name: str):
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attribute = target
    value = getattr(import_module(module_name, __name__), attribute)
    globals()[name] = value
    return value
