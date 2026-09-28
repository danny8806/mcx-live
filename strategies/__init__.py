"""Strategy API with lazy compatibility exports.

Importing a small strategy contract (for example ``strategies.types.Signal``)
must not also import every strategy, indicator, data source and API module.
Factories remain available from this package for existing callers.
"""
from __future__ import annotations

from importlib import import_module

from .types import Signal, SignalType, StrategyState

_LAZY_EXPORTS = {
    "GoldStrategy01": (".gold", "GoldStrategy01"),
    "GoldStrategy02": (".gold", "GoldStrategy02"),
    "GoldStrategy03": (".gold", "GoldStrategy03"),
    "GoldStrategy04": (".gold", "GoldStrategy04"),
    "SilverStrategy01": (".silver", "SilverStrategy01"),
    "SilverStrategy02": (".silver", "SilverStrategy02"),
    "SilverStrategy03": (".silver", "SilverStrategy03"),
    "SilverStrategy04": (".silver", "SilverStrategy04"),
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
