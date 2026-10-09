"""Indicators available to the SuperTrend-only application runtime."""
from importlib import import_module

_LAZY_EXPORTS = {
    "SuperTrend": (".supertrend", "SuperTrend"),
}
__all__ = list(_LAZY_EXPORTS)


def __getattr__(name: str):
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module, attribute = target
    value = getattr(import_module(module, __name__), attribute)
    globals()[name] = value
    return value
