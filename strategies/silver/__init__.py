"""The sole SILVERM production strategy: configurable 15-minute SuperTrend."""
from __future__ import annotations

from strategies.supertrend import SuperTrendStrategyInstance


def _silver_security_id(security_id: str = "") -> str:
    """Resolve the current SILVERM contract from config (rollover-safe)."""
    if security_id:
        return str(security_id)
    try:
        from config import Config
        value = Config.instrument("SILVERM").get("security_id", "") or ""
        if value:
            return str(value)
    except Exception:
        pass
    return "483080"


def create_silver_15m(strategy_id: str = "silver_01", instrument: str = "SILVERM",
                      **kwargs) -> SuperTrendStrategyInstance:
    """Create SILVERM SuperTrend; this factory has no legacy-strategy fallback."""
    strategy_type = kwargs.get("strategy_type", "supertrend")
    if strategy_type != "supertrend" or strategy_id != "silver_01" or instrument != "SILVERM":
        raise ValueError("SILVERM runtime only supports silver_01 SuperTrend on 15m")
    params = kwargs.get("supertrend") or {}
    return SuperTrendStrategyInstance(
        strategy_id=strategy_id,
        instrument=instrument,
        security_id=_silver_security_id(kwargs.get("security_id", "")),
        quantity=kwargs.get("quantity", 1),
        capital=kwargs.get("capital", 300_000.0),
        multiplier=kwargs.get("multiplier", 5.0),
        atr_period=params.get("atr_period", kwargs.get("atr_period", 76)),
        atr_multiplier=params.get("multiplier", kwargs.get("atr_multiplier", 3.7)),
        source=params.get("source", kwargs.get("source", "low")),
        atr_method=params.get("atr_method", kwargs.get("atr_method", "sma_tr")),
        # Top-1 Pine v6 profile uses ta.tr(true), whose first value is H-L.
        first_tr=params.get("first_tr", "high_low"),
    )
