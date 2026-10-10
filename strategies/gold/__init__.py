"""The sole GOLDM production strategy: configurable 15-minute SuperTrend."""
from __future__ import annotations

from strategies.supertrend import SuperTrendStrategyInstance


def _gold_security_id(security_id: str = "") -> str:
    """Resolve the current GOLDM contract from config (rollover-safe)."""
    if security_id:
        return str(security_id)
    from config import Config
    value = Config.instrument("GOLDM").get("security_id", "") or ""
    if not value:
        raise RuntimeError("GOLDM security_id is missing from active instrument config")
    return str(value)


def create_gold_15m(strategy_id: str = "gold_02", instrument: str = "GOLDM",
                    **kwargs) -> SuperTrendStrategyInstance:
    """Create GOLDM SuperTrend; this factory has no legacy-strategy fallback."""
    strategy_type = kwargs.get("strategy_type", "supertrend")
    if strategy_type != "supertrend" or strategy_id != "gold_02" or instrument != "GOLDM":
        raise ValueError("GOLDM runtime only supports gold_02 SuperTrend on 15m")
    params = kwargs.get("supertrend") or {}
    return SuperTrendStrategyInstance(
        strategy_id=strategy_id,
        instrument=instrument,
        security_id=_gold_security_id(kwargs.get("security_id", "")),
        quantity=kwargs.get("quantity", 1),
        capital=kwargs.get("capital", 300_000.0),
        multiplier=kwargs.get("multiplier", 10.0),
        atr_period=params.get("atr_period", kwargs.get("atr_period", 46)),
        atr_multiplier=params.get("multiplier", kwargs.get("atr_multiplier", 3.9)),
        source=params.get("source", kwargs.get("source", "ohlc4")),
        atr_method=params.get("atr_method", kwargs.get("atr_method", "sma_tr")),
        first_tr=params.get("first_tr", "na"),
    )
