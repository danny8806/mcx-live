"""Single execution-price rule for the 15-minute SuperTrend system."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ExecutionPricePlan:
    order_type: str
    kind: str
    price: Optional[float] = None
    trigger_price: Optional[float] = None


@dataclass(frozen=True)
class PricePreset:
    """Market execution at the completed signal candle's close."""

    def plan_for(self, signal, side: str) -> ExecutionPricePlan:
        metadata = getattr(signal, "metadata", None) or {}
        if str(side).upper() not in {"BUY", "SELL"}:
            raise ValueError("order side must be BUY or SELL")
        is_exit = bool(metadata.get("exit") or metadata.get("is_exit"))
        operator_exit = is_exit and bool(metadata.get("operator_action"))
        if not operator_exit:
            if metadata.get("strategy_type") != "supertrend":
                raise ValueError("only the configured SuperTrend strategy can execute")
            if not metadata.get("market_on_signal_close"):
                raise ValueError("SuperTrend orders must execute after a completed candle")
        return ExecutionPricePlan(
            order_type="MARKET",
            kind="signal_close_exit_market" if is_exit else "signal_close_entry_market",
        )
