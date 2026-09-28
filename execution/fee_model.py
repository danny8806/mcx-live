"""Fee model for MCX futures trading."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class FeeBreakdown:
    """Breakdown of trading fees."""
    brokerage: float = 0.0
    stt: float = 0.0
    exchange: float = 0.0
    sebi: float = 0.0
    gst: float = 0.0
    stamp_duty: float = 0.0
    total: float = 0.0


class MCXFeeModel:
    """Configurable MCX fee model.
    
    Separates:
    - Estimated live paper charges
    - Actual broker charges
    
    Clearly labels P&L as:
    - GROSS
    - ESTIMATED NET
    - ACTUAL NET
    """

    def __init__(
        self,
        brokerage_per_side: float = 20.0,
        stt_sell_pct: float = 0.01,
        exchange_pct: float = 0.0026,
        sebi_pct: float = 0.0001,
        gst_pct: float = 18.0,
        stamp_duty_pct: float = 0.005,
    ):
        self.brokerage_per_side = brokerage_per_side
        self.stt_sell_pct = stt_sell_pct / 100
        self.exchange_pct = exchange_pct / 100
        self.sebi_pct = sebi_pct / 100
        self.gst_pct = gst_pct / 100
        self.stamp_duty_pct = stamp_duty_pct / 100

    def calculate(
        self,
        entry_price: float,
        exit_price: float,
        quantity: int,
        multiplier: float = 1.0,
        side: str = "LONG",
    ) -> FeeBreakdown:
        """Calculate round-trip fees.
        
        Args:
            entry_price: Average entry price
            exit_price: Average exit price
            quantity: Number of contracts
            multiplier: Contract multiplier
            side: Position side ("LONG" or "SHORT")
            
        Returns:
            FeeBreakdown with itemized charges
        """
        buy_turnover = entry_price * quantity * multiplier
        sell_turnover = exit_price * quantity * multiplier
        if side == "SHORT":
            buy_turnover, sell_turnover = sell_turnover, buy_turnover

        brokerage = self.brokerage_per_side * 2
        stt = sell_turnover * self.stt_sell_pct
        exchange = (buy_turnover + sell_turnover) * self.exchange_pct
        sebi = (buy_turnover + sell_turnover) * self.sebi_pct
        stamp = buy_turnover * self.stamp_duty_pct
        gst = (brokerage + exchange + sebi) * self.gst_pct

        total = brokerage + stt + exchange + sebi + gst + stamp

        return FeeBreakdown(
            brokerage=round(brokerage, 2),
            stt=round(stt, 2),
            exchange=round(exchange, 2),
            sebi=round(sebi, 2),
            gst=round(gst, 2),
            stamp_duty=round(stamp, 2),
            total=round(total, 2),
        )

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "MCXFeeModel":
        """Create fee model from configuration."""
        return cls(
            brokerage_per_side=float(config.get("brokerage_per_side", 20.0)),
            stt_sell_pct=float(config.get("stt_sell_pct", 0.01)),
            exchange_pct=float(config.get("exchange_pct", 0.0026)),
            sebi_pct=float(config.get("sebi_pct", 0.0001)),
            gst_pct=float(config.get("gst_pct", 18.0)),
            stamp_duty_pct=float(config.get("stamp_duty_pct", 0.005)),
        )
