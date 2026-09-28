"""Pure DEMA/ATR signal rules shared by every strategy runtime.

Keep market intent here, separate from state tracking, execution and broker
order lifecycle. A new runtime can reuse these functions without changing the
trading rules.
"""
from __future__ import annotations

from typing import Optional


def long_crossover(
    close: float,
    previous_close: float,
    htf_line: Optional[float],
    mid_line: Optional[float] = None,
) -> bool:
    """Cross above the 1h line, confirmed by 15m being below that line."""
    if htf_line is None or not (close > htf_line and previous_close <= htf_line):
        return False
    return mid_line is None or mid_line < htf_line


def short_crossover(
    close: float,
    previous_close: float,
    htf_line: Optional[float],
    mid_line: Optional[float] = None,
) -> bool:
    """Cross below the 1h line, confirmed by 15m being above that line."""
    if htf_line is None or not (close < htf_line and previous_close >= htf_line):
        return False
    return mid_line is None or mid_line > htf_line


def entry_levels(
    side: str,
    high: float,
    low: float,
    previous_high: Optional[float] = None,
    previous_low: Optional[float] = None,
) -> tuple[float, float]:
    """Return (breakout trigger, structural stop) for the signal candle."""
    if side.upper() == "LONG":
        return high, min(low, previous_low if previous_low is not None else low)
    if side.upper() == "SHORT":
        return low, max(high, previous_high if previous_high is not None else high)
    raise ValueError(f"unsupported strategy side: {side!r}")


def reversal_levels(
    side: str,
    high: float,
    low: float,
    previous_high: Optional[float],
    previous_low: Optional[float],
    gap: float = 0,
) -> tuple[float, float, float]:
    """Return (exit trigger, opposite-entry trigger, new stop) for a reversal."""
    exit_trigger, stop = entry_levels(
        side, high, low, previous_high, previous_low)
    entry_trigger = exit_trigger + gap if side.upper() == "LONG" else exit_trigger - gap
    return exit_trigger, entry_trigger, stop
