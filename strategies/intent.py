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
    candle_low: Optional[float] = None,
) -> bool:
    """Close above the 1h line after the old close-cross OR a candle cross."""
    if htf_line is None or close <= htf_line:
        return False
    if not (previous_close <= htf_line
            or (candle_low is not None and candle_low < htf_line)):
        return False
    return mid_line is None or mid_line < htf_line


def short_crossover(
    close: float,
    previous_close: float,
    htf_line: Optional[float],
    mid_line: Optional[float] = None,
    candle_high: Optional[float] = None,
) -> bool:
    """Close below the 1h line after the old close-cross OR a candle cross."""
    if htf_line is None or close >= htf_line:
        return False
    if not (previous_close >= htf_line
            or (candle_high is not None and candle_high > htf_line)):
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
    """Return (exit trigger, opposite-entry trigger, new stop) for a reversal.

    The opposite entry trigger must sit STRICTLY BEYOND the exit trigger, in
    the direction the new position trades:

      LONG -> SHORT: exit needs price to FALL to ``exit_trigger``; the SHORT
        entry therefore must be BELOW it, so the new position can only be
        opened after the move has continued past the old exit level.
      SHORT -> LONG: symmetric — the LONG entry must be ABOVE the exit.

    A zero or negative ``gap`` would place the new entry at or behind the exit,
    letting the reversal entry become reachable at the same instant as the exit
    (or before the old position is flat).  That is a structurally invalid
    reversal, so it is rejected here rather than discovered in production.
    """
    exit_trigger, stop = entry_levels(
        side, high, low, previous_high, previous_low)
    if side.upper() == "LONG":
        entry_trigger = exit_trigger + gap
        if not entry_trigger > exit_trigger:
            raise ValueError(
                f"REVERSAL_GAP_INVALID: LONG->SHORT entry trigger "
                f"({entry_trigger}) must be strictly BELOW the exit trigger "
                f"({exit_trigger}); increase reversal_entry_gap_points")
    else:
        entry_trigger = exit_trigger - gap
        if not entry_trigger < exit_trigger:
            raise ValueError(
                f"REVERSAL_GAP_INVALID: SHORT->LONG entry trigger "
                f"({entry_trigger}) must be strictly ABOVE the exit trigger "
                f"({exit_trigger}); increase reversal_entry_gap_points")
    return exit_trigger, entry_trigger, stop
