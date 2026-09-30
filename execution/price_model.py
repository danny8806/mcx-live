"""Central LIVE execution price model — pure functions, no IO (Phase 3).

Contract: every LIVE execution price is derived from the immutable signal's
trigger/stop values through pure functions, so a price is never improvised at
the broker and PAPER/LIVE can never diverge by mistake.

LIVE has one entry model: a local live-LTP trigger fires before the engine
sends a LIMIT at its trigger level. Exits are created only after
the system stop monitor or strategy exit fires. Broker-side protective stop
orders are disabled; the local tick monitor owns stop triggering.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional

__all__ = [
    "calculate_long_entry_price",
    "calculate_long_sl_price",
    "calculate_short_entry_price",
    "calculate_short_sl_price",
    "ExecutionPricePlan",
    "PricePreset",
    "_stop_limit_legs",
]


def calculate_long_entry_price(high: float, entry_offset: float) -> float:
    """LONG entry trigger = breakout high + entry offset margin."""
    if entry_offset < 0:
        raise ValueError("entry_offset must be >= 0")
    return float(high) + float(entry_offset)


def calculate_long_sl_price(low: float, sl_offset: float) -> float:
    """LONG stop-loss trigger = stop low - SL offset margin."""
    if sl_offset < 0:
        raise ValueError("sl_offset must be >= 0")
    return float(low) - float(sl_offset)


def calculate_short_entry_price(
    low: float, entry_offset: float, allow_negatives: bool = True,
) -> float:
    """SHORT entry trigger = breakout low - entry offset margin."""
    if entry_offset < 0:
        raise ValueError("entry_offset must be >= 0")
    px = float(low) - float(entry_offset)
    if not allow_negatives and px < 0.0:
        return 0.0
    return px


def calculate_short_sl_price(high: float, sl_offset: float) -> float:
    """SHORT stop-loss trigger = stop high + SL offset margin."""
    if sl_offset < 0:
        raise ValueError("sl_offset must be >= 0")
    return float(high) + float(sl_offset)


def _tick_round(value: float, tick: float) -> float:
    if tick <= 0:
        return float(value)
    return round(float(value) / tick) * tick


def _stop_limit_legs(*, trigger: float, side: str, tick_size: float,
                     direction: str = "same") -> tuple:
    """Split one trigger level into safe ``(limit_price, trigger_price)`` legs.

    ``direction`` controls where the LIMIT leg sits relative to the trigger:
      * ``same``  -> limit at the trigger level nudged ONE tick tradeable-side
        (minimum separation Dhan requires; offset-0 defaults).
      * ``above`` -> limit = trigger + tick (marketable once triggered BUY-side).
      * ``below`` -> limit = trigger - tick (marketable once triggered SELL-side).
    The invariant is ALWAYS enforced afterwards (Dhan DH-906): BUY -> limit > trigger;
    SELL -> limit < trigger.
    """
    tick = float(tick_size) if tick_size and tick_size > 0 else 1.0
    t = _tick_round(float(trigger), tick)
    side_u = str(side).upper()
    if direction == "same":
        # Broker requires limit != trigger; nudge the LIMIT ONE tick into the
        # direction the market must travel for the stop to activate (BUY: above
        # the trigger so a rise through it is marketable, SELL: below).
        limit = t + tick if side_u == "BUY" else t - tick
    elif direction == "above":
        limit = t + tick
    else:  # below
        limit = t - tick
    # Guarantee the Dhan DH-906 ordering invariant for every path/override.
    if side_u == "BUY" and not (limit > t):
        limit = t + tick
    elif side_u == "SELL" and not (limit < t):
        limit = t - tick
    return (float(limit), float(t))


@dataclass(frozen=True)
class ExecutionPricePlan:
    """The concrete execution intent derived from one signal.

    ``order_type`` is what gets sent to the broker transport:
      * ``LIMIT``           -> locally-triggered entries and system-triggered
                               exits, priced from the immutable signal
    ``kind`` classifies the intent for reporting and forensics.
    """
    order_type: str
    kind: str
    price: Optional[float] = None
    trigger_price: Optional[float] = None


@dataclass(frozen=True)
class PricePreset:
    """Apply configured price offsets and tick rounding to LIVE orders."""
    entry_offset: float = 0.0
    sl_offset: float = 0.0
    tick_size: float = 1.0

    def long_entry_trigger(self, high: float) -> float:
        return calculate_long_entry_price(high, self.entry_offset)

    def long_sl_trigger(self, low: float) -> float:
        return calculate_long_sl_price(low, self.sl_offset)

    def short_entry_trigger(self, low: float) -> float:
        return calculate_short_entry_price(low, self.entry_offset)

    def short_sl_trigger(self, high: float) -> float:
        return calculate_short_sl_price(high, self.sl_offset)

    def plan_for(self, signal, side: str) -> ExecutionPricePlan:
        """Map one signal to the executable price plan.

        ``signal`` only needs ``trigger_price`` / ``stop_price`` / ``metadata``
        (the strategies.types.Signal contract).  ``side`` is the resolved order
        side ("BUY"/"SELL").

        Entry signals must carry a fired local trigger. Reversal entries arrive
        here only after their trigger fires and the previous position is flat.
        """
        metadata = signal.metadata or {}
        is_exit = bool(metadata.get("exit"))
        side_u = str(side).upper()

        # ── ALL EXITS: system-side trigger → LIMIT at trigger price ──
        # Both SL exits and reversal exits are triggered by on_tick crossing
        # detection → LIMIT at trigger price.  The order watcher handles
        # cancel→verify→MARKET remaining if the LIMIT fails to fill.
        if is_exit:
            if str(metadata.get("trigger_state", "")).upper() != "FIRED":
                raise ValueError("exit must be triggered before limit planning")
            reason = str(metadata.get("exit_reason") or "").lower()
            is_sl = reason in ("stop_loss_hit", "stop_loss")
            level = None
            if is_sl and signal.stop_price:
                level = float(signal.stop_price)
            if not level:
                # Never improvise a protective price: without a stop there is
                # nothing to protect against, so fall back to the detected
                # level rather than silently inventing one.
                level = signal.trigger_price
            if not level:
                raise ValueError(
                    "exit signal carries neither stop_price nor trigger_price: "
                    "cannot build an exit plan")
            if is_sl:
                tick = float(self.tick_size)
                if tick <= 0:
                    raise ValueError("stop exit tick size must be positive")
                level = (math.floor(level / tick) * tick if side_u == "SELL"
                         else math.ceil(level / tick) * tick)
            return ExecutionPricePlan(
                order_type="LIMIT",
                kind="system_sl_exit" if is_sl else "system_exit",
                price=float(level) if is_sl else _tick_round(level, self.tick_size))

        # There is one entry mode: a LIMIT after the local trigger fires.
        if not metadata.get("triggered"):
            raise ValueError("entry must be triggered before limit submission")
        if str(metadata.get("trigger_state", "")).upper() != "FIRED":
            raise ValueError("entry trigger must be FIRED before limit planning")
        if not signal.trigger_price:
            raise ValueError("local-trigger entry requires a positive trigger price")
        observed = metadata.get("trigger_ltp")
        try:
            observed = float(observed) if observed is not None else float(signal.trigger_price)
        except (TypeError, ValueError):
            raise ValueError("entry trigger LTP is invalid") from None
        if not math.isfinite(observed) or observed <= 0:
            raise ValueError("entry trigger LTP must be finite and positive")
        tick = float(self.tick_size)
        if tick <= 0:
            raise ValueError("entry tick size must be positive")
        reversal_after_exit = bool(
            metadata.get("entry_after_confirmed_reversal_exit"))
        market_fallback = bool(metadata.get("market_fallback"))
        if side_u == "BUY":
            if (not reversal_after_exit and not market_fallback
                    and observed < float(signal.trigger_price)):
                raise ValueError("long entry price has not crossed the trigger")
            price = math.ceil((observed + self.entry_offset) / tick) * tick
            kind = "long_entry"
        else:
            if (not reversal_after_exit and not market_fallback
                    and observed > float(signal.trigger_price)):
                raise ValueError("short entry price has not crossed the trigger")
            price = math.floor((observed - self.entry_offset) / tick) * tick
            kind = "short_entry"
        return ExecutionPricePlan(
            order_type="LIMIT", kind=kind,
            price=float(price))
