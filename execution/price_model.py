"""Central LIVE execution price model — pure functions, no IO (Phase 3).

Contract: every LIVE execution price is derived from the immutable signal's
trigger/stop values through pure functions, so a price is never improvised at
the broker and PAPER/LIVE can never diverge by mistake.

LIVE uses a LIMIT-first execution model: when the system detects a trigger
crossing via Market WS, it sends a LIMIT order at the trigger price.  The
order watcher monitors the resting LIMIT and performs cancel→verify→MARKET
remaining if the LIMIT fails to fill within the configured skip policy
thresholds.  This avoids duplicate orders and ensures quantity safety (market
qty = requested - filled, never the full requested amount).

For legacy broker-side STOP_LOSS entries (``immediate_limit``), a broker-side
trigger order is still used — both price and trigger_price are sent to Dhan
(SM/SLM).  Broker-side SL orders are disabled (``broker_sl.enabled=false``).
"""
from __future__ import annotations

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
      * ``LIMIT``           -> system-triggered entries and exits (the system
                               detected the trigger crossing via Market WS;
                               LIMIT rests at the trigger price; the order
                               watcher handles cancel→verify→MARKET remaining
                               if unfilled)
      * ``STOP_LOSS``       -> legacy broker-side trigger entries (both
                               ``price`` = limit and ``trigger_price`` = trigger
                               are sent; the broker activates on the trigger)
    ``kind`` classifies the intent for reporting and forensics.
    """
    order_type: str
    kind: str
    price: Optional[float] = None
    trigger_price: Optional[float] = None


@dataclass(frozen=True)
class PricePreset:
    """Offset margins + tick grid applied to LIVE executions.

    Offset values are pure configuration; the functions stay pure.  A zero
    offset produces a broker-side STOP_LOSS with the trigger exactly at the
    signal level and the LIMIT leg one tick tradeable-side of it (degenerate
    but valid); the LIVE master gate default OFF means no real order is ever
    sent until the operator enables both the gate and this model.
    """
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

        LIMIT-first model: when the system detects a trigger crossing via
        Market WS (``metadata["triggered"] = True``), the entry/exit is
        executed as a LIMIT order at the trigger price.  The order watcher
        monitors the resting LIMIT and performs cancel→verify→MARKET remaining
        if it fails to fill within the configured skip policy thresholds.
        """
        metadata = signal.metadata or {}
        is_exit = bool(metadata.get("exit"))
        side_u = str(side).upper()

        # ── ALL EXITS: system-side trigger → LIMIT at trigger price ──
        # Both SL exits and reversal exits are triggered by on_tick crossing
        # detection → LIMIT at trigger price.  The order watcher handles
        # cancel→verify→MARKET remaining if the LIMIT fails to fill.
        if is_exit:
            # A stop-loss exit must be priced at the STOP LEVEL our own
            # monitoring watches, never at the signal's trigger price.
            # ``trigger_price`` on an exit signal is the price observed at
            # detection time (bar.close / ltp), which is a moving market
            # value: pricing the protective exit from it sent a gold SELL
            # LIMIT at 147746 while the stop was 147220, so the exit order
            # could rest on the wrong side of the market and never protect
            # the position.  The stop is the level that was actually
            # crossed, so that is the level the LIMIT must rest at.
            reason = str(metadata.get("exit_reason") or "").lower()
            is_sl = reason in ("stop_loss_hit", "stop_loss")
            level = None
            if is_sl and signal.stop_price:
                level = signal.stop_price
            if not level:
                # Never improvise a protective price: without a stop there is
                # nothing to protect against, so fall back to the detected
                # level rather than silently inventing one.
                level = signal.trigger_price
            if not level:
                raise ValueError(
                    "exit signal carries neither stop_price nor trigger_price: "
                    "cannot build an exit plan")
            return ExecutionPricePlan(
                order_type="LIMIT",
                kind="system_sl_exit" if is_sl else "system_exit",
                price=_tick_round(level, self.tick_size))

        # ── DIRECT-FIRE ENTRIES: system-side trigger → LIMIT ─────────
        # When on_tick detects the trigger crossing, ``triggered=True``
        # signals a LIMIT order at the trigger price — the order watcher
        # monitors and falls back to MARKET remaining if unfilled.
        if metadata.get("triggered"):
            trigger = _tick_round(signal.trigger_price, self.tick_size) if signal.trigger_price else None
            return ExecutionPricePlan(
                order_type="LIMIT", kind="direct_fire_entry", price=trigger)

        # ── immediate_limit entries ──────────────────────────────
        # The strategy's immediate-limit factory ALWAYS co-sets
        # metadata["triggered"]=True (strategies/instance.py
        # _create_immediate_limit_signal), so immediate-limit ENTRY signals
        # are planned above as a plain resting LIMIT (kind
        # direct_fire_entry) supervised by the order watcher's
        # cancel→verify→MARKET fallback — never as a raw STOP_LOSS.
        # A legacy broker-side STOP_LOSS stop-limit planning branch lived
        # here but was unreachable dead code; had it ever been reached it
        # would place a broker-side trigger order this flow does not
        # supervise.  Fail loudly instead of mis-planning.
        if metadata.get("immediate_limit") and not metadata.get("triggered"):
            raise ValueError(
                "immediate_limit entry signal is missing triggered=True: the "
                "immediate-limit factory must emit triggered=True so the entry "
                "plans as a direct-fire LIMIT (never a raw STOP_LOSS).")
        if side_u == "BUY":
            trigger = self.long_entry_trigger(signal.trigger_price)
            limit, trigger = _stop_limit_legs(
                trigger=trigger, side=side_u, tick_size=self.tick_size,
                direction="same")
            return ExecutionPricePlan(
                order_type="STOP_LOSS", kind="long_entry",
                price=limit, trigger_price=trigger)
        trigger = self.short_entry_trigger(signal.trigger_price)
        limit, trigger = _stop_limit_legs(
            trigger=trigger, side=side_u, tick_size=self.tick_size,
            direction="same")
        return ExecutionPricePlan(
            order_type="STOP_LOSS", kind="short_entry",
            price=limit, trigger_price=trigger)