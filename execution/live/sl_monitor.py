"""Position-owned local SL monitor — the ONLY stop-loss mechanism.

Architecture (one way, no broker-side protective order ever):

    OPEN POSITION
        -> position.stop_price
        -> SL monitor (ARMED)
        -> market tick crosses stop_price
        -> TRIGGERED
        -> EXIT_SUBMITTED (one direct exit order)
        -> broker-confirmed fill
        -> CLOSED (SL state cleared)

Hard rules enforced here, in order, before ANY exit order can be minted:

  INVARIANT 1  no open position           => no active SL monitor
  INVARIANT 2  no open position           => SL cannot submit an order
  INVARIANT 3  one open position          => at most one active SL record
  INVARIANT 4  SL exit submitted          => no second SL exit
  INVARIANT 5  position closed            => old SL can never fire
  INVARIANT 6  new position               => old position SL can never fire
  INVARIANT 7  SL belongs to the CURRENT position id + generation
  INVARIANT 8  SL quantity <= current open quantity
  INVARIANT 9  a stale DB position cannot arm an SL (broker-authoritative
                adoption happens first, in ``resync_from_broker``)
  INVARIANT 10 no broker-side protective SL is ever submitted (there is no
                code path here that creates an order at all)

There is deliberately NO:
  * ``create_protective_sl`` / resting STOP_LOSS order,
  * broker SL order id, verification, retry or cancellation,
  * stop-price invention from an old signal / trade / candle.
"""
from __future__ import annotations

import logging
import math
import threading
from dataclasses import dataclass
from enum import Enum
from typing import Optional

log = logging.getLogger("live.sl_monitor")


class SLState(str, Enum):
    """Local SL lifecycle. ARMED means *this process* is watching a price."""

    NONE = "NONE"
    ARMED = "ARMED"
    TRIGGERED = "TRIGGERED"
    EXITING = "EXITING"
    CLOSED = "CLOSED"
    UNAVAILABLE = "SL_UNAVAILABLE"


# Terminal states: once reached, this position's SL can never fire again.
_TERMINAL = (SLState.CLOSED,)

# Rejection reasons. Every one of them means "NO ORDER".
class SLReject:
    NO_POSITION = "NO_CURRENT_OPEN_POSITION"
    NOT_OPEN = "POSITION_NOT_OPEN"
    ALREADY_CLOSED = "POSITION_ALREADY_CLOSED"
    STRATEGY_MISMATCH = "POSITION_STRATEGY_MISMATCH"
    INSTRUMENT_MISMATCH = "POSITION_INSTRUMENT_MISMATCH"
    GENERATION_MISMATCH = "POSITION_GENERATION_MISMATCH"
    QUANTITY_INVALID = "POSITION_QUANTITY_INVALID"
    EXIT_ALREADY_STARTED = "POSITION_EXIT_ALREADY_STARTED"
    STOP_MISSING = "STOP_PRICE_UNAVAILABLE"
    STOP_INVALID = "STOP_PRICE_INVALID"
    NOT_ARMED = "SL_NOT_ARMED"
    ALREADY_TRIGGERED = "SL_ALREADY_TRIGGERED"
    ALREADY_EXITING = "SL_ALREADY_EXITING"
    MARKET_INVALID = "MARKET_PRICE_INVALID"
    NOT_CROSSED = "STOP_NOT_CROSSED"


@dataclass(frozen=True)
class SLDecision:
    """Outcome of one SL evaluation. ``fire`` True => an exit may be minted."""

    fire: bool
    reason: str
    position_id: str = ""
    strategy_id: str = ""
    instrument: str = ""
    quantity: int = 0
    stop_price: float = 0.0
    side: str = ""

    @property
    def exit_side(self) -> str:
        """Broker side required to flatten this position."""
        return "SELL" if str(self.side).upper() == "LONG" else "BUY"


@dataclass
class SLArm:
    """The single authoritative binding: this SL belongs to THIS position."""

    position_id: str
    strategy_id: str
    instrument: str
    side: str
    quantity: int
    stop_price: float
    position_generation: int
    state: SLState = SLState.ARMED
    exit_order_id: Optional[str] = None
    trigger_price: Optional[float] = None

    def snapshot(self) -> dict:
        return {
            "position_id": self.position_id,
            "strategy_id": self.strategy_id,
            "instrument": self.instrument,
            "side": self.side,
            "quantity": self.quantity,
            "stop_price": self.stop_price,
            "position_generation": self.position_generation,
            "state": self.state.value,
            "exit_order_id": self.exit_order_id,
            "trigger_price": self.trigger_price,
        }


def _valid_price(value) -> bool:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return False
    return f > 0.0 and not math.isnan(f) and not math.isinf(f)


class PositionOwnedSLMonitor:
    """Owns the local SL for every open position, keyed by ``position_id``.

    Thread-safe: market ticks arrive on the websocket thread while fills and
    reconciliation mutate positions on the poller thread.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        # position_id -> SLArm.  A closed position's entry is popped, so a
        # later tick can never reach it (INVARIANT 5).
        self._armed: dict[str, SLArm] = {}

    # ── arming ──────────────────────────────────────────────────────────

    def arm(self, position) -> SLState:
        """Bind the position's own ``stop_price`` to this process.

        Returns the resulting :class:`SLState`.  ``SL_UNAVAILABLE`` means the
        position has no usable stop: we do NOT invent one and do NOT borrow a
        previous position's or an old signal's stop (§2).
        """
        pid = str(getattr(position, "position_id", "") or "")
        if not pid:
            return SLState.NONE
        stop = getattr(position, "stop_price", None)
        if not _valid_price(stop):
            with self._lock:
                self._armed.pop(pid, None)
            return SLState.UNAVAILABLE
        if not getattr(position, "is_open", False):
            with self._lock:
                self._armed.pop(pid, None)
            return SLState.NONE
        try:
            qty = int(getattr(position, "quantity", 0) or 0)
        except (TypeError, ValueError):
            qty = 0
        if qty <= 0:
            # Nothing to protect: never arm an SL for a zero-quantity row.
            with self._lock:
                self._armed.pop(pid, None)
            return SLState.NONE
        side = "LONG" if getattr(position, "is_long", False) else "SHORT"
        arm = SLArm(
            position_id=pid,
            strategy_id=str(getattr(position, "strategy_id", "") or ""),
            instrument=str(getattr(position, "instrument", "") or ""),
            side=side,
            quantity=qty,
            stop_price=float(stop),
            position_generation=int(getattr(position, "position_generation", 0) or 0),
            state=SLState.ARMED,
        )
        with self._lock:
            prior = self._armed.get(pid)
            if prior is not None and prior.state in (SLState.TRIGGERED,
                                                      SLState.EXITING):
                # An exit is already in flight for this exact position; never
                # re-arm it back to ARMED and risk a second exit (INVARIANT 4).
                return prior.state
            self._armed[pid] = arm
        return SLState.ARMED

    def disarm(self, position_id: str) -> None:
        """Drop the SL record entirely (position gone / closed)."""
        with self._lock:
            self._armed.pop(str(position_id), None)

    def close(self, position_id: str) -> None:
        """Mark this position's SL closed and disarm it (INVARIANT 5)."""
        pid = str(position_id)
        with self._lock:
            arm = self._armed.get(pid)
            if arm is not None:
                arm.state = SLState.CLOSED
            self._armed.pop(pid, None)

    # ── evaluation ───────────────────────────────────────────────────────

    def evaluate(self, position, ltp: float, *,
                 strategy_id: Optional[str] = None,
                 instrument: Optional[str] = None) -> SLDecision:
        """Run the full §3 validation chain. Fire only if EVERY check passes."""
        pid = str(getattr(position, "position_id", "") or "")
        no = SLDecision(False, SLReject.NO_POSITION, position_id=pid)

        # 1. Is there a CURRENT OPEN POSITION?
        if not pid or position is None:
            return no
        if not getattr(position, "is_open", False):
            status = getattr(position, "status", None)
            if str(getattr(status, "value", status) or "").lower() in ("closed", "close"):
                return SLDecision(False, SLReject.ALREADY_CLOSED, position_id=pid)
            return SLDecision(False, SLReject.NOT_OPEN, position_id=pid)
        if getattr(position, "exit_started", False):
            return SLDecision(False, SLReject.EXIT_ALREADY_STARTED, position_id=pid)

        pos_strategy = str(getattr(position, "strategy_id", "") or "")
        pos_instrument = str(getattr(position, "instrument", "") or "")

        # 2. strategy ownership
        if strategy_id is not None and str(strategy_id) != pos_strategy:
            return SLDecision(False, SLReject.STRATEGY_MISMATCH, position_id=pid,
                              strategy_id=pos_strategy, instrument=pos_instrument)

        # 3. symbol ownership
        if instrument is not None and str(instrument) != pos_instrument:
            return SLDecision(False, SLReject.INSTRUMENT_MISMATCH, position_id=pid,
                              strategy_id=pos_strategy, instrument=pos_instrument)

        # 4. quantity
        try:
            qty = int(getattr(position, "quantity", 0) or 0)
        except (TypeError, ValueError):
            qty = 0
        if qty <= 0:
            return SLDecision(False, SLReject.QUANTITY_INVALID, position_id=pid,
                              strategy_id=pos_strategy, instrument=pos_instrument)

        # 5. stop price — never invented, never borrowed.
        stop = getattr(position, "stop_price", None)
        if stop is None or not _valid_price(stop):
            return SLDecision(False, SLReject.STOP_MISSING, position_id=pid,
                              strategy_id=pos_strategy, instrument=pos_instrument)
        stop = float(stop)

        with self._lock:
            arm = self._armed.get(pid)
            if arm is None:
                return SLDecision(False, SLReject.NOT_ARMED, position_id=pid,
                                  strategy_id=pos_strategy, instrument=pos_instrument,
                                  stop_price=stop)
            # INVARIANT 7 — the SL belongs to this exact position generation.
            if arm.position_generation != int(
                    getattr(position, "position_generation", 0) or 0):
                self._armed.pop(pid, None)
                return SLDecision(False, SLReject.GENERATION_MISMATCH,
                                  position_id=pid, strategy_id=pos_strategy,
                                  instrument=pos_instrument, stop_price=stop)
            if arm.strategy_id != pos_strategy:
                self._armed.pop(pid, None)
                return SLDecision(False, SLReject.STRATEGY_MISMATCH,
                                  position_id=pid, instrument=pos_instrument,
                                  stop_price=stop)
            if arm.instrument != pos_instrument:
                self._armed.pop(pid, None)
                return SLDecision(False, SLReject.INSTRUMENT_MISMATCH,
                                  position_id=pid, strategy_id=pos_strategy,
                                  stop_price=stop)
            # The stop is re-read from the position on every tick, so a
            # strategy-side stop update is picked up without re-arming.
            arm.stop_price = stop
            arm.side = "LONG" if getattr(position, "is_long", False) else "SHORT"
            arm.quantity = qty

            # §9 duplicate protection + §10 exit race.
            if arm.state in _TERMINAL:
                self._armed.pop(pid, None)
                return SLDecision(False, SLReject.ALREADY_CLOSED, position_id=pid,
                                  stop_price=stop)
            if arm.state == SLState.EXITING:
                return SLDecision(False, SLReject.ALREADY_EXITING, position_id=pid,
                                  stop_price=stop)
            if arm.state == SLState.TRIGGERED:
                return SLDecision(False, SLReject.ALREADY_TRIGGERED,
                                  position_id=pid, stop_price=stop)

        # 6. market price crossed the stop?
        if not _valid_price(ltp):
            return SLDecision(False, SLReject.MARKET_INVALID, position_id=pid,
                              strategy_id=pos_strategy, instrument=pos_instrument,
                              stop_price=stop)
        ltp = float(ltp)
        is_long = getattr(position, "is_long", False)
        crossed = (ltp <= stop) if is_long else (ltp >= stop)
        if not crossed:
            return SLDecision(False, SLReject.NOT_CROSSED, position_id=pid,
                              strategy_id=pos_strategy, instrument=pos_instrument,
                              quantity=qty, stop_price=stop,
                              side="LONG" if is_long else "SHORT")

        # INVARIANT 8 — never more than the currently open quantity.
        exit_qty = min(qty, int(getattr(position, "quantity", 0) or 0))
        if exit_qty <= 0:
            return SLDecision(False, SLReject.QUANTITY_INVALID, position_id=pid,
                              strategy_id=pos_strategy, instrument=pos_instrument,
                              stop_price=stop)
        return SLDecision(True, "SL_TRIGGERED", position_id=pid,
                          strategy_id=pos_strategy, instrument=pos_instrument,
                          quantity=exit_qty, stop_price=stop,
                          side="LONG" if is_long else "SHORT")

    # ── state transitions ───────────────────────────────────────────────

    def mark_triggered(self, position_id: str, ltp: Optional[float] = None) -> bool:
        """Latch: this position has fired. Further ticks cannot fire again."""
        with self._lock:
            arm = self._armed.get(str(position_id))
            if arm is None or arm.state != SLState.ARMED:
                return False
            arm.state = SLState.TRIGGERED
            if ltp is not None and _valid_price(ltp):
                arm.trigger_price = float(ltp)
            return True

    def mark_exiting(self, position_id: str, order_id: Optional[str] = None) -> bool:
        """Latch: an exit order exists for this position (INVARIANT 4/10)."""
        with self._lock:
            arm = self._armed.get(str(position_id))
            if arm is None or arm.state in (SLState.EXITING,) + _TERMINAL:
                return False
            arm.state = SLState.EXITING
            arm.exit_order_id = order_id
            return True

    def release_exit(self, position_id: str) -> None:
        """An exit attempt failed; re-arm so a later tick may retry.

        Both latched states are released, not just EXITING:

        * EXITING  — an exit order existed and died (rejected / cancelled /
          expired at the broker without closing the position).
        * TRIGGERED — the stop was crossed but the exit never reached the
          broker at all (crash, or the order could not be minted).

        Leaving either state latched would make the position permanently
        unprotectable: ``evaluate`` short-circuits to ALREADY_TRIGGERED /
        ALREADY_EXITING forever, and there is no broker-side stop backing it
        up.  Re-arming is safe because ``evaluate`` re-reads the position's
        own ``stop_price`` and the fresh LTP on every tick — a stop that has
        since been recovered above the market simply does not fire again.
        """
        with self._lock:
            arm = self._armed.get(str(position_id))
            if arm is not None and arm.state in (SLState.EXITING, SLState.TRIGGERED):
                arm.state = SLState.ARMED
                arm.exit_order_id = None

    # ── introspection / startup recovery ────────────────────────────────

    def state_of(self, position_id: str) -> SLState:
        with self._lock:
            arm = self._armed.get(str(position_id))
            return arm.state if arm is not None else SLState.NONE

    def armed_ids(self) -> list[str]:
        with self._lock:
            return sorted(self._armed.keys())

    def active_for(self, position_id: str) -> bool:
        with self._lock:
            arm = self._armed.get(str(position_id))
            return arm is not None and arm.state in (SLState.ARMED,
                                                     SLState.TRIGGERED,
                                                     SLState.EXITING)

    def snapshot(self, position_id: str) -> Optional[dict]:
        with self._lock:
            arm = self._armed.get(str(position_id))
            return arm.snapshot() if arm is not None else None

    def resync_from_broker(self, broker_positions: list,
                           local_positions: list,
                           stop_resolver=None) -> dict:
        """Startup / crash recovery — INVARIANT 9.

        The broker is the only authority for "is there an open position".
        Anything the local book holds that the broker does not confirm is
        dropped (so a stale DB row can never arm an SL); anything the broker
        confirms is armed from its OWN stop, optionally supplied by
        ``stop_resolver`` (which must never invent a level — returning None
        yields ``SL_UNAVAILABLE``).

        ``broker_positions`` rows must carry ``broker_confirmed`` — set by the
        caller after reconciling against the broker's actual net position.
        A row without it is treated as NOT confirmed and never arms.

        Returns a summary dict.
        """
        summary = {"armed": [], "unavailable": [], "dropped_local": [],
                   "broker_only": []}
        rows = list(broker_positions or [])
        consumed: set[int] = set()

        for lp in (local_positions or []):
            if not getattr(lp, "is_open", False):
                continue
            sid = str(getattr(lp, "strategy_id", "") or "")
            instrument = str(getattr(lp, "instrument", "") or "")
            match_idx = None
            for idx, p in enumerate(rows):
                if idx in consumed:
                    continue
                if str(p.get("strategy_id") or "") != sid:
                    continue
                if str(p.get("instrument") or "") != instrument:
                    continue
                if not p.get("broker_confirmed"):
                    continue
                match_idx = idx
                break
            if match_idx is None:
                # DB said OPEN, broker says FLAT (or unconfirmed) -> the
                # position cannot be trusted to exist, so it cannot arm an SL.
                self.close(getattr(lp, "position_id", ""))
                summary["dropped_local"].append(
                    {"position_id": getattr(lp, "position_id", None),
                     "strategy_id": sid, "instrument": instrument,
                     "reason": "DB_OPEN_BROKER_NOT_CONFIRMED"})
                continue

            consumed.add(match_idx)
            p = rows[match_idx]
            qty = int(p.get("quantity") or 0)
            stop = getattr(lp, "stop_price", None)
            if not _valid_price(stop) and stop_resolver is not None:
                try:
                    stop = stop_resolver(lp)
                except Exception as e:
                    log.warning("[SL] stop resolver failed for %s/%s: %s",
                                sid, instrument, e)
                    stop = None
            if not _valid_price(stop):
                lp.stop_price = None
                summary["unavailable"].append(
                    {"position_id": getattr(lp, "position_id", None),
                     "strategy_id": sid, "instrument": instrument,
                     "reason": SLReject.STOP_MISSING})
                continue
            # The broker quantity is authoritative for the SL size
            # (§12/INVARIANT 8).  A broker row carrying 0 here means "we know
            # the position exists but not its size" -> keep the local size.
            if qty > 0:
                try:
                    lp.quantity = qty
                except Exception:
                    pass
            # The resolved stop becomes the position's OWN stop, so
            # ``arm()`` and every later tick read the same single value.
            try:
                lp.stop_price = float(stop)
            except Exception:
                pass
            if self.arm(lp) == SLState.ARMED:
                summary["armed"].append(
                    {"position_id": lp.position_id, "strategy_id": sid,
                     "instrument": instrument, "quantity": int(lp.quantity),
                     "stop_price": float(stop)})

        for idx, p in enumerate(rows):
            if idx in consumed:
                continue
            if not str(p.get("strategy_id") or ""):
                summary["broker_only"].append(
                    {"strategy_id": p.get("strategy_id"),
                     "instrument": p.get("instrument"),
                     "quantity": int(p.get("quantity") or 0)})
        return summary
