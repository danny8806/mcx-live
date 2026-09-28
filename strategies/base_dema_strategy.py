"""Base DEMA-ATR strategy framework for Gold/Silver live trading."""
from __future__ import annotations

import math
import time
from typing import Any, Optional

from strategies.types import (
    SignalType, StrategyState, Signal, PendingEntry, StrategyInput,
    freeze_signal_context,
)
from strategies.intent import long_crossover, short_crossover, entry_levels
from core.timeframe_engine import Bar
from htf.confirmation import HTFMappedValue


class BaseDEMAStrategy:
    """Base DEMA-ATR crossover strategy.
    
    Implements:
    - Crossover signal detection
    - Immediate-limit entry
    - Stop loss management
    - Reversal logic
    
    Signal logic (from existing system):
    - LONG: close > htf_dema_atr AND previous_close <= previous_htf_dema_atr
    - SHORT: close < htf_dema_atr AND previous_close >= previous_htf_dema_atr
    """

    def __init__(
        self,
        strategy_id: str,
        instrument: str,
        fast_timeframe: str,
        htf_timeframe: str,
        quantity: int = 1,
        long_compare: str = ">",
        short_compare: str = "<",
        pending_timeout_bars: int = 50,
    ):
        self.strategy_id = strategy_id
        self.instrument = instrument
        self.fast_timeframe = fast_timeframe
        self.htf_timeframe = htf_timeframe
        self.quantity = quantity
        self.long_compare = long_compare
        self.short_compare = short_compare
        self.pending_timeout_bars = pending_timeout_bars
        self.enabled = True
        self.immediate_limit_sent: Optional[str] = None
        self.immediate_limit_sent: Optional[str] = None

        # State machine
        self.state = StrategyState.FLAT
        self.position_side: Optional[str] = None
        self.stop_price: Optional[float] = None
        self.pending_entry: Optional[PendingEntry] = None
        self.just_entered: bool = False
        self.last_exit_reason: Optional[str] = None

        # Same-bar stop: when a pending entry fills AND the SL breaks on the
        # SAME bar, the backtest exit fills at that bar's CLOSE (reference
        # goldm_dema_mtf_futures evaluates _check_sl_hit on the entry bar).
        self.same_bar_stop: Optional[float] = None

        # Stop-out re-fire guard (parity with StrategyInstance): a stop-loss
        # exit is emitted at most once per position; it is re-armed only when
        # the position actually closes (engine clears it in
        # _reset_strategy_state).  Prevents the infinite exit re-fire loop.
        self.stop_exit_submitted: bool = False

        # Indicator tracking
        self._prev_fast_close: Optional[float] = None
        self._prev_htf_value: Optional[float] = None
        self._prev_mid_value: Optional[float] = None
        self._prev_fast_high: Optional[float] = None
        self._prev_fast_low: Optional[float] = None
        self._bars_processed: int = 0

        # Audit trail
        self._signals: list[Signal] = []
        self._events: list[dict] = []

    @staticmethod
    def _parse_tf_seconds(tf: str) -> int:
        """Parse a timeframe string ("5m", "15m", "1h") to seconds."""
        if not tf:
            return 300
        unit = tf[-1].lower()
        try:
            n = int(tf[:-1])
        except ValueError:
            return 300
        if unit == "h":
            return n * 3600
        if unit == "m":
            return n * 60
        return 300

    def on_bar(
        self,
        bar: Bar,
        htf_mapped: HTFMappedValue,
        fast_dema_atr: float,
        mid_mapped: Optional[HTFMappedValue] = None,
    ) -> Optional[Signal]:
        """Process a closed fast timeframe bar.
        
        Args:
            bar: Closed fast timeframe bar
            htf_mapped: Mapped HTF DEMA-ATR value (1H signal line)
            fast_dema_atr: Current fast DEMA-ATR value
            mid_mapped: Mapped mid TF DEMA-ATR value (15m confirmation line)
            
        Returns:
            Signal if trade decision made, None otherwise
        """
        if not self.enabled:
            return None
        self._bars_processed += 1

        # Clear just_entered flag from previous tick-triggered entry
        self.just_entered = False

        # Extract values
        close = bar.close
        high = bar.high
        low = bar.low
        prev_close = self._prev_fast_close or close
        prev_high = self._prev_fast_high or high
        prev_low = self._prev_fast_low or low
        htf_val = htf_mapped.htf_value
        prev_htf_val = self._prev_htf_value
        # 15m confirmation line
        mid_val = mid_mapped.htf_value if mid_mapped else None
        prev_mid_val = self._prev_mid_value

        # Store for next bar
        self._prev_fast_close = close
        self._prev_htf_value = htf_val
        self._prev_mid_value = mid_val
        self._prev_fast_high = high
        self._prev_fast_low = low

        self.prev_htf = prev_htf_val
        self.prev_mid = prev_mid_val
        # Skip if no HTF value available
        if htf_val is None or prev_htf_val is None:
            return None

        signal = None

        # 1. Execute pending entry if triggered (exclusive - return immediately)
        if self.pending_entry is not None:
            # Check timeout: expire pending entries that haven't triggered
            if self.pending_entry.bars_pending >= self.pending_timeout_bars:
                self._emit("PENDING_ENTRY_EXPIRED",
                           side=self.pending_entry.side,
                           bars_pending=self.pending_entry.bars_pending)
                self.pending_entry = None
                self.state = StrategyState.FLAT
                self.position_side = None
                return None
            self.pending_entry.bars_pending += 1
            # position_side is None until the broker confirms the fill, so the
            # same-bar stop breach must be evaluated against the PENDING side.
            pending_side = self.pending_entry.side
            signal = self._check_pending_entry(bar)
            if signal is not None:
                self.just_entered = False
                # Reference flow (_execute_pending -> _check_sl_hit) also
                # evaluates the stop ON the entry bar; a same-bar break exits
                # at this bar's close via a second engine-processed signal.
                if self.stop_price is not None:
                    if (pending_side == "LONG" and bar.low <= self.stop_price) or (
                            pending_side == "SHORT" and bar.high >= self.stop_price):
                        self.same_bar_stop = bar.close
                        self.last_exit_reason = "stop_loss_hit"
                        # Reference flow evaluates the signal on the entry bar
                        # AFTER the same-bar stop exit (position now flat) →
                        # any same-bar cross re-arms a pending entry.
                        self._detect_signal(
                            close, prev_close, htf_val, prev_htf_val, high, low,
                            bar.start_ts, mid_val, prev_mid_val, prev_high, prev_low,
                            fast_dema_atr)
                return signal

        # 2. Check stop loss (skip if just entered, stop exits don't generate new signals)
        if (self.position_side is not None
                and self.stop_price is not None
                and not self.just_entered
                and not self.stop_exit_submitted):
            stop_signal = self._check_stop_loss(bar)
            if stop_signal is not None:
                self.just_entered = False
                # Reference flow (goldm_dema_mtf_futures.next) runs
                # _check_signals_for_next_bar AFTER a stop exit on the SAME
                # bar — the position is flat by then, so a same-bar cross
                # re-arms a pending entry that fills on a later bar.
                self._detect_signal(
                    close, prev_close, htf_val, prev_htf_val, high, low,
                    bar.start_ts, mid_val, prev_mid_val, prev_high, prev_low,
                    fast_dema_atr)
                return stop_signal

        # 2.5. OPPOSITE CROSSOVER OVERRIDE: cancel old pending/in-flight and
        # take the new signal on the same bar.
        if (self.position_side is None
                and self.state in (StrategyState.ENTRY_TRIGGERED,
                                   StrategyState.PENDING_LONG,
                                   StrategyState.PENDING_SHORT)):
            cancel_and_reenter = None
            if self._check_long_cross(close, prev_close, htf_val, prev_htf_val,
                                      mid_val, prev_mid_val):
                cancel_and_reenter = "LONG"
            elif self._check_short_cross(close, prev_close, htf_val, prev_htf_val,
                                         mid_val, prev_mid_val):
                cancel_and_reenter = "SHORT"
            if cancel_and_reenter is not None:
                old_pending_id = getattr(self, "_last_armed_pending_id", None)
                self.state = StrategyState.FLAT
                self.pending_entry = None
                if hasattr(self, "immediate_limit_sent"):
                    self.immediate_limit_sent = None
                if hasattr(self, "current_trade_id"):
                    self.current_trade_id = None
                if hasattr(self, "_last_armed_pending_id"):
                    self._last_armed_pending_id = None
                signal = self._detect_signal(
                    close, prev_close, htf_val, prev_htf_val, high, low, bar.start_ts,
                    mid_val, prev_mid_val, prev_high, prev_low, fast_dema_atr,
                )
                if signal is not None:
                    signal.metadata = signal.metadata or {}
                    signal.metadata["cancel_inflight"] = True
                    if old_pending_id:
                        signal.metadata["old_pending_id"] = old_pending_id
                    return signal
                if (self.pending_entry is not None
                        and self.pending_entry.signal is not None):
                    self.pending_entry.signal.metadata = self.pending_entry.signal.metadata or {}
                    self.pending_entry.signal.metadata["cancel_inflight"] = True
                    if old_pending_id:
                        self.pending_entry.signal.metadata["old_pending_id"] = old_pending_id
                    return self.pending_entry.signal
                return None

        # 3. Detect new signals.
        #    Reference flow (goldm_dema_mtf_futures._check_signals_for_next_bar)
        #    re-arms a pending on EVERY signal bar while flat: a newer cross
        #    replaces any still-unfilled pending instead of being blocked by it.
        #    So detection also runs in PENDING_* states (no position held).
        if self.state in (StrategyState.FLAT,
                          StrategyState.PENDING_LONG, StrategyState.PENDING_SHORT):
            signal = self._detect_signal(
                close, prev_close, htf_val, prev_htf_val, high, low, bar.start_ts,
                mid_val, prev_mid_val, prev_high, prev_low, fast_dema_atr,
            )
        elif self.position_side == "SHORT" and self._check_long_cross(close, prev_close, htf_val, prev_htf_val, mid_val, prev_mid_val):
            signal = self._create_reversal_signal("LONG", close, high, low, bar.start_ts, prev_high, prev_low,
                                                  htf_val=htf_val, mid_val=mid_val, fast_dema_atr=fast_dema_atr)
        elif self.position_side == "LONG" and self._check_short_cross(close, prev_close, htf_val, prev_htf_val, mid_val, prev_mid_val):
            signal = self._create_reversal_signal("SHORT", close, high, low, bar.start_ts, prev_high, prev_low,
                                                  htf_val=htf_val, mid_val=mid_val, fast_dema_atr=fast_dema_atr)

        self.just_entered = False
        return signal

    def _check_long_cross(
        self, close: float, prev_close: float,
        htf_val: float, prev_htf_val: float,
        mid_val: Optional[float] = None, prev_mid_val: Optional[float] = None,
    ) -> bool:
        """Check for long crossover signal.

        Buy = close crosses ABOVE 1H line AND 15m line is not strongly above 1H line.
        Tolerance: 15m can be up to 1.5% above 1H (accounts for DEMA drift between TFs
        caused by different data ranges in live vs backtest).
        """
        return long_crossover(close, prev_close, htf_val, mid_val)

    def _check_short_cross(
        self, close: float, prev_close: float,
        htf_val: float, prev_htf_val: float,
        mid_val: Optional[float] = None, prev_mid_val: Optional[float] = None,
    ) -> bool:
        """Check for short crossover signal.

        Sell = close crosses BELOW 1H line AND 15m line is above 1H line.
        Strict filter: 15m MUST be above 1H for SHORT confirmation.
        (15m below 1H = bullish trend, contradicting a SHORT signal)
        """
        return short_crossover(close, prev_close, htf_val, mid_val)

    def _detect_signal(
        self,
        close: float,
        prev_close: float,
        htf_val: float,
        prev_htf_val: float,
        high: float,
        low: float,
        timestamp: float,
        mid_val: Optional[float] = None,
        prev_mid_val: Optional[float] = None,
        prev_high: Optional[float] = None,
        prev_low: Optional[float] = None,
        fast_dema_atr: Optional[float] = None,
    ) -> Optional[Signal]:
        """Detect a crossover and emit the single immediate-limit entry."""
        if self.immediate_limit_sent is not None:
            return None
        if self._check_long_cross(close, prev_close, htf_val, prev_htf_val, mid_val, prev_mid_val):
            return self._create_entry_signal(
                "LONG", close, high, low, timestamp, prev_high, prev_low,
                htf_val=htf_val, mid_val=mid_val,
                fast_dema_atr=fast_dema_atr)
        elif self._check_short_cross(close, prev_close, htf_val, prev_htf_val, mid_val, prev_mid_val):
            return self._create_entry_signal(
                "SHORT", close, high, low, timestamp, prev_high, prev_low,
                htf_val=htf_val, mid_val=mid_val,
                fast_dema_atr=fast_dema_atr)
        return None

    def _create_entry_signal(
        self, side: str, close: float, high: float, low: float, timestamp: float,
        prev_high: Optional[float] = None, prev_low: Optional[float] = None,
        htf_val: Optional[float] = None, mid_val: Optional[float] = None,
        fast_dema_atr: Optional[float] = None,
    ) -> Signal:
        """Create the immediate-limit intent at the signal candle's level."""
        trigger, stop = entry_levels(side, high, low, prev_high, prev_low)

        signal = Signal(
            signal_type=SignalType.LONG if side == "LONG" else SignalType.SHORT,
            instrument=self.instrument,
            strategy_id=self.strategy_id,
            timestamp=timestamp,
            trigger_price=trigger,
            stop_price=stop,
            quantity=self.quantity,
            side=side,
            metadata={
                "pending": False,
                "triggered": True,
                "immediate_limit": True,
                "entry_price": close,
                "trigger_level": trigger,
                "signal_candle_start": timestamp,
                "signal_candle_high": high,
                "signal_candle_low": low,
                "signal_candle_close": close,
                "signal_htf_dema_atr": htf_val,
                "signal_mid_dema_atr": mid_val,
                "signal_fast_dema_atr": fast_dema_atr,
            },
        )

        freeze_signal_context(
            signal, close=close, high=high, low=low, timestamp=timestamp,
            dema=fast_dema_atr, position_side=self.position_side,
            position_stop=self.stop_price,
        )
        self.stop_price = stop
        self.state = StrategyState.ENTRY_TRIGGERED
        self.pending_entry = None
        self.immediate_limit_sent = side

        self._emit("ENTRY_ORDER_CREATED", side=side, trigger=trigger, stop=stop)
        return signal


    def _create_reversal_signal(
        self, side: str, close: float, high: float, low: float, timestamp: float,
        prev_high: Optional[float] = None, prev_low: Optional[float] = None,
        htf_val: Optional[float] = None, mid_val: Optional[float] = None,
        fast_dema_atr: Optional[float] = None,
    ) -> Signal:
        """Create a reversal: return an exit signal AND an entry signal.
        Both are returned immediately so the engine submits BOTH orders
        to Dhan in the same processing cycle.  The entry is no longer
        armed as a PendingEntry waiting for a future bar breakout.

        The entry trigger is offset from the exit trigger by
        ``reversal_entry_gap_points`` so the old position exits FIRST
        and the new opposite position enters SECOND:
          LONG → SHORT: entry trigger = signal LOW - gap
          SHORT → LONG: entry trigger = signal HIGH + gap
        """
        gap = int(getattr(self, "reversal_entry_gap_points", 0))
        if side == "LONG":
            trigger = high
            sl_low = prev_low if prev_low is not None else low
            stop = min(low, sl_low)
            entry_trigger = trigger + gap
        else:
            trigger = low
            sl_high = prev_high if prev_high is not None else high
            stop = max(high, sl_high)
            entry_trigger = trigger - gap

        # Entry signal — submitted immediately to Dhan.
        entry_signal = Signal(
            signal_type=SignalType.LONG if side == "LONG" else SignalType.SHORT,
            instrument=self.instrument,
            strategy_id=self.strategy_id,
            timestamp=timestamp,
            trigger_price=entry_trigger,
            stop_price=stop,
            quantity=self.quantity,
            side=side,
        )
        entry_signal.metadata = {
            "entry_price": close,
            "htf_value": htf_val,
            "mid_value": mid_val,
            "fast_dema_atr": fast_dema_atr,
            "trigger_level": entry_trigger,
            "reversal_exit_trigger": trigger,
            "reversal_entry_gap_points": gap,
            "is_reversal": True,
            "is_reversal_entry": True,
            "signal_candle_start": timestamp,
            "signal_candle_open": None,
            "signal_candle_high": high,
            "signal_candle_low": low,
            "signal_candle_close": close,
            "signal_htf_dema_atr": htf_val,
            "signal_mid_dema_atr": mid_val,
            "signal_fast_dema_atr": fast_dema_atr,
            "signal_side": side,
        }

        freeze_signal_context(
            entry_signal, close=close, high=high, low=low, timestamp=timestamp,
            open_=None, dema=fast_dema_atr, atr=None,
            htf_value=htf_val, mid_value=mid_val,
            position_side=self.position_side, position_stop=self.stop_price,
        )

        # Exit signal — carries the entry signal in metadata so the engine
        # can submit BOTH orders in the same processing cycle.
        reason = f"{side.lower()}_reversal"
        exit_signal = Signal(
            signal_type=SignalType.SHORT if self.position_side == "LONG" else SignalType.LONG,
            instrument=self.instrument,
            strategy_id=self.strategy_id,
            timestamp=timestamp,
            trigger_price=trigger,
            stop_price=self.stop_price,
            quantity=self.quantity,
        )
        exit_signal.signal_id = entry_signal.signal_id
        exit_signal.metadata = {
            "exit": True,
            "exit_reason": reason,
            "exit_price": close,
            "is_reversal": True,
            "is_reversal_entry": False,
            "reversal_exit_trigger": trigger,
            "reversal_entry_signal": entry_signal,
            "position_side": self.position_side,
        }

        freeze_signal_context(
            exit_signal, close=close, high=high, low=low, timestamp=timestamp,
            open_=None, dema=fast_dema_atr, atr=None,
            htf_value=htf_val, mid_value=mid_val,
            position_side=self.position_side, position_stop=self.stop_price,
        )

        self._close_position(reason, close, timestamp)

        self._emit("REVERSAL_SIGNAL", side=side, trigger=trigger, stop=stop)
        return exit_signal

    def _check_pending_entry(self, bar: Bar) -> Optional[Signal]:
        """Check if pending entry is triggered by bar."""
        if self.pending_entry is None:
            return None

        pen = self.pending_entry
        triggered = False

        if getattr(pen, "immediate", False):
            # Reversal entry whose old position was already confirmed flat.
            triggered = True
        elif pen.side == "LONG" and bar.high > pen.trigger_price:
            triggered = True
        elif pen.side == "SHORT" and bar.low < pen.trigger_price:
            triggered = True

        if triggered:
            # Once the reversal trigger crosses, emit the same immediate-limit
            # order intent used by ordinary entries.
            fill_px = pen.trigger_price
            base_md = dict(pen.signal.metadata or {})
            entry_md = dict(base_md)
            entry_md.update({
                "pending": False, "triggered": True, "immediate_limit": True,
                "entry_price": fill_px, "trigger_level": fill_px,
                "source": "reversal_trigger",
                "placement_candle_start": bar.start_ts,
            })
            signal = Signal(
                signal_type=SignalType.LONG if pen.side == "LONG" else SignalType.SHORT,
                instrument=self.instrument,
                strategy_id=self.strategy_id,
                timestamp=bar.start_ts,
                trigger_price=fill_px,
                stop_price=pen.signal.stop_price,
                quantity=self.quantity,
                side=pen.side,
                metadata=entry_md,
            )
            # Dhan-linked: state = ENTRY_TRIGGERED, NOT LONG/SHORT_POSITION.
            # position_side is NOT set here — only set by the engine on Dhan
            # fill confirmation.  On rejection the poller resets to FLAT.
            self.stop_price = pen.signal.stop_price
            self.just_entered = True
            self.state = StrategyState.ENTRY_TRIGGERED
            self.pending_entry = None

            self.immediate_limit_sent = pen.side
            self._emit("ENTRY_ORDER_CREATED", side=pen.side,
                       trigger=fill_px, stop=self.stop_price)
            return signal

        return None

    def _check_stop_loss(self, bar: Bar) -> Optional[Signal]:
        """Check if stop loss is hit. Returns exit Signal if stopped out, else None.

        Exit fills at the BAR CLOSE (backtest model: SL exits are evaluated on
        the bar that breaks the stop, and the exit fills at that bar's close).
        """
        # A stop-out is already in flight: never re-emit a duplicate exit.
        if getattr(self, "stop_exit_submitted", False):
            return None
        if self.position_side == "LONG" and bar.low <= self.stop_price:
            exit_signal = self._create_exit_signal("stop_loss_hit", bar.close, bar.start_ts)
            self._close_position("stop_loss_hit", bar.close, bar.start_ts)
            self.stop_exit_submitted = True
            return exit_signal
        elif self.position_side == "SHORT" and bar.high >= self.stop_price:
            exit_signal = self._create_exit_signal("stop_loss_hit", bar.close, bar.start_ts)
            self._close_position("stop_loss_hit", bar.close, bar.start_ts)
            self.stop_exit_submitted = True
            return exit_signal
        return None

    def _consume_same_bar_stop(self, bar: Bar) -> Optional[Signal]:
        """Build the exit Signal for a stop broken ON the entry bar (fills at
        the entry bar's close).  The engine calls this AFTER it booked the
        entry fill, so the position exists when the stop exits.

        Mirrors the reference backtest: entry at the trigger level and a
        stop-out at the same candle's close book as a same-bar round-trip.
        """
        if self.same_bar_stop is None or self.position_side is None:
            self.same_bar_stop = None
            return None
        px = self.same_bar_stop
        self.same_bar_stop = None
        side = SignalType.SHORT if self.position_side == "LONG" else SignalType.LONG
        return Signal(
            signal_type=side,
            instrument=self.instrument,
            strategy_id=self.strategy_id,
            timestamp=(bar.start_ts or 0.0) + 0.25,
            trigger_price=px,
            stop_price=0.0,
            quantity=self.quantity,
            metadata={"exit": True, "exit_reason": "stop_loss_hit",
                      "source": "same_bar_stop", "fill_price": px},
        )

    def _create_exit_signal(self, reason: str, exit_price: float, timestamp: float) -> Signal:
        """Create an exit signal for stop-loss or other exits."""
        signal_type = SignalType.SHORT if self.position_side == "LONG" else SignalType.LONG
        return Signal(
            signal_type=signal_type,
            instrument=self.instrument,
            strategy_id=self.strategy_id,
            timestamp=timestamp,
            trigger_price=exit_price,
            stop_price=0.0,
            quantity=self.quantity,
            metadata={"exit_reason": reason, "exit": True, "source": "stop_loss",
                      "fill_price": exit_price},
        )

    def _close_position(self, reason: str, exit_price: float, timestamp: float) -> None:
        """Mark an exit as pending; the engine clears state after its fill.

        Clearing state here used to orphan a live position whenever execution
        was rejected or market-data/safe-mode gating blocked the exit.
        """
        self.last_exit_reason = reason
        self._emit("POSITION_CLOSED", reason=reason, exit_price=exit_price)
        self.state = StrategyState.EXIT_ORDER_SUBMITTED

    def on_tick(self, ltp: float, timestamp: float) -> Optional[Signal]:
        """Process real-time tick for pending trigger check and stop loss monitoring.
        
        Used for live pending trigger monitoring instead of waiting
        for next bar close. Also checks stop loss on every tick.
        """
        if not self.enabled or self.just_entered:
            return None
        # Guard against non-positive / non-finite LTP (e.g. Dhan `-1` no-data
        # sentinel). A bad price must NEVER trigger a stop-loss exit or a
        # pending-entry fill -- ignore the tick entirely.
        if not (ltp is not None) or math.isnan(ltp) or math.isinf(ltp) or ltp <= 0.0:
            return None

        if self.position_side is not None and self.stop_price is not None:
            # Defensive read: the guard lives on real strategies (init/snapshot)
            # and may be absent on minimal test doubles.
            if getattr(self, "stop_exit_submitted", False):
                return None
            if self.position_side == "LONG" and ltp <= self.stop_price:
                exit_signal = self._create_exit_signal("stop_loss_hit", ltp, timestamp)
                self._close_position("stop_loss_hit", ltp, timestamp)
                self.stop_exit_submitted = True
                return exit_signal
            elif self.position_side == "SHORT" and ltp >= self.stop_price:
                exit_signal = self._create_exit_signal("stop_loss_hit", ltp, timestamp)
                self._close_position("stop_loss_hit", ltp, timestamp)
                self.stop_exit_submitted = True
                return exit_signal

        if self.pending_entry is None:
            return None

        pen = self.pending_entry
        triggered = False

        if pen.side == "LONG" and ltp >= pen.trigger_price:
            triggered = True
        elif pen.side == "SHORT" and ltp <= pen.trigger_price:
            triggered = True

        if triggered:
            fill_px = pen.trigger_price
            base_md = dict(pen.signal.metadata or {})
            entry_md = dict(base_md)
            entry_md.update({
                "entry_price": fill_px, "fill_price": fill_px, "executed": True, "source": "tick",
                "placement_candle_start": timestamp,
            })
            signal = Signal(
                signal_type=SignalType.LONG if pen.side == "LONG" else SignalType.SHORT,
                instrument=self.instrument,
                strategy_id=self.strategy_id,
                timestamp=timestamp,
                trigger_price=fill_px,
                stop_price=pen.signal.stop_price,
                quantity=self.quantity,
                metadata=entry_md,
            )
            # Dhan-linked: state = ENTRY_TRIGGERED, NOT LONG/SHORT_POSITION.
            # position_side is NOT set here — only set by the engine on Dhan
            # fill confirmation.  On rejection the poller resets to FLAT.
            self.stop_price = pen.signal.stop_price
            self.just_entered = True
            self.state = StrategyState.ENTRY_TRIGGERED
            self.pending_entry = None

            self._emit("ENTRY_TRIGGERED", side=pen.side, price=ltp, stop=self.stop_price)
            return signal

        return None

    def _emit(self, event_type: str, **kwargs) -> None:
        """Emit event for audit trail."""
        self._events.append({
            "event_type": event_type,
            "strategy_id": self.strategy_id,
            **kwargs,
        })
        if len(self._events) > 1000:
            self._events = self._events[-500:]

    @property
    def is_flat(self) -> bool:
        return self.position_side is None

    @property
    def has_position(self) -> bool:
        return self.position_side is not None

    def snapshot(self) -> dict:
        """Get strategy state for persistence."""
        return {
            "strategy_id": self.strategy_id,
            "instrument": self.instrument,
            "state": self.state.value,
            "position_side": self.position_side,
            "stop_price": self.stop_price,
            "bars_processed": self._bars_processed,
            "enabled": self.enabled,
            "pending_entry": {
                "side": self.pending_entry.side,
                "trigger_price": self.pending_entry.trigger_price,
                "stop_price": self.pending_entry.signal.stop_price if self.pending_entry.signal else 0,
                "bars_pending": self.pending_entry.bars_pending,
                "immediate": getattr(self.pending_entry, "immediate", False),
                "created_at": getattr(self.pending_entry, "created_at", 0),
                "instrument": self.pending_entry.signal.instrument if self.pending_entry.signal else self.instrument,
                "strategy_id": self.pending_entry.signal.strategy_id if self.pending_entry.signal else self.strategy_id,
                "quantity": self.pending_entry.signal.quantity if self.pending_entry.signal else self.quantity,
                "signal_candle_start": (self.pending_entry.signal.metadata or {}).get("signal_candle_start"),
                "signal_candle_open": (self.pending_entry.signal.metadata or {}).get("signal_candle_open"),
                "signal_candle_high": (self.pending_entry.signal.metadata or {}).get("signal_candle_high"),
                "signal_candle_low": (self.pending_entry.signal.metadata or {}).get("signal_candle_low"),
                "signal_candle_close": (self.pending_entry.signal.metadata or {}).get("signal_candle_close"),
                "signal_htf_dema_atr": (self.pending_entry.signal.metadata or {}).get("signal_htf_dema_atr"),
                "signal_mid_dema_atr": (self.pending_entry.signal.metadata or {}).get("signal_mid_dema_atr"),
                "signal_fast_dema_atr": (self.pending_entry.signal.metadata or {}).get("signal_fast_dema_atr"),
            } if self.pending_entry else None,
            "last_exit_reason": self.last_exit_reason,
            "stop_exit_submitted": self.stop_exit_submitted,
            "prev_fast_close": self._prev_fast_close,
            "prev_fast_high": self._prev_fast_high,
            "prev_fast_low": self._prev_fast_low,
            "prev_htf_value": self._prev_htf_value,
            "prev_mid_value": self._prev_mid_value,
        }

    def restore(self, data: dict) -> None:
        """Restore strategy state from persistence."""
        self.state = StrategyState(data.get("state", "flat"))
        self.position_side = data.get("position_side")
        self.stop_price = data.get("stop_price")
        self._bars_processed = data.get("bars_processed", 0)
        self.enabled = data.get("enabled", True)
        self._prev_fast_close = data.get("prev_fast_close")
        self._prev_fast_high = data.get("prev_fast_high")
        self._prev_fast_low = data.get("prev_fast_low")
        self._prev_htf_value = data.get("prev_htf_value")
        self._prev_mid_value = data.get("prev_mid_value")
        self.last_exit_reason = data.get("last_exit_reason")
        self.stop_exit_submitted = bool(data.get("stop_exit_submitted", False))
        if data.get("pending_entry"):
            pe = data["pending_entry"]
            sig_metadata = {
                "signal_candle_start": pe.get("signal_candle_start"),
                "signal_candle_open": pe.get("signal_candle_open"),
                "signal_candle_high": pe.get("signal_candle_high"),
                "signal_candle_low": pe.get("signal_candle_low"),
                "signal_candle_close": pe.get("signal_candle_close"),
                "signal_htf_dema_atr": pe.get("signal_htf_dema_atr"),
                "signal_mid_dema_atr": pe.get("signal_mid_dema_atr"),
                "signal_fast_dema_atr": pe.get("signal_fast_dema_atr"),
                "signal_side": pe.get("side"),
            }
            self.pending_entry = PendingEntry(
                signal=Signal(
                    signal_type=SignalType.LONG if pe["side"] == "LONG" else SignalType.SHORT,
                    instrument=self.instrument,
                    strategy_id=self.strategy_id,
                    timestamp=0,
                    trigger_price=pe["trigger_price"],
                    stop_price=pe.get("stop_price", 0),
                    quantity=self.quantity,
                    metadata=sig_metadata,
                ),
                trigger_price=pe["trigger_price"],
                side=pe["side"],
                created_at=pe.get("created_at", time.time()),
                bars_pending=pe.get("bars_pending", 0),
                immediate=pe.get("immediate", False),
            )
