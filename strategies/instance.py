"""StrategyInstance — one completely independent strategy with own indicators, state, and HTF tracking.

Each of the four strategies (GOLDM_5M, GOLDM_15M, SILVERM_5M, SILVERM_15M)
gets its own StrategyInstance. No shared mutable state between instances.
"""
from __future__ import annotations

import logging
import time
import uuid
from typing import Optional

from events.types import CandleEvent
from core.timeframe_engine import Bar
from htf.confirmation import HTFMappedValue
from indicators.dema_atr import DEMAATR
from strategies.htf_state import HTFState
from strategies.types import (
    SignalType, StrategyState, Signal, PendingEntry, freeze_signal_context,
)
from strategies.intent import long_crossover, short_crossover, entry_levels, reversal_levels
from indicators.shared import (
    IndicatorStream, StrategyIndicatorView, StreamHTFStateView,
)

log = logging.getLogger(__name__)

# Parameters — must match backtest exactly
DEMA_PERIOD = 3
ATR_PERIOD = 6
ATR_FACTOR = 1.0


class StrategyInstance:
    """One completely independent strategy instance.

    Owns:
        - strategy_id, instrument, security_id
        - fast_indicator (DEMAATR for primary timeframe)
        - mid_indicator (DEMAATR for 15m confirmation)
        - slow_indicator (DEMAATR for 1H signal line)
        - mid_htf_state (HTFState for 15m)
        - slow_htf_state (HTFState for 1H)
        - all strategy state (FLAT/LONG/SHORT, stop, pending, etc.)
        - previous values (prev_close, prev_htf, prev_mid)

    Does NOT own:
        - CandleFetcher
        - EventBus
        - ExecutionEngine
        - PositionManager
        - TradeLifecycleManager
    """

    def __init__(
        self,
        strategy_id: str,
        instrument: str,
        security_id: str,
        fast_timeframe: str,
        mid_timeframe: str = "15m",
        htf_timeframe: str = "1h",
        quantity: int = 1,
        pending_timeout_bars: int = 50,
        capital: float = 300_000.0,
        multiplier: float = 10.0,
    ):
        self.strategy_id = strategy_id
        self.execution_model = "local_trigger_limit"
        # Each strategy owns its armed entry and reversal-exit trigger.
        # Trigger identity is carried by the immutable signal and generation.
        self.pending_exit_trigger: Optional[PendingEntry] = None
        self._trigger_generation = 0
        self._last_fired_trigger_signal_id: Optional[str] = None
        # Track the signal_id of the last LIVE armed pending order so the
        # engine can terminalize it on cancel_inflight (opposite crossover).
        self._last_armed_pending_id: Optional[str] = None
        self.instrument = instrument
        self.security_id = security_id
        self.fast_timeframe = fast_timeframe
        self.mid_timeframe = mid_timeframe
        self.htf_timeframe = htf_timeframe
        self.quantity = quantity
        self.pending_timeout_bars = pending_timeout_bars
        self.capital = capital
        self.multiplier = multiplier

        # ── Subscriptions ──
        self.subscriptions = list(dict.fromkeys([
            f"{instrument}:{fast_timeframe}",
            f"{instrument}:{mid_timeframe}",
            f"{instrument}:{htf_timeframe}",
        ]))

        # ── Own indicators (per-strategy, NOT shared) ──
        self.fast_indicator = DEMAATR(DEMA_PERIOD, ATR_PERIOD, ATR_FACTOR)
        self.mid_indicator = DEMAATR(DEMA_PERIOD, ATR_PERIOD, ATR_FACTOR)
        self.slow_indicator = DEMAATR(DEMA_PERIOD, ATR_PERIOD, ATR_FACTOR)

        # ── Own HTF state (per-strategy, NOT shared) ──
        self.mid_htf_state = HTFState(instrument, mid_timeframe, DEMA_PERIOD, ATR_PERIOD, ATR_FACTOR)
        self.slow_htf_state = HTFState(instrument, htf_timeframe, DEMA_PERIOD, ATR_PERIOD, ATR_FACTOR)

        # ── Strategy state machine ──
        self.state = StrategyState.FLAT
        self.position_side: Optional[str] = None
        self.current_position_id: Optional[str] = None
        self.position_generation: Optional[int] = None
        self.position_quantity: Optional[int] = None
        self.stop_price: Optional[float] = None
        self.pending_entry: Optional[PendingEntry] = None
        self.just_entered: bool = False
        self.last_exit_reason: Optional[str] = None
        self.enabled: bool = True

        # ── Stop-out re-fire guard ──
        # Once a stop-loss exit is raised, no further stop-out is emitted
        # until the position actually closes (engine resets this in
        # _reset_strategy_state on the exit fill).  Prevents the infinite
        # stop re-fire loop (thousands of phantom SELL exit signals) when an
        # exit cannot complete.
        self.stop_exit_submitted: bool = False

        # ── Indicator tracking (previous values) ──
        self._prev_fast_close: Optional[float] = None
        self._prev_htf_value: Optional[float] = None
        self._prev_mid_value: Optional[float] = None
        self._prev_fast_high: Optional[float] = None
        self._prev_fast_low: Optional[float] = None
        self._bars_processed: int = 0

        # ── Current trade reference ──
        self.current_trade_id: Optional[str] = None

        # ── Shared indicator binding (set by bind_shared_indicators) ──
        self._shared_indicators_bound: bool = False
        self._shared_streams: dict = {}

        # ── Audit trail ──
        self._signals: list[Signal] = []
        self._events: list[dict] = []

        log.info("[StrategyInstance] %s initialized: %s %s/%s/%s",
                 strategy_id, instrument, fast_timeframe, mid_timeframe, htf_timeframe)

    # ═══════════════════════════════════════════════════════════════════════
    # SHARED INDICATOR BINDING (mission §7–§12)
    # ═══════════════════════════════════════════════════════════════════════

    def bind_shared_indicators(self, engine) -> None:
        """Bind this strategy's indicator slots to shared IndicatorStreams.

        Minimal-bind: replaces the strategy's self-owned DEMAATR / HTFState
        objects with thin views over the SharedNativeIndicatorEngine's streams,
        keyed by (security_id, timeframe). The strategy evaluation hot path
        (on_bar) is NOT changed — it keeps calling .update / .get_mapped_value
        / .value exactly as before, only the underlying storage is now shared
        so each (security_id, timeframe) DEMA-ATR is calculated once.

        The previous fast/mid/slow splitting on a single stream collapses to
        one stream per timeframe: fast == mid for anything whose fast timeframe
        equals a shared 15m stream, and the slow 1H stream serves the 1H line.
        """
        mid = engine.get_or_create(self.security_id, self.mid_timeframe)
        slow = engine.get_or_create(self.security_id, self.htf_timeframe)

        # fast indicator stream: the strategy's primary timeframe. For a 5m
        # strategy this is its own 5m stream; for a 15m strategy it is the same
        # 15m stream it shares with the 5m strategy's mid timeframe.
        fast = engine.get_or_create(self.security_id, self.fast_timeframe)

        self.fast_indicator = StrategyIndicatorView(fast)
        self.mid_indicator = StrategyIndicatorView(mid)
        self.slow_indicator = StrategyIndicatorView(slow)
        self.mid_htf_state = StreamHTFStateView(mid)
        self.slow_htf_state = StreamHTFStateView(slow)

        self._shared_streams = {
            "fast": fast,
            "mid": mid,
            "slow": slow,
        }
        self._shared_indicators_bound = True

    # ═══════════════════════════════════════════════════════════════════════
    # EVENT HANDLERS — called by EventBus routing
    # ═══════════════════════════════════════════════════════════════════════

    def on_candle(self, event: CandleEvent) -> Optional[Signal]:
        """Route incoming candle to correct handler based on timeframe.

        This is the primary entry point for candle events.
        """
        if event.timeframe == self.fast_timeframe:
            return self._on_fast_candle(event)
        elif event.timeframe == self.mid_timeframe:
            self._on_mid_htf_candle(event)
            return None
        elif event.timeframe == self.htf_timeframe:
            self._on_slow_htf_candle(event)
            return None
        return None

    def _on_fast_candle(self, event: CandleEvent) -> Optional[Signal]:
        """Process fast candle: update indicator, map HTF, check signals.

        This is the hot path — must be minimal.
        """
        bar = Bar(
            instrument=event.instrument,
            timeframe=event.timeframe,
            start_ts=event.start_ts,
            end_ts=event.end_ts,
            open=event.open,
            high=event.high,
            low=event.low,
            close=event.close,
            volume=int(event.volume),
        )

        # 1. Update own fast indicator (idempotent by candle_end_ts)
        self.fast_indicator.update(bar.open, bar.high, bar.low, bar.close, bar.end_ts)
        fast_dema_atr = self.fast_indicator.value

        # 1b. For strategies where fast == mid, the fast bar IS the mid bar —
        # keep the mid HTF state live so 15m confirmation never goes stale.
        if self.fast_timeframe == self.mid_timeframe:
            self.mid_htf_state.update(bar)

        # 2. Map HTF values from OWN state (not global engine)
        slow_mapped = self.slow_htf_state.get_mapped_value(bar)
        mid_mapped = self.mid_htf_state.get_mapped_value(bar)

        # 3. Run strategy evaluation
        return self.on_bar(bar, slow_mapped, fast_dema_atr, mid_mapped)

    def _on_mid_htf_candle(self, event: CandleEvent) -> None:
        """Update own mid HTF state (15m)."""
        bar = Bar(
            instrument=event.instrument,
            timeframe=event.timeframe,
            start_ts=event.start_ts,
            end_ts=event.end_ts,
            open=event.open,
            high=event.high,
            low=event.low,
            close=event.close,
            volume=int(event.volume),
        )
        self.mid_htf_state.update(bar)

    def _on_slow_htf_candle(self, event: CandleEvent) -> None:
        """Update own slow HTF state (1H)."""
        bar = Bar(
            instrument=event.instrument,
            timeframe=event.timeframe,
            start_ts=event.start_ts,
            end_ts=event.end_ts,
            open=event.open,
            high=event.high,
            low=event.low,
            close=event.close,
            volume=int(event.volume),
        )
        self.slow_htf_state.update(bar)

    # ═══════════════════════════════════════════════════════════════════════
    # STRATEGY EVALUATION
    # ═══════════════════════════════════════════════════════════════════════

    def on_bar(
        self,
        bar: Bar,
        htf_mapped: HTFMappedValue,
        fast_dema_atr: Optional[float],
        mid_mapped: Optional[HTFMappedValue] = None,
    ) -> Optional[Signal]:
        """Process a fast timeframe bar.

        This is the core strategy evaluation. Called ONLY on fast TF close.
        """
        if self.enabled is False:
            return None

        self._bars_processed += 1
        self.just_entered = False

        close = bar.close
        high = bar.high
        low = bar.low
        prev_close = self._prev_fast_close or close
        prev_high = self._prev_fast_high or high
        prev_low = self._prev_fast_low or low
        htf_val = htf_mapped.htf_value
        prev_htf_val = self._prev_htf_value
        mid_val = mid_mapped.htf_value if mid_mapped else None
        prev_mid_val = self._prev_mid_value

        # Store for next bar
        self._prev_fast_close = close
        self._prev_htf_value = htf_val
        self._prev_mid_value = mid_val
        self._prev_fast_high = high
        self._prev_fast_low = low

        # Skip if no HTF value available
        if htf_val is None or prev_htf_val is None:
            return None

        signal = None

        # 1. Pending triggers are evaluated only from live LTP ticks. Candle
        # OHLC values are never used to fire an order.
        if self.pending_entry is not None and self.pending_entry.status == "pending":
            if self.pending_entry.bars_pending >= self.pending_timeout_bars:
                self._cancel_trigger(self.pending_entry)
                self.pending_entry = None
                self.state = StrategyState.FLAT
                self.position_side = None
                return None
            self.pending_entry.bars_pending += 1

        # 2. OPPOSITE CROSSOVER OVERRIDE: cancel old pending/in-flight and
        # take the new signal on the same bar.  Applies when:
        #   a) ENTRY_TRIGGERED + no position → a LIMIT rests at the broker
        #   b) PENDING_LONG / PENDING_SHORT → a trigger is waiting to cross
        # In both cases the old signal is superseded by the new opposite
        # crossover.  Strategy state resets to FLAT, then _detect_signal arms
        # the new pending/limit.  The signal carries cancel_inflight=True so
        # the engine can terminalize the old durable pending row (if LIVE).
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
                old_pending_id = self._last_armed_pending_id
                self._cancel_trigger(self.pending_entry)
                self.state = StrategyState.FLAT
                self.pending_entry = None
                self.current_trade_id = None
                self._last_armed_pending_id = None
                signal = self._detect_signal(
                    close, prev_close, htf_val, prev_htf_val, high, low, bar.start_ts,
                    mid_val, prev_mid_val, prev_high, prev_low, fast_dema_atr,
                    open_=bar.open,
                )
                if signal is not None:
                    signal.metadata = signal.metadata or {}
                    signal.metadata["cancel_inflight"] = True
                    if old_pending_id:
                        signal.metadata["old_pending_id"] = old_pending_id
                    return signal
                # The strategy arms pending_entry as a side effect; return
                # that pending signal when it was created.
                if (self.pending_entry is not None
                        and self.pending_entry.signal is not None):
                    self.pending_entry.signal.metadata = self.pending_entry.signal.metadata or {}
                    self.pending_entry.signal.metadata["cancel_inflight"] = True
                    if old_pending_id:
                        self.pending_entry.signal.metadata["old_pending_id"] = old_pending_id
                    return self.pending_entry.signal
                # No new signal detected (indicator conditions not met).
                # Emit a cancel-only signal so the engine can terminalize
                # the old pending row and cancel any in-flight order.
                cancel_signal = Signal(
                    signal_type=SignalType.LONG if cancel_and_reenter == "LONG"
                    else SignalType.SHORT,
                    instrument=self.instrument,
                    strategy_id=self.strategy_id,
                    timestamp=bar.start_ts or time.time(),
                    trigger_price=close,
                    stop_price=close,
                    quantity=self.quantity,
                )
                cancel_signal.metadata = {
                    "cancel_inflight": True,
                    "cancel_only": True,
                    "exit": False,
                    "pending": False,
                    "triggered": False,
                }
                if old_pending_id:
                    cancel_signal.metadata["old_pending_id"] = old_pending_id
                self._signals.append(cancel_signal)
                return cancel_signal

        # 3. Detect new signals
        if self.state in (StrategyState.FLAT,
                          StrategyState.PENDING_LONG, StrategyState.PENDING_SHORT):
            signal = self._detect_signal(
                close, prev_close, htf_val, prev_htf_val, high, low, bar.start_ts,
                mid_val, prev_mid_val, prev_high, prev_low, fast_dema_atr,
                open_=bar.open,
            )
        elif self.state not in (StrategyState.EXIT_ORDER_SUBMITTED,):
            # Skip reversal detection when an exit order is already in flight
            # (EXIT_ORDER_SUBMITTED) to prevent double reversal on consecutive
            # bars before the first reversal's entry fills.
            if self.position_side == "SHORT" and self._check_long_cross(close, prev_close, htf_val, prev_htf_val, mid_val, prev_mid_val):
                signal = self._create_reversal_signal("LONG", close, high, low, bar.start_ts, prev_high, prev_low,
                                                      htf_val=htf_val, mid_val=mid_val, fast_dema_atr=fast_dema_atr,
                                                      open_=bar.open)
            elif self.position_side == "LONG" and self._check_short_cross(close, prev_close, htf_val, prev_htf_val, mid_val, prev_mid_val):
                signal = self._create_reversal_signal("SHORT", close, high, low, bar.start_ts, prev_high, prev_low,
                                                      htf_val=htf_val, mid_val=mid_val, fast_dema_atr=fast_dema_atr,
                                                      open_=bar.open)

        self.just_entered = False
        return signal

    # ═══════════════════════════════════════════════════════════════════════
    # CROSSOVER DETECTION
    # ═══════════════════════════════════════════════════════════════════════

    def _check_long_cross(
        self, close: float, prev_close: float,
        htf_val: float, prev_htf_val: float,
        mid_val: Optional[float] = None, prev_mid_val: Optional[float] = None,
    ) -> bool:
        return long_crossover(close, prev_close, htf_val, mid_val)

    def _check_short_cross(
        self, close: float, prev_close: float,
        htf_val: float, prev_htf_val: float,
        mid_val: Optional[float] = None, prev_mid_val: Optional[float] = None,
    ) -> bool:
        return short_crossover(close, prev_close, htf_val, mid_val)

    # ═══════════════════════════════════════════════════════════════════════
    # SIGNAL CREATION
    # ═══════════════════════════════════════════════════════════════════════

    def _detect_signal(
        self, close, prev_close, htf_val, prev_htf_val, high, low, timestamp,
        mid_val=None, prev_mid_val=None, prev_high=None, prev_low=None,
        fast_dema_atr=None, open_=None,
    ) -> Optional[Signal]:
        """Detect a crossover and arm a local trigger; it never submits here."""
        if self.pending_entry is not None or self.pending_exit_trigger is not None:
            return None
        if self._check_long_cross(close, prev_close, htf_val, prev_htf_val, mid_val, prev_mid_val):
            return self._entry_signal("LONG", close, high, low, timestamp, prev_high, prev_low,
                                      htf_val=htf_val, mid_val=mid_val, fast_dema_atr=fast_dema_atr,
                                      open_=open_)
        elif self._check_short_cross(close, prev_close, htf_val, prev_htf_val, mid_val, prev_mid_val):
            return self._entry_signal("SHORT", close, high, low, timestamp, prev_high, prev_low,
                                      htf_val=htf_val, mid_val=mid_val, fast_dema_atr=fast_dema_atr,
                                      open_=open_)
        return None

    def _entry_signal(
        self, side, close, high, low, timestamp,
        prev_high=None, prev_low=None,
        htf_val=None, mid_val=None, fast_dema_atr=None, open_=None,
    ) -> Signal:
        """Build the sole entry intent; a live tick must fire it before orders."""
        return self._create_triggered_entry_signal(
            side, close, high, low, timestamp, prev_high, prev_low,
            htf_val=htf_val, mid_val=mid_val, fast_dema_atr=fast_dema_atr,
            open_=open_)

    def _create_triggered_entry_signal(
        self, side, close, high, low, timestamp,
        prev_high=None, prev_low=None,
        htf_val=None, mid_val=None, fast_dema_atr=None, open_=None,
    ) -> Signal:
        """Create a signal and arm its strategy-local live-LTP trigger."""
        trigger, stop = entry_levels(side, high, low, prev_high, prev_low)
        self._trigger_generation += 1

        signal = Signal(
            signal_type=SignalType.LONG if side == "LONG" else SignalType.SHORT,
            instrument=self.instrument,
            strategy_id=self.strategy_id,
            timestamp=timestamp,
            trigger_price=trigger,
            stop_price=stop,
            quantity=self.quantity,
        )
        signal.metadata = {
            "pending": True,
            "triggered": False,
            "trigger_state": "ARMED",
            "trigger_generation": self._trigger_generation,
            "trigger_source": "market_websocket_ltp",
            "entry_price": close,
            "htf_value": htf_val,
            "mid_value": mid_val,
            "fast_dema_atr": fast_dema_atr,
            "trigger_level": trigger,
            "signal_candle_start": timestamp,
            "signal_candle_open": open_,
            "signal_candle_high": high,
            "signal_candle_low": low,
            "signal_candle_close": close,
            "signal_htf_dema_atr": htf_val,
            "signal_mid_dema_atr": mid_val,
            "signal_fast_dema_atr": fast_dema_atr,
        }

        # Phase 4 — freeze the executing context at signal time.
        freeze_signal_context(
            signal, close=close, high=high, low=low, timestamp=timestamp,
            open_=open_, dema=fast_dema_atr, atr=self.fast_indicator.atr_value,
            htf_value=htf_val, mid_value=mid_val,
            position_side=self.position_side, position_stop=self.stop_price,
        )

        self.pending_entry = PendingEntry(
            signal=signal, trigger_price=trigger, side=side,
            created_at=time.time(), status="pending")
        self.state = (StrategyState.PENDING_LONG if side == "LONG"
                      else StrategyState.PENDING_SHORT)
        self._signals.append(signal)
        return signal

    def _create_reversal_signal(
        self, side, close, high, low, timestamp,
        prev_high=None, prev_low=None,
        htf_val=None, mid_val=None, fast_dema_atr=None, open_=None,
    ) -> Signal:
        """Arm a reversal-exit trigger and an opposite entry trigger.

        No order is produced until the local exit trigger crosses on a live
        market tick. The opposite entry trigger remains blocked until flat.

        ``reversal_entry_gap_points`` offsets the opposite entry trigger from
        the signal candle's reversal trigger:
          LONG → SHORT: entry trigger = signal LOW - gap
          SHORT → LONG: entry trigger = signal HIGH + gap
        """
        gap = int(getattr(self, "reversal_entry_gap_points", 0))
        self._cancel_trigger(self.pending_exit_trigger)
        self._cancel_trigger(self.pending_entry)
        trigger, entry_trigger, stop = reversal_levels(
            side, high, low, prev_high, prev_low, gap)
        self._trigger_generation += 1

        # The opposite entry is a distinct lifecycle and stays armed, but
        # cannot fire until the old position has been confirmed flat.
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
            "pending": True,
            "triggered": False,
            "trigger_state": "ARMED",
            "trigger_generation": self._trigger_generation,
            "trigger_source": "market_websocket_ltp",
            "signal_candle_start": timestamp,
            "signal_candle_open": open_,
            "signal_candle_high": high,
            "signal_candle_low": low,
            "signal_candle_close": close,
            "signal_htf_dema_atr": htf_val,
            "signal_mid_dema_atr": mid_val,
            "signal_fast_dema_atr": fast_dema_atr,
        }

        freeze_signal_context(
            entry_signal, close=close, high=high, low=low, timestamp=timestamp,
            open_=open_, dema=fast_dema_atr, atr=self.fast_indicator.atr_value,
            htf_value=htf_val, mid_value=mid_val,
            position_side=self.position_side, position_stop=self.stop_price,
        )

        # Exit intent belongs to the currently open position. The entry intent
        # remains pending until the exit fill and flat reconciliation finish.
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
        exit_signal.metadata = {
            "exit": True,
            "pending": True,
            "triggered": False,
            "trigger_state": "ARMED",
            "trigger_generation": self._trigger_generation,
            "trigger_source": "market_websocket_ltp",
            "pending_trigger_kind": "REVERSAL_EXIT",
            "exit_reason": reason,
            "exit_price": close,
            "is_reversal": True,
            "is_reversal_entry": False,
            "reversal_exit_trigger": trigger,
            "reversal_entry_signal": entry_signal,
            "position_side": self.position_side,
        }
        entry_signal.metadata["reversal_parent_signal_id"] = exit_signal.signal_id

        freeze_signal_context(
            exit_signal, close=close, high=high, low=low, timestamp=timestamp,
            open_=open_, dema=fast_dema_atr, atr=self.fast_indicator.atr_value,
            htf_value=htf_val, mid_value=mid_val,
            position_side=self.position_side, position_stop=self.stop_price,
        )

        self.pending_entry = PendingEntry(
            signal=entry_signal, trigger_price=entry_trigger, side=side,
            created_at=time.time(), status="waiting_for_flat")
        self.pending_exit_trigger = PendingEntry(
            signal=exit_signal, trigger_price=trigger,
            side="SHORT" if self.position_side == "LONG" else "LONG",
            created_at=time.time(), status="pending")
        self.state = StrategyState.EXIT_PENDING
        self.last_exit_reason = reason
        self._signals.append(entry_signal)
        self._signals.append(exit_signal)
        return exit_signal

    @staticmethod
    def _cancel_trigger(trigger: Optional[PendingEntry]) -> None:
        if trigger is None or trigger.signal is None:
            return
        trigger.status = "cancelled"
        metadata = trigger.signal.metadata or {}
        metadata.update(pending=False, triggered=False, trigger_state="CANCELLED")
        trigger.signal.metadata = metadata

    def _tick_reversal_exit_trigger(self, pen: PendingEntry,
                                    ltp: float) -> Optional[Signal]:
        """Fire the old-position exit trigger once, from live LTP only."""
        metadata = pen.signal.metadata or {} if pen.signal is not None else {}
        if (pen.status != "pending" or pen.signal is None
                or pen.signal.strategy_id != self.strategy_id
                or pen.signal.instrument != self.instrument
                or metadata.get("trigger_generation") != self._trigger_generation
                or metadata.get("position_side") != self.position_side):
            self._cancel_trigger(pen)
            self.pending_exit_trigger = None
            return None
        crossed = ((pen.side == "LONG" and ltp >= pen.trigger_price)
                   or (pen.side == "SHORT" and ltp <= pen.trigger_price))
        if not crossed:
            return None
        pen.status = "fired"
        metadata.update(pending=False, triggered=True, trigger_state="FIRED",
                        trigger_ltp=float(ltp), trigger_source="market_websocket_ltp")
        pen.signal.metadata = metadata
        self._last_fired_trigger_signal_id = pen.signal.signal_id
        self.pending_exit_trigger = None
        self.state = StrategyState.EXIT_ORDER_SUBMITTED
        self.stop_exit_submitted = True
        return pen.signal

    # ═══════════════════════════════════════════════════════════════════════
    # PENDING ENTRY + STOP LOSS
    # ═══════════════════════════════════════════════════════════════════════

    def _close_position(self, reason: str) -> None:
        """Mark exit as pending; engine clears state after fill."""
        self.last_exit_reason = reason
        self.state = StrategyState.EXIT_ORDER_SUBMITTED

    # ═══════════════════════════════════════════════════════════════════════
    # TICK HANDLER — for live LTP processing
    # ═══════════════════════════════════════════════════════════════════════

    def on_tick(self, ltp: float, timestamp: float) -> Optional[Signal]:
        """Process LTP tick. Only checks pending triggers and stop loss.

        Must NOT recalculate indicators or run full strategy logic.
        """
        if not self.enabled:
            return None

        if self.just_entered:
            return None

        if ltp <= 0:
            return None

        # Check stop loss on tick
        if (self.position_side is not None
                and self.stop_price is not None
                and not self.just_entered
                and not self.stop_exit_submitted):
            if self.position_side == "LONG" and ltp <= self.stop_price:
                return self._tick_stop_loss(ltp, timestamp)
            elif self.position_side == "SHORT" and ltp >= self.stop_price:
                return self._tick_stop_loss(ltp, timestamp)

        if (self.pending_exit_trigger is not None
                and self.pending_exit_trigger.status == "pending"):
            exit_signal = self._tick_reversal_exit_trigger(
                self.pending_exit_trigger, ltp)
            if exit_signal is not None:
                self._signals.append(exit_signal)
                return exit_signal

        # Check pending entry trigger on tick
        if self.pending_entry is not None and self.pending_entry.status == "pending":
            pen = self.pending_entry
            if (pen.signal is None or pen.signal.strategy_id != self.strategy_id
                    or pen.signal.instrument != self.instrument
                    or (pen.signal.metadata or {}).get("trigger_state") == "CANCELLED"):
                self._cancel_trigger(pen)
                self.pending_entry = None
                return None
            if pen.side == "LONG" and ltp >= pen.trigger_price:
                return self._tick_entry_trigger(pen, ltp, timestamp)
            elif pen.side == "SHORT" and ltp <= pen.trigger_price:
                return self._tick_entry_trigger(pen, ltp, timestamp)

        return None

    def _tick_stop_loss(self, ltp: float, timestamp: float) -> Optional[Signal]:
        """Execute stop loss from tick."""
        exit_signal = Signal(
            signal_type=SignalType.SHORT if self.position_side == "LONG" else SignalType.LONG,
            instrument=self.instrument,
            strategy_id=self.strategy_id,
            timestamp=timestamp,
            trigger_price=ltp,
            stop_price=self.stop_price,
            quantity=self.position_quantity or self.quantity,
        )
        exit_signal.metadata = {
            "exit": True,
            "exit_reason": "stop_loss_hit",
            "exit_price": ltp,
            "source": "tick",
            "triggered": True,
            "trigger_state": "FIRED",
            "trigger_generation": self._trigger_generation,
            "trigger_source": "market_websocket_ltp",
        }
        self._cancel_trigger(self.pending_exit_trigger)
        self.pending_exit_trigger = None
        self._cancel_trigger(self.pending_entry)
        self.pending_entry = None
        self._last_fired_trigger_signal_id = exit_signal.signal_id
        # Phase 4 — tick exits freeze the LTP snapshot (no candle bar).
        freeze_signal_context(
            exit_signal, close=ltp, high=ltp, low=ltp, timestamp=timestamp,
            dema=self.fast_indicator.value, atr=self.fast_indicator.atr_value,
            position_side=self.position_side, position_stop=self.stop_price,
        )
        self._close_position("stop_loss_hit")
        self.stop_exit_submitted = True
        self._signals.append(exit_signal)
        return exit_signal

    def _tick_entry_trigger(self, pen: PendingEntry, ltp: float, timestamp: float) -> Optional[Signal]:
        """Execute pending entry from tick."""
        metadata = pen.signal.metadata or {} if pen.signal is not None else {}
        if (pen.status != "pending" or pen.signal is None
                or pen.signal.strategy_id != self.strategy_id
                or pen.signal.instrument != self.instrument
                or metadata.get("trigger_generation") != self._trigger_generation
                or metadata.get("trigger_state") != "ARMED"):
            self._cancel_trigger(pen)
            if self.pending_entry is pen:
                self.pending_entry = None
            return None
        # Dhan-linked: state = ENTRY_TRIGGERED, NOT LONG/SHORT_POSITION.
        # position_side is NOT set here — only set by _sync_strategy_on_entry_fill
        # when Dhan confirms the fill.
        self.stop_price = pen.signal.stop_price
        self.just_entered = True
        self.state = StrategyState.ENTRY_TRIGGERED
        pen.status = "fired"
        self.pending_entry = None
        metadata.update(
            pending=False, triggered=True, trigger_state="FIRED",
            trigger_ltp=float(ltp), trigger_source="market_websocket_ltp")
        pen.signal.metadata = metadata
        self._last_fired_trigger_signal_id = pen.signal.signal_id
        return pen.signal

    # ═══════════════════════════════════════════════════════════════════════
    # WARMUP — per-strategy indicator warmup
    # ═══════════════════════════════════════════════════════════════════════

    def warmup_indicator(self, bar: Bar) -> None:
        """Warm up fast indicator from a historical bar."""
        self.fast_indicator.update(bar.open, bar.high, bar.low, bar.close, bar.end_ts)

    def warmup_htf(self, bar: Bar) -> None:
        """Warm up HTF state from a historical bar."""
        tf_min = self._tf_to_minutes(bar.timeframe)
        if tf_min == self._tf_to_minutes(self.mid_timeframe):
            self.mid_htf_state.update(bar)
        elif tf_min == self._tf_to_minutes(self.htf_timeframe):
            self.slow_htf_state.update(bar)

    def warmup_indicator_htf(self, bar: Bar) -> None:
        """Warm up HTF indicator (not state) from historical bar."""
        tf_min = self._tf_to_minutes(bar.timeframe)
        if tf_min == self._tf_to_minutes(self.mid_timeframe):
            self.mid_indicator.update(bar.open, bar.high, bar.low, bar.close, bar.end_ts)
        elif tf_min == self._tf_to_minutes(self.htf_timeframe):
            self.slow_indicator.update(bar.open, bar.high, bar.low, bar.close, bar.end_ts)

    def reset(self) -> None:
        """Reset all strategy state. Used before warmup."""
        self.state = StrategyState.FLAT
        self.position_side = None
        self.stop_price = None
        self._cancel_trigger(self.pending_entry)
        self._cancel_trigger(self.pending_exit_trigger)
        self.pending_entry = None
        self.pending_exit_trigger = None
        self._last_fired_trigger_signal_id = None
        self._prev_fast_close = None
        self._prev_htf_value = None
        self._prev_mid_value = None
        self._prev_fast_high = None
        self._prev_fast_low = None
        self.current_trade_id = None
        self._last_armed_pending_id = None
        # §9: bound strategies MUST NOT mutate shared indicator state — the
        # shared streams belong to SharedNativeIndicatorEngine and are used by
        # every strategy subscribed to the same (security_id, timeframe).
        if not getattr(self, "_shared_indicators_bound", False):
            self.fast_indicator.reset()
            self.mid_indicator.reset()
            self.slow_indicator.reset()
            self.mid_htf_state.reset()
            self.slow_htf_state.reset()

    @staticmethod
    def _tf_to_minutes(tf: str) -> int:
        if not tf:
            return 5
        unit = tf[-1].lower()
        try:
            n = int(tf[:-1])
        except ValueError:
            return 5
        if unit == "h":
            return n * 60
        if unit == "m":
            return n
        return 5

    # ═══════════════════════════════════════════════════════════════════════
    # PROPERTIES + SNAPSHOTS
    # ═══════════════════════════════════════════════════════════════════════

    @property
    def enabled(self) -> bool:
        return getattr(self, "_enabled", True)

    @enabled.setter
    def enabled(self, value: bool) -> None:
        self._enabled = bool(value)

    @property
    def is_flat(self) -> bool:
        return self.position_side is None and self.pending_entry is None

    @property
    def has_position(self) -> bool:
        return self.position_side is not None

    @property
    def has_pending(self) -> bool:
        return self.pending_entry is not None

    def _pending_entry_snapshot(
        self, trigger: Optional[PendingEntry] = None, *, use_current: bool = True,
    ) -> Optional[dict]:
        """Emit the pending entry as a dict keyed for the dashboard panel.

        Captures strategy state (fast O(1) — no broker calls).
        All values are the frozen signal-time snapshot captured when the entry
        was armed; the live signal candle stays unchanged until the trigger
        crosses, then the engine acts on it.
        """
        pe = self.pending_entry if use_current and trigger is None else trigger
        if pe is None:
            return None
        sig = pe.signal
        md = (sig.metadata or {}) if sig else {}
        ctx = getattr(sig, "context", None) if sig else None

        def val(*keys, ctx_key=None, default=None):
            for k in keys:
                v = md.get(k)
                if v is not None:
                    return v
            if ctx is not None and ctx_key is not None:
                v = getattr(ctx, ctx_key, None)
                if v is not None:
                    return v
            return default

        return {
            "side": pe.side,
            "trigger_price": pe.trigger_price,
            "stop_price": (sig.stop_price if sig else None) or 0,
            "bars_pending": pe.bars_pending,
            "created_at": getattr(pe, "created_at", 0) or 0.0,
            "instrument": sig.instrument if sig else self.instrument,
            "strategy_id": sig.strategy_id if sig else self.strategy_id,
            "quantity": sig.quantity if sig else self.quantity,
            "signal_id": sig.signal_id if sig else None,
            "signal_type": sig.signal_type.value if sig else None,
            "timestamp": sig.timestamp if sig else 0.0,
            "status": pe.status,
            "trigger_state": md.get("trigger_state", "ARMED"),
            "trigger_generation": md.get("trigger_generation"),
            "trigger_source": md.get("trigger_source", "market_websocket_ltp"),
            "exit": bool(md.get("exit")),
            "lifecycle_id": getattr(sig, "lifecycle_id", None) if sig else None,
            "parent_position_id": getattr(sig, "parent_position_id", None) if sig else None,
            "position_generation": getattr(sig, "position_generation", None) if sig else None,
            "metadata": dict(sig.metadata or {}) if sig else {},
            "signal_candle_start": val("signal_candle_start", ctx_key="timestamp"),
            "signal_candle_open": val("signal_candle_open", ctx_key="open"),
            "signal_candle_high": val("signal_candle_high", ctx_key="high"),
            "signal_candle_low": val("signal_candle_low", ctx_key="low"),
            "signal_candle_close": val("signal_candle_close", ctx_key="close"),
            "signal_htf_dema_atr": val("signal_htf_dema_atr", "htf_value", ctx_key="htf_value"),
            "signal_mid_dema_atr": val("signal_mid_dema_atr", "mid_value", ctx_key="mid_value"),
            "signal_fast_dema_atr": val("signal_fast_dema_atr", "fast_dema_atr", ctx_key="dema"),
        }

    def snapshot(self) -> dict:
        """Return diagnostic snapshot."""
        return {
            "strategy_id": self.strategy_id,
            "instrument": self.instrument,
            "fast_timeframe": self.fast_timeframe,
            "enabled": self.enabled,
            "state": self.state.value,
            "position_side": self.position_side,
            "stop_price": self.stop_price,
            "has_pending": self.has_pending,
            "bars_processed": self._bars_processed,
            "signals_generated": len(self._signals),
            "fast_indicator_count": self.fast_indicator._count,
            "mid_htf_bars": self.mid_htf_state.bar_count(),
            "slow_htf_bars": self.slow_htf_state.bar_count(),
            "slow_htf_value": self.slow_htf_state.last_value,
            "mid_htf_value": self.mid_htf_state.last_value,
            "prev_fast_close": self._prev_fast_close,
            "prev_htf_value": self._prev_htf_value,
            "prev_mid_value": self._prev_mid_value,
            "last_exit_reason": self.last_exit_reason,
            "just_entered": self.just_entered,
            "pending_entry": self._pending_entry_snapshot(),
            "pending_exit_trigger": self._pending_entry_snapshot(
                self.pending_exit_trigger, use_current=False),
            "trigger_generation": self._trigger_generation,
            "last_fired_trigger_signal_id": self._last_fired_trigger_signal_id,
            "stop_exit_submitted": self.stop_exit_submitted,
            "current_trade_id": self.current_trade_id,
            "current_position_id": self.current_position_id,
            "position_generation": self.position_generation,
            "position_quantity": self.position_quantity,
            "last_armed_pending_id": self._last_armed_pending_id,
        }

    @staticmethod
    def _unpack_pending_entry(pending_entry) -> tuple:
        """Normalize a pending_entry snapshot to (signal_id, trigger, side, bars).

        Accepts the legacy 4-tuple snapshot format as well as the new dict
        format so restores keep working across version boundaries.
        """
        if isinstance(pending_entry, (list, tuple)) and len(pending_entry) >= 4:
            return pending_entry[0], pending_entry[1], pending_entry[2], pending_entry[3], "pending"
        if isinstance(pending_entry, dict):
            return (pending_entry.get("signal_id"),
                    pending_entry.get("trigger_price"),
                    pending_entry.get("side"),
                    pending_entry.get("bars_pending"),
                    pending_entry.get("status", "pending"))
        raise ValueError("unsupported pending_entry snapshot format: "
                         f"{type(pending_entry).__name__}")

    def restore(self, snapshot: dict) -> None:
        """Restore strategy state from a snapshot dict."""
        state_value = snapshot.get("state", "flat")
        try:
            self.state = StrategyState(state_value)
        except (ValueError, KeyError):
            self.state = StrategyState.FLAT
        self.position_side = snapshot.get("position_side")
        self.stop_price = snapshot.get("stop_price")
        self._bars_processed = snapshot.get("bars_processed", 0)
        self._prev_fast_close = snapshot.get("prev_fast_close")
        self._prev_htf_value = snapshot.get("prev_htf_value")
        self._prev_mid_value = snapshot.get("prev_mid_value")
        self.last_exit_reason = snapshot.get("last_exit_reason")
        self.just_entered = bool(snapshot.get("just_entered", False))
        self.stop_exit_submitted = bool(snapshot.get("stop_exit_submitted", False))
        self.current_trade_id = snapshot.get("current_trade_id")
        self.current_position_id = snapshot.get("current_position_id")
        self.position_generation = snapshot.get("position_generation")
        self.position_quantity = snapshot.get("position_quantity")
        self._last_armed_pending_id = snapshot.get("last_armed_pending_id")
        self.enabled = bool(snapshot.get("enabled", True))
        self._trigger_generation = int(snapshot.get("trigger_generation", 0) or 0)
        self._last_fired_trigger_signal_id = snapshot.get("last_fired_trigger_signal_id")

        def restore_trigger(pending_entry):
            if not pending_entry:
                return None
            signal_id, trigger, side, bars, pending_status = self._unpack_pending_entry(pending_entry)
            signal_type_value = (pending_entry.get("signal_type")
                                 if isinstance(pending_entry, dict) else None)
            signal_type = (SignalType(signal_type_value)
                           if signal_type_value in {item.value for item in SignalType}
                           else SignalType.LONG if side == "LONG" else SignalType.SHORT)
            restored_signal = Signal(
                    signal_type=signal_type,
                    instrument=self.instrument, strategy_id=self.strategy_id,
                    timestamp=float(pending_entry.get("timestamp", 0.0) or 0.0)
                    if isinstance(pending_entry, dict) else 0.0,
                    trigger_price=trigger,
                    stop_price=(pending_entry.get("stop_price") if isinstance(
                        pending_entry, dict) else None) or self.stop_price or 0.0,
                    quantity=int(pending_entry.get("quantity", self.quantity) or self.quantity)
                    if isinstance(pending_entry, dict) else self.quantity,
                    metadata=dict(pending_entry.get("metadata") or {})
                    if isinstance(pending_entry, dict) else {"pending": True},
                )
            if signal_id:
                restored_signal.signal_id = signal_id
            if isinstance(pending_entry, dict):
                restored_signal.lifecycle_id = pending_entry.get("lifecycle_id")
                restored_signal.parent_position_id = pending_entry.get("parent_position_id")
                restored_signal.position_generation = pending_entry.get("position_generation")
            return PendingEntry(
                signal=restored_signal, trigger_price=trigger, side=side,
                status=pending_status,
                created_at=float(pending_entry.get("created_at", 0.0) or 0.0)
                if isinstance(pending_entry, dict) else 0.0,
                bars_pending=bars if bars is not None else 0,
            )
        self.pending_entry = restore_trigger(snapshot.get("pending_entry"))
        self.pending_exit_trigger = restore_trigger(snapshot.get("pending_exit_trigger"))
