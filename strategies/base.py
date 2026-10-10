"""Small state adapter shared by the production SuperTrend strategies.

Signal calculation lives in :mod:`strategies.supertrend`; this class only
supplies the strategy identity, position ownership, and durable reversal
handoff expected by the broker lifecycle. It contains no indicator or stop
calculation.
"""
from __future__ import annotations

import time
from collections import OrderedDict
from typing import Optional

from strategies.types import PendingEntry, Signal, SignalType, StrategyState


class StrategyBase:
    """Lifecycle state for one instrument's completed-candle strategy."""

    def __init__(self, strategy_id: str, instrument: str, security_id: str,
                 quantity: int = 1, capital: float = 300_000.0,
                 multiplier: float = 1.0, **_ignored):
        self.strategy_id = str(strategy_id)
        self.instrument = str(instrument)
        self.security_id = str(security_id)
        self.quantity = int(quantity)
        self.capital = float(capital)
        self.multiplier = float(multiplier)
        self.subscriptions = [f"{self.instrument}:15m"]
        self.timeframe = "15m"
        self.signal_timeframe = "15m"
        self.strategy_type = "supertrend"
        self.execution_model = "completed_candle_market"

        self.state = StrategyState.FLAT
        self.position_side: Optional[str] = None
        self.current_position_id: Optional[str] = None
        self.current_trade_id: Optional[str] = None
        self.position_generation: Optional[int] = None
        self.position_quantity: Optional[int] = None
        self.pending_entry: Optional[PendingEntry] = None
        self.pending_exit_trigger: Optional[PendingEntry] = None
        self.last_exit_reason: Optional[str] = None
        self.just_entered = False
        self.enabled = True
        self._bars_processed = 0
        self._trigger_generation = 0
        self._last_armed_pending_id: Optional[str] = None
        self._last_fired_trigger_signal_id: Optional[str] = None
        self._fired_trigger_signal_ids: OrderedDict[str, None] = OrderedDict()
        self._max_fired_trigger_ids = 16
        self._signals: list[Signal] = []

    def bind_shared_indicators(self, _engine) -> None:
        """This strategy owns its SuperTrend state; no shared indicator feeds."""

    def on_candle(self, event):
        if event.timeframe != "15m":
            return None
        return self._on_fast_candle(event)

    def warmup_indicator(self, _bar) -> None:
        """Implemented by the concrete indicator strategy."""

    def _create_entry_signal(self, side, close, high, low, timestamp) -> Signal:
        self._trigger_generation += 1
        signal = Signal(
            signal_type=SignalType.LONG if side == "LONG" else SignalType.SHORT,
            instrument=self.instrument,
            strategy_id=self.strategy_id,
            timestamp=float(timestamp),
            trigger_price=float(close),
            stop_price=0.0,
            quantity=self.quantity,
            side=side,
            metadata={
                "strategy_type": "supertrend",
                "trigger_generation": self._trigger_generation,
                "signal_candle_start": float(timestamp),
                "signal_candle_open": None,
                "signal_candle_high": float(high),
                "signal_candle_low": float(low),
                "signal_candle_close": float(close),
            },
        )
        self.pending_entry = PendingEntry(
            signal=signal, trigger_price=float(close), side=side,
            created_at=time.time(), status="fired")
        self._signals.append(signal)
        return signal

    def _create_reversal_signal(self, side, close, high, low, timestamp,
                                _prev_high=None, _prev_low=None,
                                **_ignored) -> Signal:
        """Create the paired exit/entry record for a broker-confirmed reversal."""
        if self.position_side not in ("LONG", "SHORT"):
            return None
        self._cancel_trigger(self.pending_entry)
        self._cancel_trigger(self.pending_exit_trigger)
        self._trigger_generation += 1
        ts, close = float(timestamp), float(close)
        signal_candle = {
            "strategy_type": "supertrend",
            "strategy_timeframe": "15m",
            "signal_source": "completed_native_15m_candle",
            "signal_candle_start": ts,
            "signal_candle_open": None,
            "signal_candle_high": float(high),
            "signal_candle_low": float(low),
            "signal_candle_close": close,
            "trigger_generation": self._trigger_generation,
        }
        entry = Signal(
            signal_type=SignalType.LONG if side == "LONG" else SignalType.SHORT,
            instrument=self.instrument, strategy_id=self.strategy_id,
            timestamp=ts, trigger_price=close, stop_price=0.0,
            quantity=self.quantity, side=side,
            metadata={**signal_candle, "is_reversal": True,
                      "is_reversal_entry": True, "pending": False,
                      "triggered": True, "trigger_state": "FIRED",
                      "market_on_signal_close": True,
                      "no_separate_stop": True,
                      "trigger_source": "broker_confirmed_reversal_flat"},
        )
        exit_signal = Signal(
            signal_type=SignalType.SHORT if self.position_side == "LONG"
            else SignalType.LONG,
            instrument=self.instrument, strategy_id=self.strategy_id,
            timestamp=ts, trigger_price=close, stop_price=0.0,
            quantity=self.quantity,
            metadata={**signal_candle, "exit": True, "pending": False,
                      "triggered": True, "trigger_state": "FIRED",
                      "trigger_generation": self._trigger_generation,
                      "trigger_source": "completed_signal_candle_close",
                      "market_on_signal_close": True,
                      "no_separate_stop": True, "is_reversal": True,
                      "is_reversal_entry": False,
                      "exit_reason": f"{side.lower()}_reversal",
                      "position_side": self.position_side,
                      "reversal_entry_signal": entry},
        )
        exit_signal.lifecycle_id = self.current_trade_id
        exit_signal.parent_position_id = self.current_position_id
        exit_signal.position_generation = self.position_generation
        entry.metadata["reversal_parent_signal_id"] = exit_signal.signal_id
        self.pending_entry = PendingEntry(
            signal=entry, trigger_price=close, side=side,
            created_at=time.time(), status="waiting_for_flat")
        self.pending_exit_trigger = None
        self.last_exit_reason = f"{side.lower()}_reversal"
        self.state = StrategyState.EXIT_ORDER_SUBMITTED
        self._signals.extend((entry, exit_signal))
        return exit_signal

    @staticmethod
    def _cancel_trigger(trigger: Optional[PendingEntry]) -> None:
        if trigger is None:
            return
        trigger.status = "cancelled"
        if trigger.signal is not None:
            trigger.signal.metadata = dict(trigger.signal.metadata or {})
            trigger.signal.metadata.update(
                pending=False, triggered=False, trigger_state="CANCELLED")

    def _register_fired_trigger(self, signal_id: Optional[str]) -> None:
        self._last_fired_trigger_signal_id = signal_id
        if signal_id:
            self._fired_trigger_signal_ids[signal_id] = None
            self._fired_trigger_signal_ids.move_to_end(signal_id)
            while len(self._fired_trigger_signal_ids) > self._max_fired_trigger_ids:
                self._fired_trigger_signal_ids.popitem(last=False)

    def is_fired_trigger_signal(self, signal_id: Optional[str]) -> bool:
        return bool(signal_id and (
            signal_id in self._fired_trigger_signal_ids
            or signal_id == self._last_fired_trigger_signal_id))

    @property
    def has_position(self) -> bool:
        return self.position_side in ("LONG", "SHORT")

    @property
    def is_flat(self) -> bool:
        return not self.has_position

    @property
    def has_pending(self) -> bool:
        return bool(self.pending_entry or self.pending_exit_trigger)

    def _close_position(self, reason: str) -> None:
        self.last_exit_reason = reason
        self.state = StrategyState.EXIT_ORDER_SUBMITTED

    @staticmethod
    def _pending_entry_snapshot(pending=None, use_current=True):
        if pending is None:
            return None
        signal = getattr(pending, "signal", None)
        if signal is None:
            return None
        return {
            "signal_id": signal.signal_id,
            "signal_type": signal.signal_type.value,
            "trigger_price": float(pending.trigger_price),
            "stop_price": float(signal.stop_price or 0.0),
            "side": pending.side,
            "status": pending.status,
            "bars_pending": int(getattr(pending, "bars_pending", 0)),
            "timestamp": float(signal.timestamp),
            "quantity": int(signal.quantity),
            "metadata": dict(signal.metadata or {}),
        }

    def snapshot(self) -> dict:
        return {
            "strategy_id": self.strategy_id,
            "instrument": self.instrument,
            "strategy_type": "supertrend",
            "timeframe": "15m",
            "enabled": self.enabled,
            "state": self.state.value,
            "position_side": self.position_side,
            "current_position_id": self.current_position_id,
            "current_trade_id": self.current_trade_id,
            "position_generation": self.position_generation,
            "position_quantity": self.position_quantity,
            "bars_processed": self._bars_processed,
            "last_exit_reason": self.last_exit_reason,
            "pending_entry": self._pending_entry_snapshot(self.pending_entry),
            "pending_exit_trigger": self._pending_entry_snapshot(self.pending_exit_trigger),
            "trigger_generation": self._trigger_generation,
            "last_armed_pending_id": self._last_armed_pending_id,
        }

    def restore(self, snapshot: dict) -> None:
        """Restore broker-owned position identity; never revive old triggers."""
        try:
            self.state = StrategyState(snapshot.get("state", "flat"))
        except (ValueError, KeyError):
            self.state = StrategyState.FLAT
        self.position_side = snapshot.get("position_side")
        self.current_position_id = snapshot.get("current_position_id")
        self.current_trade_id = snapshot.get("current_trade_id")
        self.position_generation = snapshot.get("position_generation")
        self.position_quantity = snapshot.get("position_quantity")
        self._bars_processed = int(snapshot.get("bars_processed", 0) or 0)
        self.last_exit_reason = snapshot.get("last_exit_reason")
        self.enabled = bool(snapshot.get("enabled", True))
        self._trigger_generation = int(snapshot.get("trigger_generation", 0) or 0)
        self._last_armed_pending_id = snapshot.get("last_armed_pending_id")
        # Outstanding signals are restored only from the canonical DB journal.
        self.pending_entry = None
        self.pending_exit_trigger = None
        self._fired_trigger_signal_ids.clear()
        self._last_fired_trigger_signal_id = None
