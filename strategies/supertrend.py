"""15-minute SuperTrend strategy using the existing live trade lifecycle."""
from __future__ import annotations

import time
from typing import Optional

from core.timeframe_engine import Bar
from indicators.supertrend import SuperTrend
from strategies.base import StrategyBase
from strategies.types import Signal, SignalType, StrategyState, freeze_signal_context


class SuperTrendStrategyInstance(StrategyBase):
    """SuperTrend signal generator with the established Dhan execution path.

    Signals are emitted only from completed native 15m candles. The signal is
    submitted immediately after that candle closes; opposite flips use the
    shared broker-confirmed exit-then-entry lifecycle. These profiles do not
    create a separate stop-loss.
    """

    def __init__(self, *, atr_period: int, atr_multiplier: float, source: str,
                 atr_method: str = "sma_tr", first_tr: str = "high_low", **kwargs):
        super().__init__(**kwargs)
        self.execution_model = "completed_candle_market"
        self.strategy_type = "supertrend"
        self.supertrend = SuperTrend(atr_period, atr_multiplier, source, atr_method,
                                     first_tr=first_tr)
        self.subscriptions = [f"{self.instrument}:15m"]

    def bind_shared_indicators(self, engine) -> None:
        """SuperTrend is strategy-owned and needs no shared indicator stream."""
        return None

    def _on_fast_candle(self, event) -> Optional[Signal]:
        """Feed the completed native 15m candle directly to SuperTrend."""
        bar = Bar(
            instrument=event.instrument, timeframe=event.timeframe,
            start_ts=event.start_ts, end_ts=event.end_ts,
            open=event.open, high=event.high, low=event.low,
            close=event.close, volume=int(event.volume),
        )
        return self.on_bar(bar)

    def warmup_indicator(self, bar: Bar) -> None:
        if bar.timeframe == "15m":
            self.supertrend.update(bar.open, bar.high, bar.low, bar.close, bar.end_ts)

    def on_bar(self, bar: Bar) -> Optional[Signal]:
        if not self.enabled:
            return None
        self._bars_processed += 1
        self.just_entered = False
        calculation_started = time.perf_counter()
        value = self.supertrend.update(
            bar.open, bar.high, bar.low, bar.close, bar.end_ts)
        calculation_ms = (time.perf_counter() - calculation_started) * 1000
        if not (value.buy_signal or value.sell_signal):
            return None
        side = "LONG" if value.buy_signal else "SHORT"

        if self.state == StrategyState.EXIT_ORDER_SUBMITTED:
            return None

        if self.position_side is None:
            signal = self._create_entry_signal(
                side, bar.close, bar.high, bar.low, bar.start_ts)
            signal.trigger_price = float(bar.close)
            signal.metadata.update(
                pending=False, triggered=True, trigger_state="FIRED",
                trigger_ltp=float(bar.close),
                trigger_source="completed_signal_candle_close",
                market_on_signal_close=True,
                no_separate_stop=True,
            )
            if self.pending_entry is not None:
                self.pending_entry.status = "fired"
            self._register_fired_trigger(signal.signal_id)
            self.state = StrategyState.ENTRY_TRIGGERED
            signal.metadata.update(self._supertrend_metadata(
                value, bar, calculation_ms))
            self._freeze_signal_inputs(signal, value, bar)
            return signal

        if self.position_side == side:
            return None
        signal = self._create_reversal_signal(
            side, bar.close, bar.high, bar.low, bar.start_ts)
        if signal is not None:
            # A completed opposite SuperTrend flip closes the old position at
            # market, then the paired opposite entry is submitted only after
            # Dhan confirms the old position is flat. Neither leg has a stop.
            signal.trigger_price = float(bar.close)
            signal.metadata.update(
                pending=False, triggered=True, trigger_state="FIRED",
                trigger_ltp=float(bar.close),
                trigger_source="completed_signal_candle_close",
                market_on_signal_close=True,
                no_separate_stop=True,
            )
            signal.metadata.pop("reversal_entry_gap_points", None)
            self._register_fired_trigger(signal.signal_id)
            paired = signal.metadata.get("reversal_entry_signal")
            if paired is not None:
                paired.trigger_price = float(bar.close)
                paired.metadata.pop("reversal_entry_gap_points", None)
                paired.metadata.update(
                    strategy_type="supertrend",
                    market_on_signal_close=True,
                    no_separate_stop=True,
                    trigger_source="broker_confirmed_reversal_flat",
                )
                if self.pending_entry is not None:
                    self.pending_entry.trigger_price = float(bar.close)
                    self.pending_entry.status = "waiting_for_flat"
            self.pending_exit_trigger = None
            signal.metadata.update(self._supertrend_metadata(
                value, bar, calculation_ms))
            self._freeze_signal_inputs(signal, value, bar)
            if paired is not None:
                paired.metadata.update(self._supertrend_metadata(
                    value, bar, calculation_ms))
                self._freeze_signal_inputs(paired, value, bar)
        return signal

    @staticmethod
    def _freeze_signal_inputs(signal, value, bar) -> None:
        """Keep the exact completed candle and indicator inputs with the signal."""
        indicator_values = {
            "supertrend_atr": value.atr,
            "supertrend_upper": value.upper,
            "supertrend_lower": value.lower,
            "supertrend_trend": value.trend,
        }
        freeze_signal_context(
            signal, bar=bar, indicator_values=indicator_values)

    @staticmethod
    def _supertrend_metadata(value, bar, calculation_ms: float = 0.0) -> dict:
        return {
            "strategy_type": "supertrend",
            "strategy_timeframe": "15m",
            "supertrend_atr": value.atr,
            "supertrend_upper": value.upper,
            "supertrend_lower": value.lower,
            "supertrend_trend": value.trend,
            "signal_candle_start": bar.start_ts,
            "signal_candle_open": bar.open,
            "signal_candle_high": bar.high,
            "signal_candle_low": bar.low,
            "signal_candle_close": bar.close,
            "signal_candle_volume": bar.volume,
            "signal_source": "completed_native_15m_candle",
            "indicator_calculation_ms": round(float(calculation_ms), 3),
        }

    def snapshot(self) -> dict:
        result = super().snapshot()
        result.update({
            "strategy_type": self.strategy_type,
            "timeframe": "15m",
            "supertrend": self.supertrend.snapshot(),
        })
        return result
