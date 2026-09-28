"""Market tick, candle, and broker-fill event adapters."""
from __future__ import annotations

import logging
import math
import time
from typing import Optional

from core.timeframe_engine import Bar
from monitoring.health import SystemStatus
from strategies.instance import StrategyInstance

log = logging.getLogger(__name__)

class MarketEventFlowMixin:
    """Translate external feed callbacks into the engine event flow."""

    def _make_candle_handler(self, strategy: StrategyInstance, env_name: str = "paper"):
        def handler(event):
            if not self._running:
                return
            with self._lock:
                self.health.record_bar()
                self.market_status.mark_rest_data_fresh()
                self._maybe_enable_trading()

                bar = Bar(
                    instrument=event.instrument,
                    timeframe=event.timeframe,
                    start_ts=event.start_ts,
                    end_ts=event.end_ts,
                    open=event.open, high=event.high,
                    low=event.low, close=event.close,
                    volume=int(event.volume),
                )
                is_fast = (event.timeframe == strategy.fast_timeframe)
                signal = strategy.on_candle(event)
                if signal and is_fast:
                    self._bind_signal_position(signal, strategy, env_name)
                    self._process_signal(signal, env_name)
                    stop2 = strategy._consume_same_bar_stop(bar)
                    if stop2 is not None:
                        self._bind_signal_position(stop2, strategy, env_name)
                        self._process_signal(stop2, env_name)
        return handler
    def _make_tick_handler(self, strategy: StrategyInstance, env_name: str = "paper"):
        def handler(event):
            if not self._running:
                return
            if strategy.instrument != event.instrument:
                return
            if not (strategy.pending_entry is not None or strategy.position_side is not None):
                return
            with self._lock:
                try:
                    tick_signal = strategy.on_tick(event.ltp, event.timestamp)
                    if tick_signal:
                        self._bind_signal_position(tick_signal, strategy, env_name)
                        self._process_signal(tick_signal, env_name)
                except Exception as e:
                    log.warning("[Engine] tick handler error for %s: %s",
                                strategy.strategy_id, e)
        return handler
    def _bind_signal_position(self, signal, strategy, env_name: str) -> None:
        """Freeze current position ownership on exit/reversal signals at birth."""
        if not signal or not (getattr(signal, "metadata", None) or {}).get("exit"):
            return
        env = self._env_for(env_name)
        pos_id = getattr(strategy, "current_position_id", None)
        positions = env.position_manager.get_positions_by_strategy(signal.strategy_id)
        position = next((p for p in positions if p.is_open
                         and p.instrument == signal.instrument
                         and (not pos_id or p.position_id == pos_id)), None)
        if position is None:
            return
        signal.lifecycle_id = position.trade_id
        signal.parent_position_id = position.position_id
        signal.position_generation = position.position_generation
        md = signal.metadata or {}
        md.update({"lifecycle_id": position.trade_id,
                   "parent_position_id": position.position_id,
                   "position_generation": position.position_generation})
        signal.metadata = md
    def _on_tick(self, tick) -> None:
        """Handle WebSocket tick — update execution price + position marks + publish to EventBus.

        Accepts both dict ticks (Dhan adapter canonical format, and test
        harness) and dataclass/object ticks.
        """
        from events.types import TickEvent

        if isinstance(tick, dict):
            instrument = tick.get("instrument")
            ltp = tick.get("ltp", 0.0)
            timestamp = tick.get("event_timestamp") or tick.get("timestamp") or time.time()
            volume = tick.get("volume", 0.0)
        else:
            instrument = getattr(tick, "instrument", None)
            ltp = getattr(tick, "ltp", 0.0)
            timestamp = (getattr(tick, "event_timestamp", None)
                         or getattr(tick, "timestamp", None) or time.time())
            volume = getattr(tick, "volume", 0.0)
        if not instrument:
            return

        valid_ltp = (isinstance(ltp, (int, float))
                     and ltp > 0.0
                     and not (isinstance(ltp, float) and (math.isnan(ltp) or math.isinf(ltp))))

        # Market-data bookkeeping (always, even for a bad-LTP sentinel tick)
        ws = getattr(self.data_adapter, "ws", None)
        ws_connected = bool(ws and ws.connected)
        self.market_status.update_data_status(
            connected=ws_connected,
            last_tick_time=(ws._last_tick_time if ws else 0.0),
        )
        if ws_connected:
            ws_stats = ws._stats if hasattr(ws, "_stats") else {}
            self.health.update_component(
                "data_adapter", SystemStatus.HEALTHY,
                f"{ws_stats.get('tick', 0) if ws_stats else 0} ticks")
            if ws.is_stale() and ws_stats.get("tick", 0) > 0:
                print("[Engine] WARNING: WebSocket stale - no ticks received recently", flush=True)
                if self.market_status.is_trading_allowed:
                    self.safe_mode.enter_safe_mode("market_data_uncertain",
                                                   "WebSocket stale during trading hours")
                    try:
                        self.publish_event("safe_mode_entered", {
                            "timestamp": time.time(),
                            "reason": "market_data_uncertain",
                        })
                    except Exception:
                        pass
        else:
            self.health.update_component("data_adapter", SystemStatus.ERROR, "WebSocket disconnected")

        self.health.record_tick()
        self._maybe_enable_trading()

        with self._lock:
            if valid_ltp:
                # Feed every active execution environment the same reference
                # prices and mark each environment's own open positions.
                for env in self._envs.values():
                    try:
                        env.execution_engine.update_price(instrument, ltp)
                        for pos in env.position_manager.get_positions_by_instrument(instrument):
                            if pos.is_open:
                                pos.update_mark(ltp)
                        # Appendix I (I3/I4) — continuous fast LTP into the
                        # order watcher so resting LIMIT entries are evaluated
                        # for skip/cancel/fallback on every tick.
                        watcher = getattr(env, "order_watcher", None)
                        if watcher is not None:
                            watcher.feed_market(instrument, ltp)
                    except Exception:
                        pass

            # Always publish the tick — strategies guard on ltp <= 0/sentinels.
            event = TickEvent(
                instrument=instrument, ltp=float(ltp) if valid_ltp else 0.0,
                timestamp=float(timestamp or time.time()), volume=float(volume or 0.0),
            )
            self.event_bus.publish(f"tick:{instrument}", event)
    def _on_bar_closed(self, bar: Bar) -> None:
        """Handle closed bar — route through NativeCandleRouter to EventBus.

        Called by replay scripts and CandleFetcher callback. The router
        de-duplicates (security_id, timeframe, candle_end_ts) and drops
        out-of-order bars so replays can overlap live data safely.
        """
        if not self._running:
            return
        self.health.record_bar()
        self.market_status.mark_rest_data_fresh()

        router = getattr(self, "candle_router", None)
        if router is not None:
            router.on_candle(bar, is_complete=True)
            return

        from events.types import CandleEvent
        event = CandleEvent(
            instrument=bar.instrument, timeframe=bar.timeframe,
            start_ts=bar.start_ts, end_ts=bar.end_ts,
            open=bar.open, high=bar.high, low=bar.low,
            close=bar.close, volume=float(bar.volume),
            source="rest",
        )
        self.event_bus.publish(f"candle:{bar.instrument}:{bar.timeframe}", event)
    def _on_status(self, status) -> None:
        pass
    def _on_fill(self, fill) -> None:
        """Compatibility callback. Routes through the broker router by explicit
        broker_order_id mapping (§39); order submission passes the signal id."""
        router = getattr(self, "broker_router", None)
        if router is not None:
            router.route_fill(fill, self._handle_fill,
                              entry_signal_id=getattr(fill, "entry_signal_id", None))
        else:
            self._handle_fill(fill, getattr(fill, "entry_signal_id", None))
