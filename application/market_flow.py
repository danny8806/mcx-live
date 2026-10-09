"""Market tick, candle, and broker-fill event adapters."""
from __future__ import annotations

import logging
import math
import time
from typing import Optional

from monitoring.health import SystemStatus
from strategies.base import StrategyBase

log = logging.getLogger(__name__)

class MarketEventFlowMixin:
    """Translate external feed callbacks into the engine event flow."""

    def _make_candle_handler(self, strategy: StrategyBase, env_name: str = "paper"):
        def handler(event):
            if not self._running:
                return
            with self._lock:
                self.health.record_bar()
                self.market_status.mark_rest_data_fresh()
                self._maybe_enable_trading()

                env = self._env_for(env_name)
                # REST recovery can return several already-closed bars at
                # once. Keep SuperTrend's indicator state current by warming
                # through old bars, but never turn a stale historical flip
                # into a late live market order. Only fresh completed REST
                # candles are actionable in the live strategy.
                if (getattr(env, "is_live", False)
                        and getattr(event, "source", None) == "rest"):
                    live_cfg = self.config.get("live", {}) or {}
                    market_data_cfg = live_cfg.get("market_data", {}) or {}
                    max_age = float(market_data_cfg.get(
                        "max_signal_age_seconds", 60.0))
                    age_seconds = max(0.0, time.time() - float(event.end_ts))
                    if age_seconds > max_age:
                        if event.timeframe == strategy.signal_timeframe:
                            from core.timeframe_engine import Bar
                            strategy.warmup_indicator(Bar(
                                instrument=event.instrument,
                                timeframe=event.timeframe,
                                start_ts=event.start_ts,
                                end_ts=event.end_ts,
                                open=event.open, high=event.high,
                                low=event.low, close=event.close,
                                volume=int(event.volume), source="rest_catchup",
                            ))
                        self.publish_event("stale_signal_candle_skipped", {
                            "strategy_id": strategy.strategy_id,
                            "instrument": event.instrument,
                            "timeframe": event.timeframe,
                            "candle_start": event.start_ts,
                            "candle_end": event.end_ts,
                            "age_seconds": round(age_seconds, 3),
                            "max_actionable_age_seconds": max_age,
                            "action": "indicator_warmed_no_order",
                            "execution_mode": env.mode,
                        }, env_name=env.name)
                        return

                is_signal_bar = (event.timeframe == strategy.signal_timeframe)
                signal = strategy.on_candle(event)
                registry = getattr(env, "pending_triggers", None)
                if registry is not None:
                    registry.sync_strategy(strategy)
                if signal and is_signal_bar:
                    self._bind_signal_position(signal, strategy, env_name)
                    self._process_signal(signal, env_name)
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
            received_at = tick.get("receive_timestamp") or timestamp
            volume = tick.get("volume", 0.0)
        else:
            instrument = getattr(tick, "instrument", None)
            ltp = getattr(tick, "ltp", 0.0)
            timestamp = (getattr(tick, "event_timestamp", None)
                         or getattr(tick, "timestamp", None) or time.time())
            received_at = getattr(tick, "receive_timestamp", None) or timestamp
            volume = getattr(tick, "volume", 0.0)
        if not instrument:
            return

        valid_ltp = (isinstance(ltp, (int, float))
                     and ltp > 0.0
                     and not (isinstance(ltp, float) and (math.isnan(ltp) or math.isinf(ltp))))

        # Market-data bookkeeping (always, even for a bad-LTP sentinel tick)
        ws = getattr(self.data_adapter, "ws", None)
        ws_connected = bool(ws and ws.connected)

        # Per-instrument WebSocket tick freshness. REST candles are handled
        # separately and cannot make a disconnected tick feed look healthy.
        health = getattr(self, "market_data_health", None)
        if health is not None:
            if valid_ltp and (ws is None or ws_connected):
                health.record_tick(instrument, received_at)
            elif ws is not None and not ws_connected:
                health.mark_unhealthy(instrument)
            # An old exchange timestamp, or a callback arriving after the
            # socket closed, cannot drive a local stop or strategy trigger.
            if not health.is_healthy(instrument):
                valid_ltp = False
        if ws is not None and not ws_connected:
            valid_ltp = False

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
