"""Environment construction and dependency wiring for the trading engine."""
from __future__ import annotations

import logging
import math
import time
from typing import Any, Optional

from config import Config
from data.dhan import DhanDataAdapter
from core.market_status import MarketStatus, EnvMarketStatus
from core.safe_mode import SafeModeManager
from core.environments import Environment
from events.bus import EventBus
from data.native_streams import NativeCandleDistributor
from data.native_router import NativeCandleRouter
from strategies.gold import create_gold_15m
from strategies.silver import create_silver_15m
from execution.broker_router import BrokerEventRouter
from execution.fee_model import MCXFeeModel
from execution.order_manager import OrderManager, OrderManagerFacade
from portfolio.position_manager import PositionManager, PositionManagerFacade
from portfolio.pnl import PNLEngine
from portfolio.account import AccountEngine
from monitoring.health import HealthMonitor
from core.risk_engine import RiskEngine
from strategies.runtime import StrategyRuntime, StrategyRuntimeRegistry
from core.lifecycle import TradeLifecycleManager
from execution.live.ownership import validate_live_order_ownership

log = logging.getLogger(__name__)

STRATEGY_FACTORIES = {
    "gold_02": create_gold_15m,
    "silver_01": create_silver_15m,
}

# This application has exactly two production profiles. Configuration can tune
# their SuperTrend parameters and quantities, but cannot add a profile or feed.
EXCLUSIVE_STRATEGY_CONTRACT = {
    "gold_02": {"strategy_type": "supertrend", "instrument": "GOLDM",
                "timeframe": "15m"},
    "silver_01": {"strategy_type": "supertrend", "instrument": "SILVERM",
                  "timeframe": "15m"},
}

class EnvironmentFactoryMixin:
    """Build isolated environment infrastructure and wire dependencies."""

    def _runtime_instrument_configs(self) -> dict:
        """Resolve the only instruments the exclusive runtime may subscribe to."""
        configured = self.config.get("instruments", {}) or {}
        policy = self.config.get("strategy_runtime", {}) or {}
        if not policy.get("exclusive"):
            return dict(configured)
        allowlist = policy.get("allowlist", {}) or {}
        if set(allowlist) != set(EXCLUSIVE_STRATEGY_CONTRACT):
            raise RuntimeError(
                "STRATEGY_RUNTIME_ALLOWLIST_MISMATCH: exclusive LIVE must allow "
                "exactly gold_02 and silver_01")
        instruments = {}
        for strategy_id, expected in EXCLUSIVE_STRATEGY_CONTRACT.items():
            allowed = allowlist.get(strategy_id) or {}
            cfg = (self.config.get("strategies", {}) or {}).get(strategy_id) or {}
            timeframe = cfg.get("signal_timeframe", "15m")
            if any(allowed.get(key) != value for key, value in expected.items()):
                raise RuntimeError(
                    f"STRATEGY_RUNTIME_ALLOWLIST_MISMATCH: {strategy_id} must be "
                    f"{expected}")
            if (cfg.get("instrument") != expected["instrument"]
                    or cfg.get("strategy_type") != expected["strategy_type"]
                    or timeframe != expected["timeframe"]):
                raise RuntimeError(
                    f"STRATEGY_RUNTIME_POLICY_MISMATCH: {strategy_id} config must "
                    f"match {expected}")
            if cfg.get("strategy_type") == "disabled":
                raise RuntimeError(
                    f"STRATEGY_RUNTIME_DISABLED: required profile {strategy_id} "
                    "cannot be removed from the exclusive runtime")
            if not cfg.get("enabled", True):
                raise RuntimeError(
                    f"STRATEGY_RUNTIME_DISABLED: {strategy_id} must remain "
                    "constructed; pause entries with its strategy gate")
            instrument = expected["instrument"]
            if instrument not in configured:
                raise RuntimeError(
                    f"STRATEGY_RUNTIME_INSTRUMENT_MISSING: {instrument}")
            instruments[instrument] = configured[instrument]
        return instruments

    def _scope_live_candles_to_runtime(self, env: Environment) -> None:
        """Fetch strategy candles only for active profiles, plus open positions.

        Keeping open-position instruments in scope preserves candle-based
        protection/reconciliation data for positions that predate a strategy
        profile change. Idle instruments without a strategy or position do not
        consume Dhan candle requests.
        """
        fetcher = getattr(env, "candle_fetcher", None)
        policy = self.config.get("strategy_runtime", {}) or {}
        if fetcher is None or not policy.get("exclusive"):
            return

        needed = {
            str(strategy.instrument)
            for strategy in (getattr(env, "strategies", {}) or {}).values()
            if getattr(strategy, "enabled", True)
        }
        for candidate in getattr(self, "_envs", {}).values():
            manager = getattr(candidate, "position_manager", None)
            for position in getattr(manager, "open_positions", ()):
                if getattr(position, "is_open", False):
                    instrument = getattr(position, "instrument", None)
                    if instrument:
                        needed.add(str(instrument))

        # The live feed itself is restricted to the two approved instruments.
        # Historical orphan positions remain visible in account reconciliation,
        # but cannot silently add a third candle/tick subscription.
        adapter = getattr(env, "data_adapter", None)
        registered = set(getattr(adapter, "_instruments", {}) or {})
        if registered:
            needed.intersection_update(registered)

        configured = self.config.get("instruments", {}) or {}
        fetcher.instruments = {name: cfg for name, cfg in configured.items()
                               if name in needed}
        log.info("[Engine] exclusive live candle scope: %s (%s)",
                 env.name, sorted(fetcher.instruments))

    def _configured_candle_timeframes(self, env_name: str) -> list[str]:
        """This application consumes completed native 15-minute candles only."""
        configured = self.config.get("strategies", {}) or {}
        runtime_policy = self.config.get("strategy_runtime", {}) or {}
        allowlist = runtime_policy.get("allowlist", {}) or {}
        exclusive = bool(runtime_policy.get("exclusive", False))
        env_overrides = ((self.config.get(env_name, {}) or {}).get("strategies")
                         or {})
        result = []
        for strategy_id, cfg in configured.items():
            if exclusive and strategy_id not in allowlist:
                continue
            if not isinstance(cfg, dict) or not cfg.get("enabled", True):
                continue
            override = env_overrides.get(strategy_id)
            if isinstance(override, dict) and not override.get("enabled", True):
                continue
            if cfg.get("strategy_type") == "disabled":
                continue
            if cfg.get("strategy_type") == "supertrend":
                result.append("15m")
        return ["15m"]

    def _init_market_status(self) -> None:
        self.market_status = MarketStatus()
    def _init_data_adapter(self) -> None:
        # A LIVE-only engine builds its Dhan adapter inside the LIVE
        # environment below. Creating the shared/PAPER adapter here would
        # start a second token-renewal scheduler which is never connected or
        # shut down by that runtime.
        if getattr(self, "_live_only", False):
            self.data_adapter = None
            return
        dhan_config = self.config.get("dhan")
        self.data_adapter = DhanDataAdapter(
            client_id=dhan_config["client_id"],
            token_file=dhan_config.get("token_file", "dhan_token.json"),
            pin=dhan_config.get("pin", ""),
            totp_secret=dhan_config.get("totp_secret", ""),
            on_tick=self._on_tick,
            on_status=self._on_status,
        )
        instruments = self._runtime_instrument_configs()
        self.data_adapter.register_instruments(instruments)
    def _init_indicator_engines(self) -> None:
        """Create candle infrastructure; each profile owns SuperTrend state."""
        if not hasattr(self, "event_bus") or self.event_bus is None:
            self.event_bus = EventBus()
        if not hasattr(self, "candle_distributor") or self.candle_distributor is None:
            self.candle_distributor = NativeCandleDistributor(self.event_bus)
        self.indicator_engine = None
        self.indicators: dict[str, Any] = {}
    def _init_candle_fetcher(self) -> None:
        """Initialize CandleFetcher and wire to EventBus via NativeCandleRouter
        -> NativeCandleDistributor."""
        # LIVE-only builds its candle fetcher with the environment-owned
        # adapter in _build_env_data_infra; don't create an unused shared one.
        if getattr(self, "_live_only", False):
            self.candle_fetcher = None
            return
        from core.candle_fetcher import CandleFetcher
        instruments = self._runtime_instrument_configs()
        first_inst = list(instruments.values())[0] if instruments else {}
        session_open = first_inst.get("session_open", "09:00")
        session_close = first_inst.get("session_close", "23:30")
        if not hasattr(self, 'candle_router') or self.candle_router is None:
            self.candle_router = NativeCandleRouter(
                distributor=self.candle_distributor.on_candle_closed,
                instruments=instruments,
            )
        self.candle_fetcher = CandleFetcher(
            data_adapter=self.data_adapter,
            instruments=instruments,
            on_candle_closed=self.candle_router.on_candle_closed,
            session_open=session_open,
            session_close=session_close,
            market_status=self.market_status,
            timeframes=self._configured_candle_timeframes("paper"),
        )
    def _init_timeframe_engine(self) -> None:
        """Initialize the REST CandleFetcher as the strategy candle source.

        Backward-compatible alias for _init_candle_fetcher: closed bars flow
        from the CandleFetcher through EventBus to per-strategy handlers.
        """
        self._init_candle_fetcher()
    def _build_env_data_infra(self, env: Environment) -> None:
        """Create fully independent data infrastructure for a LIVE environment.

        Each LIVE env gets its own DhanDataAdapter, EventBus,
        NativeCandleDistributor, NativeCandleRouter, CandleFetcher,
        MarketStatus, and SafeModeManager.

        PAPER environments use the shared engine-level instances (no-op here).
        """
        if not env.is_live:
            return

        from core.candle_fetcher import CandleFetcher
        from core.market_status import EnvMarketStatus
        from core.safe_mode import SafeModeManager

        # Resolve LIVE data config — fall back to global dhan config
        live_cfg = self.config.get("live", {})
        dhan_cfg = live_cfg.get("dhan") or self.config.get("dhan", {})
        instruments = self._runtime_instrument_configs()

        # Per-env EventBus
        env.event_bus = EventBus()

        # Per-env CandleDistributor (wired to env's own EventBus)
        env.candle_distributor = NativeCandleDistributor(env.event_bus)

        # Per-env CandleRouter (wired to env's CandleDistributor)
        env.candle_router = NativeCandleRouter(
            distributor=env.candle_distributor.on_candle_closed,
            instruments=instruments,
        )

        # Per-env IndicatorEngine
        env.indicator_engine = None

        # Per-env MarketStatus + SafeMode
        env.market_status = EnvMarketStatus(self.market_status)
        env.safe_mode = SafeModeManager(env.market_status)

        # Per-env DhanDataAdapter
        on_tick = self._make_env_tick_handler(env)
        on_status = self._make_env_status_handler(env)
        client_id = dhan_cfg.get("client_id")
        if not client_id:
            log.warning("[Engine] LIVE env %s: no dhan.client_id — data "
                        "adapter deferred", env.name)
        else:
            env.data_adapter = DhanDataAdapter(
                client_id=client_id,
                token_file=dhan_cfg.get("token_file", "dhan_token.json"),
                pin=dhan_cfg.get("pin", ""),
                totp_secret=dhan_cfg.get("totp_secret", ""),
                on_tick=on_tick,
                on_status=on_status,
            )
            env.data_adapter.register_instruments(instruments)

            # Per-env CandleFetcher (wired to env's CandleRouter)
            first_inst = list(instruments.values())[0] if instruments else {}
            session_open = first_inst.get("session_open", "09:00")
            session_close = first_inst.get("session_close", "23:30")

            def on_candle_closed(bar, _router=env.candle_router):
                _router.on_candle_closed(bar)

            env.candle_fetcher = CandleFetcher(
                data_adapter=env.data_adapter,
                instruments=instruments,
                on_candle_closed=on_candle_closed,
                session_open=session_open,
                session_close=session_close,
                market_status=env.market_status,
                timeframes=self._configured_candle_timeframes(env.name),
            )
            log.info("[Engine] LIVE env data infra: separate Dhan adapter + "
                     "EventBus + candle pipeline + indicators + market status")
    def _make_env_tick_handler(self, env: Environment):
        """Create a tick handler bound to a specific LIVE environment.

        The handler updates the env's own MarketStatus and feeds ticks
        through the env's own EventBus + execution engine.
        """
        def handler(tick):
            if not self._running:
                return
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

            # Update env's own market status
            ws = getattr(env.data_adapter, "ws", None)
            ws_connected = bool(ws and ws.connected)
            env.market_status.update_data_status(
                connected=ws_connected,
                last_tick_time=(ws._last_tick_time if ws else 0.0),
            )

            # Track feed freshness and refuse stale prices for marks and order
            # recovery. SuperTrend decisions themselves use completed 15m bars.
            health = getattr(self, "market_data_health", None)
            if health is not None:
                if valid_ltp and (ws is None or ws_connected):
                    health.record_tick(instrument, received_at)
                elif ws is not None and not ws_connected:
                    health.mark_unhealthy(instrument)
                if not health.is_healthy(instrument):
                    valid_ltp = False
            if ws is not None and not ws_connected:
                valid_ltp = False

            with self._lock:
                if valid_ltp:
                    try:
                        env.execution_engine.update_price(instrument, ltp)
                        for pos in env.position_manager.get_positions_by_instrument(instrument):
                            if pos.is_open:
                                pos.update_mark(ltp)
                    except Exception:
                        pass

                event = TickEvent(
                    instrument=instrument, ltp=float(ltp) if valid_ltp else 0.0,
                    timestamp=float(timestamp or time.time()), volume=float(volume or 0.0),
                )
                env.event_bus.publish(f"tick:{instrument}", event)

        return handler
    def _make_env_status_handler(self, env: Environment):
        """Create a status handler bound to a specific LIVE environment."""
        def handler(status):
            pass
        return handler
    def _legacy_paper_env(self) -> Optional[Environment]:
        """Resolve the PAPER environment, lazily building it for the legacy
        ``__new__``-constructed engine surface (crash-replay harness wires the
        engine via _init_strategies/_init_execution/... directly)."""
        if getattr(self, "paper", None) is not None:
            return self.paper
        if getattr(self, "_envs", None) is None:
            self._envs = {}
        if "paper" not in self._envs:
            self._envs["paper"] = self._build_environment("paper")
        self.strategies = self._envs["paper"].strategies
        self.runtimes = self._envs["paper"].runtimes
        self.execution_engine = self._envs["paper"].execution_engine
        self.order_manager = self._envs["paper"].order_manager
        self.broker_router = self._envs["paper"].broker_router
        self.position_manager = self._envs["paper"].position_manager
        self.pnl_engines = self._envs["paper"].pnl_engines
        self.account_engines = self._envs["paper"].account_engines
        self.account_engine = self._envs["paper"].account_engine
        self.risk_engine = self._envs["paper"].risk_engine
        return self._envs["paper"]
    def _init_strategies(self) -> None:
        """Backward-compat: rebuild the PAPER environment's strategies."""
        env = self._legacy_paper_env()
        if env is not None:
            self._build_strategies_for_env(env)
            self.strategies = env.strategies
    def _build_strategies_for_env(self, env: Environment) -> None:
        """Create the two isolated SuperTrend profiles and wire candle events.

        Each environment uses its OWN event_bus and indicator engine:
        - PAPER: the engine-level shared instances (backward-compatible)
        - LIVE: the per-env independent instances (LIVE independence)

        Both produce identical signals for the same input candles (parity by
        construction), but are fully independent so data issues in one env
        never affect the other.
        """
        env.strategies = {}
        strategies_config = self.config.get("strategies", {})
        instruments_config = self.config.get("instruments", {})
        runtime_policy = self.config.get("strategy_runtime", {}) or {}
        if env.is_live and not runtime_policy.get("exclusive"):
            raise RuntimeError(
                "STRATEGY_RUNTIME_POLICY_REQUIRED: LIVE requires the exclusive "
                "GOLDM/SILVERM 15m SuperTrend policy")
        env_cfg = self.config.get(env.name) or {}
        env_strats_cfg = env_cfg.get("strategies", {}) or {}

        # SuperTrend owns its indicator state; the event bus only transports
        # completed native candles and lifecycle events.
        event_bus = env.event_bus if env.event_bus is not None else getattr(
            self, "event_bus", None)
        indicator_engine = None

        for strat_name, strat_config in strategies_config.items():
            allowlist = runtime_policy.get("allowlist", {}) or {}
            if runtime_policy.get("exclusive") and strat_name not in allowlist:
                log.info("[Engine] strategy excluded by exclusive runtime policy: %s",
                         strat_name)
                continue
            if not strat_config.get("enabled", True):
                continue
            if strat_config.get("strategy_type") != "supertrend":
                raise RuntimeError(
                    f"STRATEGY_TYPE_RETIRED: {strat_name} must use SuperTrend")
            # Optional per-env override: env.<name>.strategies.<id>.enabled
            over = env_strats_cfg.get(strat_name)
            if isinstance(over, dict) and over.get("enabled", True) is False:
                continue
            factory = STRATEGY_FACTORIES.get(strat_name)
            if not factory:
                log.warning("[Engine] Unknown strategy: %s", strat_name)
                continue

            instrument = strat_config.get("instrument", "GOLDM")
            expected_profile = EXCLUSIVE_STRATEGY_CONTRACT.get(strat_name)
            if expected_profile is None:
                raise RuntimeError(
                    f"STRATEGY_ID_RETIRED: {strat_name} is not an active profile")
            configured_timeframe = strat_config.get("signal_timeframe", "15m")
            if (instrument != expected_profile["instrument"]
                    or strat_config.get("strategy_type") != "supertrend"
                    or configured_timeframe != "15m"):
                raise RuntimeError(
                    f"STRATEGY_RUNTIME_POLICY_MISMATCH: {strat_name} must match "
                    f"{expected_profile}")
            if runtime_policy.get("exclusive"):
                allowed = allowlist[strat_name]
                if (strat_config.get("instrument") != allowed.get("instrument")
                        or strat_config.get("strategy_type") != allowed.get("strategy_type")
                        or configured_timeframe != allowed.get("timeframe")):
                    raise RuntimeError(
                        "STRATEGY_RUNTIME_POLICY_MISMATCH: "
                        f"{strat_name} config must match its exclusive runtime "
                        f"allowlist ({allowed})")
            inst_cfg = instruments_config.get(instrument, {})
            strategy_kwargs = dict(
                strategy_id=strat_name,
                instrument=instrument,
                quantity=strat_config.get("quantity", 1),
                capital=strat_config.get("capital", 300_000.0),
                multiplier=inst_cfg.get("multiplier", 10.0),
                security_id=str(inst_cfg.get("security_id", "") or ""),
            )
            strategy_kwargs["strategy_type"] = strat_config.get("strategy_type", "supertrend")
            if strategy_kwargs["strategy_type"] == "supertrend":
                strategy_kwargs["supertrend"] = strat_config.get("supertrend", {})
            strategy = factory(**strategy_kwargs)
            env.strategies[strat_name] = strategy

            # Each SuperTrend profile calculates its own ATR and bands.
            strategy.bind_shared_indicators(indicator_engine)

            # Subscribe to candle events (env-aware handler -> env signal path)
            strategy_handlers = []
            for sub in strategy.subscriptions:
                topic = f"candle:{sub}"
                callback = self._make_candle_handler(strategy, env.name)
                event_bus.subscribe(
                    topic, callback)
                strategy_handlers.append((topic, callback))
            if not hasattr(env, "strategy_handlers"):
                env.strategy_handlers = {}
            env.strategy_handlers[strat_name] = strategy_handlers
    def _init_execution(self) -> None:
        """Backward-compat: rebuild the PAPER environment's execution."""
        env = self._legacy_paper_env()
        if env is not None:
            self._build_execution_for_env(env)
            self.execution_engine = env.execution_engine
            self.order_manager = env.order_manager
            self.broker_router = env.broker_router
    def _build_execution_for_env(self, env: Environment) -> None:
        """Give each environment its own execution transport.

        PAPER -> PaperExecutionEngine (synthetic fills, random-free slippage).
        LIVE  -> LiveExecutionEngine over a broker client. The default is the
        deterministic StubLiveBroker with the master gate OFF, so the live
        environment exists, generates/persists signals and refuses every order
        (LIVE_GATE_CLOSED). Setting ``live.broker: "dhan"`` selects the Phase-2
        DhanRestTransport (the real Dhan v2 REST account authority); both honor
        the master gate.
        """
        if env.is_live:
            from execution.live.engine import LiveExecutionEngine
            live_cfg = self.config.get("live", {})
            # Entries allowed only when the master gate is ON.  The initial
            # state was set on the env by _build_environment (live.gate).
            gate_enabled = bool(env.gate_enabled)
            broker = self._build_live_broker(live_cfg, gate_enabled, db_path=env.db_path)
            env.broker = broker
            env.execution_engine = LiveExecutionEngine(
                broker=broker, price_preset=self._build_price_preset(live_cfg))
            env.execution_engine.submission_guard = (
                lambda order, _env=env: self._validate_live_order_ownership(_env, order))
        else:
            # PAPER-only backward-compat branch — reachable ONLY from the
            # legacy crash-replay/test harness that builds a paper environment.
            # Imported lazily so the LIVE runtime never loads execution.paper_broker.
            from execution.paper_broker import PaperExecutionEngine
            paper_config = self.config.get("paper_execution", {})
            env.execution_engine = PaperExecutionEngine(
                slippage_ticks=paper_config.get("slippage_ticks", 1),
                latency_ms=paper_config.get("latency_ms", 100),
                partial_fill_probability=paper_config.get("partial_fill_probability", 0.0),
            )
        env.order_manager = OrderManagerFacade(execution_engine=env.execution_engine)
        # §39–40 — BrokerEventRouter is the single broker-event choke point:
        # every fill/order event routes by EXPLICIT broker_order_id -> strategy
        # mapping (never symbol/side/latest order); unmappable events are
        # quarantined. Each environment gets its own router + persistence.
        env.broker_router = BrokerEventRouter(persistence=None)
        env.execution_engine.broker_router = env.broker_router
    def _build_price_preset(self, live_cfg: dict):
        """Build the fixed SuperTrend market-at-close execution plan."""
        from execution.price_model import PricePreset
        return PricePreset()
    def _validate_live_order_ownership(self, env, order) -> Optional[str]:
        """Fail-closed strategy, execution-model, and position checks at Dhan."""
        reason = validate_live_order_ownership(env, order)
        if reason:
            return reason
        order_type = str(getattr(order, "order_type", "") or "").upper()
        if order_type != "MARKET":
            return "SUPERTREND_MARKET_ONLY"
        role = str(getattr(order, "order_role", "") or "").upper()
        if role in {"EXIT", "REVERSAL_EXIT", "EMERGENCY_EXIT"}:
            reason = self._validate_broker_position_for_exit(env, order)
            if reason:
                return reason
        if role == "EMERGENCY_EXIT":
            return None
        strategy = (getattr(env, "strategies", {}) or {}).get(order.strategy_id)
        if strategy is None:
            return "ORDER_STRATEGY_UNKNOWN"
        if role in {"ENTRY", "REVERSAL_ENTRY", "FALLBACK_MARKET"}:
            if not getattr(strategy, "enabled", True):
                return "STRATEGY_DISABLED"
            if not getattr(env, "gate_enabled", False):
                return "LIVE_GATE_CLOSED"
            gate = self._gate_for(order.strategy_id)
            if not gate.entries_allowed:
                return "STRATEGY_ENTRY_GATE_CLOSED"
            # Preserve reversal intent when applying its entry gate.
            is_reversal_intent = (
                role == "REVERSAL_ENTRY"
                or (role == "FALLBACK_MARKET" and bool(
                    getattr(order, "reversal_parent_signal_id", None)))
            )
            if is_reversal_intent and not gate.reversal_enabled:
                return "STRATEGY_REVERSAL_GATE_CLOSED"
        elif role == "REVERSAL_EXIT":
            if not self._gate_for(order.strategy_id).reversal_enabled:
                return "STRATEGY_REVERSAL_GATE_CLOSED"
        elif role == "EXIT":
            if not self._gate_for(order.strategy_id).exit_enabled:
                return "STRATEGY_EXIT_GATE_CLOSED"
        if role in {"ENTRY", "REVERSAL_ENTRY", "FALLBACK_MARKET",
                    "EXIT", "REVERSAL_EXIT"}:
            if str(getattr(order, "trigger_state", "") or "").upper() != "FIRED":
                return "ORDER_TRIGGER_NOT_FIRED"
            is_fired = getattr(strategy, "is_fired_trigger_signal", None)
            if callable(is_fired):
                fired = bool(is_fired(getattr(order, "parent_signal_id", None)))
            else:  # duck-typed stand-ins in tests
                fired = (getattr(strategy, "_last_fired_trigger_signal_id", None)
                         == getattr(order, "parent_signal_id", None))
            if not fired:
                return "ORDER_TRIGGER_SIGNAL_STALE"
            if (getattr(order, "trigger_generation", None)
                    != getattr(strategy, "_trigger_generation", None)):
                return "ORDER_TRIGGER_GENERATION_STALE"
        return None

    @staticmethod
    def _validate_broker_position_for_exit(env, order) -> Optional[str]:
        """Prove Dhan still has enough matching exposure before closing it.

        The local position/lifecycle check prevents stale strategy exits. This
        second check prevents a stale local position from sending the opposite
        side after Dhan is already flat. Dhan positions are expanded to one row
        per configured strategy, so use the largest matching instrument-level
        quantity rather than summing duplicate strategy rows. Reserve the
        unfilled remainder of other active close orders to avoid overshooting
        shared instrument exposure.
        """
        broker = getattr(env, "broker", None)
        positions_fn = getattr(broker, "positions", None)
        if not callable(positions_fn):
            return "BROKER_POSITION_UNAVAILABLE"
        try:
            rows = positions_fn()
        except Exception:
            return "BROKER_POSITION_QUERY_FAILED"
        if not isinstance(rows, (list, tuple)):
            return "BROKER_POSITION_QUERY_INVALID"

        close_side = str(getattr(order, "side", "") or "").upper()
        expected_position_side = "LONG" if close_side == "SELL" else "SHORT"
        aliases = {expected_position_side,
                   "BUY" if expected_position_side == "LONG" else "SELL"}
        broker_quantity = 0
        for row in rows:
            if not isinstance(row, dict):
                continue
            if str(row.get("instrument") or "") != str(order.instrument):
                continue
            side = str(row.get("side") or "").upper()
            try:
                quantity = abs(int(row.get("quantity") or 0))
            except (TypeError, ValueError):
                continue
            if side in aliases and quantity > broker_quantity:
                broker_quantity = quantity
        if broker_quantity <= 0:
            return "BROKER_POSITION_NOT_OPEN"

        reserved = 0
        engine = getattr(env, "execution_engine", None)
        active_states = {"created", "submitted", "acknowledged", "partially_filled"}
        close_roles = {"EXIT", "REVERSAL_EXIT", "EMERGENCY_EXIT"}
        for prior in (getattr(engine, "_orders", {}) or {}).values():
            if prior is order:
                continue
            if (str(getattr(prior, "instrument", "")) != str(order.instrument)
                    or str(getattr(prior, "side", "")).upper() != close_side
                    or str(getattr(getattr(prior, "state", None), "value",
                                    getattr(prior, "state", ""))).lower()
                        not in active_states
                    or str(getattr(prior, "order_role", "") or "").upper()
                        not in close_roles):
                continue
            try:
                reserved += max(
                    0, int(getattr(prior, "quantity", 0) or 0)
                    - int(getattr(prior, "filled_quantity", 0) or 0))
            except (TypeError, ValueError):
                return "BROKER_EXIT_RESERVATION_INVALID"

        available = max(0, broker_quantity - reserved)
        if available < int(getattr(order, "quantity", 0) or 0):
            return "BROKER_POSITION_QUANTITY_INSUFFICIENT"
        return None

    def _build_live_broker(self, live_cfg: dict, gate_enabled: bool,
                           db_path: Optional[str] = None):
        """Resolve the LIVE environment's broker client from ``live.broker``.

        Default / ``stub`` -> :class:`StubLiveBroker` (deterministic, used with
        the master gate OFF and in tests).  ``dhan`` / ``dhan_rest`` ->
        :class:`DhanRestTransport` over the real Dhan v2 REST account.  Both
        enforce the master gate; polling/reconciliation is transport-agnostic.

        When ``db_path`` is supplied the real Dhan transport is wired to the
        protocol's COMPLETE broker-response audit store (appendix §L) so every
        order/market/cancel/modify/status/funds/positions/tradebook wire call
        is persisted (sanitized) for forensic reconstruction.
        """
        from execution.live.broker_client import StubLiveBroker
        mode = str(live_cfg.get("broker") or live_cfg.get("transport") or "stub").lower()
        if mode not in ("dhan", "dhan_rest"):
            return StubLiveBroker(gate_enabled=gate_enabled)
        from execution.live.dhan_transport import DhanRestTransport
        instruments: dict = {}
        instrument_strategies: dict[str, list] = {}
        for name, inst_cfg in self._runtime_instrument_configs().items():
            instruments[name] = {
                "security_id": inst_cfg.get("security_id", ""),
                "exchange_segment": inst_cfg.get("exchange_segment", "MCX_COMM"),
                "symbol": inst_cfg.get("symbol", ""),
            }
        for sid, strat_cfg in self.config.get("strategies", {}).items():
            runtime_policy = self.config.get("strategy_runtime", {}) or {}
            allowlist = runtime_policy.get("allowlist", {}) or {}
            if runtime_policy.get("exclusive") and sid not in allowlist:
                continue
            inst = strat_cfg.get("instrument")
            if inst:
                instrument_strategies.setdefault(inst, []).append(sid)
        audit_store = None
        if db_path:
            try:
                from persistence.broker_audit import BrokerAuditStore
                audit_store = BrokerAuditStore(db_path=str(db_path),
                                               execution_mode="LIVE")
            except Exception:
                audit_store = None  # audit persistence never blocks trading
        return DhanRestTransport.from_config(
            dhan_config=self.config.get("dhan", {}),
            instruments=instruments,
            instrument_strategies=instrument_strategies,
            gate_enabled=gate_enabled,
            audit_store=audit_store,
        )
    def _init_portfolio(self) -> None:
        """Backward-compat: rebuild the PAPER environment's portfolio."""
        env = self._legacy_paper_env()
        if env is not None:
            self._build_portfolio_for_env(env)
            self.position_manager = env.position_manager
            self.pnl_engines = env.pnl_engines
            self.account_engines = env.account_engines
            self.account_engine = env.account_engine
    def _build_portfolio_for_env(self, env: Environment) -> None:
        account_config = self.config.get("account", {})
        env.position_manager = PositionManagerFacade()
        default_capital = account_config.get("starting_capital_per_strategy", 300_000.0)
        margin_pct = self.config.get("risk", {}).get("margin_per_trade_pct", 6.5)

        pnl_engines = {}
        account_engines = {}
        charges_config = self.config.get("charges", {})
        for strat_name in env.strategies:
            instrument = env.strategies[strat_name].instrument
            # Fee model is derived from the instrument's own charges config
            # (e.g. stamp_duty_pct) — never global defaults that contradict it.
            inst_charges = charges_config.get(instrument, {})
            fee_model = MCXFeeModel.from_config(inst_charges) if inst_charges else MCXFeeModel()
            pnl_engines[strat_name] = PNLEngine(fee_model=fee_model)
            account_engines[strat_name] = AccountEngine(
                starting_capital=default_capital,
                margin_per_trade_pct=margin_pct,
            )
        # Global account engine (compat aggregate) — seeded with TOTAL capital
        # (account.starting_capital), NOT the per-strategy allocation.
        global_capital = account_config.get("starting_capital", 600_000.0)
        env.pnl_engines = pnl_engines
        env.account_engines = account_engines
        env.account_engine = AccountEngine(
            starting_capital=global_capital, margin_per_trade_pct=margin_pct)
    def _init_risk(self) -> None:
        """Backward-compat: rebuild the PAPER environment's risk engine."""
        env = self._legacy_paper_env()
        if env is not None:
            self._build_risk_for_env(env)
            self.risk_engine = env.risk_engine
    def _build_risk_for_env(self, env: Environment) -> None:
        risk_config = self.config.get("risk", {})
        # C7 — every risk key in config is wired into the engine.  The config
        # documents the limit as max_open_positions_per_strategy, while the
        # engine parameter has always been max_positions_per_strategy; both
        # spellings are honored (the documented one wins).
        per_strategy = risk_config.get(
            "max_open_positions_per_strategy",
            risk_config.get("max_positions_per_strategy", 1))
        env.risk_engine = RiskEngine(
            max_positions_per_strategy=int(per_strategy),
            max_positions_total=int(risk_config.get(
                "max_open_positions_total", 8) or 8),
            max_daily_loss=float(risk_config.get("max_daily_loss", 999_999_999.0)),
            max_drawdown_pct=float(risk_config.get("max_drawdown_pct", 100.0)),
            kill_switch_enabled=bool(risk_config.get("kill_switch_enabled", False)),
            kill_switch_callback=self._notify_kill_switch,
        )
    def _notify_kill_switch(self, reason: str = "") -> None:
        """Send a CRITICAL risk alert when the kill switch activates."""
        try:
            self.telegram.on_risk_alert({
                "severity": "CRITICAL",
                "type": "KILL_SWITCH",
                "message": (f"KILL SWITCH ACTIVATED — all trading halted"
                            + (f" ({reason})" if reason else "")),
                "strategy_id": "all",
                "instrument": "PORTFOLIO",
            })
        except Exception:
            pass
    def _init_monitoring(self) -> None:
        self.health = HealthMonitor()
    def _init_notifications(self) -> None:
        telegram_config = self.config.get("telegram", {})
        bot_token = telegram_config.get("bot_token", "")
        chat_id = telegram_config.get("chat_id", "")
        if bot_token and chat_id:
            from notifications.telegram_client import TelegramClient
            client = TelegramClient(bot_token=bot_token, chat_id=chat_id)
        else:
            from notifications.telegram_client import TelegramClient
            client = TelegramClient()
        # Durable alert ledger (spec §AG): the router records EVERY material
        # event then dispatches to Telegram; delivery outcome is written back
        # async.  Persisted on the LIVE db so alerts survive alongside orders.
        ledger = None
        try:
            from persistence.alert_ledger import AlertEventLedger
            from config import Config
            ledger_path = Config.resolve_path(self.config.get(
                "system", {}).get(
                    "live_db_path",
                    "live/data/db/supertrend/supertrend_live.sqlite3"))
            ledger = AlertEventLedger(db_path=str(ledger_path))
        except Exception as e:
            log.warning("Telegram alert ledger disabled: %s", e)
        from notifications.telegram_router import TelegramRouter
        self.telegram = TelegramRouter(client=client, ledger=ledger)
    def _build_runtimes(self, persistence, env_name: str = "paper") -> None:
        """Backward-compat: build runtimes for the PAPER environment."""
        env = self._legacy_paper_env() if env_name == "paper" else self._env_for(env_name)
        self._build_runtimes_for_env(env, persistence)
        if env.name == "paper":
            self.runtimes = env.runtimes
    def _build_runtimes_for_env(self, env: Environment, persistence=None) -> None:
        """Build (or rebuild) one isolated StrategyRuntime per strategy in the
        given environment.

        Each runtime owns its own TradeLifecycleManager (scoped to the
        strategy), OrderManager (over the environment's execution transport),
        and PositionManager. When persistence is available the lifecycle
        caches are restored from the ENVIRONMENT's own db filtered by
        strategy_id. This is the ONLY place StrategyRuntime objects are
        created — set_persistence() no longer wipes strategy/position state.
        """
        if persistence is not None:
            repair_fill_links = getattr(persistence, "backfill_trade_fill_links", None)
            if callable(repair_fill_links):
                try:
                    repaired = repair_fill_links()
                    if repaired:
                        log.warning("[Engine] repaired %d persisted fill/trade links",
                                    repaired)
                except Exception as exc:
                    log.error("[Engine] fill lineage repair failed: %s", exc)
        registry = StrategyRuntimeRegistry()
        for sid, strategy in env.strategies.items():
            lifecycle = TradeLifecycleManager(
                persistence=persistence,
                event_store=env.event_store,
                trade_ledger=env.trade_ledger,
                strategy_id=sid,
            )
            if persistence is not None:
                try:
                    restore_ok = lifecycle.restore_from_db()
                except Exception as e:  # defensive: never let restore abort env build
                    restore_ok = False
                    log.error("[Engine] %s %s restore raised: %s",
                              env.name, sid, e)
                if not restore_ok:
                    self._restore_failures.setdefault(env.name, []).append(sid)
                    log.error(
                        "[Engine] %s strategy %s trade-restore FAILED — the "
                        "engine stands READY over un-restored open state; "
                        "broker-authoritative position reconciliation is required",
                        env.name, sid)
            order_manager = OrderManager(execution_engine=env.execution_engine)
            position_manager = PositionManager()
            runtime = StrategyRuntime(
                strategy_id=sid,
                strategy=strategy,
                lifecycle=lifecycle,
                order_manager=order_manager,
                position_manager=position_manager,
            )
            runtime.current_trade_id = getattr(strategy, "current_trade_id", None)
            registry.register(runtime)
            env.order_manager.register(sid, order_manager)
            env.position_manager.register(sid, position_manager)
        env.runtimes = registry

        # Compat view: indicator components are owned per-strategy now; expose
        # them so boot audits / dashboards still find 'engine.indicators'.
        env.indicators = {}
        for sid, strategy in env.strategies.items():
            if getattr(strategy, "strategy_type", None) == "supertrend":
                env.indicators[f"{sid}_supertrend"] = strategy.supertrend
    def _runtime(self, strategy_id: str) -> StrategyRuntime:
        """Resolve the isolated runtime (PAPER env) for a strategy (authoritative)."""
        return self.runtimes.require(strategy_id)
