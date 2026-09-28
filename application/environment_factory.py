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
from strategies.gold import create_gold_5m, create_gold_15m
from strategies.silver import create_silver_5m, create_silver_15m
from indicators.shared import SharedNativeIndicatorEngine
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
    "gold_01": create_gold_5m,
    "gold_02": create_gold_15m,
    "gold_03": lambda **kw: create_gold_5m(strategy_id="gold_03", **kw),
    "gold_04": lambda **kw: create_gold_5m(strategy_id="gold_04", **kw),
    "silver_01": create_silver_15m,
    "silver_02": create_silver_5m,
    "silver_03": lambda **kw: create_silver_5m(strategy_id="silver_03", **kw),
    "silver_04": lambda **kw: create_silver_5m(strategy_id="silver_04", **kw),
}

class EnvironmentFactoryMixin:
    """Build isolated environment infrastructure and wire dependencies."""

    def _init_market_status(self) -> None:
        self.market_status = MarketStatus()
    def _init_data_adapter(self) -> None:
        dhan_config = self.config.get("dhan")
        self.data_adapter = DhanDataAdapter(
            client_id=dhan_config["client_id"],
            token_file=dhan_config.get("token_file", "dhan_token.json"),
            pin=dhan_config.get("pin", ""),
            totp_secret=dhan_config.get("totp_secret", ""),
            on_tick=self._on_tick,
            on_status=self._on_status,
        )
        instruments = self.config.get("instruments", {})
        self.data_adapter.register_instruments(instruments)
    def _init_indicator_engines(self) -> None:
        """Backward-compat: build the shared EventBus + candle distributor +
        indicator engine surface for legacy ``__new__``-constructed engines.

        The full ``__init__`` path already builds these before reaching here
        (guards are therefore no-ops there); the guards purely re-create them
        for the crash-replay harness that wires the engine via `__new__` +
        _init_* calls and never runs __init__."""
        if not hasattr(self, "event_bus") or self.event_bus is None:
            self.event_bus = EventBus()
        if not hasattr(self, "candle_distributor") or self.candle_distributor is None:
            self.candle_distributor = NativeCandleDistributor(self.event_bus)
        if not hasattr(self, "indicator_engine") or self.indicator_engine is None:
            indicator_cfg = self.config.get("indicators", {})
            self.indicator_engine = SharedNativeIndicatorEngine(
                dema_period=indicator_cfg.get("dema_period", 3),
                atr_period=indicator_cfg.get("atr_period", 6),
                atr_factor=indicator_cfg.get("atr_factor", 1.0),
            )
        self.indicators: dict[str, Any] = {}
    def _init_htf_engine(self) -> None:
        """No longer needed — HTF state is per-strategy. Kept as no-op for compat."""
        self.htf_engine: Any = type("_FakeHTF", (), {"_engines": {}, "on_htf_bar_closed": lambda s, b: None, "map_to_fast_bar": lambda s, b, t: None, "map_mid_to_fast_bar": lambda s, b, t: None})()
    def _init_candle_fetcher(self) -> None:
        """Initialize CandleFetcher and wire to EventBus via NativeCandleRouter
        -> NativeCandleDistributor."""
        from core.candle_fetcher import CandleFetcher
        instruments = self.config.get("instruments", {})
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
        )
    def _init_timeframe_engine(self) -> None:
        """Initialize the REST CandleFetcher as the strategy candle source.

        Backward-compatible alias for _init_candle_fetcher: closed bars flow
        from the CandleFetcher through EventBus to per-strategy handlers.
        """
        self._init_candle_fetcher()
    def _build_env_data_infra(self, env: Environment) -> None:
        """Create fully independent data infrastructure for a LIVE environment.

        Each LIVE env gets its own: DhanDataAdapter, EventBus,
        NativeCandleDistributor, NativeCandleRouter, CandleFetcher,
        SharedNativeIndicatorEngine, MarketStatus, and SafeModeManager.

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
        instruments = self.config.get("instruments", {})
        indicator_cfg = self.config.get("indicators", {})

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
        env.indicator_engine = SharedNativeIndicatorEngine(
            dema_period=indicator_cfg.get("dema_period", 3),
            atr_period=indicator_cfg.get("atr_period", 6),
            atr_factor=indicator_cfg.get("atr_factor", 1.0),
        )

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
            env.candle_fetcher = CandleFetcher(
                data_adapter=env.data_adapter,
                instruments=instruments,
                on_candle_closed=env.candle_router.on_candle_closed,
                session_open=session_open,
                session_close=session_close,
                market_status=env.market_status,
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

            # Update env's own market status
            ws = getattr(env.data_adapter, "ws", None)
            ws_connected = bool(ws and ws.connected)
            env.market_status.update_data_status(
                connected=ws_connected,
                last_tick_time=(ws._last_tick_time if ws else 0.0),
            )

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
        """Create one isolated StrategyInstance set per environment and wire
        the indicator streams + event topics.

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
        env_cfg = self.config.get(env.name) or {}
        env_strats_cfg = env_cfg.get("strategies", {}) or {}

        # Resolve the event_bus and indicator_engine to use for this env.
        # PAPER uses the engine-level shared instances; LIVE uses its own.
        event_bus = env.event_bus if env.event_bus is not None else getattr(
            self, "event_bus", None)
        indicator_engine = env.indicator_engine if env.indicator_engine is not None else getattr(
            self, "indicator_engine", None)

        for strat_name, strat_config in strategies_config.items():
            if not strat_config.get("enabled", True):
                continue
            # Optional per-env override: env.<name>.strategies.<id>.enabled
            over = env_strats_cfg.get(strat_name)
            if isinstance(over, dict) and over.get("enabled", True) is False:
                continue
            factory = STRATEGY_FACTORIES.get(strat_name)
            if not factory:
                log.warning("[Engine] Unknown strategy: %s", strat_name)
                continue

            instrument = strat_config.get("instrument", "GOLDM")
            inst_cfg = instruments_config.get(instrument, {})
            strategy = factory(
                strategy_id=strat_name,
                instrument=instrument,
                quantity=strat_config.get("quantity", 1),
                capital=strat_config.get("capital", 300_000.0),
                multiplier=inst_cfg.get("multiplier", 10.0),
                security_id=str(inst_cfg.get("security_id", "") or ""),
            )
            strategy.reversal_entry_gap_points = int(
                ((self.config.get("live") or {}).get("reversal") or {}).get("entry_gap_points", 0))
            env.strategies[strat_name] = strategy

            # Bind this strategy's indicator slots to the environment's
            # indicator engine so each (security_id, timeframe) DEMA-ATR
            # is computed once within this env.
            strategy.bind_shared_indicators(indicator_engine)

            # Subscribe to candle events (env-aware handler -> env signal path)
            for sub in strategy.subscriptions:
                event_bus.subscribe(
                    f"candle:{sub}", self._make_candle_handler(strategy, env.name))
            # Subscribe to tick events for pending/SL
            tick_topic = f"tick:{instrument}"
            event_bus.subscribe(
                tick_topic, self._make_tick_handler(strategy, env.name))
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
        """Build the one LIVE limit plan, using zero offsets by default."""
        pm = live_cfg.get("price_model") or {}
        from execution.price_model import PricePreset
        tick_size = float(pm.get("tick_size") or 1.0)
        if tick_size <= 0:
            tick_size = 1.0
        return PricePreset(
            entry_offset=float(pm.get("entry_offset", 0.0)),
            sl_offset=float(pm.get("sl_offset", 0.0)),
            tick_size=tick_size,
        )
    def _validate_live_order_ownership(self, env, order) -> Optional[str]:
        """Fail-closed strategy, trigger, and position checks at Dhan boundary."""
        reason = validate_live_order_ownership(env, order)
        if reason:
            return reason
        role = str(getattr(order, "order_role", "") or "").upper()
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
            if (role == "REVERSAL_ENTRY" and not gate.reversal_enabled):
                return "STRATEGY_REVERSAL_GATE_CLOSED"
        elif role == "REVERSAL_EXIT":
            if not self._gate_for(order.strategy_id).reversal_enabled:
                return "STRATEGY_REVERSAL_GATE_CLOSED"
        elif role == "STOP_LOSS":
            if not self._gate_for(order.strategy_id).sl_enabled:
                return "STRATEGY_SL_GATE_CLOSED"
        elif role == "EXIT":
            if not self._gate_for(order.strategy_id).exit_enabled:
                return "STRATEGY_EXIT_GATE_CLOSED"
        if role in {"ENTRY", "REVERSAL_ENTRY", "FALLBACK_MARKET",
                    "EXIT", "STOP_LOSS", "REVERSAL_EXIT"}:
            if str(getattr(order, "trigger_state", "") or "").upper() != "FIRED":
                return "ORDER_TRIGGER_NOT_FIRED"
            if (getattr(strategy, "_last_fired_trigger_signal_id", None)
                    != getattr(order, "parent_signal_id", None)):
                return "ORDER_TRIGGER_SIGNAL_STALE"
            if (getattr(order, "trigger_generation", None)
                    != getattr(strategy, "_trigger_generation", None)):
                return "ORDER_TRIGGER_GENERATION_STALE"
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
        for name, inst_cfg in self.config.get("instruments", {}).items():
            instruments[name] = {
                "security_id": inst_cfg.get("security_id", ""),
                "exchange_segment": inst_cfg.get("exchange_segment", "MCX_COMM"),
                "symbol": inst_cfg.get("symbol", ""),
            }
        for sid, strat_cfg in self.config.get("strategies", {}).items():
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
                "system", {}).get("live_db_path", "live/data/db/live_trading.db"))
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
                        "startup reconcile + recover_missing_sl are the "
                        "safety net", env.name, sid)
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
            env.indicators[f"{sid}_fast"] = strategy.fast_indicator
            env.indicators[f"{sid}_mid"] = getattr(
                strategy, "mid_indicator", strategy.mid_htf_state)
            env.indicators[f"{sid}_slow"] = strategy.slow_htf_state
    def _runtime(self, strategy_id: str) -> StrategyRuntime:
        """Resolve the isolated runtime (PAPER env) for a strategy (authoritative)."""
        return self.runtimes.require(strategy_id)
