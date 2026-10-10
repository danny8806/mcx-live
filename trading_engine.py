"""Main trading engine — event-driven architecture with per-strategy isolation.

Architecture:
    CandleFetcher → NativeCandleDistributor → EventBus → SuperTrendStrategy.on_candle()

Each active strategy owns its own SuperTrend state and consumes completed
native 15-minute candles for GOLDM or SILVERM.

Shared infrastructure:
    - Dhan REST/WebSocket
    - EventBus
    - ExecutionEngine
    - PositionManager
    - TradeLifecycleManager
    - Database (trading.db — single canonical source)
"""
from __future__ import annotations

import json
import logging
import math
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Optional

from config import Config
from data.dhan import DhanDataAdapter
from data.dhan.candle_validation import (
    filter_completed, iso_ist, tf_minutes as _cv_tf_minutes,
    REASON_FORMING,
)
from core.timeframe_engine import Bar
from core.market_status import MarketStatus, MarketState, EngineStatus, EnvMarketStatus
from core.safe_mode import SafeModeManager
from core.environments import Environment, EnvironmentKind
from core.fill_dedup import FillDeduplicator
from events.bus import EventBus
from data.native_streams import NativeCandleDistributor
from data.native_router import NativeCandleRouter
from strategies.base import StrategyBase
from strategies.types import Signal, SignalType, StrategyState, PendingEntry, resolve_order_role
from execution.models import Fill
from execution.broker_router import BrokerEventRouter
from execution.fee_model import MCXFeeModel
from execution.live.market_health import MarketDataHealthMonitor
from execution.order_manager import OrderManager, OrderManagerFacade
from portfolio.position_manager import (
    PositionManager, PositionManagerFacade, Position,
    merge_durable_open_position_rows,
)
from portfolio.pnl import PNLEngine
from portfolio.account import AccountEngine
from monitoring.health import HealthMonitor, SystemStatus
from notifications.telegram_router import TelegramRouter
from analytics.event_store import EventStore
from analytics.trade_ledger import TradeLedger
from core.lifecycle import PendingOrderState, TradeLifecycleManager, transition_pending_state
from core.risk_engine import RiskEngine
from core.trade_close import TradeCloseManager
from strategies.runtime import StrategyRuntime, StrategyRuntimeRegistry
from core.rollover import RolloverService
from application.signal_flow import SignalFlowMixin
from application.fill_flow import FillFlowMixin
from application.live_position_flow import LivePositionFlowMixin
from application.position_reconciliation import PositionReconciliationMixin
from application.persistence_flow import PersistenceFlowMixin
from application.environment_factory import EnvironmentFactoryMixin
from application.market_flow import MarketEventFlowMixin

log = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))




class StrategyGate:
    """Per-strategy operator control state (spec §4/§5 — strategy isolation).

    Every strategy carries its own independent entry, exit, and reversal gate.

    ``live_gate`` mirrors the master LIVE gate vocabulary (ON / OFF /
    CLOSE_ONLY / EMERGENCY_STOP / LOCKED) but scoped to ONE strategy in EVERY
    environment it participates in (PAPER included), so strategy-level intent
    is uniform.  Default acts are harmless: fully open.
    """

    __slots__ = ("live_gate", "entry_enabled", "exit_enabled",
                 "reversal_enabled", "close_only",
                 "operator_override")

    def __init__(self, live_gate: str = "ON",
                 entry_enabled: bool = True, exit_enabled: bool = True,
                 reversal_enabled: bool = True,
                 close_only: bool = False, operator_override: bool = False):
        self.live_gate = str(live_gate).upper()
        self.entry_enabled = bool(entry_enabled)
        self.exit_enabled = bool(exit_enabled)
        self.reversal_enabled = bool(reversal_enabled)
        self.close_only = bool(close_only)
        # Only an explicit runtime control action may override the configured
        # restart default. Legacy snapshots have no marker and must not shadow
        # a newer deployment's strategy gate settings.
        self.operator_override = bool(operator_override)

    @property
    def entries_allowed(self) -> bool:
        """Entries are allowed only when every entry-affecting flag is open."""
        return (self.entry_enabled and not self.close_only
                and self.live_gate == "ON")

    def to_dict(self) -> dict:
        return {
            "live_gate": self.live_gate,
            "entry_enabled": self.entry_enabled,
            "exit_enabled": self.exit_enabled,
            "reversal_enabled": self.reversal_enabled,
            "close_only": self.close_only,
            "operator_override": self.operator_override,
        }

    @classmethod
    def from_dict(cls, data: Optional[dict]) -> "StrategyGate":
        if not data:
            return cls()
        return cls(
            live_gate=str(data.get("live_gate", "ON")),
            entry_enabled=bool(data.get("entry_enabled", True)),
            exit_enabled=bool(data.get("exit_enabled", True)),
            reversal_enabled=bool(data.get("reversal_enabled", True)),
            close_only=bool(data.get("close_only", False)),
            operator_override=bool(data.get("operator_override", False)),
        )


def restore_strategy_gates(configured: dict,
                           saved: Optional[dict]) -> dict:
    """Restore explicit operator overrides over the current configured gates.

    A snapshot written before ``operator_override`` was introduced is treated
    as a legacy runtime copy, not as operator intent. This lets a deployment
    change a strategy from CLOSE_ONLY to ON without an old snapshot silently
    undoing it, while newly recorded operator controls still survive restart.
    """
    restored = dict(configured or {})
    for sid, data in (saved or {}).items():
        if isinstance(data, dict) and data.get("operator_override"):
            restored[sid] = StrategyGate.from_dict(data)
    return restored


# Strategy-control actions understood by TradingEngine.control_strategy().
STRATEGY_GATE_ACTION_ERRORS = {
    "start": "entry_gate_closed",
    "stop": "entry_gate_closed",
    "close_only": "entry_gate_closed",
    "emergency_stop": "emergency_gate_closed",
    "lock": "strategy_locked",
}



class TradingEngine(SignalFlowMixin, FillFlowMixin, LivePositionFlowMixin, PositionReconciliationMixin, PersistenceFlowMixin, EnvironmentFactoryMixin, MarketEventFlowMixin):
    """Event-driven trading engine with per-strategy isolation.

    Event flow:
    Dhan WebSocket → broker/position marks
    Dhan REST → CandleFetcher → NativeCandleDistributor → SuperTrendStrategy.on_candle()
    """

    def __init__(self, config_path: Optional[str] = None, event_callback=None,
                 live_only: bool = False):
        self.config = Config()
        if config_path:
            self.config.load(Path(config_path))
        else:
            self.config.load()

        # ── Contract rollover (two trading sessions before expiry) ──
        # The service decides, per metal, when to block entries in the
        # expiring series, when to force-close on the final session, and when
        # to persist the next-series override.  The override is applied to the
        # instruments config HERE — before any environment/strategy/indicator
        # build — so a boot after a rollover warms up and trades the NEW
        # contract from scratch.
        self._rollover_blocked: set[tuple[str, str]] = set()
        self._rollover_applied: dict[str, dict] = {}
        self._rollover_watchdogs: set[str] = set()
        # Resting-order cancels are issued at most once per (env, metal, day).
        self._rollover_canceled_daily: set[tuple[str, str, str]] = set()
        _roll_cfg = (self.config.get("live") or {}).get("rollover") or {}
        self._rollover_service = RolloverService(
            config=_roll_cfg,
            state_path=Path(_roll_cfg.get(
                "state_path", "live/data/db/rollover_state.json")),
            emit=self.publish_event,
            on_alert=self._rollover_alert,
        )
        # Fail closed from process construction until the rollover watchdog has
        # verified every configured LIVE contract against Dhan's current
        # scrip master. The watchdog is started after warmup/feed setup, so
        # leaving this empty creates a startup interval where an expired or
        # mismapped contract could accept a signal before its first check.
        if self._rollover_service.enabled:
            self._rollover_blocked.update(
                ("live", str(metal))
                for metal in (self.config.get("instruments") or {})
            )
        self._apply_rollover_startup()

        self._event_callback = event_callback
        # Restore failures surfaced at boot (env_name -> [strategy_ids]) so a
        # swallowed DB-restore failure is never "armed-but-blind".
        self._restore_failures: dict[str, list[str]] = {}
        # LIVE-only mode: build ONLY the LIVE environment and rebind the
        # engine surface onto its runtime objects (own EventBus, market data,
        # indicators, execution, persistence). The DEMO (paper) path is
        # byte-identical to before.
        self._live_only = bool(live_only)

        # ── Shared infrastructure ──
        self.event_bus = EventBus()
        self.candle_distributor = NativeCandleDistributor(self.event_bus)
        # §7 — NativeCandleRouter is the single native-candle choke point:
        # dedup by (security_id, timeframe, candle_end_ts), out-of-order
        # detection, incomplete-candle guard, then forward to the distributor.
        self.candle_router = NativeCandleRouter(
            distributor=self.candle_distributor.on_candle_closed,
            instruments=self._runtime_instrument_configs(),
        )
        self.indicator_engine = None
        # §38 — market-data health.  The stop is local, so a silent feed is an
        # UNPROTECTED position, not a cosmetic dashboard warning.  Fresh ticks
        # or a completed candle both refresh this; losing both is an outage.
        _live_cfg = self.config.get("live") or {}
        self.market_data_health = MarketDataHealthMonitor(
            stale_after=float((_live_cfg.get("market_data") or {})
                              .get("stale_after_seconds", 90.0)),
        )
        # §35 — environments whose broker position state is KNOWN at startup.
        # A LIVE env absent from this set must not open new exposure.
        self._reconciled_envs: set[str] = set()

        # ── Initialize (shared market-data infrastructure) ──
        self._init_market_status()
        self._init_data_adapter()
        self._init_indicator_engines()
        self._init_candle_fetcher()
        self._init_monitoring()
        self._init_notifications()

        # ── Environments (LIVE + PAPER parallel architecture) ──
        # Every environment is an isolated execution context (own strategies,
        # execution, portfolio, risk, persistence). Market data, shared
        # indicator streams and strategy *logic* are common; only execution
        # state is isolated. The default build is ["paper"] so existing
        # production/test behavior is byte-identical; adding "live" enables
        # the parallel LIVE environment (entries gated OFF by default).
        # In LIVE-only mode the engine builds ONLY the live environment and
        # its engine-level surface (below) becomes the LIVE runtime itself.
        self._envs: dict[str, Environment] = {}
        self.environments: list[str] = self._load_environment_names()
        self._build_environments()
        self.paper = None
        self.live = None
        if self._live_only:
            live_env = self._envs["live"]
            self.live = live_env
            log.info("[Engine] LIVE-only engine (no PAPER runtime); master gate %s",
                     "ON" if live_env.gate_enabled else "OFF")
        else:
            paper_env = self._envs["paper"]
            self.paper = paper_env
            if "live" in self._envs:
                self.live = self._envs["live"]
                log.info("[Engine] LIVE environment present (master gate %s)",
                         "ON" if self.live.gate_enabled else "OFF")

        # ── Per-strategy operator gates (spec §4/§5) ──
        # In-memory operator intent, seeded from the strategy's own config
        # block so the yaml/json is the restart default and runtime changes
        # survive restart via the engine snapshot (strategy_gates payload).
        self._strategy_gates: dict[str, StrategyGate] = {}
        self._gate_epoch = 0.0
        self._reversal_today: dict[tuple, int] = {}
        active_strategy_ids = {
            sid for runtime_env in self._envs.values()
            for sid in (getattr(runtime_env, "strategies", {}) or {})
        }
        for sid in active_strategy_ids:
            self._strategy_gates[sid] = self._default_strategy_gate(sid)

        # ── Backward-compatible aliases ──
        # In the dual-env engine every existing consumer (server, tests,
        # scripts) keeps talking to engine.strategies / engine.execution_engine
        # / ... — those resolve to the PAPER environment; LIVE is always
        # addressed explicitly. In LIVE-only mode the aliases (plus the shared
        # infrastructure) resolve to the LIVE environment's owned runtime, so
        # the same code paths operate on LIVE state alone.
        pivot_env = self.live if self._live_only else self._envs["paper"]
        self._bind_env_aliases(pivot_env)
        if self._live_only:
            # LIVE app: the engine's shared-ish infra is the LIVE env's own
            # (per-env data adapter, event bus, candle pipeline, indicators,
            # market status, safe mode, candle fetcher).
            self._bind_env_infra(pivot_env)

        if not self._live_only:
            self.safe_mode = SafeModeManager(self.market_status)
        elif self.safe_mode is None:
            self.safe_mode = SafeModeManager(self.market_status)
        # §34 — cross-strategy protection: rejected/quarantined events are
        # logged as ERROR and recorded here; they never mutate lifecycle state.
        self.quarantine_count: int = 0
        self._quarantined_events: list[dict] = []
        # No global TradeLifecycleManager: one per StrategyRuntime.
        self._lifecycle = None
        self._trade_close_manager = None

        # ── State ──
        self._running = False
        self._lock = threading.RLock()
        self._persistence = None
        # The orphan-protective-SL entry block is gone: there is no broker-side
        # protective stop any more, so no resting order can outlive a position.

        # ── Warmup forensics + latest-completed watermark (mission: direct
        # Dhan REST source + latest-available backfill) ──
        self._warmup_forensics: dict[str, dict] = {}
        self._warmup_watermark: dict[str, dict] = {}

        log.info("[Engine] Initialized with %d strategies across %d env(s)",
                 len(self.strategies), len(self._envs))

    # ═══════════════════════════════════════════════════════════════════
    # CONTRACT ROLLOVER (GOLDM/SILVERM, two sessions before expiry)
    # ═══════════════════════════════════════════════════════════════════

    def _rollover_alert(self, alert: dict) -> None:
        """Route a rollover alert to Telegram + the event bus (never raises)."""
        telegram = getattr(self, "telegram", None)
        if telegram is not None:
            try:
                telegram.on_risk_alert(alert)
            except Exception as e:
                log.warning("[Rollover] telegram alert failed: %s", e)
        try:
            self.publish_event("contract_rollover_alert", alert)
        except Exception as e:
            log.warning("[Rollover] event publish failed: %s", e)

    def _apply_rollover_startup(self) -> None:
        """Apply persisted contract overrides to the instruments config.

        Runs at the top of ``__init__`` — before any environment, strategy,
        indicator, router, fetcher, adapter or broker is built — so a boot
        after a rollover warms up and trades the NEW series from scratch.
        """
        if not self._rollover_service.enabled:
            return
        instruments = self.config.get("instruments", {}) or {}
        for metal, over in self._rollover_service.overrides().items():
            inst = instruments.get(metal)
            if not isinstance(inst, dict):
                continue
            want_symbol = over.get("active_symbol")
            want_sid = over.get("active_security_id")
            if not want_symbol or not want_sid:
                continue
            cur_symbol = inst.get("symbol")
            cur_sid = str(inst.get("security_id", "") or "")
            if cur_symbol == want_symbol and cur_sid == str(want_sid):
                continue
            self._rollover_applied[metal] = {
                "from_symbol": cur_symbol,
                "to_symbol": want_symbol,
                "from_security_id": cur_sid,
                "to_security_id": str(want_sid),
                "switched_on": over.get("switched_on"),
            }
            inst["symbol"] = want_symbol
            inst["security_id"] = str(want_sid)
            log.warning(
                "[Rollover] boot: %s switched %s(sid %s) -> %s(sid %s)",
                metal, cur_symbol, cur_sid, want_symbol, want_sid)
            try:
                self.publish_event("contract_rollover_applied_at_boot", {
                    "instrument": metal,
                    "from_symbol": cur_symbol,
                    "to_symbol": want_symbol,
                    "from_security_id": cur_sid,
                    "to_security_id": str(want_sid),
                    "switched_on": over.get("switched_on"),
                })
            except Exception as e:
                log.warning("[Rollover] boot event failed: %s", e)

    def _start_rollover_watchdog(self, env: Environment) -> None:
        """One daemon thread per LIVE env evaluating rollover decisions."""
        if not self._rollover_service.enabled:
            return
        if env.name in self._rollover_watchdogs:
            return
        self._rollover_watchdogs.add(env.name)
        interval = max(5.0, float(
            (((self.config.get("live") or {}).get("rollover") or {})
             .get("tick_interval_seconds", 15)) or 15))

        def loop() -> None:
            while self._running:
                try:
                    self._rollover_tick(env.name)
                except Exception as e:
                    log.warning("[Rollover] tick error: %s", e)
                time.sleep(interval)

        threading.Thread(target=loop, daemon=True,
                         name=f"rollover-watchdog-{env.name}").start()

    def _rollover_metal_open(self, env: Environment, metal: str,
                             security_id: str) -> bool:
        """True unless Dhan and local state both confirm the metal is flat.

        Rollover is a contract-level operation, so the broker check uses the
        exact expiring security ID. Missing broker support, malformed data, or
        an API error is treated as unknown/open and prevents a contract switch.
        """
        broker = getattr(env, "broker", None)
        flat_check = getattr(broker, "instrument_flat", None)
        if not callable(flat_check):
            self._rollover_service.alert_once(
                metal, "position_check_unavailable", datetime.now(IST).date(),
                f"{metal}: cannot verify the expiring contract is flat with Dhan; "
                "rollover remains blocked", security_id=str(security_id))
            return True
        try:
            if not flat_check(metal, str(security_id)):
                self._rollover_service.alert_once(
                    metal, "position_still_open", datetime.now(IST).date(),
                    f"{metal}: Dhan still reports an open position in the "
                    "expiring contract; rollover will wait for broker-flat "
                    "confirmation",
                    security_id=str(security_id))
                return True
        except Exception as e:
            log.warning("[Rollover] %s exact broker-flat check failed: %s",
                        metal, e)
            self._rollover_service.alert_once(
                metal, "position_check_failed", datetime.now(IST).date(),
                f"{metal}: Dhan could not confirm the expiring contract is flat; "
                "rollover remains blocked", security_id=str(security_id),
                error=str(e))
            return True
        if broker is not None and hasattr(broker, "_own_net_positions"):
            try:
                if any(p.get("instrument") == metal
                       for p in broker._own_net_positions()):
                    return True
            except Exception as e:
                log.warning("[Rollover] %s local broker-book check failed: %s",
                            metal, e)
                return True
        for strategy in env.strategies.values():
            if getattr(strategy, "instrument", None) != metal:
                continue
            if getattr(strategy, "position_side", None):
                return True
            if getattr(strategy, "pending_entry", None):
                return True
            if getattr(strategy, "current_trade_id", None):
                return True
        return False

    def _rollover_tick(self, env_name: Optional[str] = None) -> None:
        """Evaluate + act on the contract rollover for every metal."""
        env = self._env_for(env_name)
        if not env.is_live:
            return
        if not self._rollover_service.enabled:
            return
        broker = getattr(env, "broker", None)
        today = datetime.now(IST).date()
        instruments = self.config.get("instruments", {}) or {}
        for metal, inst in instruments.items():
            if not isinstance(inst, dict):
                continue
            symbol = inst.get("symbol")
            sid = str(inst.get("security_id", "") or "")
            if not symbol or not sid:
                continue
            needs_broker_check = self._rollover_service.requires_broker_confirmation(
                metal, symbol, sid, today)
            # Avoid repeated position/order REST requests throughout the
            # contract month. These broker checks are required only after the
            # configured rollover window begins.
            has_positions = False
            orders_clear = True
            if needs_broker_check:
                has_positions = self._rollover_metal_open(env, metal, sid)
                check_orders = getattr(broker, "instrument_orders_clear", None)
                orders_clear = False
                if callable(check_orders):
                    try:
                        orders_clear = bool(check_orders(metal, sid))
                        if not orders_clear:
                            self._rollover_service.alert_once(
                                metal, "working_orders_remain", today,
                                f"{metal}: Dhan reports a working order for the "
                                "expiring contract; system-owned orders will be "
                                "cancelled and rollover will retry",
                                security_id=sid)
                    except Exception as e:
                        log.warning("[Rollover] %s order-clear check failed: %s",
                                    metal, e)
                        self._rollover_service.alert_once(
                            metal, "order_check_failed", today,
                            f"{metal}: Dhan order book could not confirm the "
                            "expiring contract has no working orders; rollover "
                            "will retry",
                            security_id=sid, error=str(e))
                else:
                    self._rollover_service.alert_once(
                        metal, "order_check_unavailable", today,
                        f"{metal}: broker order-book check is unavailable; "
                        "rollover will retry",
                        security_id=sid)
            actions = self._rollover_service.evaluate(
                metal, symbol, sid, today, has_positions,
                resting_orders_clear=orders_clear)
            if "block_entries" in actions:
                self._rollover_blocked.add((env.name, metal))
            else:
                self._rollover_blocked.discard((env.name, metal))
            if "cancel_resting" in actions and broker is not None:
                cancel = getattr(broker, "cancel_instrument_orders", None)
                if callable(cancel):
                    try:
                        # Retry unresolved cancellations on every watchdog
                        # pass. The transport verifies each broker status and
                        # treats already-settled orders idempotently.
                        result = cancel(metal)
                        self.publish_event("contract_rollover_cancel_resting", {
                            "instrument": metal, "result": result,
                            "orders_clear_after_cancel": bool(
                                getattr(broker, "instrument_orders_clear",
                                        lambda _i, _sid=None: False)(metal, sid)),
                        }, env_name=env.name)
                    except Exception as e:
                        log.warning("[Rollover] cancel resting failed: %s", e)
            if "force_close" in actions and broker is not None:
                try:
                    result = self.emergency_exit_all(env.name, instrument=metal)
                    self.publish_event("contract_rollover_force_close", {
                        "instrument": metal, "result": result,
                    }, env_name=env.name)
                    if (not result.get("closed") and not result.get("errors")):
                        self._rollover_service.alert_once(
                            metal, "force_close_no_owned_position",
                            today,
                            f"{metal}: Dhan is not flat but the engine has no "
                            "locally tracked position to close; manual broker "
                            "reconciliation is required",
                            security_id=sid)
                except Exception as e:
                    log.warning("[Rollover] force-close failed: %s", e)
            if "persist_switch" in actions:
                self.publish_event("contract_rollover_switched", {
                    "instrument": metal,
                    **self._rollover_service.overrides().get(metal, {}),
                }, env_name=env.name)

    def _load_environment_names(self) -> list[str]:
        """Resolve the enabled execution environments from config.

        Default is ["paper"] only; the LIVE environment is added by listing
        "live" under system.environments. "paper" is always kept first for the
        dual-env engine. In LIVE-only mode exactly ["live"] is built (the
        standalone LIVE app has no PAPER runtime). Ambiguous configurations are
        rejected at load (validate_environment_config)."""
        from core.environments import validate_environment_config
        validate_environment_config(self.config)
        raw = self.config.get("system", {}).get("environments", ["paper"])
        names = [str(e).strip().lower() for e in raw]
        if self._live_only:
            return ["live"] if "live" in names else list(dict.fromkeys(names + ["live"]))
        if not names or "paper" not in names:
            names.insert(0, "paper")
        return list(dict.fromkeys(names))

    def _env_for(self, name: Optional[str] = None) -> Environment:
        if name is None:
            name = "live" if self._live_only else "paper"
        return self._envs.get(name) or (self._envs["live"] if self._live_only else self._envs["paper"])

    def _build_environments(self) -> None:
        # §9.3 — a LIVE environment must never share an execution DB with PAPER
        # (PAPER rows carry execution_mode='PAPER', LIVE rows 'LIVE'; a single
        # file would mix authorities).  Distinct resolved paths are enforced.
        resolved = {}
        for env_name in ("paper", "live"):
            key = "db_path" if env_name == "paper" else "live_db_path"
            resolved[env_name] = Config.resolve_path(self.config.get(
                "system", {}).get(
                    key,
                    ("data/db/supertrend_paper.sqlite3" if env_name == "paper"
                     else "live/data/db/supertrend/supertrend_live.sqlite3")))
        if {"paper", "live"} <= set(self.environments) and resolved["paper"] == resolved["live"]:
            raise RuntimeError(
                "environment config: system.db_path and system.live_db_path "
                "must be distinct files (PAPER/LIVE isolation)")
        for name in self.environments:
            self._envs[name] = self._build_environment(name)

    def _build_environment(self, name: str) -> Environment:
        is_live = name != "paper"
        mode = "LIVE" if is_live else "PAPER"
        if is_live:
            db_path = Config.resolve_path(self.config.get("system", {}).get(
                "live_db_path", "live/data/db/supertrend/supertrend_live.sqlite3"))
        else:
            db_path = Config.resolve_path(self.config.get("system", {}).get(
                "db_path", "data/db/supertrend_paper.sqlite3"))
        try:
            event_store = EventStore(db_path=db_path, execution_mode=mode)
        except Exception:
            event_store = None
        try:
            trade_ledger = TradeLedger(db_path=db_path)
        except Exception:
            trade_ledger = None
        fill_dedup = FillDeduplicator(db_path=db_path)
        env = Environment(
            name=name, mode=mode, is_live=is_live,
            kind=EnvironmentKind.LIVE if is_live else EnvironmentKind.PAPER,
            db_path=db_path,
            event_store=event_store, trade_ledger=trade_ledger,
            fill_dedup=fill_dedup,
        )
        # Master LIVE gate: initial state comes from live.gate (validated by
        # validate_environment_config).  Entries are allowed only when the
        # gate is ON.  PAPER envs are always tradeable.
        if is_live:
            gate_init = str(self.config.get("live", {}).get("gate", "OFF")).upper()
            env.gate_state = gate_init
            env.gate_enabled = gate_init == "ON"
        else:
            env.gate_enabled = True
        # Build per-env data infrastructure BEFORE strategies so the env's
        # own event_bus is available for strategy subscriptions.
        self._build_env_data_infra(env)
        self._build_strategies_for_env(env)
        self._build_execution_for_env(env)
        self._build_portfolio_for_env(env)
        self._build_risk_for_env(env)
        self._build_runtimes_for_env(env)
        return env

    # ═══════════════════════════════════════════════════════════════════
    # PER-STRATEGY OPERATOR GATES (spec §4/§5 — strategy isolation)
    # ═══════════════════════════════════════════════════════════════════

    def _default_strategy_gate(self, strategy_id: str) -> StrategyGate:
        """Seed a strategy's gate from its own config block (the restart
        default).  Any key may be spelled at the strategy level; a missing key
        falls back to the safe default (fully open)."""
        scfg = self.config.strategy(strategy_id) or {}
        return StrategyGate(
            live_gate=str(scfg.get("live_gate", "ON")),
            entry_enabled=bool(scfg.get("entry_enabled", True)),
            exit_enabled=bool(scfg.get("exit_enabled", True)),
            reversal_enabled=bool(scfg.get("reversal_enabled", True)),
            close_only=bool(scfg.get("close_only", False)),
        )

    def _gate_for(self, strategy_id: str) -> StrategyGate:
        config = getattr(self, "config", None)
        policy = (config.get("strategy_runtime", {}) or {}) if config is not None else {}
        if policy.get("exclusive") and strategy_id not in self.strategies:
            raise KeyError(f"strategy {strategy_id!r} is outside the exclusive runtime")
        gates_map = getattr(self, "_strategy_gates", None)
        if gates_map is None:
            gates_map = {}
            self._strategy_gates = gates_map
        gate = gates_map.get(strategy_id)
        if gate is None:
            gate = self._default_strategy_gate(strategy_id)
            gates_map[strategy_id] = gate
        return gate

    def strategy_gates(self) -> dict:
        """All per-strategy gates (read-only views)."""
        active = set(self.strategies or {})
        return {sid: gate.to_dict() for sid, gate in self._strategy_gates.items()
                if sid in active}

    def strategy_gate(self, strategy_id: str) -> dict:
        config = getattr(self, "config", None)
        policy = (config.get("strategy_runtime", {}) or {}) if config is not None else {}
        if policy.get("exclusive") and strategy_id not in self.strategies:
            raise KeyError(f"strategy {strategy_id!r} is outside the exclusive runtime")
        if strategy_id not in self._strategy_gates:
            self._strategy_gates[strategy_id] = self._default_strategy_gate(strategy_id)
        return self._strategy_gates[strategy_id].to_dict()

    def set_strategy_gate(self, strategy_id: str, **fields) -> dict:
        """Set individual gate flags for one strategy (validated, additive).

        Accepted fields: live_gate, entry_enabled, exit_enabled,
        reversal_enabled, close_only. Unknown keys are rejected.
        """
        gate = self._gate_for(strategy_id)
        allowed = {"live_gate", "entry_enabled", "exit_enabled",
                   "reversal_enabled", "close_only"}
        unknown = [k for k in fields if k not in allowed]
        if unknown:
            raise ValueError(f"unknown gate field(s): {unknown}")
        if "live_gate" in fields:
            gate.live_gate = str(fields["live_gate"]).upper()
        if "entry_enabled" in fields:
            gate.entry_enabled = bool(fields["entry_enabled"])
        if "exit_enabled" in fields:
            gate.exit_enabled = bool(fields["exit_enabled"])
        if "reversal_enabled" in fields:
            gate.reversal_enabled = bool(fields["reversal_enabled"])
        if "close_only" in fields:
            gate.close_only = bool(fields["close_only"])
        if fields:
            gate.operator_override = True
        self.publish_event("strategy_gate_changed", {
            "strategy_id": strategy_id,
            "gate": gate.to_dict(),
            "timestamp": time.time(),
        })
        return gate.to_dict()

    def control_strategy(self, strategy_id: str, action: str) -> dict:
        # A tick, broker fill, or order-book update can otherwise replace a
        # pending trigger between its durable retirement and memory cleanup.
        with getattr(self, "_lock", threading.RLock()):
            return self._control_strategy_locked(strategy_id, action)

    def _control_strategy_locked(self, strategy_id: str, action: str) -> dict:
        """High-level operator control for ONE strategy (spec §5).

        start          -> fully open (entries + all exit classes).
        resume         -> alias of start (legacy name kept).
        stop           -> CLOSE_ONLY: block new entries, keep standard exits.
        close_only     -> block new entries + reversals, keep exits active.
        emergency_stop -> block entries immediately and keep exits active.
        lock           -> freeze the strategy entirely (entries + every exit).
        pause (legacy) -> clear pending entry and freeze signal generation.
        """
        a = str(action or "").lower()
        if strategy_id not in self.strategies and strategy_id not in self._strategy_gates:
            return {"success": False, "strategy_id": strategy_id, "action": action,
                    "error": f"Strategy {strategy_id} not found"}
        gate = self._gate_for(strategy_id)
        strat = self.strategies.get(strategy_id)
        if a == "start" or a == "resume":
            gate.live_gate = "ON"
            gate.entry_enabled = True
            gate.exit_enabled = True
            gate.reversal_enabled = True
            gate.close_only = False
            if strat is not None:
                strat.enabled = True
        elif a == "stop" or a == "close_only":
            # stop == close_only: entries blocked, risk-reducing exits active.
            gate.live_gate = "CLOSE_ONLY"
            gate.entry_enabled = False
            gate.exit_enabled = True
            gate.reversal_enabled = False
            gate.close_only = True
        elif a == "emergency_stop":
            gate.live_gate = "EMERGENCY_STOP"
            gate.entry_enabled = False
            gate.exit_enabled = True
            gate.reversal_enabled = False
            gate.close_only = True
            self.publish_event("strategy_emergency_stop", {
                "strategy_id": strategy_id, "timestamp": time.time(),
            })
        elif a == "lock":
            gate.live_gate = "LOCKED"
            gate.entry_enabled = False
            gate.exit_enabled = False
            gate.reversal_enabled = False
            gate.close_only = True
        elif a == "pause":
            # Pause this current strategy profile and freeze signal generation
            # (this also sets the entry gate OFF).
            if strat is not None:
                open_positions = self.position_manager.get_positions_by_strategy(strategy_id)
                if any(pos.is_open for pos in open_positions):
                    return {"success": False, "strategy_id": strategy_id, "action": action,
                            "error": "Cannot pause a strategy with an open position; close it first."}
                env = next((candidate for candidate in getattr(self, "_envs", {}).values()
                            if candidate.strategies.get(strategy_id) is strat), None)
                pending = getattr(strat, "pending_entry", None)
                pending_id = getattr(getattr(pending, "signal", None), "signal_id", None)
                if pending_id and env is not None and getattr(env, "is_live", False):
                    persistence = getattr(env, "persistence", None)
                    if persistence is None:
                        return {"success": False, "strategy_id": strategy_id, "action": action,
                                "error": "Cannot pause: LIVE pending trigger store unavailable."}
                    try:
                        retired = persistence.terminalize_pending_order(
                            str(pending_id), status="resolved", reason="operator_paused")
                        if not retired:
                            row = persistence.get_pending_order(
                                str(pending_id), execution_mode="LIVE")
                            if row is None or str(row.get("status", "")).lower() not in (
                                    "resolved", "expired", "cancelled_by_reversal",
                                    "cancelled_by_indicator_change"):
                                raise RuntimeError("durable trigger remained active")
                    except Exception as exc:
                        return {"success": False, "strategy_id": strategy_id, "action": action,
                                "error": f"Cannot pause pending trigger: {exc}"}
                if pending is not None:
                    strat._cancel_trigger(pending)
                strat.pending_entry = None
                if hasattr(strat, "state"):
                    strat.state = StrategyState.FLAT
                if hasattr(strat, "_last_armed_pending_id"):
                    strat._last_armed_pending_id = None
                registry = getattr(env, "pending_triggers", None) if env is not None else None
                if registry is not None:
                    registry.sync_strategy(strat)
                strat.enabled = False
            gate.live_gate = "CLOSE_ONLY"
            gate.entry_enabled = False
            gate.close_only = True
        else:
            return {"success": False, "strategy_id": strategy_id, "action": action,
                    "error": f"Unknown action '{action}' "
                             "(expected start, stop, close_only, emergency_stop, lock, "
                             "pause or resume)"}
        gate.operator_override = True
        self.publish_event("strategy_control", {
            "strategy_id": strategy_id, "action": a, "gate": gate.to_dict(),
            "timestamp": time.time(),
        })
        return {"success": True, "strategy_id": strategy_id, "action": a,
                "gate": gate.to_dict()}

    def _publish_gate_block(self, signal, reason: str, detail: Optional[dict],
                            env) -> None:
        self.publish_event("strategy_gate_blocked", {
            "signal_id": signal.signal_id, "strategy_id": signal.strategy_id,
            "instrument": signal.instrument,
            "signal_type": getattr(signal.signal_type, "name", ""),
            "reason": reason, "detail": detail or {},
            "execution_mode": env.mode}, env_name=env.name)

    def _pending_orders_count(self, env, strategy_id: str) -> int:
        """Number of the strategy's not-yet-filled orders (PAPER + LIVE)."""
        try:
            om = env.runtimes.require(strategy_id).order_manager
        except Exception:
            om = env.order_manager
        try:
            if hasattr(om, "pending_orders"):
                return len([o for o in om.pending_orders
                            if getattr(o, "strategy_id", None) == strategy_id])
        except Exception:
            pass
        # Count the durable execution engine's active states as well as the
        # order-manager view.  In particular ACKNOWLEDGED is still working at
        # the broker and must occupy the strategy's one-entry slot.
        active_states = {"created", "submitted", "acknowledged", "partially_filled"}
        orders = {}
        try:
            for order in om.get_active_orders(strategy_id) or []:
                orders[str(getattr(order, "order_id", id(order)))] = order
        except Exception:
            pass
        execution = getattr(om, "execution_engine", None)
        for order in (getattr(execution, "_orders", {}) or {}).values():
            if (getattr(order, "strategy_id", None) == strategy_id
                    and str(getattr(getattr(order, "state", None), "value",
                                    getattr(order, "state", ""))).lower()
                    in active_states):
                orders[str(getattr(order, "order_id", id(order)))] = order
        return len(orders)

    def _reversal_under_cap(self, signal) -> bool:
        """True when the strategy's daily reversal cap (config max_reversals)
        still has room.  Also records the accepted reversal for the day."""
        scfg = self.config.strategy(signal.strategy_id) or {}
        mrev = scfg.get("max_reversals")
        if mrev is None:
            return True
        today = datetime.now(timezone.utc).date().isoformat()
        key = (signal.strategy_id, today)
        cnt = self._reversal_today.get(key, 0)
        if cnt + 1 > int(mrev):
            return False
        self._reversal_today[key] = cnt + 1
        return True

    def _validate_strategy_risk_gate(self, signal, env, gates) -> tuple:
        """Lots/quantity + per-strategy risk caps for an incoming ENTRY.

        Resolves the traded quantity from the strategy config (explicit
        ``quantity`` wins for back-compat; otherwise ``lots * lot_size``) and
        enforces, per strategy: quantity > 0, lot-multiple, max_quantity,
        max_order_value (notional), max_daily_trades, max_daily_loss,
        max_drawdown_pct and max_pending_orders.  On success the signal's
        quantity is set to the approved lot-derived value."""
        sid = signal.strategy_id
        scfg = self.config.strategy(sid) or {}
        icfg = self.config.instrument(signal.instrument) or {}
        multiplier = float(icfg.get("multiplier", 1.0))
        lot_size = int(icfg.get("lot_size", 1) or 1)
        if scfg.get("quantity") is not None:
            qty = int(scfg.get("quantity") or 0)
        else:
            qty = int(scfg.get("lots", 1) or 1) * lot_size
        if qty <= 0:
            return False, "invalid_quantity"
        if qty % lot_size != 0:
            return False, "quantity_not_lot_multiple"
        max_qty = scfg.get("max_quantity")
        if max_qty is not None and qty > int(max_qty):
            return False, "quantity_cap"
        max_notional = scfg.get("max_order_value")
        notional = qty * float(signal.trigger_price or 0.0) * multiplier
        if max_notional is not None and notional > float(max_notional):
            return False, "order_value_cap"
        signal.quantity = qty
        pnl = env.pnl_engines.get(sid)
        mt = scfg.get("max_daily_trades")
        if mt is not None and pnl is not None and int(pnl.trade_count) >= int(mt):
            return False, "daily_trades_cap"
        ml = scfg.get("max_daily_loss")
        if ml is not None and pnl is not None and float(pnl.realized_net) <= -abs(float(ml)):
            return False, "daily_loss_cap"
        md = scfg.get("max_drawdown_pct")
        if md is not None:
            acct = env.account_engines.get(sid)
            if (acct is not None
                    and float(acct.starting_capital or 0.0) > 0):
                ddpct = ((float(acct.starting_capital) - float(acct.equity))
                         / float(acct.starting_capital) * 100.0)
                if ddpct >= float(md):
                    return False, "drawdown_cap"
        # This runtime has one position per profile. Keep one working-order
        # slot even if an older/external config omits the field.
        mo = scfg.get("max_pending_orders", 1)
        if self._pending_orders_count(env, sid) >= int(mo):
            return False, "pending_orders_cap"
        return True, ""

    # ═══════════════════════════════════════════════════════════════════
    # INIT METHODS (kept for backward compatibility with tests/scripts)
    # ═══════════════════════════════════════════════════════════════════







    # ═══════════════════════════════════════════════════════════════════
    # PER-ENV DATA INFRASTRUCTURE (LIVE independence)
    # ═══════════════════════════════════════════════════════════════════






















    # ═══════════════════════════════════════════════════════════════════
    # CANDLE + TICK HANDLERS (EventBus-driven)
    # ═══════════════════════════════════════════════════════════════════








    # ═══════════════════════════════════════════════════════════════════
    # SIGNAL / EXIT / TRADE LIFECYCLE
    # ═══════════════════════════════════════════════════════════════════

    def _reversal_entry_block_reason(self, env, strategy_id: str,
                                     instrument: str) -> Optional[str]:
        """C4 — the REVERSAL_ENTRY leg must pass the same invariant gates a
        fresh entry passes before any opposite position is placed.

        A reversal signal jumps straight into the exit branch of
        ``_process_signal``, so the entry-side gates (safe mode, market
        tradable, risk kill-switch / daily loss) never ran for its entry leg.
        The old orphan-protective-SL block is gone: no broker-side stop can
        outlive its position, so there is nothing left to block on.

        Returns a reason string to block the entry, or None when clear.
        """
        try:
            safe_mode = getattr(env, "safe_mode", None)
            if safe_mode is not None:
                if getattr(safe_mode, "is_active", False):
                    return "safe_mode_active"
            ms = getattr(env, "market_status", None)
            if ms is not None:
                if not getattr(ms, "is_trading_allowed", True):
                    return "market_not_trading"
        except Exception:
            pass
        risk = getattr(env, "risk_engine", None)
        if risk is not None:
            try:
                if getattr(risk, "kill_switch_active", False):
                    return "kill_switch_active"
                daily = getattr(risk, "daily_pnl", 0.0) or 0.0
                daily_limit = getattr(risk, "max_daily_loss", None)
                if daily_limit is not None and daily <= -abs(float(daily_limit)):
                    return "daily_loss_limit_reached"
            except Exception:
                pass
        return None

        # Reversal entries remain armed on the strategy and are submitted only
        # after the old position is broker-confirmed flat and their own trigger fires.



    # ── Phase 9.6 — durable LIVE pending-order lifecycle ──────────────────
    # States: PENDING → ARMED → (ENTRY_SENT | EXPIRED | CANCELLED_BY_REVERSAL).
    # Rows live in the LIVE environment's own pending_orders table, keyed by
    # signal_id (pending orders are 1:1 with pending signals; the born trade
    # links via trades.entry_signal_id). PAPER never writes these rows.






    # ── V4 — broker-side protective stop-loss (spec §22-24) ──────────────




    def _notify_signal(self, signal, env_name: Optional[str]) -> None:
        try:
            meta = getattr(signal, 'metadata', None) or {}
            account_dict = {
                "equity": 0.0,
                "used_margin": 0.0,
                "source": "engine",
            }
            env_obj = self._envs.get(env_name) if env_name else None
            if env_obj is not None and getattr(env_obj, "is_live", False):
                # LIVE signals must show the REAL Dhan account like fills do.
                broker_acct = self._live_account_snapshot(env_obj)
                if broker_acct:
                    account_dict = {
                        "equity": broker_acct.get("equity", 0.0),
                        "used_margin": broker_acct.get("used_margin", 0.0),
                        "available_margin": broker_acct.get("available_margin", 0.0),
                        "realized_pnl": broker_acct.get("realized_pnl", 0.0),
                        "unrealized_pnl": broker_acct.get("unrealized_pnl", 0.0),
                        "net_pnl": broker_acct.get("net_pnl", 0.0),
                        "dhan_client_id": broker_acct.get("dhan_client_id", ""),
                        "source": "dhan",
                    }
            candle_time = None
            for key in ("signal_candle_start", "signal_candle_time"):
                val = meta.get(key)
                if val:
                    try:
                        candle_time = datetime.fromtimestamp(
                            float(val), tz=timezone(timedelta(hours=5, minutes=30))
                        ).strftime("%H:%M %d-%b")
                    except Exception:  # noqa: BLE001
                        candle_time = str(val)
                    break
            self.telegram.on_signal({
                "side": signal.signal_type.name,
                "instrument": signal.instrument,
                "strategy_id": signal.strategy_id,
                "signal_time": signal.timestamp,
                "signal_trigger_price": signal.trigger_price,
                "stop_price": signal.stop_price,
                "quantity": signal.quantity,
                "signal_candle_close": meta.get("signal_candle_close"),
                "signal_candle_high": meta.get("signal_candle_high"),
                "signal_candle_low": meta.get("signal_candle_low"),
                "signal_candle_time": candle_time,
                "no_separate_stop": bool(meta.get("no_separate_stop", True)),
                "supertrend_atr": meta.get("supertrend_atr"),
                "supertrend_upper": meta.get("supertrend_upper"),
                "supertrend_lower": meta.get("supertrend_lower"),
                "fill_price": signal.trigger_price,
                "mode": env_obj.mode if env_obj is not None else "PAPER",
                "account": account_dict,
            })
        except Exception as e:
            log.warning("[Engine] telegram signal notify failed: %s", e)

    # ── REVERSAL — durable lifecycle record (SAME trigger / TWO orders) ──
    # The old exit (REVERSAL_EXIT) and the new opposite entry (REVERSAL_ENTRY)
    # always share ONE SIG-X signal id and ONE reversal trigger price.  The
    # record is the dashboard's "reversal chain": REVERSAL SIGNAL -> trigger ->
    # OLD EXIT (order/status) -> OLD FLAT -> NEW ENTRY (order/status) -> NEW
    # POSITION -> NEW SL.  COMPLETE is emitted ONLY when old is flat AND the
    # new entry fill is broker-confirmed AND the new SL is placed.













    # ═══════════════════════════════════════════════════════════════════
    # SL RECOVERY — startup + unprotected position recovery
    # ═══════════════════════════════════════════════════════════════════














    # ═══════════════════════════════════════════════════════════════════
    # PERSISTENCE / EVENTS
    # ═══════════════════════════════════════════════════════════════════

    def _bind_env_aliases(self, env) -> None:
        """Point every engine-level legacy alias at THIS environment's owned
        runtime (strategies, execution, portfolio, risk...)."""
        self.strategies = env.strategies
        self.runtimes = env.runtimes
        self.execution_engine = env.execution_engine
        self.order_manager = env.order_manager
        self.broker_router = env.broker_router
        self.position_manager = env.position_manager
        self.pnl_engines = env.pnl_engines
        self.account_engines = env.account_engines
        self.account_engine = env.account_engine
        self.risk_engine = env.risk_engine
        self.event_store = env.event_store
        self.trade_ledger = env.trade_ledger
        self.fill_dedup = env.fill_dedup
        self.indicators = env.indicators

    def _bind_env_infra(self, env) -> None:
        """LIVE-only: bind the engine's shared-ish infrastructure (event bus,
        candle pipeline, indicators, market status, safe mode, data adapter,
        candle fetcher) to the LIVE environment's OWN runtime."""
        for _attr, _val in (
            ("event_bus", env.event_bus),
            ("candle_distributor", env.candle_distributor),
            ("candle_router", env.candle_router),
            ("indicator_engine", env.indicator_engine),
            ("market_status", env.market_status),
        ):
            if _val is not None:
                setattr(self, _attr, _val)
        if env.safe_mode is not None:
            self.safe_mode = env.safe_mode
        if env.data_adapter is not None:
            self.data_adapter = env.data_adapter
        if env.candle_fetcher is not None:
            self.candle_fetcher = env.candle_fetcher

    def set_persistence(self, persistence, env_name: str = "paper") -> None:
        env = self._env_for(env_name)
        # §9.3 — LIVE env isolation guard: the attached persistence MUST point
        # at the live env's own canonical DB file (snapshotted at build time;
        # immune to later Config singleton mutation).  PAPER attach stays
        # lenient for backward compatibility (paths only warn).
        if persistence is not None and env.is_live:
            expected = Config.resolve_path(env.db_path)
            actual = str(persistence.db_path)
            if Config.resolve_path(actual) != expected:
                raise RuntimeError(
                    f"set_persistence: live env requires the live DB "
                    f"({expected}); got {actual}")
        env.persistence = persistence
        # Stamp the environment's execution mode onto the persistence so every
        # canonical row (signals/orders/fills/trades/positions) is durably
        # tagged with the env it came from — callers need not pre-configure it.
        if persistence is not None:
            try:
                persistence.execution_mode = env.mode
            except Exception:
                pass
        if persistence is not None and getattr(persistence, "db_path", None):
            exp = Config.resolve_path(self.config.get("system", {}).get(
                "db_path", "data/db/supertrend_paper.sqlite3"))
            if not env.is_live and Config.resolve_path(str(persistence.db_path)) != exp:
                log.warning("[Engine] paper persistence path %s != configured %s "
                            "(check wiring)", persistence.db_path, exp)
        if env_name == "paper":
            self._persistence = persistence
        elif self._live_only:
            self._persistence = persistence
        if getattr(env, "broker_router", None) is not None:
            # §40 — broker mappings survive restart: reload the explicit
            # broker_order_id -> strategy mapping from canonical persistence so
            # late-arriving broker fills still route to the correct strategy.
            env.broker_router.set_persistence(persistence)
            if env.is_live and persistence is not None:
                def validate_current_mapping(mapping):
                    get_orders = getattr(persistence, "get_orders", None)
                    if not callable(get_orders):
                        return False
                    rows = get_orders(order_id=str(mapping.get("order_id") or ""))
                    if not rows:
                        return False
                    row = rows[0]
                    matches_mapping = (
                        str(row.get("strategy_id") or "")
                            == str(mapping.get("strategy_id") or "")
                        and str(row.get("instrument") or "").upper()
                            == str(mapping.get("instrument") or "").upper()
                    )
                    valid = matches_mapping and self._is_current_supertrend_order(
                        env, persistence, row, allow_terminal=True)
                    active_states = {"created", "submitted", "acknowledged",
                                     "partially_filled"}
                    if (not valid
                            and str(row.get("state") or "").lower() in active_states
                            and env.safe_mode is not None):
                        env.safe_mode.enter_safe_mode(
                            "unverified_strategy_order",
                            f"Broker mapping {mapping.get('broker_order_id')} points "
                            "to a working order without current SuperTrend lineage")
                    return valid

                set_validator = getattr(env.broker_router,
                                        "set_mapping_validator", None)
                if callable(set_validator):
                    set_validator(validate_current_mapping)
            try:
                env.broker_router.restore()
            except Exception as e:
                log.error("[Engine] broker router restore failed: %s", e)
        # Rebuild every StrategyRuntime lifecycle for THIS environment with the
        # real persistence and restore that strategy's OWN trades from the
        # environment's db. Rebuilding on set_persistence() is safe now (the
        # old wipe bug): runtimes only carry lifecycle caches; strategy state,
        # positions, orders and fills live elsewhere and are preserved.
        self._build_runtimes_for_env(env, persistence)
        if env_name == "paper":
            # _build_runtimes_for_env replaced the env's registry; keep the
            # paper backward-compat alias pointing at the SAME registry the
            # engine hot-path actually uses.
            self.runtimes = env.runtimes
        elif self._live_only:
            # LIVE-only: _build_runtimes_for_env replaced the live env's
            # runtimes registry — refresh the engine surface so every alias
            # still IS the live env's owned runtime.
            self._bind_env_aliases(env)
            self._bind_env_infra(env)

    def publish_event(self, event_type: str, data: dict,
                      env_name: Optional[str] = None) -> None:
        if self._event_callback:
            try:
                self._event_callback(event_type, data)
            except Exception:
                pass
        persistence = (self._env_for(env_name).persistence
                       if env_name is not None else self._persistence)
        if persistence:
            try:
                persistence.save_event({
                    "event_type": event_type,
                    "strategy_id": data.get("strategy_id", ""),
                    "instrument": data.get("instrument", ""),
                    "details": data,
                })
            except Exception:
                pass
        # Telegram is a best-effort presentation channel. Route only the
        # router's curated actionable events; never let alert delivery affect
        # order/state transitions or the dashboard event callback.
        telegram = getattr(self, "telegram", None)
        if telegram is not None:
            try:
                notify = getattr(telegram, "on_engine_event", None)
                if callable(notify):
                    notify(event_type, data if isinstance(data, dict) else {})
            except Exception as exc:
                log.warning("[Engine] Telegram event alert failed (%s): %s",
                            event_type, exc)

    def _publish_order_failure_alert(self, event: dict,
                                     env_name: Optional[str] = None) -> None:
        """Persist order-watcher failure evidence and alert on material failures."""
        event = event if isinstance(event, dict) else {}
        try:
            env = self._env_for(env_name) if env_name is not None else None
            persistence = env.persistence if env is not None else self._persistence
            if persistence is not None:
                persistence.save_execution_failure_event(event)
        except Exception as exc:
            log.warning("[Engine] execution failure persistence failed: %s", exc)
        try:
            telegram = getattr(self, "telegram", None)
            notify = getattr(telegram, "on_engine_event", None)
            if callable(notify):
                notify(str(event.get("event_type") or "ORDER_FAILURE"), event)
        except Exception as exc:
            log.warning("[Engine] Telegram order failure alert failed: %s", exc)

    # ── §34 cross-strategy quarantine ───────────────────────────────────

    def _quarantine_event(self, reason: str, details: dict, persist: bool = True) -> None:
        """Reject a cross-strategy/mismatched lifecycle event.

        Logs ERROR, records the event, counts it, and — when persistence is
        available — writes a quarantine_records row. NEVER mutates lifecycle
        state (that is the caller's contract).
        """
        if not hasattr(self, "quarantine_count"):
            self.quarantine_count = 0
        if not hasattr(self, "_quarantined_events"):
            self._quarantined_events = []
        record = {"reason": reason, "details": dict(details), "timestamp": time.time()}
        self.quarantine_count += 1
        self._quarantined_events.append(record)
        if len(self._quarantined_events) > 500:
            self._quarantined_events = self._quarantined_events[-500:]
        log.error("[Engine] QUARANTINE %s details=%s", reason, details)
        if persist and self._persistence is not None:
            try:
                self._persistence.save_quarantine_record({
                    "original_type": "lifecycle_event",
                    "original_id": str(details.get("trade_id") or details.get("fill_id")
                                       or details.get("signal_id") or "?"),
                    "reason": reason,
                    "payload": details,
                })
            except Exception as e:
                log.error("[Engine] quarantine persist failed: %s", e)
        try:
            self.publish_event("quarantine_event", {"reason": reason, **details})
        except Exception:
            pass

    def quarantine_snapshot(self) -> dict:
        return {
            "count": getattr(self, "quarantine_count", 0),
            "events": list(getattr(self, "_quarantined_events", [])[-100:]),
        }

    # ═══════════════════════════════════════════════════════════════════
    # LIFECYCLE
    # ═══════════════════════════════════════════════════════════════════

    def start(self) -> None:
        self._running = True
        self.market_status.set_engine_status(EngineStatus.RECONCILING)
        for env in self._envs.values():
            if env.market_status is not None:
                env.market_status.set_engine_status(EngineStatus.RECONCILING)

        # Wire TradeCloseManager(s) — one per environment (paper keeps the
        # legacy self._trade_close_manager alias; LIVE gets its own whose
        # portfolio/persistence/ledger are the LIVE ones).
        self._trade_close_manager = TradeCloseManager(
            position_manager=self.position_manager,
            pnl_engines=self.pnl_engines,
            account_engines=self.account_engines,
            global_account=self.account_engine,
            risk_engine=self.risk_engine,
            persistence=self._persistence,
            event_store=self.event_store,
            telegram=self.telegram,
            event_callback=self._event_callback,
            trade_ledger=self.trade_ledger,
        )
        for name, env in self._envs.items():
            if name == "paper":
                continue
            if env.trade_close_manager is None:
                env.trade_close_manager = TradeCloseManager(
                    position_manager=env.position_manager,
                    pnl_engines=env.pnl_engines,
                    account_engines=env.account_engines,
                    global_account=env.account_engine,
                    risk_engine=env.risk_engine,
                    persistence=env.persistence,
                    event_store=env.event_store,
                    telegram=self.telegram,
                    event_callback=self._event_callback,
                    trade_ledger=env.trade_ledger,
                )
        # ── Position-owned SL recovery (INVARIANT 9) ──────────────────────
        # The BROKER is the only authority for "is there an open position".
        # Never arm the local SL from the database alone; missing broker rows
        # remain unresolved, while only a verified entry/order match may
        # recover a prior false broker-flat close.
        for name, env in self._envs.items():
            if not env.is_live:
                continue
            try:
                summary = self.sync_positions_from_broker(env_name=name)
            except Exception as e:
                log.error("[Engine] startup position reconciliation failed for %s: %s", name, e)
                summary = {"status": "failed", "error": str(e)}
            # §35 — trading must not begin on an unreconciled book.  An unknown
            # broker state is not "flat": if the position query failed we do not
            # know what is open, so entries stay closed until it succeeds.
            # A broker position we cannot attribute to any local strategy is
            # equally unsafe to stack new risk onto: it carries no stop we own
            # and can never be exited by us.
            if summary.get("status") == "reconciled" and not summary.get(
                    "orphan_exposure"):
                self._reconciled_envs.add(name)
                log.info("[Engine] startup position reconciliation for env %s: %s",
                         name, summary)
            else:
                self._reconciled_envs.discard(name)
                if summary.get("orphan_exposure"):
                    log.error("[Engine] env %s holds ORPHAN broker position(s) "
                              "%s with no local record — entries stay blocked "
                              "until they are resolved", name,
                              summary.get("orphans"))
                else:
                    log.error("[Engine] startup reconciliation FAILED for %s "
                              "(%s) — entries stay blocked until the broker "
                              "position state is known", name,
                              summary.get("error"))

        self.market_status.set_engine_status(EngineStatus.WARMING_UP)
        for env in self._envs.values():
            if env.market_status is not None:
                env.market_status.set_engine_status(EngineStatus.WARMING_UP)
        # Warm up PAPER from shared data adapter; LIVE from its own adapter.
        self._warmup_from_rest()
        if not self._live_only:
            # Start PAPER data feeds (shared infrastructure). In LIVE-only mode
            # the engine's candle_fetcher/data_adapter ARE the live env's, so
            # they are started by the per-env loop below (never twice).
            self.candle_fetcher.start()
            self.data_adapter.connect()
        # Start LIVE data feeds (per-env independent infrastructure).
        for name, env in self._envs.items():
            if not env.is_live:
                continue
            try:
                if env.candle_fetcher is not None:
                    self._scope_live_candles_to_runtime(env)
                    env.candle_fetcher.start()
                if env.data_adapter is not None:
                    env.data_adapter.connect()
                log.info("[Engine] LIVE env data feeds started for %s", name)
            except Exception as e:
                log.error("[Engine] LIVE env data feed start failed for %s: %s",
                          name, e)
        # READY: no trading yet. The first fresh candle/tick transitions
        # READY -> TRADING via _maybe_enable_trading().
        self.market_status.set_engine_status(EngineStatus.READY)
        for env in self._envs.values():
            if env.market_status is not None:
                env.market_status.set_engine_status(EngineStatus.READY)

        self._start_live_pollers()

        log.info("[Engine] Started — %d strategies active", len(self.strategies))
        print(f"[Engine] Started — {len(self.strategies)} strategies active", flush=True)
        for name, strat in self.strategies.items():
            print(f"  {name}: {strat.instrument} SuperTrend 15m", flush=True)
        try:
            self.publish_event("engine_started", {
                "timestamp": time.time(),
                "strategies": sorted(self.strategies),
            })
        except Exception:
            pass

        try:
            self.telegram.start()
            strategy_list = ", ".join(sorted(self.strategies))
            # Attach the REAL Dhan account snapshot (funds + P&L) to the
            # startup alert so the operator sees actual live capital.
            account_data: dict = {}
            live_env = getattr(self, "live", None)
            if live_env is not None and getattr(live_env, "is_live", False):
                account_data = self._live_account_snapshot(live_env)
            if not account_data:
                for _name, _env in self._envs.items():
                    if getattr(_env, "is_live", False):
                        account_data = self._live_account_snapshot(_env)
                        if account_data:
                            break
            self.telegram.on_startup({
                "timestamp": time.time(),
                "strategies": sorted(self.strategies),
                "strategy_count": len(self.strategies),
                "strategy_list": strategy_list,
                "account": account_data,
            })
        except Exception as e:
            log.warning("[Engine] telegram startup notify failed: %s", e)

    def stop(self) -> None:
        self._running = False
        for env in self._envs.values():
            sync = getattr(env, "sync_service", None)
            if sync is not None:
                try:
                    sync.stop()
                except Exception:
                    pass
                continue
            poller = getattr(env, "poller", None)
            if poller is not None:
                try:
                    poller.stop()
                except Exception:
                    pass
        # Stop PAPER data feeds (shared infrastructure).
        if hasattr(self, 'candle_fetcher') and not self._live_only:
            self.candle_fetcher.stop()
        if hasattr(self, 'data_adapter') and not self._live_only:
            self.data_adapter.disconnect()
        # Stop LIVE data feeds (per-env independent infrastructure).
        for name, env in self._envs.items():
            if not env.is_live:
                continue
            try:
                if env.candle_fetcher is not None:
                    env.candle_fetcher.stop()
                if env.data_adapter is not None:
                    env.data_adapter.disconnect()
            except Exception:
                pass
        # Mark every environment's market status STOPPED.
        for env in self._envs.values():
            if env.market_status is not None:
                try:
                    env.market_status.set_engine_status(EngineStatus.STOPPED)
                except Exception:
                    pass
        # Send shutdown notification then stop the Telegram worker thread.
        try:
            self.telegram.on_shutdown({
                "reason": "normal",
                "strategies": sorted(self.strategies),
                "strategy_count": len(self.strategies),
            })
        except Exception:
            pass
        try:
            self.telegram.stop()
        except Exception:
            pass
        try:
            self.publish_event("engine_stopped", {"timestamp": time.time()})
        except Exception:
            pass
        log.info("[Engine] Stopped")

    def _start_live_pollers(self) -> None:
        """Phase 9.8 — start one BrokerSyncService per LIVE environment.

        The sync service owns the REST poller (orders/positions/account/
        reconcile), optionally wires the WS order feed (accelerator only), and
        exposes a health watchdog.  Handlers are bound to the LIVE env so a
        broker fill/late event can never mutate the PAPER ledger.
        """
        for name, env in self._envs.items():
            if not env.is_live:
                continue
            if getattr(env, "poller", None) is not None:
                continue
            if env.broker is None or env.execution_engine is None:
                continue
            from execution.live.broker_sync import BrokerSyncService
            def handle_fill(fill, sid, is_exit, _env=env):
                # REST/WS fill application mutates the same strategy, order,
                # and position state as tick-trigger submission. Serialize it
                # with the market callbacks so a late fill/reset cannot erase
                # a just-fired trigger between crossing and broker POST.
                with self._lock:
                    return self._handle_fill(
                        fill, sid, is_exit=is_exit, env_name=_env.name)

            def on_reconcile(_env=env):
                # Reconciliation performs broker REST reads; its state-changing
                # portions acquire the engine lock after those reads so a slow
                # response cannot stall market ticks and trigger processing.
                return self._reconcile_strategy_positions(_env.name)
            def reset_strategy_fn(sid, *, signal_id=None, trade_id=None, _env=env):
                # Poller terminal-order cleanup races tick/candle callbacks.
                # Serialize the ownership check and reset with those callbacks,
                # and only release state if this exact terminal order still
                # owns the strategy (a newer pending/fired signal wins).
                with self._lock:
                    strategy = _env.strategies.get(sid)
                    if signal_id is not None and strategy is not None:
                        pending = getattr(strategy, "pending_entry", None)
                        pending_signal_id = getattr(
                            getattr(pending, "signal", None), "signal_id", None)
                        fired_signal_id = getattr(
                            strategy, "_last_fired_trigger_signal_id", None)
                        current_trade_id = getattr(strategy, "current_trade_id", None)
                        if pending is not None:
                            owns = str(pending_signal_id or "") == str(signal_id)
                        elif fired_signal_id:
                            owns = str(fired_signal_id) == str(signal_id)
                        elif trade_id and current_trade_id:
                            owns = str(current_trade_id) == str(trade_id)
                        else:
                            owns = False
                        if not owns:
                            return False
                    self._reset_strategy_state(sid, env_name=_env.name)
                    return True
            env.sync_service = BrokerSyncService(
                env,
                self.config,
                clock=time.time,
                wire_now=True,
                handle_fill=handle_fill,
                on_reconcile=on_reconcile,
                reset_strategy_fn=reset_strategy_fn,
                state_lock=self._lock,
                    on_status=lambda status: self.publish_event(
                        "order_websocket_status_changed", {
                            "component": "Dhan order update WebSocket",
                            "status": ("CONNECTED" if str(status).lower() == "connected"
                                      else "DISCONNECTED" if any(x in str(status).lower()
                                                                 for x in ("closed", "disconnect", "stale"))
                                      else "ERROR" if any(x in str(status).lower()
                                                          for x in ("error", "failed"))
                                      else "STATUS"),
                            "detail": str(status)[:240],
                            "environment": name,
                        }, env_name=name),
            )
            # Order Watcher — continuous broker+market+intent observation with
            # REST-verified recovery (WS fast path → targeted REST verify →
            # priority-safe recovery decision tree).  Observability + durable
            # failure events; entries never run ahead of exits/SL/reversal.
            try:
                from execution.live.order_watcher import OrderWatcher
                watcher = OrderWatcher(
                    engine=env.execution_engine,
                    broker=env.broker,
                    config=self.config,
                    clock=time.time,
                    on_event=lambda kind, data: self.publish_event(
                        kind, data, env_name=name),
                    on_failure_event=lambda ev: self._publish_order_failure_alert(
                        ev, env_name=name),
                    blocker_fn=lambda sid, rec: self._entry_priority_blocker(
                        sid, name, rec=rec),
                    preflight_fn=lambda sid, rec: self._market_fallback_preflight(
                        sid, name, rec),
                    strategy_lookup=lambda sid: env.strategies.get(sid),
                    position_lookup=lambda sid, instrument: next((p for p in
                        env.position_manager.get_positions_by_strategy(sid)
                        if p.is_open and p.instrument == instrument), None),
                    # Fill fast path: fills the WS path surfaces via REST
                    # verification are routed into the strategy lifecycle at
                    # once (kill the ≤2 s poll lag), round-tripping through
                    # the same router the poller uses.
                    on_fills=env.sync_service.route_fills,
                    # Watcher-created fallback orders do not pass through
                    # SignalFlow; persist their order row before submission so
                    # broker fills satisfy the same DB lineage constraints.
                    on_order=lambda order, signal, _env=env: self._persist_order(
                        order, signal, env_name=_env.name),
                )
            except Exception as e:
                log.error("[Engine] order watcher init failed for %s: %s", name, e)
                watcher = None
            env.order_watcher = watcher
            env.sync_service._order_watcher = watcher
            if getattr(env.sync_service, "_poller", None) is not None:
                env.sync_service._poller._order_watcher = watcher
            # Retro-delegate the poller reference (some code paths read
            # env.poller / call poller directly); worker == sync service poller.
            env.poller = env.sync_service._poller
            # §9.11 — pull broker truth at boot so orders this system placed
            # in a previous process run are adopted back into the transport
            # book BEFORE the poller starts consuming statuses.  Reconcile
            # adoption runs ahead of the poller thread so the first poll /
            # reconcile cycle never observes an EMPTY order book (which would
            # otherwise mint spurious reconcile diffs once per restart).
            # Adoption never places/reverses/exits — it only resumes tracking
            # our own day orders and surfaces broker positions/trades.
            try:
                boot = env.sync_service.startup_reconcile()
                log.info("[Engine] %s startup reconcile: status=%s adopted=%d "
                         "orders=%d positions=%d trades=%d errors=%d",
                         name, boot.get("status"),
                         len(boot.get("adopted") or []),
                         len(boot.get("orders") or []),
                         len(boot.get("positions") or []),
                         len(boot.get("trades") or []),
                         len(boot.get("errors") or []))
            except Exception as e:
                log.error("[Engine] %s startup reconcile failed: %s", name, e)
            env.sync_service.start()
            log.info("[Engine] live broker sync started for %s", name)
            self._start_rollover_watchdog(env)

        if self._restore_failures:
            # M7 — a DB trade-restore failed: surface it so the boot is never
            # "armed-but-blind" (engine READY over un-restored open state).
            try:
                self.publish_event("startup_restore_failed", {
                    "failures": {
                        env_name: sids
                        for env_name, sids in self._restore_failures.items()
                    },
                })
            except Exception:
                pass
            for env_name, sids in self._restore_failures.items():
                log.error("[Engine] %s TRADE-RESTORE FAILED for strategies %s — "
                          "broker-authoritative position reconciliation is "
                          "required before entry", env_name, sids)

    def _warmup_from_rest(self, now_epoch: Optional[int] = None) -> None:
        """Warm up each strategy from Dhan REST historical data.

        PAPER strategies use the shared (engine-level) data adapter.
        LIVE strategies use their own per-env data adapter (LIVE independence).

        Active strategy contract (MCX + Dhan):
          - requested end = fetch boundary, NOT a market-close rule;
          - the returned dataset decides the latest available candle;
          - only completed native 15-minute candles update SuperTrend;
          - per instrument and timeframe source forensics.
        """
        import pandas as pd
        warmup_cfg = self.config.get("warmup", {})
        last_days = int(warmup_cfg.get("last_trading_days", 5))
        fetch_days = int(warmup_cfg.get("fetch_calendar_days", 14))
        now = datetime.now(timezone(timedelta(hours=5, minutes=30)))
        base_from = (now - timedelta(days=fetch_days)).date()
        to_date = now.date()
        if now_epoch is None:
            now_epoch = int(time.time())

        # Warm up PAPER strategies using the shared data adapter.
        paper_env = self._envs.get("paper")
        if paper_env is not None:
            self._warmup_env_strategies(
                paper_env, self.data_adapter, self.strategies,
                last_days, fetch_days, base_from, to_date, now_epoch)

        # Warm up LIVE strategies using the per-env data adapter.
        for env_name, env in self._envs.items():
            if not env.is_live:
                continue
            if env.data_adapter is None:
                continue
            self._warmup_env_strategies(
                env, env.data_adapter, env.strategies,
                last_days, fetch_days, base_from, to_date, now_epoch)

    def _warmup_env_strategies(
        self,
        env: Environment,
        data_adapter,
        strategies: dict,
        last_days: int,
        fetch_days: int,
        base_from,
        to_date,
        now_epoch: int,
    ) -> None:
        """Warm up one environment's strategies using the given data adapter."""
        import pandas as pd
        for name, strategy in strategies.items():
            # Strategy IDs (gold_02 / silver_01) are not instrument keys.
            # Warmup must use the actual contract config for Dhan security ID
            # and exchange session boundaries.
            inst_cfg = (self.config.get("instruments", {}) or {}).get(
                strategy.instrument, {})
            session_open = inst_cfg.get("session_open", "09:00")
            session_close = inst_cfg.get("session_close", "23:30")
            security_id = str(inst_cfg.get("security_id", ""))
            try:
                timeframe = "15m"
                accepted, _ = self._fetch_warmup_candles(
                    name, strategy.instrument, "15", timeframe, 15,
                    base_from, to_date, now_epoch, session_open, session_close,
                    security_id, last_days, data_adapter=data_adapter)
                for _, row in accepted.iterrows():
                    open_ts = float(row["timestamp"])
                    bar = Bar(
                        instrument=strategy.instrument, timeframe=timeframe,
                        start_ts=open_ts, end_ts=open_ts + 15 * 60,
                        open=row["open"], high=row["high"], low=row["low"],
                        close=row["close"], volume=int(row["volume"]),
                    )
                    strategy.warmup_indicator(bar)
                log.info("[Engine] %s warmed (%s): SuperTrend %d completed 15m bars",
                         name, env.name, len(accepted))
            except Exception as e:
                log.error("[Engine] %s warmup failed (%s): %s", name, env.name, e)

    def _fetch_warmup_candles(
        self,
        name: str,
        instrument: str,
        tf_id: str,
        tf_name: str,
        tf_min: int,
        base_from: datetime.date,
        to_date: datetime.date,
        now_epoch: int,
        session_open: str,
        session_close: str,
        security_id: str,
        last_days: int,
        data_adapter=None,
    ):
        """Fetch one (symbol, interval) range from Dhan REST, classify it, feed
        forensics, and return only the completed candles (ascending).

        ``data_adapter`` defaults to the engine-level shared adapter (PAPER).
        LIVE callers pass their own per-env adapter.
        """
        import pandas as pd
        if data_adapter is None:
            data_adapter = self.data_adapter
        raw = data_adapter.fetch_historical_candles(
            instrument, tf_id, base_from, to_date)
        key = f"{name}_{tf_id}"
        fore = {
            "requested_start": base_from.isoformat(),
            "requested_end": to_date.isoformat(),
            "security_id": security_id,
            "instrument": name,
            "timeframe": tf_name,
            "interval": tf_id,
            "source": "DHAN_REST",
            "dhan_return_count": len(raw) if raw else 0,
            "first_dhan_candle": None,
            "last_dhan_candle": None,
            "last_completed_candle": None,
            "forming_candle_count": 0,
            "rejected_candle_count": 0,
            "duplicate_count": 0,
            "upsert_count": 0,
            "watermark_before": self._warmup_watermark.get(key),
            "watermark_after": None,
            "rejected": [],
        }
        if not raw:
            log.info("[Engine] warmup %s: no candles", key)
            self._record_warmup_forensics(key, fore)
            return pd.DataFrame(), fore
        fore["first_dhan_candle"] = iso_ist(raw[0][0])
        fore["last_dhan_candle"] = iso_ist(raw[-1][0])
        fore["duplicate_count"] = len(raw) - len({float(c[0]) for c in raw})
        df = pd.DataFrame(raw, columns=["timestamp", "open", "high", "low", "close", "volume"])
        # The raw epoch is already a true UTC instant of the bar open.
        # Convert once for CALENDAR-DAY filtering only (IST wall clock);
        # never re-derive the feed timestamp from the tz-converted
        # wall time, which is host-tz-dependent (naive .timestamp()
        # resolves in the process locale and silently shifts every bar
        # by +5:30 on non-IST hosts and mis-anchoring candle data.
        df["datetime"] = pd.to_datetime(df["timestamp"], unit="s", utc=True).dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
        df = df.sort_values("datetime").reset_index(drop=True)
        if last_days > 0:
            dates = sorted(df["datetime"].dt.date.unique())
            keep = set(dates[-last_days:])
            df = df[df["datetime"].dt.date.isin(keep)].reset_index(drop=True)
        rows = df.values.tolist()
        accepted, rejected = filter_completed(
            rows, tf_id, now_epoch, session_open, session_close)
        fore["forming_candle_count"] = sum(
            1 for r, _ in rejected if r == REASON_FORMING)
        fore["rejected_candle_count"] = len(rejected)
        fore["rejected"] = [
            {"time": iso_ist(c[0]), "reason": r} for r, c in rejected]
        fore["upsert_count"] = len(accepted)
        acc_df = (pd.DataFrame(accepted, columns=df.columns)
                  if accepted else df.iloc[0:0])
        if accepted:
            last_open = float(accepted[-1][0])
            fore["last_completed_candle"] = iso_ist(last_open)
            fore["watermark_after"] = {
                "open_ts": last_open,
                "end_ts": last_open + tf_min * 60,
                "source": "DHAN_REST",
            }
            self._warmup_watermark[key] = {
                "security_id": security_id,
                "timeframe": tf_name,
                "interval": tf_id,
                "latest_completed_open_ts": last_open,
                "latest_completed_end_ts": last_open + tf_min * 60,
                "source": "DHAN_REST",
                "asof": iso_ist(time.time()),
            }
            acc_df = acc_df.sort_values("datetime").reset_index(drop=True)
        self._record_warmup_forensics(key, fore)
        return acc_df, fore

    def _record_warmup_forensics(self, key: str, fore: dict) -> None:
        self._warmup_forensics[key] = dict(fore)
        log.info("[Engine] warmup forensics %s: %s", key,
                 json.dumps(fore, default=str))

    @staticmethod
    def _is_current_supertrend_position(env, persistence, position_data) -> bool:
        """Only rehydrate exposure whose entry signal identifies this runtime.

        A same-named position row from an older strategy is not valid evidence
        that the current SuperTrend generated the trade. Unknown or untagged
        rows are left for broker reconciliation/safe mode instead of being
        silently attached to the current strategy.
        """
        strategy_id = str(position_data.get("strategy_id") or "")
        strategy = (getattr(env, "strategies", {}) or {}).get(strategy_id)
        signal_id = str(position_data.get("entry_signal_id") or "")
        if (strategy is None or getattr(strategy, "strategy_type", None) != "supertrend"
                or not signal_id or persistence is None
                or not callable(getattr(persistence, "get_signal", None))):
            return False
        try:
            signal = persistence.get_signal(signal_id)
            metadata = json.loads((signal or {}).get("signal_metadata") or "{}")
        except Exception:
            return False
        return bool(
            signal
            and str(signal.get("strategy_id") or "") == strategy_id
            and str(signal.get("instrument") or "").upper()
                == str(position_data.get("instrument") or "").upper()
            and metadata.get("strategy_type") == "supertrend"
            and metadata.get("strategy_timeframe") == "15m"
            and metadata.get("market_on_signal_close") is True
            and metadata.get("no_separate_stop") is True
        )

    def _current_supertrend_position_rows(self, env, rows):
        """Filter position rows and fail closed when their strategy lineage is unknown."""
        persistence = getattr(env, "persistence", None)
        accepted = []
        for row in rows or []:
            if self._is_current_supertrend_position(env, persistence, row):
                accepted.append(row)
                continue
            position_id = str(row.get("position_id") or "unknown")
            if env.safe_mode is not None:
                env.safe_mode.enter_safe_mode(
                    "unverified_strategy_position",
                    f"Position {position_id} lacks current SuperTrend lineage")
            log.error("[Engine] did not restore unverified position %s; "
                      "broker reconciliation required", position_id)
        return accepted

    @staticmethod
    def _is_current_supertrend_order(env, persistence, order_data,
                                     allow_terminal: bool = False) -> bool:
        """Validate a working order before restoring it into the live engine.

        Active broker orders without a verifiable current-strategy signal must
        not be rebound to the current strategy after restart. Such orders are
        left for broker reconciliation and safe mode instead.
        """
        strategy_id = str(order_data.get("strategy_id") or "")
        strategy = (getattr(env, "strategies", {}) or {}).get(strategy_id)
        instrument = str(order_data.get("instrument") or "").upper()
        strategy_instrument = str(getattr(strategy, "instrument", "") or "").upper()
        order_type = str(order_data.get("order_type") or "MARKET").upper()
        order_role = str(order_data.get("order_role") or "").upper()
        state = str(order_data.get("state") or "").lower()
        allowed_states = {"created", "submitted", "acknowledged", "partially_filled"}
        if allow_terminal:
            allowed_states.update({"filled", "rejected", "canceled", "cancelled", "expired"})
        if (strategy is None
                or getattr(strategy, "strategy_type", None) != "supertrend"
                or not instrument
                or (strategy_instrument and strategy_instrument != instrument)
                or order_type != "MARKET"
                or order_role not in {"ENTRY", "EXIT", "REVERSAL_ENTRY",
                                      "REVERSAL_EXIT", "EMERGENCY_EXIT"}
                or state not in allowed_states
                or persistence is None
                or not callable(getattr(persistence, "get_signal", None))):
            return False
        signal_id = str(
            order_data.get("signal_id")
            or order_data.get("entry_signal_id")
            or order_data.get("parent_signal_id")
            or ""
        )
        if not signal_id:
            return False
        try:
            signal = persistence.get_signal(signal_id)
            metadata = json.loads((signal or {}).get("signal_metadata") or "{}")
        except Exception:
            return False
        if not (signal
                and str(signal.get("strategy_id") or "") == strategy_id
                and str(signal.get("instrument") or "").upper() == instrument
                and metadata.get("strategy_type") == "supertrend"
                and metadata.get("market_on_signal_close") is True):
            return False
        # Strategy-generated entry/reversal signals include timeframe and
        # no-stop markers. Operator exit signals are current execution actions
        # and carry `exit: true` instead of a candle timeframe.
        return bool(
            (metadata.get("strategy_timeframe") == "15m"
             and metadata.get("signal_source")
                 == "completed_native_15m_candle"
             and metadata.get("no_separate_stop") is True)
            or (metadata.get("is_reversal_entry") is True
                and metadata.get("strategy_timeframe") == "15m"
                and metadata.get("signal_source")
                    == "completed_native_15m_candle"
                and metadata.get("no_separate_stop") is True)
            or metadata.get("exit") is True
        )

    def _current_supertrend_order_rows(self, env, rows):
        """Keep verified active current-strategy orders; quarantine unknown ones."""
        persistence = getattr(env, "persistence", None)
        accepted = []
        active_states = {"created", "submitted", "acknowledged", "partially_filled"}
        for row in rows or []:
            if str(row.get("state") or "").lower() not in active_states:
                accepted.append(row)
                continue
            if self._is_current_supertrend_order(env, persistence, row):
                accepted.append(row)
                continue
            order_id = str(row.get("order_id") or "unknown")
            if env.safe_mode is not None:
                env.safe_mode.enter_safe_mode(
                    "unverified_strategy_order",
                    f"Working order {order_id} lacks current SuperTrend lineage")
            log.error("[Engine] did not restore unverified working order %s; "
                      "broker reconciliation required", order_id)
        return accepted

    def restore(self, saved_state: dict, env_name: str = "paper") -> None:
        """Restore ONE environment's engine state from a saved snapshot.

        Restores that environment's strategy and pending-order state
        AND its per-strategy position managers (each position back into its
        OWN runtime). Account margin is then reconstituted from the restored
        open positions so the startup reconciliation margin check is exactly
        consistent. PAPER is the default target (legacy callers unchanged);
        the LIVE environment restores its own snapshot/state file.
        """
        if not saved_state and env_name != "live":
            return
        saved_state = saved_state or {}
        env = self._env_for(env_name)
        strategies_state = saved_state.get("strategies", {})
        for name, strat in env.strategies.items():
            if name in strategies_state:
                strategy_snapshot = strategies_state[name]
                current_snapshot = (isinstance(strategy_snapshot, dict)
                                    and strategy_snapshot.get("strategy_type") == "supertrend"
                                    and strategy_snapshot.get("timeframe") == "15m")
                if not current_snapshot:
                    if (isinstance(strategy_snapshot, dict)
                            and strategy_snapshot.get("position_side")):
                        reason = "unverified_strategy_snapshot_position"
                        if env.safe_mode is not None:
                            env.safe_mode.enter_safe_mode(
                                reason, f"Ignored non-current strategy snapshot for {name}")
                        log.error("[Engine] ignored non-current strategy snapshot with "
                                  "position for %s; broker reconciliation required", name)
                    strategy_snapshot = None
            else:
                strategy_snapshot = None
            if strategy_snapshot is not None:
                try:
                    strat.restore(strategy_snapshot)
                except Exception:
                    pass
            registry = getattr(env, "pending_triggers", None)
            if registry is not None:
                registry.sync_strategy(strat)
        persistence = getattr(env, "persistence", None)
        positions_state = saved_state.get("positions")
        if isinstance(positions_state, dict):
            open_positions = positions_state.get("open_positions", {}) or {}
            if isinstance(open_positions, dict):
                accepted = {}
                for key, position_data in open_positions.items():
                    if (isinstance(position_data, dict)
                            and self._is_current_supertrend_position(
                                env, persistence, position_data)):
                        accepted[key] = position_data
                    else:
                        if env.safe_mode is not None:
                            env.safe_mode.enter_safe_mode(
                                "unverified_strategy_position",
                                f"Position {key} lacks current SuperTrend lineage")
                        log.error("[Engine] did not restore unverified open position %s; "
                                  "broker reconciliation required", key)
                positions_state = dict(positions_state)
                positions_state["open_positions"] = accepted
        if (env.mode == "LIVE"
                and not (positions_state and positions_state.get("open_positions"))):
            get_open_positions = getattr(persistence, "get_open_positions", None)
            if callable(get_open_positions):
                try:
                    durable_positions = get_open_positions()
                    restored_open = {}
                    for row in self._current_supertrend_position_rows(
                            env, durable_positions):
                        pid = str(row.get("position_id") or "")
                        if not pid:
                            continue
                        strategy = env.strategies.get(str(row.get("strategy_id") or ""))
                        entry_time = row.get("entry_time") or row.get("entry_timestamp") or 0.0
                        try:
                            entry_time = float(entry_time)
                        except (TypeError, ValueError):
                            try:
                                from datetime import datetime
                                entry_time = datetime.fromisoformat(
                                    str(entry_time).replace("Z", "+00:00")).timestamp()
                            except (TypeError, ValueError):
                                entry_time = 0.0
                        average_entry = float(row.get("average_entry_price") or 0.0)
                        restored_open[pid] = {
                            "position_id": pid,
                            "strategy_id": row.get("strategy_id"),
                            "instrument": row.get("instrument"),
                            "side": str(row.get("side") or "LONG").upper(),
                            "quantity": int(row.get("quantity") or 0),
                            "average_entry": average_entry,
                            "entry_timestamp": entry_time,
                            "entry_fill_ids": [],
                            "stop_price": row.get("stop_price"),
                            "current_mark": average_entry,
                            "realized_pnl": float(row.get("realized_pnl") or 0.0),
                            "unrealized_pnl": 0.0,
                            "margin": 0.0,
                            "trade_id": row.get("trade_id"),
                            "exit_reason": None,
                            "exit_fills": [],
                            "status": "open",
                            "multiplier": float(getattr(strategy, "multiplier", 1.0) or 1.0),
                            "entry_signal_id": row.get("entry_signal_id"),
                            "exit_signal_id": row.get("exit_signal_id"),
                            "sl_state": row.get("sl_state"),
                            "sl_trigger_price": row.get("sl_trigger_price"),
                            "sl_protected_at": row.get("sl_protected_at"),
                            "entry_order_id": row.get("entry_order_id"),
                            "exit_order_id": row.get("exit_order_id"),
                            "position_generation": int(row.get("position_generation") or 0),
                            "exit_started": bool(row.get("exit_started")),
                            "lifecycle_id": row.get("lifecycle_id") or row.get("trade_id"),
                        }
                    if restored_open:
                        positions_state = dict(positions_state or {})
                        positions_state["open_positions"] = restored_open
                        positions_state.setdefault("closed_positions", [])
                        positions_state.setdefault("generation_counters", {})
                        log.warning("[Engine] restored %d LIVE open position(s) "
                                    "from canonical DB without a JSON snapshot",
                                    len(restored_open))
                except Exception as e:
                    log.error("[Engine] durable open-position recovery failed: %s", e)
        if positions_state:
            try:
                persistence = getattr(env, "persistence", None)
                get_open_positions = getattr(persistence, "get_open_positions", None)
                if callable(get_open_positions):
                    try:
                        durable_rows = self._current_supertrend_position_rows(
                            env, get_open_positions())
                        positions_state = merge_durable_open_position_rows(
                            positions_state, durable_rows)
                    except Exception:
                        log.exception(
                            "[Engine] durable open-position hydration failed; "
                            "restoring the runtime snapshot as-is")
                env.position_manager.restore(positions_state)
            except Exception:
                pass
        # Rebind the strategy's ownership to the durable open position. The
        # configured SuperTrend strategies do not maintain a separate stop.
        for sid, strat in env.strategies.items():
            try:
                open_pos = next((p for p in env.position_manager.get_positions_by_strategy(sid)
                                 if p.is_open), None)
                if open_pos is None:
                    continue
                # The canonical open position owns current exposure after a
                # restart. Rebind strategy memory to that exact identity so a
                # DB-restored position cannot coexist with a flat strategy
                # state (or a different stale snapshot position).
                strat.position_side = "LONG" if open_pos.is_long else "SHORT"
                strat.current_position_id = open_pos.position_id
                strat.position_generation = open_pos.position_generation
                strat.position_quantity = open_pos.quantity
                strat.current_trade_id = open_pos.trade_id
                strat.state = (StrategyState.LONG_POSITION if open_pos.is_long
                               else StrategyState.SHORT_POSITION)
            except Exception as e:
                log.error("[Engine] restore: position binding failed for %s: %s", sid, e)
        # Reconcile exit state after restart when no order remains in flight.
        for name, strat in env.strategies.items():
            if strat.state == StrategyState.EXIT_ORDER_SUBMITTED:
                open_pos = next((
                    p for p in env.position_manager.get_positions_by_strategy(name)
                    if p.is_open), None)
                if open_pos is not None and strat.position_side is not None:
                    strat.state = (StrategyState.LONG_POSITION if strat.position_side == "LONG"
                                   else StrategyState.SHORT_POSITION)
                elif open_pos is None:
                    strat.state = StrategyState.FLAT
                    strat.position_side = None
        # Reconstitute per-strategy + global used_margin from restored open
        # positions so reconciliation (account vs position margins) is exact.
        for strat_id, account in env.account_engines.items():
            account.used_margin = sum(
                p.margin for p in env.position_manager.get_positions_by_strategy(strat_id)
                if p.is_open
            )
        env.account_engine.used_margin = sum(
            p.margin for p in env.position_manager.open_positions if p.is_open
        )
        # ### OBSERVATION (risk restore)
        # kill_switch_active / daily_pnl / peak_equity were memory-only, so a
        # restart silently dropped an engaged kill switch and reset the daily
        # loss budget.  The risk payload is now part of every saved engine
        # snapshot; load it back so risk enforcement is continuous.
        risk_state = saved_state.get("risk")
        if risk_state:
            try:
                env.risk_engine.restore(risk_state)
            except Exception:
                pass
        # ### FIX (restart exec-book restore)
        # The live execution engine's in-memory orders/fills/current_prices are
        # restored from the snapshot so (a) the order watcher resumes tracking
        # resting orders after the reboot, (b) broker REST statuses polled after
        # the restart map onto the correct internal orders through the broker
        # router, and (c) already-confirmed fills stay visible to reconciliation.
        exec_state = saved_state.get("live_execution")
        if exec_state:
            try:
                if env.mode == "LIVE" and isinstance(exec_state, dict):
                    exec_state = dict(exec_state)
                    exec_state["orders"] = self._current_supertrend_order_rows(
                        env, exec_state.get("orders", []))
                if getattr(env.execution_engine, "restore", None) is not None:
                    env.execution_engine.restore(exec_state)
                    router = getattr(env.execution_engine, "broker_router", None)
                    if router is not None and hasattr(router, "register_from_kwargs"):
                        for o in (getattr(env.execution_engine, "_orders", {})
                                  or {}).values():
                            bid = getattr(o, "_broker_order_id", None)
                            current_order = {
                                "strategy_id": getattr(o, "strategy_id", ""),
                                "instrument": getattr(o, "instrument", ""),
                                "order_type": getattr(o, "order_type", "MARKET"),
                                "order_role": getattr(o, "order_role", ""),
                                "state": getattr(getattr(o, "state", None),
                                                  "value", getattr(o, "state", "")),
                                "entry_signal_id": getattr(o, "entry_signal_id", None),
                            }
                            if (bid and router.resolve(str(bid)) is None
                                    and self._is_current_supertrend_order(
                                        env, env.persistence, current_order,
                                        allow_terminal=True)):
                                try:
                                    router.register_from_kwargs(
                                        broker_order_id=str(bid),
                                        order_id=o.order_id,
                                        trade_id=getattr(o, "trade_id", "") or "",
                                        strategy_id=o.strategy_id,
                                        instrument=getattr(o, "instrument", ""),
                                    )
                                except Exception:
                                    pass
            except Exception as e:
                log.warning("[Engine] %s live execution restore failed: %s",
                            env_name, e)
        # The snapshot is only a cache. Recover any still-working LIVE orders
        # that were durably written to SQLite but absent from that snapshot so
        # the broker status map can resolve to an internal order after restart.
        if env.mode == "LIVE" and env.persistence is not None:
            restore_orders = getattr(
                env.execution_engine, "restore_orders_from_persistence", None)
            get_orders = getattr(env.persistence, "get_orders", None)
            if callable(restore_orders) and callable(get_orders):
                try:
                    order_rows = self._current_supertrend_order_rows(env, get_orders())
                    count = restore_orders(order_rows)
                    if count:
                        log.warning("[Engine] restored %d LIVE order(s) from canonical DB "
                                    "missing in execution snapshot", count)
                except Exception as e:
                    log.error("[Engine] %s durable order restore failed: %s",
                              env_name, e)
        # Per-strategy operator gates survive restart via the saved snapshot.
        gates_state = saved_state.get("strategy_gates")
        if gates_state:
            try:
                runtime_policy = self.config.get("strategy_runtime", {}) or {}
                if runtime_policy.get("exclusive"):
                    active_ids = set(env.strategies or {})
                    gates_state = {sid: gate for sid, gate in gates_state.items()
                                   if sid in active_ids}
                self._strategy_gates = restore_strategy_gates(
                    self._strategy_gates, gates_state)
            except Exception:
                pass
        # ### OBSERVATION (ledger rebuild on restore)
        # The per-strategy PNLEngines are memory-only accumulate/dervive:
        # realized P&L, trade_count, win_rate and charges were zeroed on every
        # restart, dragging /api/overview, /api/pnl, /api/strategies and the
        # legacy reconciliation "P&L mismatch" check back to 0/error.  The
        # persisted closed trades are the source of truth, so rebuild the
        # engines AND the realized/charges figures on the root + per-strategy
        # accounts from them.  DB-in + DB-out keeps every consumer correct.
        try:
            if env.persistence is not None:
                all_trades = env.persistence.get_trades()
                closed = [
                    t for t in all_trades
                    if (t.get("status") or "").lower() == "closed"
                    and t.get("net_pnl") is not None
                ]
                for name, pnl_eng in env.pnl_engines.items():
                    pnl_eng.rebuild_from_ledger(
                        [t for t in all_trades if t.get("strategy_id") == name]
                    )
                closed_net = sum(float(t.get("net_pnl") or 0.0) for t in closed)
                closed_charges = sum(float(t.get("charges") or 0.0) for t in closed)
                env.account_engine.realized_pnl = closed_net
                env.account_engine.charges = closed_charges
                env.account_engine.cash = (
                    env.account_engine.starting_capital + closed_net
                )
                by_sid: dict = {}
                for t in closed:
                    by_sid.setdefault(t.get("strategy_id"), []).append(t)
                for sid, acct in env.account_engines.items():
                    acct.realized_pnl = sum(
                        float(t.get("net_pnl") or 0.0) for t in by_sid.get(sid, [])
                    )
                    acct.charges = sum(
                        float(t.get("charges") or 0.0) for t in by_sid.get(sid, [])
                    )
        except Exception:
            pass
        # Mirror current trade ids into each runtime from the restored strategy.
        if env.runtimes is not None:
            for rt in env.runtimes.all():
                rt.current_trade_id = getattr(rt.strategy, "current_trade_id", None)
        # DB recovery is a one-time startup operation (before start() enables
        # WebSocket ticks). Never query SQLite from the per-tick callback.
        self._restore_live_pending_triggers(env)
        try:
            self.publish_event("engine_restored", {
                "timestamp": time.time(),
                "strategies": sorted(env.strategies),
            }, env_name=env.name)
        except Exception:
            pass

    def snapshot(self, env_name: Optional[str] = None) -> dict:
        env = self._env_for(env_name)
        router_stats = getattr(self, "candle_router", None)
        router_stats = router_stats.stats() if router_stats is not None else {}
        positions = env.position_manager.snapshot()
        unrealized = 0.0
        try:
            open_pos = positions.get("open_positions", {})
            unrealized = sum(
                float(pos.get("unrealized_pnl", 0) or 0) for pos in open_pos.values()
            )
            # ### OBSERVATION (account.unrealized_pnl feed)
            # The account-level unrealized P&L is a derived figure: it is the
            # sum of every open position's unrealized P&L. Previously nothing
            # in production ever called update_unrealized_pnl, so the WS
            # engine_state push carried a permanent 0 and the browser's
            # engine_state override stomped the correct REST /api/overview
            # equity/net with a wrong 12,00,000/0. Recomputing it here (every
            # snapshot -> both the 0.5s WS push and the 60s persistence) makes
            # the returned account snapshot economically correct.
            env.account_engine.update_unrealized_pnl(unrealized)
        except Exception:
            pass
        # ### OBSERVATION (risk feed)
        # Risk enforcement (daily-loss limit + kill switch + drawdown) needs
        # the live open unrealized P&L to react to a floating drawdown. We feed
        # it here every snapshot and also keep peak equity moving with the
        # freshly computed account equity. The risk dict itself is returned so
        # the WS engine_state push carries today_pnl / kill-switch live state
        # to the browser instead of only the connection-time REST values.
        try:
            env.risk_engine.set_open_unrealized(unrealized)
        except Exception:
            pass
        account = env.account_engine.snapshot()
        try:
            env.risk_engine.update_peak_equity(float(account.get("equity", 0.0) or 0.0))
        except Exception:
            pass
        # Live execution-book snapshot: the in-memory orders/fills/current
        # prices survive restart so REST statuses arriving after the reboot map
        # onto the correct orders and the order watcher resumes tracking them.
        live_execution = None
        exec_engine = getattr(env, "execution_engine", None)
        if exec_engine is not None:
            snap = getattr(exec_engine, "snapshot", None)
            if snap is not None:
                try:
                    live_execution = snap()
                except Exception:
                    live_execution = None
        log.info("[Engine] %s snapshot: live_execution=%s", env_name,
                 "present" if live_execution else "none")
        return {
            "running": self._running,
            "execution_mode": env.mode,
            "strategies": {name: strat.snapshot() for name, strat in env.strategies.items()},
            "strategy_gates": self.strategy_gates(),
            "positions": positions,
            "account": account,
            "risk": env.risk_engine.snapshot(),
            "event_bus": self.event_bus.snapshot(),
            "candle_distributor": self.candle_distributor.candle_count,
            "candle_router": router_stats,
            "live_execution": live_execution,
        }

    # ── Aggregate lifecycle views for shared API/WS infrastructure ──
    # These are read-only aggregations over the per-strategy lifecycles; no
    # shared mutable lifecycle state exists.

    def get_trade(self, trade_id: str) -> Optional[Any]:
        """Find a trade across the per-strategy lifecycles (read-only)."""
        for rt in self.runtimes.all():
            trade = rt.lifecycle.get_trade(trade_id)
            if trade is not None:
                return trade
        return None

    def reconcile_trades(self) -> dict:
        """Aggregate lifecycle.reconcile() across per-strategy lifecycles."""
        errors: list[Any] = []
        warnings: list[Any] = []
        stats = {"total_trades": 0, "open": 0, "closed": 0, "pending": 0}
        for rt in self.runtimes.all():
            res = rt.lifecycle.reconcile()
            errors.extend(res.get("errors", []))
            warnings.extend(res.get("warnings", []))
            for k, v in res.get("stats", {}).items():
                stats[k] = stats.get(k, 0) + v
        return {"errors": errors, "warnings": warnings, "stats": stats}

    def orphan_scan(self) -> dict:
        """Aggregate orphan_scan() across per-strategy lifecycles."""
        merged = {
            "orphan_fills": [], "orphan_orders": [], "orphan_positions": [],
            "orphan_pending_orders": [], "trades_without_signals": [],
            "trades_without_positions": [], "trades_with_wrong_exit_state": [],
            "mismatched_memory_db": [], "total_orphans": 0, "is_clean": False,
        }
        for rt in self.runtimes.all():
            try:
                res = rt.lifecycle.orphan_scan()
            except Exception:
                continue
            for key in ("orphan_fills", "orphan_orders", "orphan_positions",
                        "orphan_pending_orders", "trades_without_signals",
                        "trades_without_positions", "trades_with_wrong_exit_state",
                        "mismatched_memory_db"):
                merged[key].extend(res.get(key, []))
            merged["total_orphans"] += res.get("total_orphans", 0)
        merged["is_clean"] = merged["total_orphans"] == 0
        return merged

    def notify_settings_refreshed(self) -> None:
        self.publish_event("settings_refreshed", {"timestamp": time.time()})

    def _reconcile_strategy_positions(self, env_name: Optional[str] = None) -> None:
        """Reconcile strategy state with actual positions for one (or every)
        environment.

        Heals the crash/REST restart gap where a strategy may be persisted as
        FLAT while the (per-strategy) position manager still holds an open
        position: re-derive the strategy's state/side/stop from the live open
        position so a restart never double-entries into a held position.
        """
        targets = [self._env_for(env_name)] if env_name is not None else list(self._envs.values())
        for env in targets:
            if env.position_manager is None:
                continue
            # §35 — the periodic reconcile is also a chance to RE-ESTABLISH a
            # known broker position state.  A startup query that failed must not
            # block entries forever: the first successful broker-authoritative
            # sync re-opens the gate, and any later failure closes it again.
            if getattr(env, "is_live", False):
                try:
                    sl_summary = self.sync_positions_from_broker(env_name=env.name)
                except Exception as e:
                    sl_summary = {"status": "failed", "error": str(e)}
                with self._lock:
                    if sl_summary.get("status") == "reconciled" and not \
                            sl_summary.get("orphan_exposure"):
                        if env.name not in self._reconciled_envs:
                            log.info("[Engine] broker position state re-established "
                                     "for %s — entries re-enabled", env.name)
                        self._reconciled_envs.add(env.name)
                    else:
                        # A stale/incomplete snapshot keeps entry admission shut
                        # until the next broker-authoritative pass is consistent.
                        self._reconciled_envs.discard(env.name)
                if sl_summary.get("status") == "reconciled" and not \
                        sl_summary.get("orphan_exposure"):
                    # The old reversal exit may be filled before Dhan's
                    # position endpoint reports flat. Keep its opposite entry
                    # parked until this broker-authoritative reconciliation,
                    # then submit it immediately (there is no second trigger).
                    for sid, strategy in list(env.strategies.items()):
                        pending = getattr(strategy, "pending_entry", None)
                        signal = getattr(pending, "signal", None)
                        if (pending is not None
                                and getattr(pending, "status", None)
                                    == "waiting_for_flat"
                                and bool((getattr(signal, "metadata", None) or {})
                                         .get("is_reversal_entry"))):
                            self._activate_reversal_entry_after_flat(env, sid)
            with self._lock:
                for sid, strategy in list(env.strategies.items()):
                    open_pos = next((
                        p for p in env.position_manager.get_positions_by_strategy(sid)
                        if p.is_open), None)
                    if open_pos is None:
                        continue
                    if strategy.state not in (StrategyState.LONG_POSITION, StrategyState.SHORT_POSITION):
                        side_val = getattr(getattr(open_pos, "side", None), "value", None)
                        if side_val is None:
                            side_val = "LONG" if bool(getattr(open_pos, "is_long", False)) else "SHORT"
                        is_long = (side_val == "LONG")
                        strategy.state = (StrategyState.LONG_POSITION if is_long
                                          else StrategyState.SHORT_POSITION)
                        strategy.position_side = "LONG" if is_long else "SHORT"

        # Phase 9.7 — broker-authoritative position resolution (LIVE only,
        # opt-in via `live.reconcile_auto_resolve`; default OFF keeps the
        # legacy report-only contract identical).  The broker is the authority:
        #   1. broker flat  + local open  -> mirror the local position closed
        #      (audit event; exits stay possible regardless of the entry gate).
        #   2. broker open  + local flat  -> adopt the broker position into the
        #      local mirror (audit event + risk/margin check; only when the
        #      entry gate is ON).
        # Any other mismatch is REPORTED as reconciliation_mismatch and never
        # auto-invented.
        if env.is_live:
            live_cfg = self.config.get("live", {}) or {}
            if bool(live_cfg.get("reconcile_auto_resolve", False)):
                self._resolve_broker_positions(env, live_cfg)

    def _resolve_broker_positions(self, env, live_cfg: dict) -> None:
        """Resolve local position mirrors against the broker's position report.

        Gated (rollback): only runs inside ``_reconcile_strategy_positions``
        when ``live.reconcile_auto_resolve`` is explicitly true.  Every action
        is logged as an audit event; a conflicting/unknown case is surfaced as
        ``reconciliation_mismatch`` and never guessed.
        """
        brokers_positions = getattr(getattr(env, "broker", None), "positions", None)
        if not callable(brokers_positions):
            return
        with self._lock:
            before_fingerprint = self._sl_reconciliation_fingerprint(env)
        try:
            broker_rows = list(brokers_positions() or [])
        except Exception:
            broker_rows = []
        # The optional auto-resolution path also consumes a slow broker REST
        # snapshot. Never let an older flat/open response close or adopt over a
        # position/order transition that completed while that request ran.
        with self._lock:
            if self._sl_reconciliation_fingerprint(env) != before_fingerprint:
                self.publish_event("reconciliation_snapshot_discarded", {
                    "reason": "local_lifecycle_changed_during_broker_query",
                    "execution_mode": env.mode,
                }, env_name=env.name)
                return
            return self._resolve_broker_positions_snapshot(
                env, live_cfg, broker_rows)

    def _resolve_broker_positions_snapshot(self, env, live_cfg,
                                           broker_rows: list) -> None:
        broker_map = {}
        for r in broker_rows:
            key = (r.get("strategy_id"), r.get("instrument"))
            broker_map[key] = r
        local_map = {}
        for pos in list(env.position_manager.open_positions or []):
            sid = getattr(pos, "strategy_id", None)
            if sid:
                local_map[(sid, pos.instrument)] = pos
        seen = set()
        for (sid, instrument) in (set(broker_map) | set(local_map)):
            seen.add((sid, instrument))
            if not sid or not instrument:
                continue
            local_pos = local_map.get((sid, instrument))
            broker_row = broker_map.get((sid, instrument))
            if local_pos is None:
                if broker_row is not None:
                    self._adopt_broker_position(env, live_cfg, sid, instrument, broker_row)
                continue
            if broker_row is None:
                self._close_local_mirror(env, sid, local_pos)
                continue
            bside = str(broker_row.get("side") or "").upper()
            bqty = int(broker_row.get("quantity") or 0)
            lside = "LONG" if local_pos.is_long else "SHORT"
            if (bside != lside or bqty != int(getattr(local_pos, "quantity", 0))
                    or bside not in ("LONG", "SHORT")):
                self.publish_event("reconciliation_mismatch", {
                    "strategy_id": sid, "instrument": instrument,
                    "broker_side": bside, "broker_quantity": bqty,
                    "local_side": lside,
                    "local_quantity": int(getattr(local_pos, "quantity", 0)),
                    "reason": "position_mismatch_unresolved",
                    "execution_mode": env.mode}, env_name=env.name)
        # broker rows whose strategy did not map to an env strategy are still
        # surfaced (never silently dropped)
        for (sid, instrument) in broker_map:
            if (sid, instrument) in seen:
                continue
            if sid not in env.strategies and env.is_live:
                self.publish_event("reconciliation_mismatch", {
                    "strategy_id": sid, "instrument": instrument,
                    "broker_side": str(broker_map[(sid, instrument)].get("side") or "").upper(),
                    "broker_quantity": int(broker_map[(sid, instrument)].get("quantity") or 0),
                    "local_side": None, "local_quantity": 0,
                    "reason": "broker_position_unowned_strategy",
                    "execution_mode": env.mode}, env_name=env.name)




    def _maybe_enable_trading(self) -> None:
        """Transition READY -> TRADING when the market is open and live market
        data is confirmed (via WebSocket ticks OR fresh REST candles).

        Called after every tick/candle. Trading becomes allowed only when:
        engine READY, MarketState LIVE_TRADING, and data is live.

        Also transitions every environment's own market_status to TRADING so
        per-env gate checks see the live/ready state.
        """
        if (self.market_status.engine_status == EngineStatus.READY
                and self.market_status.state == MarketState.LIVE_TRADING
                and self.market_status.has_live_market_data):
            self.market_status.set_engine_status(EngineStatus.TRADING)
            for env in getattr(self, "_envs", {}).values():
                ms = env.market_status
                if ms is None or ms.engine_status != EngineStatus.READY:
                    continue
                if (ms.state == MarketState.LIVE_TRADING
                        and ms.has_live_market_data):
                    try:
                        ms.set_engine_status(EngineStatus.TRADING)
                    except Exception:
                        pass
