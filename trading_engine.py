"""Main trading engine — event-driven architecture with per-strategy isolation.

Architecture:
    CandleFetcher → NativeCandleDistributor → EventBus → StrategyInstance.on_candle()

Each StrategyInstance owns its own:
    - DEMAATR indicators (fast, mid, slow)
    - HTF state (mid_htf_state, slow_htf_state)
    - Strategy state (FLAT/LONG/SHORT, pending, stop, etc.)

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
from strategies.instance import StrategyInstance
from strategies.types import Signal, SignalType, StrategyState, PendingEntry, resolve_order_role
from strategies.gold import create_gold_5m, create_gold_15m
from strategies.silver import create_silver_5m, create_silver_15m
from indicators.shared import SharedNativeIndicatorEngine
from execution.models import Fill
from execution.broker_router import BrokerEventRouter
from execution.fee_model import MCXFeeModel
from execution.order_manager import OrderManager, OrderManagerFacade
from portfolio.position_manager import PositionManager, PositionManagerFacade, Position
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

log = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))


def _strategy_positions_for_risk(signal_type, open_positions) -> int:
    """Number of the strategy's open positions to apply against the per-strategy
    position cap for an incoming order."""
    open_held = [p for p in open_positions if getattr(p, "is_open", False)]
    holds_short = any(getattr(p, "is_short", False) for p in open_held)
    holds_long = any(getattr(p, "is_long", False) for p in open_held)
    if signal_type.name == "LONG" and holds_short:
        return max(0, len(open_held) - 1)
    if signal_type.name == "SHORT" and holds_long:
        return max(0, len(open_held) - 1)
    return len(open_held)


class StrategyGate:
    """Per-strategy operator control state (spec §4/§5 — strategy isolation).

    Every strategy carries its own independent gate that the operator controls
    via start / stop / close_only / emergency_stop / lock / resume.  The gate
    ONLY affects NEW ENTRIES and (via the *_enabled flags) which exit classes
    the engine is allowed to keep placing.  Existing positions are always
    eligible for protective SL handling unless sl_enabled is explicitly off.

    ``live_gate`` mirrors the master LIVE gate vocabulary (ON / OFF /
    CLOSE_ONLY / EMERGENCY_STOP / LOCKED) but scoped to ONE strategy in EVERY
    environment it participates in (PAPER included), so strategy-level intent
    is uniform.  Default acts are harmless: fully open.
    """

    __slots__ = ("live_gate", "entry_enabled", "exit_enabled",
                 "reversal_enabled", "sl_enabled", "close_only")

    def __init__(self, live_gate: str = "ON",
                 entry_enabled: bool = True, exit_enabled: bool = True,
                 reversal_enabled: bool = True, sl_enabled: bool = True,
                 close_only: bool = False):
        self.live_gate = str(live_gate).upper()
        self.entry_enabled = bool(entry_enabled)
        self.exit_enabled = bool(exit_enabled)
        self.reversal_enabled = bool(reversal_enabled)
        self.sl_enabled = bool(sl_enabled)
        self.close_only = bool(close_only)

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
            "sl_enabled": self.sl_enabled,
            "close_only": self.close_only,
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
            sl_enabled=bool(data.get("sl_enabled", True)),
            close_only=bool(data.get("close_only", False)),
        )


# Strategy-control actions understood by TradingEngine.control_strategy().
STRATEGY_GATE_ACTION_ERRORS = {
    "start": "entry_gate_closed",
    "stop": "entry_gate_closed",
    "close_only": "entry_gate_closed",
    "emergency_stop": "emergency_gate_closed",
    "lock": "strategy_locked",
}


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


class TradingEngine:
    """Event-driven trading engine with per-strategy isolation.

    Event flow:
    Dhan WebSocket → Tick → EventBus → StrategyInstance.on_tick()
    Dhan REST → CandleFetcher → NativeCandleDistributor → EventBus → StrategyInstance.on_candle()
    """

    def __init__(self, config_path: Optional[str] = None, event_callback=None,
                 live_only: bool = False):
        self.config = Config()
        if config_path:
            self.config.load(Path(config_path))
        else:
            self.config.load()

        # ── Contract rollover (expiry −1 trading day for GOLDM/SILVERM) ──
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
            instruments=self.config.get("instruments", {}),
        )
        indicator_cfg = self.config.get("indicators", {})
        self.indicator_engine = SharedNativeIndicatorEngine(
            dema_period=indicator_cfg.get("dema_period", 3),
            atr_period=indicator_cfg.get("atr_period", 6),
            atr_factor=indicator_cfg.get("atr_factor", 1.0),
        )

        # ── Initialize (shared market-data infrastructure) ──
        self._init_market_status()
        self._init_data_adapter()
        self._init_indicator_engines()
        self._init_htf_engine()
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
        for sid in (self.config.get("strategies", {}) or {}):
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
        # §22-25 — protective-SL that could not be cancelled at exit: entries
        # for (env, strategy, instrument) stay BLOCKED until the resting order
        # is confirmed cancelled, so an orphan SLM can never fire against a
        # new opposite position.
        self._uncancelled_sl: dict[tuple[str, str, str], str] = {}

        # ── Warmup forensics + latest-completed watermark (mission: direct
        # Dhan REST source + latest-available backfill) ──
        self._warmup_forensics: dict[str, dict] = {}
        self._warmup_watermark: dict[str, dict] = {}

        # Live vs bar-model signal routing
        self.tick_signal_processing = True

        log.info("[Engine] Initialized with %d strategies across %d env(s)",
                 len(self.strategies), len(self._envs))

    # ═══════════════════════════════════════════════════════════════════
    # CONTRACT ROLLOVER (GOLDM/SILVERM expiry −1 trading day)
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

    def _rollover_metal_open(self, env: Environment, metal: str) -> bool:
        """True when the metal still has exposure or a live pending entry."""
        broker = getattr(env, "broker", None)
        if broker is not None and hasattr(broker, "_own_net_positions"):
            try:
                if any(p.get("instrument") == metal
                       for p in broker._own_net_positions()):
                    return True
            except Exception:
                pass
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
            actions = self._rollover_service.evaluate(
                metal, symbol, sid, today,
                self._rollover_metal_open(env, metal))
            if "block_entries" in actions:
                self._rollover_blocked.add((env.name, metal))
            if "cancel_resting" in actions and broker is not None:
                cancel = getattr(broker, "cancel_instrument_orders", None)
                if callable(cancel):
                    day_key = (env.name, metal, today.isoformat())
                    if day_key not in self._rollover_canceled_daily:
                        self._rollover_canceled_daily.add(day_key)
                        try:
                            result = cancel(metal)
                            self.publish_event("contract_rollover_cancel_resting", {
                                "instrument": metal, "result": result,
                            }, env_name=env.name)
                        except Exception as e:
                            log.warning("[Rollover] cancel resting failed: %s", e)
            if "force_close" in actions and broker is not None:
                try:
                    result = self.emergency_exit_all(env.name, instrument=metal)
                    self.publish_event("contract_rollover_force_close", {
                        "instrument": metal, "result": result,
                    }, env_name=env.name)
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
                "system", {}).get(key, "data/db/trading.db"))
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
                "live_db_path", "data/db/live_trading.db"))
        else:
            db_path = Config.resolve_path(self.config.get("system", {}).get(
                "db_path", "data/db/trading.db"))
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
            sl_enabled=bool(scfg.get("sl_enabled", True)),
            close_only=bool(scfg.get("close_only", False)),
        )

    def _gate_for(self, strategy_id: str) -> StrategyGate:
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
        return {sid: gate.to_dict() for sid, gate in self._strategy_gates.items()}

    def strategy_gate(self, strategy_id: str) -> dict:
        if strategy_id not in self._strategy_gates:
            self._strategy_gates[strategy_id] = self._default_strategy_gate(strategy_id)
        return self._strategy_gates[strategy_id].to_dict()

    def set_strategy_gate(self, strategy_id: str, **fields) -> dict:
        """Set individual gate flags for one strategy (validated, additive).

        Accepted fields: live_gate, entry_enabled, exit_enabled,
        reversal_enabled, sl_enabled, close_only.  Unknown keys are rejected.
        """
        gate = self._gate_for(strategy_id)
        allowed = {"live_gate", "entry_enabled", "exit_enabled",
                   "reversal_enabled", "sl_enabled", "close_only"}
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
        if "sl_enabled" in fields:
            gate.sl_enabled = bool(fields["sl_enabled"])
        if "close_only" in fields:
            gate.close_only = bool(fields["close_only"])
        self.publish_event("strategy_gate_changed", {
            "strategy_id": strategy_id,
            "gate": gate.to_dict(),
            "timestamp": time.time(),
        })
        return gate.to_dict()

    def control_strategy(self, strategy_id: str, action: str) -> dict:
        """High-level operator control for ONE strategy (spec §5).

        start          -> fully open (entries + all exit classes).
        resume         -> alias of start (legacy name kept).
        stop           -> CLOSE_ONLY: block new entries, keep SL + exits so a
                          running position is still managed to the exit.
        close_only     -> block new entries + reversals, keep exits + SL.
        emergency_stop -> block entries immediately; keep SL + standard exits
                          active (the safe unwind path).
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
            gate.sl_enabled = True
            gate.close_only = False
            if strat is not None:
                strat.enabled = True
        elif a == "stop" or a == "close_only":
            # stop == close_only: entries blocked, risk-reducing exits active.
            gate.live_gate = "CLOSE_ONLY"
            gate.entry_enabled = False
            gate.exit_enabled = True
            gate.reversal_enabled = False
            gate.sl_enabled = True
            gate.close_only = True
        elif a == "emergency_stop":
            gate.live_gate = "EMERGENCY_STOP"
            gate.entry_enabled = False
            gate.exit_enabled = True
            gate.reversal_enabled = False
            gate.sl_enabled = True
            gate.close_only = True
            self.publish_event("strategy_emergency_stop", {
                "strategy_id": strategy_id, "timestamp": time.time(),
            })
        elif a == "lock":
            gate.live_gate = "LOCKED"
            gate.entry_enabled = False
            gate.exit_enabled = False
            gate.reversal_enabled = False
            gate.sl_enabled = False
            gate.close_only = True
        elif a == "pause":
            # Legacy pause semantics preserved: drop the pending breakout and
            # freeze all signal generation (this also sets gate entry OFF).
            if strat is not None:
                open_positions = self.position_manager.get_positions_by_strategy(strategy_id)
                if any(pos.is_open for pos in open_positions):
                    return {"success": False, "strategy_id": strategy_id, "action": action,
                            "error": "Cannot pause a strategy with an open position; close it first."}
                strat.pending_entry = None
                strat.enabled = False
            gate.live_gate = "CLOSE_ONLY"
            gate.entry_enabled = False
            gate.close_only = True
        else:
            return {"success": False, "strategy_id": strategy_id, "action": action,
                    "error": f"Unknown action '{action}' "
                             "(expected start, stop, close_only, emergency_stop, lock, "
                             "pause or resume)"}
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
        try:
            return len(om.get_active_orders(strategy_id) or [])
        except Exception:
            return 0

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
        mo = scfg.get("max_pending_orders")
        if mo is not None and self._pending_orders_count(env, sid) >= int(mo):
            return False, "pending_orders_cap"
        return True, ""

    # ═══════════════════════════════════════════════════════════════════
    # INIT METHODS (kept for backward compatibility with tests/scripts)
    # ═══════════════════════════════════════════════════════════════════

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

    # ═══════════════════════════════════════════════════════════════════
    # PER-ENV DATA INFRASTRUCTURE (LIVE independence)
    # ═══════════════════════════════════════════════════════════════════

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
            if env.is_live and (self.config.get("live") or {}).get("execution_model") == "immediate_limit":
                exec_model = "immediate_limit"
            else:
                exec_model = "pending_breakout"
            strategy = factory(
                strategy_id=strat_name,
                instrument=instrument,
                quantity=strat_config.get("quantity", 1),
                capital=strat_config.get("capital", 300_000.0),
                multiplier=inst_cfg.get("multiplier", 10.0),
                security_id=str(inst_cfg.get("security_id", "") or ""),
                execution_model=exec_model,
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
        """Resolve the Phase-3 LIVE price model from ``live.price_model``.

        Disabled (default) returns None -> LIVE keeps the legacy MARKET
        behavior exactly as before.  Enabled offsets the broker-side STOP_LOSS
        trigger by entry_offset/high+ and low- and plans protective SL exits at
        sl_offset-adjusted stops, both as STOP_LOSS stop-limits on the
        instrument tick grid (Dhan DH-906: BUY limit>trigger, SELL limit<trigger).
        PAPER never consults this knob.
        """
        pm = live_cfg.get("price_model") or {}
        if not bool(pm.get("enabled", False)):
            return None
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
        """Fail-closed ownership validation at the sole Dhan submit boundary."""
        role = str(getattr(order, "order_role", "") or "").upper()
        safe_mode = getattr(env, "safe_mode", None)
        if safe_mode is not None and safe_mode.is_active and role in (
                "ENTRY", "REVERSAL_ENTRY", "FALLBACK_MARKET"):
            return "SAFE_MODE_ACTIVE"
        lifecycle_id = getattr(order, "lifecycle_id", None) or getattr(order, "trade_id", None)
        signal_id = getattr(order, "parent_signal_id", None) or getattr(order, "entry_signal_id", None)
        if not lifecycle_id or lifecycle_id != getattr(order, "trade_id", None):
            return "ORDER_LIFECYCLE_MISMATCH"
        if not signal_id:
            return "ORDER_SIGNAL_OWNERSHIP_MISSING"
        positions = env.position_manager.get_positions_by_strategy(order.strategy_id)
        current = next((p for p in positions
                        if p.is_open and p.instrument == order.instrument), None)
        if role in ("ENTRY", "REVERSAL_ENTRY", "FALLBACK_MARKET"):
            if current is None:
                return None
            if (role == "FALLBACK_MARKET"
                    and current.trade_id == lifecycle_id
                    and current.position_id == getattr(order, "parent_position_id", None)
                    and current.position_generation == getattr(order, "position_generation", None)):
                return None
            return "ENTRY_BLOCKED_POSITION_NOT_FLAT"
        if role not in ("EXIT", "STOP_LOSS", "REVERSAL_EXIT", "EMERGENCY_EXIT"):
            return "ORDER_ROLE_INVALID"
        parent_position_id = (getattr(order, "parent_position_id", None)
                              or getattr(order, "position_id", None))
        if not parent_position_id or current is None:
            return "EXIT_POSITION_OWNERSHIP_MISSING"
        if current.position_id != parent_position_id:
            return "STALE_LIFECYCLE_TRIGGER_REJECTED:position_mismatch"
        if current.trade_id != lifecycle_id:
            return "STALE_LIFECYCLE_TRIGGER_REJECTED:lifecycle_mismatch"
        if (getattr(order, "position_generation", None) is None
                or current.position_generation != order.position_generation):
            return "STALE_LIFECYCLE_TRIGGER_REJECTED:generation_mismatch"
        if (current.strategy_id != order.strategy_id
                or current.instrument != order.instrument
                or current.quantity <= 0):
            return "STALE_LIFECYCLE_TRIGGER_REJECTED:position_invalid"
        if current.exit_started and role != "EMERGENCY_EXIT":
            return "POSITION_EXIT_ALREADY_STARTED"
        if current.exit_started:
            return "POSITION_EXIT_ALREADY_STARTED"
        expected_side = "SELL" if current.is_long else "BUY"
        if str(order.side).upper() != expected_side:
            return "STALE_LIFECYCLE_TRIGGER_REJECTED:side_mismatch"
        if int(order.quantity or 0) <= 0 or int(order.quantity) > int(current.quantity):
            return "ORDER_QUANTITY_EXCEEDS_POSITION"
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

    # ═══════════════════════════════════════════════════════════════════
    # CANDLE + TICK HANDLERS (EventBus-driven)
    # ═══════════════════════════════════════════════════════════════════

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
            if not self._running or not self.tick_signal_processing:
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
        The decisive additional gate is the orphan-SL block: when the old
        protective SL could not be cancelled above, a NEW opposite position
        must not be placed while that resting STOP_LOSS could still fire.

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
        if (env.name, strategy_id, instrument) in self._uncancelled_sl:
            return "orphan_sl_unreleased"
        return None

    def _process_signal(self, signal, env_name: Optional[str] = None) -> None:
        """Move one strategy signal through the explicit durable lifecycle.

        Signal creation and breakout execution are deliberately separate: a
        pending breakout only writes the immutable signal; a trade id is born
        only after a trigger has actually occurred.

        All mutable lifecycle/execution/position state is resolved from the
        signal's OWN StrategyRuntime *inside the owning environment* — a signal
        can never touch another strategy's lifecycle caches, order state, or
        positions, nor another environment's execution state.  PAPER and LIVE
        both process the identical signal stream end-to-end; only their
        execution transports, persistence and portfolios differ.
        """
        env = self._env_for(env_name)
        metadata = signal.metadata or {}
        is_exit = bool(metadata.get("exit"))
        is_pending = bool(metadata.get("pending")) and not bool(metadata.get("triggered"))
        strategy = env.strategies.get(signal.strategy_id)
        if strategy is None:
            log.error("Dropping signal for unknown strategy %s in %s",
                      signal.strategy_id, env.name)
            self._quarantine_event(
                "unknown_strategy_signal",
                {"signal_id": signal.signal_id, "strategy_id": signal.strategy_id,
                 "execution_mode": env.mode})
            return
        try:
            runtime = env.runtimes.require(signal.strategy_id)
        except (KeyError, ValueError):
            self._quarantine_event(
                "no_runtime_for_strategy",
                {"signal_id": signal.signal_id, "strategy_id": signal.strategy_id,
                 "execution_mode": env.mode})
            return
        if runtime is None:
            self._quarantine_event(
                "no_runtime_for_strategy",
                {"signal_id": signal.signal_id, "strategy_id": signal.strategy_id})
            return
        lifecycle = runtime.lifecycle
        order_manager = runtime.order_manager
        position_manager = runtime.position_manager

        # A bare opposite-side signal while this strategy holds an open
        # position is a REVERSAL: it closes the held position (never opens a
        # phantom/duplicate trade). Re-entry on the opposite side happens only
        # via a later breakout trigger armed by the strategy.
        if not is_exit and not is_pending:
            sig_side = (signal.side or getattr(signal.signal_type, "value", "")).upper()
            if sig_side in ("LONG", "SHORT"):
                open_pos = next((
                    p for p in position_manager.get_positions_by_strategy(signal.strategy_id)
                    if p.is_open and p.instrument == signal.instrument), None)
                if open_pos is not None:
                    held_side = "LONG" if open_pos.is_long else "SHORT"
                    if held_side != sig_side:
                        from strategies.types import Signal as StratSignal
                        reversal = StratSignal(
                            signal_type=signal.signal_type,
                            instrument=signal.instrument,
                            strategy_id=signal.strategy_id,
                            timestamp=signal.timestamp,
                            trigger_price=signal.trigger_price,
                            stop_price=signal.stop_price,
                            quantity=signal.quantity,
                        )
                        reversal.signal_id = signal.signal_id
                        reversal.lifecycle_id = signal.lifecycle_id
                        reversal.parent_position_id = signal.parent_position_id
                        reversal.position_generation = signal.position_generation
                        reversal.metadata = dict(signal.metadata or {})
                        reversal.metadata.update({
                            "exit": True,
                            "exit_reason": f"{held_side.lower()}_reversal",
                            "is_reversal": True,
                        })
                        signal = reversal
                        metadata = signal.metadata
                        is_exit = True

        # ═══════════════════════════════════════════════════════════════
        # CANCEL IN-FLIGHT: when the strategy detected an opposite crossover
        # while ENTRY_TRIGGERED or PENDING_* (a LIMIT rests at the broker,
        # or a trigger is waiting), the signal carries cancel_inflight=True.
        # Find and cancel any in-flight entry order, terminalize the old
        # durable pending row (if LIVE), reset strategy state, then fall
        # through to process the new signal normally.
        # ═══════════════════════════════════════════════════════════════
        if (not is_exit and bool(metadata.get("cancel_inflight"))
                and strategy is not None):
            old_trade_id = getattr(strategy, "current_trade_id", None)
            if old_trade_id is not None:
                exe = self.execution_engine
                for o in list(getattr(exe, "_orders", {}).values()):
                    if (o.strategy_id == signal.strategy_id
                            and o.trade_id == old_trade_id
                            and o.state.value in ("created", "submitted")):
                        role = (getattr(o, "order_role", "") or "").upper()
                        if role.startswith("ENTRY") or role == "REVERSAL_ENTRY":
                            log.info("[Engine] cancel_inflight: cancelling %s "
                                     "for %s (opposite signal %s)",
                                     o.order_id, signal.strategy_id,
                                     signal.signal_id)
                            exe.cancel_order(o.order_id)
                            break
            # Terminalize the old durable pending row (Phase 9.6) so the
            # superseded pending order doesn't stay ARMED forever in the DB.
            # Use old_pending_id from metadata if available, otherwise fall
            # back to _last_armed_pending_id.
            old_pending_id = (metadata.get("old_pending_id")
                              or getattr(strategy, "_last_armed_pending_id", None))
            if old_pending_id is not None and env.persistence is not None:
                try:
                    env.persistence.terminalize_pending_order(
                        old_pending_id, status="cancelled_by_reversal",
                        reason="opposite_crossover_superseded")
                except Exception as e:
                    log.warning("[Engine] cancel_inflight: failed to "
                                "terminalize pending %s: %s", old_pending_id, e)
            self._reset_strategy_state(signal.strategy_id, env_name=env.name)
            # If this is a cancel-only signal (no new entry intended),
            # skip further processing — no trade/order to create.
            if bool(metadata.get("cancel_only")):
                return

        # §66 — IDEMPOTENT REPLAY: a signal whose own trade already executed
        # its entry (entry_fill recorded => a real position exists) must never
        # be executed again.  Replaying the same entry signal upstream (crash
        # replay, WS+REST double delivery, operator retry, backfill) must not
        # mint a second trade, a second order, a second fill or an additional
        # position on the FIRST trade.  A trade that exists but has NOT
        # executed its entry (placement was rejected / retry pending) still
        # re-places through the existing trade — never a fresh one.
        if not is_exit and not is_pending:
            prior_trade = lifecycle.resolve_trade_from_signal(signal.signal_id)
            if prior_trade is not None and getattr(prior_trade, "entry_fill_id", None):
                self.publish_event("signal_replayed_ignored", {
                    "signal_id": signal.signal_id,
                    "trade_id": prior_trade.trade_id,
                    "strategy_id": signal.strategy_id,
                    "already_filled": prior_trade.entry_fill_id,
                    "execution_mode": env.mode}, env_name=env.name)
                return

        # ═══════════════════════════════════════════════════════════════
        # PER-STRATEGY OPERATOR GATE + LOTS + STRATEGY RISK GATE (§4/§5/§7).
        # Enforced BEFORE any durable row is written so a blocked signal never
        # spawns an orphan signal/trade.  Entries must pass the strategy's own
        # gate (independent of the environment's master gate); exits stay
        # available unless the operator explicitly disabled that exit class.
        # A reversal is an exit + the seed of the opposite entry: it obeys the
        # reversal_enabled flag specifically.
        # ═══════════════════════════════════════════════════════════════
        gates = self._gate_for(signal.strategy_id)
        reversal_sig = (bool((metadata or {}).get("is_reversal"))
                        and not bool((metadata or {}).get("is_reversal_entry")))
        if reversal_sig and not gates.reversal_enabled:
            self._publish_gate_block(signal, "reversal_disabled",
                                     {"gate": gates.to_dict()}, env)
            self._reset_strategy_state(signal.strategy_id, env_name=env.name)
            return
        if reversal_sig and not self._reversal_under_cap(signal):
            self._publish_gate_block(signal, "reversal_daily_cap",
                                     {"gate": gates.to_dict()}, env)
            self._reset_strategy_state(signal.strategy_id, env_name=env.name)
            return
        if is_exit:
            exit_reason_md = str((metadata or {}).get("exit_reason") or "").lower()
            is_sl_exit = ("stop_loss" in exit_reason_md
                          or resolve_order_role(signal) == "STOP_LOSS")
            if is_sl_exit:
                if not gates.sl_enabled:
                    self._publish_gate_block(signal, "sl_disabled",
                                             {"gate": gates.to_dict()}, env)
                    self._reset_strategy_state(signal.strategy_id, env_name=env.name)
                    return
            elif not gates.exit_enabled:
                self._publish_gate_block(signal, "exit_disabled",
                                         {"gate": gates.to_dict()}, env)
                self._reset_strategy_state(signal.strategy_id, env_name=env.name)
                return
        else:
            # Contract rollover: entries in an EXPIRING series are blocked
            # from expiry −1 trading day until the next boot applies the
            # switch.  This supersedes every other entry gate.
            if (env.name, signal.instrument) in self._rollover_blocked:
                self._publish_gate_block(
                    signal, "contract_rollover_blocked",
                    {"rollover": "expiring_series_window"}, env)
                try:
                    self.telegram.on_risk_alert({
                        "severity": "WARNING",
                        "type": "contract_rollover_blocked",
                        "message": f"{signal.instrument}: entries blocked in the "
                                   f"expiring series (rollover window)",
                        "strategy_id": signal.strategy_id,
                        "instrument": signal.instrument,
                    })
                except Exception as e:
                    log.warning("[Engine] rollover telegraph failed: %s", e)
                self._reset_strategy_state(signal.strategy_id, env_name=env.name)
                return
            # Entries must pass the strategy's own gate AND the legacy enabled
            # flag (defense in depth: the config/operator can freeze here too).
            if (not getattr(strategy, "enabled", True)
                    or not gates.entries_allowed):
                reason = ("strategy_disabled" if not getattr(strategy, "enabled", True)
                          else gates.live_gate if gates.live_gate != "ON"
                          else "entry_disabled" if not gates.entry_enabled
                          else "close_only")
                self._publish_gate_block(signal, reason,
                                         {"gate": gates.to_dict()}, env)
                self._reset_strategy_state(signal.strategy_id, env_name=env.name)
                return
            ok, reject_reason = self._validate_strategy_risk_gate(signal, env, gates)
            if not ok:
                self._publish_gate_block(signal, reject_reason,
                                         {"gate": gates.to_dict()}, env)
                try:
                    self.telegram.on_risk_alert({
                        "severity": "WARNING",
                        "type": "strategy_gate_blocked",
                        "message": reject_reason,
                        "strategy_id": signal.strategy_id,
                        "instrument": signal.instrument,
                        "side": signal.signal_type.name,
                        "trigger_price": signal.trigger_price,
                    })
                except Exception as e:
                    log.warning("[Engine] telegram risk alert failed: %s", e)
                self._reset_strategy_state(signal.strategy_id, env_name=env.name)
                return

        self._persist_signal(signal, "exit" if is_exit else "entry", env_name)
        self.publish_event("signal_created", {
            "signal_id": signal.signal_id, "strategy_id": signal.strategy_id,
            "instrument": signal.instrument, "signal_type": signal.signal_type.name,
            "trigger_price": signal.trigger_price, "stop_price": signal.stop_price,
            "pending": is_pending,
        }, env_name=env.name)
        if not is_exit:
            self._notify_signal(signal, env_name)
        if is_pending:
            # Phase 9.6 — a LIVE pending breakout gets a durable lifecycle row
            # (PENDING -> ARMED) in the live DB immediately, so every LIVE
            # pending order owns one durable state that survives restart.
            # PAPER is untouched (strategy memory only, as before). The
            # strategy itself is never modified (read-only for 9.6).
            if env.mode == "LIVE":
                self._arm_live_pending(signal, env)
            return

        # Exits reduce risk and remain available during a safety halt. Entries
        # must pass both the session/data gate and the (environment's) risk gate.
        if not is_exit:
            safe_mode = env.safe_mode if env.safe_mode is not None else self.safe_mode
            market_status = env.market_status if env.market_status is not None else self.market_status
            if safe_mode.is_active or not market_status.is_trading_allowed:
                self._reset_strategy_state(signal.strategy_id, env_name=env.name)
                return
            account = env.account_engines.get(signal.strategy_id)
            multiplier = self.config.instrument(signal.instrument).get("multiplier", 1.0)
            required_margin = self._calculate_margin(signal.instrument, signal.trigger_price, signal.quantity)
            held = position_manager.get_positions_by_strategy(signal.strategy_id)
            allowed, reason = env.risk_engine.check_order(
                signal, len(env.position_manager.open_positions),
                _strategy_positions_for_risk(signal.signal_type, held),
                account.available_margin if account else 0.0, required_margin,
                account.equity if account else 0.0,
            )
            if not allowed:
                # risk.allow_broker_margin_reject: when enabled, an entry that
                # fails ONLY the local margin pre-check is still sent to the
                # broker so the broker (RMS/DH-905) is the rejecting authority
                # (matches the live condition where qty=100 > available margin).
                # Every OTHER risk rejection (kill switch, position limits,
                # daily loss, drawdown) still blocks locally.
                broker_margin_reject = bool((self.config.get("risk") or {}).get(
                    "allow_broker_margin_reject", False))
                if broker_margin_reject and reason == "insufficient_margin":
                    log.warning("Local margin pre-check bypassed for %s (%s): "
                                "%s -> delegating to broker", signal.strategy_id,
                                env.name, reason)
                else:
                    log.warning("Order rejected for %s (%s): %s",
                                signal.strategy_id, env.name, reason)
                    self._reset_strategy_state(signal.strategy_id, env_name=env.name)
                    self.publish_event("order_rejected", {"signal_id": signal.signal_id,
                        "strategy_id": signal.strategy_id, "instrument": signal.instrument,
                        "reason": reason, "execution_mode": env.mode}, env_name=env.name)
                    return

        multiplier = self.config.instrument(signal.instrument).get("multiplier", 1.0)
        # Phase 9.6 — never auto-place a broker-unknown entry. When a signal
        # maps to a durable LIVE pending order, the entry may be placed ONLY
        # while that pending order is ARMED. A missing/terminal row (lost on a
        # restart, or already EXPIRED/CANCELLED_BY_REVERSAL) blocks placement
        # BEFORE any trade is born, so no orphan trade is created.
        live_pending = None
        if env.mode == "LIVE" and not is_exit:
            live_pending = self._live_pending_row(env, signal)
            if live_pending is not None:
                pend_status = (live_pending.get("status") or "").lower()
                if pend_status != PendingOrderState.ARMED.value:
                    self.publish_event("pending_order_blocked", {
                        "signal_id": signal.signal_id,
                        "pending_order_id": live_pending.get("pending_order_id"),
                        "state": pend_status,
                        "reason": "durable_pending_not_armed"
                                    if pend_status else "durable_pending_missing",
                        "execution_mode": env.mode}, env_name=env.name)
                    return

        if is_exit:
            position = next((p for p in position_manager.get_positions_by_strategy(signal.strategy_id)
                             if p.instrument == signal.instrument and p.is_open), None)
            if env.mode == "LIVE":
                if (position is None
                        or not signal.parent_position_id
                        or not signal.lifecycle_id
                        or signal.parent_position_id != position.position_id
                        or signal.lifecycle_id != position.trade_id
                        or signal.position_generation != position.position_generation):
                    self.publish_event("stale_lifecycle_trigger_rejected", {
                        "signal_id": signal.signal_id,
                        "strategy_id": signal.strategy_id,
                        "instrument": signal.instrument,
                        "lifecycle_id": signal.lifecycle_id,
                        "position_id": signal.parent_position_id,
                        "position_generation": signal.position_generation,
                        "reason": "current_position_ownership_mismatch",
                        "execution_mode": env.mode}, env_name=env.name)
                    return
            trade = lifecycle.get_trade(position.trade_id) if position else None
            if trade is None:
                log.error("Exit signal %s has no explicit open trade", signal.signal_id)
                return
            # §22-24 — LIVE positions already protected by a resting broker-side
            # SL must NOT mint a duplicate broker exit order when the strategy
            # stop fires: the broker SL is the exit mechanism.  Suppressed only
            # for stop-loss exits AND only while the protection is active
            # (placed/verified).  A failed/cancelled protection lets the
            # strategy's own SL exit act as the safety net.
            if env.mode == "LIVE" and position is not None:
                exit_reason_md = str((signal.metadata or {}).get("exit_reason") or "").lower()
                is_sl_signal = (exit_reason_md in ("stop_loss_hit", "stop_loss")
                                or resolve_order_role(signal) == "STOP_LOSS")
                if is_sl_signal and getattr(position, "sl_state", None) in ("placed", "verified"):
                    self.publish_event("sl_exit_suppressed", {
                        "signal_id": signal.signal_id,
                        "position_id": position.position_id,
                        "sl_order_id": position.sl_order_id,
                        "strategy_id": signal.strategy_id,
                        "instrument": signal.instrument,
                        "reason": "broker_protective_sl_active",
                        "execution_mode": env.mode}, env_name=env.name)
                    return
        else:
            # §25/§28/§108 — EXIT-FIRST: a LIVE entry is only placed after the
            # broker proves the instrument FLAT (only when live.exit_first is
            # enabled).  A residual broker position blocks the entry and keeps
            # the pending breakout armed for the next verified cycle.
            if env.mode == "LIVE" and not is_pending:
                exit_first = ((self.config.get("live") or {}).get("exit_first")
                              or {}).get("enabled", False)
                if exit_first:
                    flat, detail = self._broker_flat_for_entry(env, signal)
                    if not flat:
                        if bool((signal.metadata or {}).get("is_reversal_entry")):
                            side = str(signal.side or signal.signal_type.value).upper()
                            strategy.pending_entry = PendingEntry(
                                signal=signal, trigger_price=signal.trigger_price,
                                side=side, status="pending", created_at=time.time())
                            strategy.state = (StrategyState.PENDING_LONG if side == "LONG"
                                              else StrategyState.PENDING_SHORT)
                        self.publish_event("reversal_flat_gate_blocked", {
                            "signal_id": signal.signal_id,
                            "strategy_id": signal.strategy_id,
                            "instrument": signal.instrument,
                            "reason": detail.get("reason"),
                            "detail": detail,
                            "execution_mode": env.mode}, env_name=env.name)
                        if env.persistence is not None:
                            import uuid as _uuid
                            try:
                                env.persistence.save_execution_failure_event({
                                    "event_id": f"FLTG-{_uuid.uuid4().hex}",
                                    "event_type": "REVERSAL_FLAT_GATE_BLOCKED",
                                    "strategy_id": signal.strategy_id,
                                    "signal_id": signal.signal_id,
                                    "instrument": signal.instrument,
                                    "error": "broker not flat (exit-first)",
                                    "action": "blocked_entry",
                                    "final_state": "RECONCILIATION_REQUIRED",
                                    "details": detail,
                                })
                            except Exception:
                                pass
                        return
            trade = lifecycle.resolve_trade_from_signal(signal.signal_id)
            if trade is None:
                trade = lifecycle.create_trade_from_signal(
                    signal, signal.strategy_id, signal.strategy_id, signal.instrument,
                    signal.quantity, multiplier,
                )
            strategy.current_trade_id = trade.trade_id
            runtime.current_trade_id = trade.trade_id
            signal.lifecycle_id = trade.trade_id
            if signal.position_generation is None:
                signal.position_generation = position_manager.allocate_generation(
                    signal.strategy_id, signal.instrument)
            signal.metadata = dict(signal.metadata or {})
            signal.metadata["position_generation"] = signal.position_generation

        # ── Exit side: must be the opposite of the open position ──────────
        # LIVE broker-fill-vs-position logic (phase 9.7) derives is_exit from
        # side direction; an exit signal submitted with the same side as the
        # position would be misread as an augment.  The caller supplies the
        # correct SELL/BUY side so the fill always routes to close.
        exit_side = None
        if is_exit and position is not None:
            exit_side = "SELL" if position.is_long else "BUY"
            if (env.mode == "LIVE"
                    and resolve_order_role(signal) != "STOP_LOSS"
                    and getattr(position, "sl_order_id", None)
                    and getattr(position, "sl_state", None) in ("placed", "verified")):
                try:
                    cancelled = bool(env.execution_engine.cancel_order(position.sl_order_id))
                except Exception:
                    cancelled = False
                if not cancelled:
                    self._uncancelled_sl[(env.name, position.strategy_id,
                                          position.instrument)] = position.sl_order_id
                    if env.safe_mode is not None:
                        env.safe_mode.enter_safe_mode(
                            "order_state_uncertain", "protective stop cancel unconfirmed")
                    self.publish_event("exit_blocked_sl_cancel_unconfirmed", {
                        "signal_id": signal.signal_id,
                        "position_id": position.position_id,
                        "sl_order_id": position.sl_order_id,
                        "execution_mode": env.mode}, env_name=env.name)
                    return
                position.sl_state = "cancelled"
                self._persist_position(position, env.name)
            if (env.mode == "LIVE"
                    and resolve_order_role(signal) != "STOP_LOSS"
                    and getattr(position, "sl_order_id", None)
                    and getattr(position, "sl_state", None) in ("placed", "verified")):
                try:
                    cancelled = bool(env.execution_engine.cancel_order(position.sl_order_id))
                except Exception:
                    cancelled = False
                if not cancelled:
                    self._uncancelled_sl[(env.name, position.strategy_id,
                                          position.instrument)] = position.sl_order_id
                    if env.safe_mode is not None:
                        env.safe_mode.enter_safe_mode(
                            "order_state_uncertain", "protective stop cancel unconfirmed")
                    self.publish_event("exit_blocked_sl_cancel_unconfirmed", {
                        "signal_id": signal.signal_id,
                        "position_id": position.position_id,
                        "sl_order_id": position.sl_order_id,
                        "execution_mode": env.mode}, env_name=env.name)
                    return
                position.sl_state = "cancelled"
                self._persist_position(position, env.name)

        order = order_manager.submit_signal(
            signal, multiplier=multiplier, trade_id=trade.trade_id, side=exit_side,
        )
        if order is None:
            if is_exit and position is not None:
                position.exit_started = False
                strategy.position_side = "LONG" if position.is_long else "SHORT"
                strategy.state = (StrategyState.LONG_POSITION if position.is_long
                                  else StrategyState.SHORT_POSITION)
                strategy.stop_exit_submitted = False
                if (position.sl_state == "cancelled"
                        and bool((self.config.get("live") or {}).get(
                            "broker_sl", {}).get("enabled", False))):
                    position.sl_state = "failed"
                    self._guard_live_position(env, position,
                                              position.entry_signal_id or "",
                                              trade, position.entry_signal_id)
                if reversal_sig:
                    strategy.pending_entry = None
            else:
                self._reset_strategy_state(signal.strategy_id, env_name=env.name)
            return
        if is_exit and position is not None and order.state.value in (
                "submitted", "acknowledged", "partially_filled", "filled"):
            position.exit_started = True
        # §22-24 — register the exit as STOP_LOSS when the signal classifies as
        # one, so the lifecycle + exit_reason stay canonical even without a
        # strategy last_exit_reason (broker-driven protective SL fills).
        order_role = resolve_order_role(signal)
        lifecycle.register_order(trade.trade_id, order.order_id,
                                 order_role or ("EXIT" if is_exit else "ENTRY"))
        # REVERSAL — SAME TRIGGER, EXIT FIRST, ENTRY SECOND: a REVERSAL_EXIT
        # opens a durable reversal lifecycle record (old trade/position + the
        # old exit order); the OPPOSITE REVERSAL_ENTRY order later merges the
        # new trade/order onto the SAME record via the shared signal id.  The
        # record is never marked COMPLETE until the old position is flat AND
        # the new entry fill is broker-confirmed AND the new SL is placed.
        if env.mode == "LIVE" and env.persistence is not None:
            if order_role == "REVERSAL_EXIT":
                self._record_reversal_open(env, signal, trade, position, order)
            elif order_role == "REVERSAL_ENTRY":
                self._update_reversal_entry_created(
                    env, (signal.metadata or {}).get("reversal_parent_signal_id")
                    or signal.signal_id,
                                                    trade, order)
        # §41 — trade row MUST exist before the order row: the integrity
        # triggers reject any order whose trade_id has no trades row.  This
        # idempotent upsert re-ensures the row on every submit (entry AND
        # exit), covering any earlier silent persist failure so an exit can
        # never be dead-locked on `order references missing trade`.
        if lifecycle is not None and not lifecycle.persist_trade(trade):
            log.error("[Engine] trade row could not be ensured before order %s "
                      "(trade %s); order will fail persistence.",
                      order.order_id, trade.trade_id)
        self._persist_order(order, signal, env_name)
        self.publish_event("order_created", {"trade_id": trade.trade_id, "order_id": order.order_id,
            "signal_id": signal.signal_id, "strategy_id": signal.strategy_id,
            "instrument": signal.instrument, "state": order.state.value,
            "execution_mode": env.mode}, env_name=env.name)
        # Phase 9.6 — the LIVE pending order whose trigger fired is now sent to
        # the broker: record ENTRY_SENT with the broker correlation tie-back.
        if live_pending is not None:
            self._mark_live_pending_entry_sent(env, signal, trade, order)
        for fill in order_manager.drain_fills():
            # §39 — every broker fill routes by explicit broker_order_id ->
            # strategy mapping (never symbol/side/latest order). Unmappable or
            # conflicting fills are quarantined, never applied.
            router = env.broker_router
            if router is not None:
                router.route_fill(
                    fill,
                    lambda f, es, ix: self._handle_fill(
                        f, es, is_exit=ix, env_name=env.name),
                    entry_signal_id=signal.signal_id, is_exit=is_exit)
            else:
                self._handle_fill(fill, signal.signal_id, is_exit=is_exit,
                                  env_name=env.name)
        # Reversal entries remain armed on the strategy and are submitted only
        # after the old position is broker-confirmed flat and their own trigger fires.

    def _persist_signal(self, signal, signal_type: str,
                        env_name: Optional[str] = None) -> None:
        env = self._env_for(env_name)
        if not env.persistence:
            return
        # F3 — persist the full frozen signal-candle snapshot (Phase 4 signal
        # context) into the signals row: dedicated OHLC / indicator columns plus
        # the JSON blobs.  Previously only the base identity fields were written
        # and later lifecycle writes were dropped by INSERT-OR-IGNORE, leaving
        # every candle column NULL and the blobs absent.
        ctx = getattr(signal, "context", None)
        metadata = getattr(signal, "metadata", None) or {}
        candle_blob = None
        indicator_blob = None
        if ctx is not None:
            candle_blob = {
                "timestamp": ctx.timestamp, "open": ctx.open, "high": ctx.high,
                "low": ctx.low, "close": ctx.close,
            }
            indicator_blob = {
                "fast_dema": ctx.dema, "fast_atr": ctx.atr,
                "htf_value": ctx.htf_value, "mid_value": ctx.mid_value,
            }
        env.persistence.save_signal({
            "signal_id": signal.signal_id, "strategy_id": signal.strategy_id,
            "instrument": signal.instrument, "side": signal.signal_type.value,
            "signal_type": signal_type, "timestamp": signal.timestamp,
            "trigger_price": signal.trigger_price, "stop_price": signal.stop_price,
            "quantity": signal.quantity,
            "candle_timestamp": ctx.timestamp if ctx is not None else None,
            "open": ctx.open if ctx is not None else None,
            "high": ctx.high if ctx is not None else None,
            "low": ctx.low if ctx is not None else None,
            "close": ctx.close if ctx is not None else None,
            "htf_value": ctx.htf_value if ctx is not None else None,
            "mid_value": ctx.mid_value if ctx is not None else None,
            "fast_dema": ctx.dema if ctx is not None else None,
            "fast_atr": ctx.atr if ctx is not None else None,
            "signal_reason": metadata.get("reason") or metadata.get("trigger_reason"),
            "candle_data": candle_blob,
            "indicator_data": indicator_blob,
        })

    def _persist_order(self, order, signal,
                       env_name: Optional[str] = None) -> None:
        env = self._env_for(env_name)
        if env.persistence:
            env.persistence.save_order({
                "order_id": order.order_id, "strategy_id": order.strategy_id,
                "instrument": order.instrument, "side": order.side, "quantity": order.quantity,
                "order_type": order.order_type,
                "price": order.price if order.price is not None else signal.trigger_price,
                "trigger_price": getattr(order, "trigger_price", None),
                "planned_entry_price": getattr(order, "planned_entry_price", None),
                "planned_sl": getattr(order, "planned_sl", None),
                "planned_order_type": getattr(order, "planned_order_type", None),
                "order_role": (getattr(order, "order_role", None)
                               or resolve_order_role(signal)),
                "protected_order_id": getattr(order, "protected_order_id", None),
                "correlation_id": getattr(order, "correlation_id", None),
                "state": order.state.value, "filled_quantity": order.filled_quantity,
                "average_fill_price": order.average_fill_price,
                "created_at": datetime.fromtimestamp(order.created_at, tz=timezone.utc).isoformat(),
                "updated_at": datetime.fromtimestamp(order.updated_at, tz=timezone.utc).isoformat(),
                "signal_id": signal.signal_id, "trade_id": order.trade_id,
                "lifecycle_id": getattr(order, "lifecycle_id", None) or order.trade_id,
                "parent_signal_id": getattr(order, "parent_signal_id", None) or signal.signal_id,
                "position_id": getattr(order, "position_id", None),
                "parent_position_id": getattr(order, "parent_position_id", None),
                "position_generation": getattr(order, "position_generation", None),
                "original_order_id": getattr(order, "original_order_id", None),
            })

    # ── Phase 9.6 — durable LIVE pending-order lifecycle ──────────────────
    # States: PENDING → ARMED → (ENTRY_SENT | EXPIRED | CANCELLED_BY_REVERSAL).
    # Rows live in the LIVE environment's own pending_orders table, keyed by
    # signal_id (pending orders are 1:1 with pending signals; the born trade
    # links via trades.entry_signal_id). PAPER never writes these rows.

    def _live_pending_row(self, env, signal) -> Optional[dict]:
        """The durable pending-order row for a signal (LIVE env only)."""
        if env.persistence is None:
            return None
        rows = env.persistence.get_pending_orders(execution_mode="LIVE")
        return next((r for r in rows if r.get("pending_order_id") == signal.signal_id), None)

    def _arm_live_pending(self, signal, env) -> None:
        """Register a LIVE pending breakout durably: PENDING -> ARMED.

        The row is written PENDING first (the moment the strategy armed the
        breakout) then immediately promoted to ARMED (accepted into LIVE
        execution).  A replayed pending signal for an already-ARMED or
        terminal row is a no-op (idempotent)."""
        pend_id = signal.signal_id
        base = {
            "pending_order_id": pend_id,
            "signal_id": pend_id,
            "side": (signal.side or getattr(signal.signal_type, "value", "LONG")).upper(),
            "order_type": "LIMIT",
            "trigger_price": signal.trigger_price,
            "quantity": signal.quantity,
            "trade_id": None,
        }
        existing = self._live_pending_row(env, signal)
        if existing is not None:
            try:
                transition_pending_state(
                    existing.get("status"), PendingOrderState.ARMED.value, pend_id)
            except ValueError:
                return  # terminal / already ENTRY_SENT: replay must not re-arm
            row = dict(existing)
            row.update(base)
            row["status"] = PendingOrderState.ARMED.value
            row["armed_at"] = datetime.now(timezone.utc).isoformat()
            env.persistence.save_pending_order(row)
            return
        row = dict(base)
        row["status"] = PendingOrderState.PENDING.value
        env.persistence.save_pending_order(row)
        self.publish_event("pending_order_created", {
            "pending_order_id": pend_id, "signal_id": pend_id,
            "state": row["status"], "execution_mode": env.mode}, env_name=env.name)
        row["status"] = transition_pending_state(
            row["status"], PendingOrderState.ARMED.value, pend_id)
        row["armed_at"] = datetime.now(timezone.utc).isoformat()
        env.persistence.save_pending_order(row)
        self.publish_event("pending_order_armed", {
            "pending_order_id": pend_id, "signal_id": pend_id,
            "state": row["status"], "execution_mode": env.mode}, env_name=env.name)

    def _mark_live_pending_entry_sent(self, env, signal, trade, order) -> None:
        """Record ENTRY_SENT on the durable pending order once the entry was
        placed at the broker, tying the broker correlation back to the row."""
        row = self._live_pending_row(env, signal)
        if row is None:
            self.publish_event("pending_order_blocked", {
                "signal_id": signal.signal_id,
                "reason": "durable_pending_missing_at_entry",
                "execution_mode": env.mode}, env_name=env.name)
            return
        state_value = getattr(order.state, "value", "")
        if state_value not in ("submitted", "filled"):
            # The entry never reached the broker (e.g. gate closed / broker
            # unavailable): ENTRY_SENT is reserved for actual placement. The
            # pending row stays ARMED and may be retried.
            return
        pend_id = row.get("pending_order_id") or signal.signal_id
        try:
            status = transition_pending_state(
                row.get("status"), PendingOrderState.ENTRY_SENT.value, pend_id)
        except ValueError:
            return
        env.persistence.save_pending_order({
            "pending_order_id": pend_id,
            "signal_id": pend_id,
            "trade_id": trade.trade_id,
            "status": status,
            "correlation_id": getattr(order, "correlation_id", None),
            "broker_order_id": getattr(order, "_broker_order_id", None),
        })
        self.publish_event("pending_order_entry_sent", {
            "pending_order_id": pend_id, "signal_id": pend_id,
            "trade_id": trade.trade_id, "order_id": order.order_id,
            "correlation_id": getattr(order, "correlation_id", None),
            "broker_order_id": getattr(order, "_broker_order_id", None),
            "execution_mode": env.mode}, env_name=env.name)

    def _handle_fill(self, fill, signal_id: str | None, is_exit: bool | None = None,
                     env_name: Optional[str] = None) -> None:
        """Apply a fill exactly once, using explicit IDs throughout.

        All mutable state is resolved from the fill's OWN environment so a live
        fill can never mutate a paper position / ledger / dedup state and vice
        versa.
        """
        env = self._env_for(env_name)
        # C1 — atomic in-process claim (check + hold in one critical section).
        # is_duplicate()+note_processed() was two separate lock acquisitions and
        # two racing callers could both pass the check before either claimed.
        if env.fill_dedup.is_duplicate(fill.fill_id):
            return
        if not env.fill_dedup.claim(fill.fill_id):
            return
        # Phase 9.7 — broker-authoritative fill admission for LIVE: a fill
        # whose broker execution identity is already persisted, or whose broker
        # cumulative quantity is already fully accounted, is never applied a
        # second time (restart re-poll, WS+REST duplicate, replay).
        if env.mode == "LIVE":
            verdict = self._reconcile_live_fill(env, fill)
            if verdict in ("duplicate", "already_synced", "divergence"):
                env.fill_dedup.mark_processed(fill.fill_id)
                return
        if fill.price <= 0 or (isinstance(fill.price, float) and not math.isfinite(fill.price)):
            env.fill_dedup.mark_processed(fill.fill_id)
            return

        # §34 — validate fill strategy identity via its owning runtime.
        # Unknown strategy ids are quarantined: the fill is never applied.
        runtime = None
        if env.runtimes is not None:
            try:
                runtime = env.runtimes.require(fill.strategy_id)
            except (KeyError, ValueError):
                runtime = None
        if runtime is None:
            self._quarantine_event("fill_unknown_strategy", {
                "fill_id": fill.fill_id, "order_id": fill.order_id,
                "strategy_id": fill.strategy_id, "trade_id": getattr(fill, "trade_id", ""),
                "execution_mode": env.mode})
            env.fill_dedup.mark_processed(fill.fill_id)
            return
        lifecycle = runtime.lifecycle
        position_manager = runtime.position_manager
        current = next((p for p in position_manager.get_positions_by_strategy(fill.strategy_id)
                        if p.instrument == fill.instrument and p.is_open), None)
        if env.mode == "LIVE":
            source_order = (env.execution_engine.get_order(fill.order_id)
                            if env.execution_engine is not None else None)
            source_role = str(getattr(source_order, "order_role", "") or "").upper()
            if source_role in ("EXIT", "STOP_LOSS", "REVERSAL_EXIT", "EMERGENCY_EXIT"):
                owner_pos = getattr(source_order, "parent_position_id", None)
                owner_trade = getattr(source_order, "lifecycle_id", None) or getattr(source_order, "trade_id", None)
                owner_gen = getattr(source_order, "position_generation", None)
                if (current is None or not owner_pos
                        or current.position_id != owner_pos
                        or current.trade_id != owner_trade
                        or current.position_generation != owner_gen):
                    self._quarantine_event("stale_lifecycle_fill_rejected", {
                        "fill_id": fill.fill_id, "order_id": fill.order_id,
                        "strategy_id": fill.strategy_id,
                        "instrument": fill.instrument,
                        "parent_position_id": owner_pos,
                        "current_position_id": current.position_id if current else None,
                        "lifecycle_id": owner_trade,
                        "position_generation": owner_gen,
                        "execution_mode": env.mode}, persist=True)
                    if env.safe_mode is not None:
                        env.safe_mode.enter_safe_mode("position_mismatch",
                                                      "stale lifecycle fill")
                    env.fill_dedup.mark_processed(fill.fill_id)
                    return
        # Phase 9.7 — a PARTIAL-fill leg from the broker must never be
        # misread as an exit: direction is decided by side-vs-position for
        # broker fills, so same-side legs aggregate onto the open position
        # (one logical trade/position, multiple broker fills). Paper and
        # legacy paths keep the historic position-presence rule.
        broker_live = env.mode == "LIVE" and bool(getattr(fill, "broker_fill_id", None))
        same_side = (current is not None
                     and ((current.is_long and fill.side == "BUY")
                          or (not current.is_long and fill.side == "SELL")))
        if broker_live:
            is_exit = current is not None and not same_side
        else:
            is_exit = bool(is_exit) if is_exit is not None else current is not None
        if not is_exit:
            trade = lifecycle.get_trade(fill.trade_id) or lifecycle.resolve_trade_from_signal(signal_id)
            # §34 — entry fill must have an explicit trade reference in this
            # strategy's scope. A fill that resolves to a cross-strategy trade
            # (different strategy_id) is quarantined — never applied.
            if trade is None or trade.strategy_id != fill.strategy_id:
                self._quarantine_event("entry_fill_no_trade_or_mismatch", {
                    "fill_id": fill.fill_id, "order_id": fill.order_id,
                    "trade_id": getattr(fill, "trade_id", None),
                    "resolved_trade_id": getattr(trade, "trade_id", None) if trade else None,
                    "fill_strategy_id": fill.strategy_id,
                    "trade_strategy_id": getattr(trade, "strategy_id", None) if trade else None,
                    "signal_id": signal_id, "execution_mode": env.mode})
                env.fill_dedup.mark_processed(fill.fill_id)
                return
            account = env.account_engines[fill.strategy_id]
            margin = self._calculate_margin(fill.instrument, fill.price, fill.quantity)
            account_blocked = account.block_margin(margin)
            # Only block the global account if the per-strategy block
            # succeeded (avoids a double-release of the same margin).
            global_blocked = env.account_engine.block_margin(margin) if account_blocked else False
            if not (account_blocked and global_blocked):
                if account_blocked:
                    account.release_margin(margin)
                # The broker has ALREADY executed this entry (LIVE): refusing
                # the fill orphans a REAL position with no tracking and no
                # protective stop.  Over-allocate the margin (the position
                # already exists at the broker) and surface a margin-breach
                # alert, then let the normal booking below proceed so the
                # entry is tracked and _guard_live_position arms its stop.
                # Paper has no broker side, so a reset + drop stays correct.
                if env.mode == "LIVE":
                    account.block_margin(margin, force=True)
                    env.account_engine.block_margin(margin, force=True)
                    self.publish_event("margin_breach_entry", {
                        "fill_id": fill.fill_id, "trade_id": trade.trade_id,
                        "strategy_id": fill.strategy_id,
                        "instrument": fill.instrument,
                        "quantity": fill.quantity, "price": fill.price,
                        "margin": margin, "execution_mode": env.mode},
                        env_name=env.name)
                    try:
                        self.telegram.on_risk_alert({
                            "kind": "margin_breach_entry",
                            "message": (
                                f"MARGIN BREACH: broker-confirmed entry "
                                f"{fill.fill_id} {fill.instrument} "
                                f"x{fill.quantity} @ {fill.price} — margin "
                                f"{margin:.2f} over limit; position tracked "
                                f"and will be protected."),
                        })
                    except Exception:
                        pass
                else:
                    self._reset_strategy_state(fill.strategy_id, env_name=env.name)
                    return
            if broker_live and same_side:
                # Phase 9.7 — aggregate the leg onto the open position (one
                # logical trade, multiple broker fills) and persist the leg.
                old_qty = int(current.quantity)
                old_avg = float(current.average_entry or 0.0)
                new_qty = old_qty + int(fill.quantity)
                new_avg = ((old_avg * old_qty + float(fill.price) * int(fill.quantity))
                           / new_qty) if new_qty else old_avg
                current.quantity = new_qty
                current.average_entry = round(new_avg, 4)
                current.entry_fill_ids.append(fill.fill_id)
                current.margin = float(getattr(current, "margin", 0.0) or 0.0) + margin
                self._persist_fill(fill, trade.trade_id, signal_id, env_name)
                lifecycle.register_entry_fill(trade.trade_id, fill.fill_id,
                                              current.average_entry, fill.timestamp)
                if env.trade_ledger is not None:
                    try:
                        env.trade_ledger.record_fill(
                            trade_id=trade.trade_id,
                            fill_id=fill.fill_id,
                            order_id=fill.order_id,
                            side="BUY" if current.is_long else "SELL",
                            quantity=fill.quantity,
                            price=fill.price,
                            timestamp=fill.timestamp,
                            is_entry=True,
                        )
                    except Exception as e:
                        log.error("[Engine] ledger projection write failed for %s: %s",
                                  trade.trade_id, e)
                self.publish_event("position_augmented", {
                    "trade_id": trade.trade_id,
                    "position_id": current.position_id,
                    "fill_id": fill.fill_id,
                    "strategy_id": fill.strategy_id,
                    "instrument": fill.instrument,
                    "quantity": new_qty, "average_entry": current.average_entry,
                    "execution_mode": env.mode}, env_name=env.name)
                # §22-24 — a broker-side protective SLM guards the OPEN live
                # position as soon as the entry leg is on the books.
                self._guard_live_position(env, current, fill.order_id, trade,
                                          signal_id)
                self._notify_entry_fill(fill, current, env, signal_id)
                self._sync_strategy_on_entry_fill(
                    env, fill.strategy_id, "LONG" if current.is_long else "SHORT",
                    current)
            else:
                # stop_price: prefer the entry order's own execution plan
                # (planned_sl).  For a REVERSAL_ENTRY that is the NEW opposite
                # side's stop computed from the entry signal (instance.py
                # plan_for/create_order), never the stale pre-reversal stop
                # still held by strategy.state / the shared signals row.
                entry_order = (env.execution_engine.get_order(fill.order_id)
                               if env.execution_engine is not None else None)
                _stop = (getattr(entry_order, "planned_sl", None)
                         if entry_order is not None else None)
                if _stop is None:
                    _stop = getattr(env.strategies.get(fill.strategy_id),
                                    "stop_price", None)
                if _stop is None and signal_id and env.persistence is not None:
                    try:
                        _sig = env.persistence.query_one(
                            "SELECT stop_price FROM signals WHERE signal_id=?",
                            (signal_id,))
                        if _sig and _sig.get("stop_price") is not None:
                            _stop = float(_sig["stop_price"])
                            # Restore strategy stop_price for future use
                            env.strategies[fill.strategy_id].stop_price = _stop
                    except Exception:
                        log.error("[Engine] SL recovery failed for signal %s: %s",
                                  signal_id, exc_info=True)
                if _stop is None and signal_id:
                    # A broker-confirmed entry fill with no recoverable stop is
                    # an UNPROTECTED live position: _check_stop_loss() bails on
                    # `stop_price is None`, so the exit-on-stop path is dead.
                    # Never let that pass silently.
                    log.error("[Engine] entry fill %s has NO stop price "
                              "(planned_sl/strategy/signal all empty) - "
                              "position will be UNPROTECTED",
                              fill.fill_id)
                    self.publish_event("entry_stop_missing", {
                        "fill_id": fill.fill_id, "signal_id": signal_id,
                        "strategy_id": fill.strategy_id,
                        "instrument": fill.instrument,
                        "reason": "no_stop_price_recoverable",
                        "execution_mode": env.mode}, env_name=env.name)
                position = position_manager.open_position(
                    fill, multiplier=fill.multiplier, margin=margin,
                    stop_price=_stop,
                    entry_signal_id=signal_id, trade_id=trade.trade_id,
                    position_generation=(getattr(entry_order, "position_generation", None)
                                         if entry_order is not None else None),
                )
                fill.position_id = position.position_id
                fill.lifecycle_id = trade.trade_id
                fill.position_generation = position.position_generation
                self._persist_fill(fill, trade.trade_id, signal_id, env_name)
                self._persist_position(position, env_name)
                lifecycle.register_entry_fill(trade.trade_id, fill.fill_id,
                                              fill.price, fill.timestamp)
                lifecycle.register_position(trade.trade_id, position.position_id)
                # Keep the analytics read-model (trade ledger) in lock-step at
                # entry: the OPEN projection must exist as soon as the position
                # opens so a crash/restart never has a position without a
                # ledger trade.
                if env.trade_ledger is not None:
                    try:
                        if env.trade_ledger.get_trade(trade.trade_id) is None:
                            env.trade_ledger.create_trade(
                                strategy_id=fill.strategy_id,
                                instrument=fill.instrument,
                                side="LONG" if position.is_long else "SHORT",
                                entry_quantity=position.quantity,
                                signal_time=fill.timestamp,
                                trigger_price=position.average_entry,
                                stop_price=getattr(position, "stop_price", None) or 0.0,
                                multiplier=fill.multiplier,
                                entry_reason="signal",
                                trade_id=trade.trade_id,
                                position_id=position.position_id,
                            )
                        env.trade_ledger.record_fill(
                            trade_id=trade.trade_id,
                            fill_id=fill.fill_id,
                            order_id=fill.order_id,
                            side="BUY" if position.is_long else "SELL",
                            quantity=fill.quantity,
                            price=fill.price,
                            timestamp=fill.timestamp,
                            is_entry=True,
                        )
                    except Exception as e:
                        log.error("[Engine] ledger projection write failed for %s: %s",
                                  trade.trade_id, e)
                self.publish_event("position_opened", {"trade_id": trade.trade_id,
                    "position_id": position.position_id, "fill_id": fill.fill_id,
                    "strategy_id": fill.strategy_id, "instrument": fill.instrument,
                    "execution_mode": env.mode}, env_name=env.name)
                # §22-24 — broker-side protective SLM placement right after the
                # entry fill opens the position.
                self._guard_live_position(env, position, fill.order_id, trade,
                                          signal_id)
                # REVERSAL — the broker-confirmed NEW opposite entry creates
                # the NEW position; record its fill + SL on the reversal.
                if getattr(entry_order, "order_role", "") == "REVERSAL_ENTRY":
                    self._update_reversal_entry_fill(
                        env, getattr(entry_order, "reversal_parent_signal_id", None)
                        or signal_id, fill,
                                                     position)
                self._notify_entry_fill(fill, position, env, signal_id)
                self._sync_strategy_on_entry_fill(
                    env, fill.strategy_id, "LONG" if position.is_long else "SHORT",
                    position)
        else:
            if current is None or not current.trade_id:
                self._quarantine_event("exit_fill_no_position", {
                    "fill_id": fill.fill_id, "order_id": fill.order_id,
                    "strategy_id": fill.strategy_id,
                    "instrument": fill.instrument, "trade_id": getattr(fill, "trade_id", ""),
                    "execution_mode": env.mode})
                env.fill_dedup.mark_processed(fill.fill_id)
                return
            close_manager = env.trade_close_manager or self._trade_close_manager
            if close_manager is None:
                raise RuntimeError("trade close manager is not initialized")
            # §27 — stop-loss closes carry NO strategy signal: exit_signal_id
            # must stay NULL and exit_reason must be the canonical STOP_LOSS.
            # Reversal/signal exits keep their explicit exit signal id.
            raw_reason = (env.strategies[fill.strategy_id].last_exit_reason
                          or "signal_exit")
            is_stop_loss = (raw_reason or "").lower() in ("stop_loss_hit", "stop_loss")
            # V4 — a fill that belongs to a STOP_LOSS-role order (a broker-side
            # protective SLM that triggered, OR the strategy's own SL exit
            # order) is always a stop-loss regardless of stale strategy
            # last_exit_reason state.  Broker-driven SL fills carry no signal.
            sl_order = env.execution_engine.get_order(fill.order_id) \
                if env.execution_engine is not None else None
            sl_role = getattr(sl_order, "order_role", None)
            if sl_role == "STOP_LOSS":
                is_stop_loss = True
            exit_reason = "STOP_LOSS" if is_stop_loss else raw_reason
            exit_signal_id = "" if is_stop_loss else (signal_id or "")
            if sl_role == "EMERGENCY_EXIT":
                # §24 — fail-closed protective-SL market close: canonical
                # EMERGENCY_EXIT reason, no strategy signal is ever attached.
                exit_reason = "EMERGENCY_EXIT"
                exit_signal_id = ""
            # §22-24 — protective-SL lifecycle: when the closing order IS the
            # protective SLM fill the position's protection is consumed; any
            # OTHER exit (reversal/strategy/emergency) must cancel the resting
            # protective SLM FIRST so it can never fire against a later
            # opposite position.
            # C2 — a PARTIAL exit leaves the position OPEN on the remaining
            # quantity; the protective SL and strategy position state must
            # stay armed until the position is fully gone.
            exit_qty = int(fill.quantity or 0)
            pos_qty = int(current.quantity or 0)
            if 0 < exit_qty < pos_qty:
                self._handle_partial_exit(
                    env, fill, current, trade, signal_id,
                    exit_reason, exit_signal_id)
                env.fill_dedup.mark_processed(fill.fill_id)
                return
            if exit_qty > pos_qty:
                self._quarantine_event("exit_fill_exceeds_position", {
                    "fill_id": fill.fill_id, "order_id": fill.order_id,
                    "position_id": current.position_id,
                    "position_quantity": pos_qty, "fill_quantity": exit_qty,
                    "execution_mode": env.mode}, persist=True)
                if env.safe_mode is not None:
                    env.safe_mode.enter_safe_mode("position_mismatch",
                                                  "exit fill exceeds local position")
                env.fill_dedup.mark_processed(fill.fill_id)
                return
            self._release_live_sl(env, current, fill.order_id, sl_role)
            result = close_manager.close_position(
                fill, current, fill.strategy_id, fill.multiplier,
                exit_reason=exit_reason,
                exit_signal_id=exit_signal_id or None,
            )
            if result is False:
                return
            if env.persistence is not None and hasattr(env.persistence, "close_position_record"):
                try:
                    env.persistence.close_position_record(current)
                except Exception as e:
                    log.error("[Engine] close_position_record failed for %s: %s",
                              current.position_id, e)
            lifecycle.register_exit_fill(current.trade_id, fill.fill_id, fill.price,
                fill.timestamp, exit_signal_id, exit_reason=exit_reason)
            lifecycle.close_trade(current.trade_id, result["gross_pnl"], result["charges"], result["net_pnl"])
            # Phase 9.7 — mirror the exit fill in the reconciliation ledger too.
            self._update_fill_reconciliation(env, fill)
            # REVERSAL — the broker-confirmed old-position exit is the
            # transition point: record the exit fill on the reversal record.
            # Only when it is provably flat does the new opposite entry flow.
            if sl_role == "REVERSAL_EXIT" or (exit_reason
                                              and "reversal" in exit_reason.lower()):
                self._update_reversal_exit_fill(env, signal_id, fill)
            # Reversal exits arm an OPPOSITE pending breakout entry which must
            # survive the close: keep it if the strategy has one armed.
            pending_armed = env.strategies[fill.strategy_id].pending_entry is not None
            self._reset_strategy_state(fill.strategy_id, keep_pending=pending_armed,
                                       env_name=env.name)
        env.fill_dedup.mark_processed(fill.fill_id)

    def _handle_partial_exit(self, env, fill, position, trade, signal_id: Optional[str],
                             exit_reason: str, exit_signal_id: str) -> None:
        """C2 — apply a partial exit fill (exit qty < held qty).

        Books realized P&L ONLY on the portion that exited, releases margin
        proportionally, records the leg (persistence, reconciliation ledger,
        lifecycle exit-fill) and reduces the open position — without closing
        the trade, the position or the protective SL.  The strategy position
        state stays armed for the remaining quantity.
        """
        runtime = None
        if getattr(env, "runtimes", None) is not None:
            try:
                runtime = env.runtimes.get(fill.strategy_id) or \
                    env.runtimes[fill.strategy_id]
            except Exception:
                runtime = None
        pm = getattr(runtime, "position_manager", None) or \
            getattr(env, "position_manager", None)
        if pm is None:
            self._quarantine_event("partial_exit_no_position_manager", {
                "fill_id": fill.fill_id, "strategy_id": fill.strategy_id,
                "instrument": fill.instrument,
                "execution_mode": env.mode}, persist=True)
            return
        try:
            remaining = pm.reduce_position(
                position.position_id, fill, reason=exit_reason,
                exit_signal_id=exit_signal_id or None)
        except Exception as e:
            self._quarantine_event("partial_exit_reduce_failed", {
                "fill_id": fill.fill_id, "strategy_id": fill.strategy_id,
                "instrument": fill.instrument, "error": str(e),
                "execution_mode": env.mode}, persist=True)
            return
        # P&L on the exiting quantity only (entry side == position side).
        pnl_engine = env.pnl_engines.get(fill.strategy_id)
        if pnl_engine is not None:
            entry_fill = Fill(
                fill_id=(position.entry_fill_ids[0]
                         if position.entry_fill_ids else ""),
                order_id="",
                instrument=position.instrument,
                side="BUY" if position.is_long else "SELL",
                quantity=int(fill.quantity),
                price=float(position.average_entry or 0.0),
                timestamp=float(position.entry_timestamp or 0.0),
                strategy_id=position.strategy_id,
                multiplier=fill.multiplier,
            )
            gross_pnl, charges, net_pnl = pnl_engine.calculate_realized_pnl(
                entry_fill=entry_fill, exit_fill=fill, multiplier=fill.multiplier)
        else:
            gross_pnl, charges, net_pnl = 0.0, 0.0, 0.0
        try:
            self._persist_fill(fill, trade.trade_id, signal_id, env.name)
        except Exception as e:
            log.warning("[Engine] partial exit fill persist failed: %s", e)
        try:
            self._persist_position(position, env.name)
        except Exception as e:
            log.warning("[Engine] partial exit position persist failed: %s", e)
        # Account: book the realized leg, release the exited margin share (the
        # position's margin was already reduced by reduce_position).
        strat_account = env.account_engines.get(fill.strategy_id)
        try:
            released_expected = max(
                0.0, float(position.margin) * int(fill.quantity)
                / int(remaining)) if int(remaining) > 0 else 0.0
            if strat_account is not None:
                strat_account.update_realized_pnl(net_pnl, charges)
                strat_account.release_margin(released_expected)
            if env.account_engine is not None:
                env.account_engine.update_realized_pnl(net_pnl, charges)
                env.account_engine.release_margin(released_expected)
        except Exception as e:
            log.warning("[Engine] partial exit account update failed: %s", e)
        try:
            if env.risk_engine is not None:
                env.risk_engine.update_daily_pnl(net_pnl)
        except Exception as e:
            log.warning("[Engine] partial exit risk update failed: %s", e)
        # Reconciliation ledger mirror leg (position still open: no close_trade).
        if env.trade_ledger is not None:
            try:
                env.trade_ledger.record_fill(
                    trade_id=trade.trade_id, fill_id=fill.fill_id,
                    order_id=fill.order_id, side=fill.side,
                    quantity=int(fill.quantity), price=fill.price,
                    timestamp=fill.timestamp, is_entry=False)
            except Exception as e:
                log.error("[Engine] partial exit ledger write failed for %s: %s",
                          trade.trade_id, e)
        try:
            self._update_fill_reconciliation(env, fill)
        except Exception as e:
            log.debug("[Engine] partial exit reconcile mirror skipped: %s", e)
        try:
            lifecycle = getattr(runtime, "lifecycle", None)
            if lifecycle is not None:
                lifecycle.register_exit_fill(
                    trade.trade_id, fill.fill_id, fill.price, fill.timestamp,
                    exit_signal_id, exit_reason=exit_reason)
        except Exception as e:
            log.warning("[Engine] partial exit lifecycle update failed: %s", e)
        self.publish_event("position_partially_exited", {
            "trade_id": trade.trade_id,
            "position_id": position.position_id,
            "fill_id": fill.fill_id,
            "strategy_id": fill.strategy_id,
            "instrument": fill.instrument,
            "exited_quantity": int(fill.quantity),
            "remaining_quantity": int(remaining),
            "gross_pnl": gross_pnl, "charges": charges, "net_pnl": net_pnl,
            "execution_mode": env.mode}, env_name=env.name)

    # ── V4 — broker-side protective stop-loss (spec §22-24) ──────────────

    def _live_account_snapshot(self, env) -> dict:
        """Best-effort REAL Dhan account snapshot for LIVE notifications.

        Prefers the poller's cached /fundlimit snapshot (updated every few
        seconds, zero cost); falls back to one fresh broker call bounded to
        5s via a daemon thread so a hung REST call can never stall the caller.
        Returns {} when the account cannot be read.
        """
        if not getattr(env, "is_live", False):
            return {}
        poller = getattr(env, "poller", None)
        if poller is not None:
            try:
                cached = getattr(poller, "_last_account", {}) or {}
            except Exception:
                cached = {}
            if isinstance(cached, dict) and cached.get("equity"):
                return cached
        broker = getattr(env, "broker", None)
        if broker is None or not hasattr(broker, "account_status"):
            return {}
        import threading
        result: dict = {}

        def _fetch():
            try:
                acct = broker.account_status() or {}
            except Exception:  # noqa: BLE001
                acct = {}
            if isinstance(acct, dict) and acct.get("equity"):
                result.update(acct)

        t = threading.Thread(target=_fetch, daemon=True)
        t.start()
        t.join(timeout=5.0)
        return result

    def _sync_strategy_on_entry_fill(self, env, strategy_id: str, side: str,
                                     position) -> None:
        """Settle an entry fill into the strategy's execution state.

        The pending-breakout model transitions the strategy optimistically
        inside ``_tick_entry_trigger`` before the order is even submitted
        (position_side/state set at trigger-cross).  The Appendix I
        immediate-limit model does NO strategy state transition at signal
        time — the LIMIT intentionally rests flat at the broker — so the fill
        arriving from the broker is the moment the strategy becomes an open
        position.  This mirror-step is idempotent, so it is safe for both.
        """
        strat = env.strategies.get(strategy_id)
        if strat is None:
            return
        strat.position_side = side
        strat.current_position_id = position.position_id
        strat.position_generation = position.position_generation
        strat.position_quantity = position.quantity
        strat.current_trade_id = position.trade_id
        strat.state = (StrategyState.LONG_POSITION if side == "LONG"
                       else StrategyState.SHORT_POSITION)
        if getattr(position, "stop_price", None) is not None:
            strat.stop_price = position.stop_price
        strat.just_entered = True
        strat.pending_entry = None
        setattr(strat, "immediate_limit_sent", None)
        setattr(strat, "stop_exit_submitted", False)

    def _notify_entry_fill(self, fill, position, env, signal_id: Optional[str]) -> None:
        try:
            strat_obj = env.strategies.get(fill.strategy_id)
            account = env.account_engines.get(fill.strategy_id)
            strategy_dict = {
                "entry_value": fill.price * fill.quantity * fill.multiplier,
                "stop_price": getattr(position, "stop_price", None) or 0.0,
                "htf_dema_atr": getattr(strat_obj, 'htf_dema_atr', 0) if strat_obj else 0,
            }
            account_dict = {
                "equity": account.equity if account else 0.0,
                "used_margin": account.used_margin if account else 0.0,
                "source": "engine",
            }
            # LIVE fills must show the REAL Dhan account, never the configured
            # starting capital.
            if env.is_live:
                broker_acct = self._live_account_snapshot(env)
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
            self.telegram.on_fill({
                "side": "BUY" if position.is_long else "SELL",
                "instrument": fill.instrument,
                "strategy_id": fill.strategy_id,
                "price": fill.price,
                "quantity": fill.quantity,
                "multiplier": fill.multiplier,
                "order_id": fill.order_id,
                "execution_mode": env.mode,
            }, strategy_dict, account_dict)
        except Exception as e:
            log.warning("[Engine] telegram fill notify failed: %s", e)

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
                "signal_htf_dema_atr": meta.get("signal_htf_dema_atr"),
                "signal_mid_dema_atr": meta.get("signal_mid_dema_atr"),
                "signal_fast_dema_atr": meta.get("signal_fast_dema_atr"),
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

    def _record_reversal_open(self, env, signal, trade, position, order) -> None:
        """Open a durable reversal lifecycle record on REVERSAL_EXIT submit.

        The exit ALWAYS carries the SAME trigger price the OPPOSITE entry will
        use: captured from the still-armed strategy reversal pending_entry
        (candle HIGH for a LONG->SHORT reversal / LOW for SHORT->LONG), falling
        back to the signal's own trigger.  Old position + old exit order + the
        currently resting old SL are stamped so the chain has a start.
        """
        try:
            reversal_id = f"RV-{uuid.uuid4().hex[:12]}"
            strat = env.strategies.get(signal.strategy_id)
            trigger = None
            if strat is not None:
                pen = getattr(strat, "pending_entry", None)
                if pen is not None:
                    trigger = getattr(pen, "trigger_price", None)
            if trigger is None:
                trigger = getattr(signal, "trigger_price", None)
            old_sl_id = getattr(position, "sl_order_id", None)
            old_sl_state = getattr(position, "sl_state", None)
            env.persistence.save_reversal({
                "reversal_id": reversal_id,
                "signal_id": signal.signal_id,
                "strategy_id": signal.strategy_id,
                "instrument": signal.instrument,
                "old_trade_id": getattr(trade, "trade_id", None),
                "old_position_id": getattr(position, "position_id", None),
                "old_exit_order_id": getattr(order, "order_id", None),
                "old_sl_order_id": old_sl_id,
                "old_sl_state": old_sl_state,
                "reversal_trigger_price": trigger,
                "status": "PENDING_EXIT",
            })
            try:
                setattr(order, "reversal_id", reversal_id)
            except Exception:
                pass
        except Exception as e:
            log.warning("[Engine] reversal record open failed for %s: %s",
                        signal.signal_id, e)

    def _find_reversal(self, env, signal_id: Optional[str]) -> Optional[dict]:
        if env.persistence is None or not signal_id:
            return None
        try:
            return env.persistence.get_reversal_by_signal_id(signal_id)
        except Exception as e:
            log.warning("[Engine] reversal lookup failed for %s: %s", signal_id, e)
            return None

    def _update_reversal_exit_fill(self, env, signal_id: Optional[str],
                                   fill) -> None:
        """Stamp the broker-confirmed old-exit fill (transition point)."""
        rev = self._find_reversal(env, signal_id)
        if rev is None:
            return
        try:
            env.persistence.update_reversal(rev["reversal_id"], {
                "old_exit_fill_price": fill.price,
                "old_exit_filled_quantity": fill.quantity,
                "old_exit_broker_status": "FILLED",
                "exit_verified_at": datetime.now(timezone.utc).isoformat(),
                "old_sl_state": "cancelled",
                "status": "EXIT_FILLED",
            })
        except Exception as e:
            log.warning("[Engine] reversal exit-fill stamp failed for %s: %s",
                        rev["reversal_id"], e)

    def _update_reversal_entry_created(self, env, signal_id: Optional[str],
                                       trade, order) -> None:
        """Merge the NEW opposite entry order + NEW trade onto the record.

        The new entry is SUBMITTED ONLY after the old position is provably
        flat (engine exit-first gate).  A fresh trade id means a fresh
        position birth, never a quantity merge with the old side.
        """
        rev = self._find_reversal(env, signal_id)
        if rev is None:
            return
        try:
            env.persistence.update_reversal(rev["reversal_id"], {
                "new_trade_id": getattr(trade, "trade_id", None),
                "new_entry_order_id": getattr(order, "order_id", None),
                "new_broker_order_id": getattr(order, "_broker_order_id", None),
                "status": "ENTRY_SUBMITTED",
            })
            try:
                setattr(order, "reversal_id", rev["reversal_id"])
            except Exception:
                pass
        except Exception as e:
            log.warning("[Engine] reversal entry-created stamp failed for %s: %s",
                        rev["reversal_id"], e)

    def _update_reversal_entry_fill(self, env, signal_id: Optional[str],
                                    fill, position) -> None:
        """Stamp the broker-confirmed NEW entry fill ONLY on a new position.

        The record stays EXIT_FILLED (never COMPLETE) until this broker
        confirmation arrives; the reverse directional order's fill always
        creates/occupies the NEW position row.
        """
        rev = self._find_reversal(env, signal_id)
        if rev is None:
            return
        try:
            env.persistence.update_reversal(rev["reversal_id"], {
                "new_entry_fill_price": fill.price,
                "new_entry_filled_quantity": fill.quantity,
                "new_entry_broker_status": "FILLED",
                "new_position_id": getattr(position, "position_id", None),
                "entry_fill_confirmed_at": datetime.now(timezone.utc).isoformat(),
                "status": "COMPLETE",
            })
        except Exception as e:
            log.warning("[Engine] reversal entry-fill stamp failed for %s: %s",
                        rev["reversal_id"], e)

    def _update_reversal_sl_placed(self, env, signal_id: Optional[str],
                                   sl_order) -> None:
        """Stamp the NEW protective SL placed for the NEW position."""
        rev = self._find_reversal(env, signal_id)
        if rev is None:
            return
        try:
            env.persistence.update_reversal(rev["reversal_id"], {
                "new_sl_order_id": getattr(sl_order, "order_id", None),
                "new_sl_state": "placed",
            })
        except Exception as e:
            log.warning("[Engine] reversal SL-placed stamp failed for %s: %s",
                        rev["reversal_id"], e)

    def _guard_live_position(self, env, position, entry_order_id: str, trade,
                             signal_id: Optional[str]) -> None:
        """Place a resting broker-side STOP_LOSS_MARKET to protect an OPEN live
        position immediately after its entry fill (spec §22-24).

        Protected positions carry sl_state lineage: placed -> (verified) ->
        filled (SLM triggered) | cancelled (another exit) | failed.
        Protection placement failures follow the broker_sl policy: alert +
        retry-loop by the poller, and — when ``live.broker_sl.fail_closed`` is
        set — an immediate EMERGENCY market close of the position.
        """
        if not env.is_live or env.mode != "LIVE":
            return
        engine = env.execution_engine
        if engine is None or not hasattr(engine, "create_protective_sl"):
            return
        live_cfg = self.config.get("live") or {}
        cfg = live_cfg.get("broker_sl") or {}
        if not bool(cfg.get("enabled", False)):
            return
        if getattr(position, "sl_state", None) in ("placed", "verified",
                                                    "filled", "cancelled"):
            return
        # stop_price is the STRATEGY's intended stop level (from signal candle
        # HIGH/LOW).  This is ALWAYS the authoritative source for protective SL.
        stop = getattr(position, "stop_price", None) or 0.0
        plan_sl = None
        plan_sl_limit = None
        # The price_plan from the entry order is ONLY used as a fallback when
        # position.stop_price is missing.  The entry plan's trigger_price is
        # the ENTRY trigger (candle LOW for SHORT), NOT the stop level.
        price_plan = getattr(engine, "price_plan", None)
        if price_plan is not None and stop <= 0:
            try:
                plan = price_plan(entry_order_id)
            except Exception:
                plan = None
            if plan is not None:
                if plan.order_type == "STOP_LOSS":
                    plan_sl = plan.trigger_price
                    plan_sl_limit = plan.price
                elif plan.order_type == "STOP_LOSS_MARKET":
                    plan_sl = plan.trigger_price
                else:
                    plan_sl = getattr(plan, "planned_sl", None)
        # SAFETY: plan_sl from the entry plan is likely the ENTRY trigger, not
        # the stop.  Only use it as a last resort and validate it's a valid SL
        # level (above for SHORT, below for LONG).
        if plan_sl is not None and stop <= 0:
            entry_trigger = getattr(position, "average_entry", None)
            if entry_trigger is not None:
                is_long = getattr(position, "is_long", False)
                is_short = getattr(position, "is_short", False)
                if is_long and plan_sl >= entry_trigger:
                    plan_sl = None
                    plan_sl_limit = None
                elif is_short and plan_sl <= entry_trigger:
                    plan_sl = None
                    plan_sl_limit = None
        # Priority: position.stop_price > plan_sl (validated) > None
        trigger = stop if stop > 0 else (plan_sl if plan_sl else None)
        if trigger is None:
            self._sl_protection_failed(env, position, trade, entry_order_id,
                                       signal_id, order=None,
                                       error="no_stop_price",
                                       action="alert")
            return
        max_attempts = int(cfg.get("retry_max_attempts", 1) or 1)
        retry_interval = float(cfg.get("retry_interval_seconds", 0.0) or 0.0)
        last_sl_order = None
        for attempt in range(1, max_attempts + 1):
            sl_order = None
            try:
                sl_order = engine.create_protective_sl(
                    strategy_id=position.strategy_id,
                    instrument=position.instrument,
                    side="SELL" if position.is_long else "BUY",
                    quantity=position.quantity,
                    trigger_price=trigger,
                    limit_price=plan_sl_limit,
                    trade_id=trade.trade_id,
                    entry_order_id=entry_order_id,
                    position_id=position.position_id,
                    position_generation=position.position_generation,
                    signal_id=signal_id or position.entry_signal_id,
                )
            except Exception as e:
                self._sl_protection_failed(env, position, trade, entry_order_id,
                                           signal_id, order=None,
                                           error=f"create_exception: {e}",
                                           action="blocked_retry")
                break
            # Persist the SLM as a STOP_LOSS-role order AFTER submit so the
            # row's state reflects the broker's acceptance; a crash between
            # placement and this write leaves the position durable but
            # un-protected on the next boot (safe side: the local stop stays
            # armed, the poller/heal re-places). identity lineage survives via
            # the reused entry signal.
            try:
                engine.submit_order(sl_order)
            except Exception:
                sl_order.state = sl_order.state
            if sl_order.state.value in ("submitted", "acknowledged") and \
                    getattr(sl_order, "_broker_order_id", None):
                try:
                    self._persist_protective_order(env, sl_order, trade,
                                                   signal_id, trigger)
                except Exception as e:
                    log.warning("[Engine] protective SL persist failed for %s: %s",
                                sl_order.order_id, e)
                position.sl_state = "placed"
                position.sl_order_id = sl_order.order_id
                position.sl_trigger_price = trigger
                position.sl_retry_count = attempt
                self._persist_position(position, env.name)
                self.publish_event("sl_protection_placed", {
                    "position_id": position.position_id,
                    "trade_id": trade.trade_id,
                    "strategy_id": position.strategy_id,
                    "instrument": position.instrument,
                    "sl_order_id": sl_order.order_id,
                    "sl_trigger_price": trigger,
                    "execution_mode": env.mode}, env_name=env.name)
                # The poller verifies placed -> verified as soon as the broker
                # status confirms the resting SLM is accepted.
                if env.persistence is not None:
                    self._update_reversal_sl_placed(env, signal_id, sl_order)
                return
            last_sl_order = sl_order
            if attempt < max_attempts:
                if retry_interval and env.is_live:
                    time.sleep(retry_interval)
        # Placement exhausted: policy is alert + retry-flag + (fail_closed)
        # market-close.  The position's LOCAL stop remains active as the
        # safety net.
        sl_order = last_sl_order
        position.sl_state = "failed"
        position.sl_retry_count = max_attempts
        self._persist_position(position, env.name)
        self._sl_protection_failed(env, position, trade, entry_order_id,
                                   signal_id, order=sl_order,
                                   error=(sl_order.reason if sl_order is not None
                                          else "sl_placement_rejected"),
                                   action="blocked_retry")
        self._maybe_fail_closed(env, position, cfg)

    def _persist_protective_order(self, env, sl_order, trade, signal_id,
                                  trigger_price: float) -> None:
        """Persist a STOP_LOSS-role protective order row (identity lineage).

        The order row reuses the ENTRY signal id (the entry signal always
        exists in the signals table because the trade row requires it) so the
        SL order's entry_signal_id never dangles.  With no entry signal id the
        row is skipped: a protective SLM is a broker-side artifact whose
        absence never blocks entry execution."""
        if not env.persistence or not signal_id:
            return
        synthetic = Signal(
            signal_type=SignalType.SHORT if sl_order.side == "SELL" else SignalType.LONG,
            instrument=sl_order.instrument, strategy_id=sl_order.strategy_id,
            timestamp=sl_order.created_at, trigger_price=trigger_price,
            stop_price=trigger_price, quantity=sl_order.quantity,
        )
        synthetic.signal_id = signal_id
        self._persist_order(sl_order, synthetic, env.name)

    def _sl_protection_failed(self, env, position, trade, entry_order_id,
                              signal_id, *, order=None, error: str,
                              action: str) -> None:
        """Durable execution-failure audit event (spec §53) for an SL failure."""
        broker_oid = None
        if order is not None:
            broker_oid = getattr(order, "_broker_order_id", None)
        import uuid
        event_id = f"SLF-{uuid.uuid4().hex}"
        _status = getattr(position, "status", None)
        _status_s = _status.value if hasattr(_status, "value") else str(_status or "open")
        details = {
            "entry_order_id": entry_order_id,
            "sl_order_id": getattr(order, "order_id", None),
            "sl_state": getattr(position, "sl_state", None),
            "status": _status_s,
        }
        if env.persistence is not None:
            try:
                env.persistence.save_execution_failure_event({
                    "event_id": event_id, "event_type": "SL_PROTECTION_FAILED",
                    "strategy_id": position.strategy_id,
                    "trade_id": getattr(trade, "trade_id", None),
                    "order_id": getattr(order, "order_id", None),
                    "broker_order_id": broker_oid,
                    "signal_id": signal_id,
                    "instrument": position.instrument,
                    "error": error, "action": action,
                    "final_state": getattr(position, "sl_state", None)
                                   or _status_s,
                    "details": details,
                })
            except Exception as e:
                log.warning("[Engine] SL failure audit write failed: %s", e)
        self.publish_event("sl_protection_failed", {
            "position_id": position.position_id,
            "trade_id": getattr(trade, "trade_id", None),
            "strategy_id": position.strategy_id,
            "instrument": position.instrument,
            "error": error, "action": action,
            "broker_order_id": broker_oid,
            "execution_mode": env.mode}, env_name=env.name)
        try:
            self.telegram.on_error({
                "component": "SL_PROTECTION",
                "message": f"SL protection failed for {position.instrument} "
                           f"({position.strategy_id}): {error} — action={action}",
            })
        except Exception:
            pass

    def _maybe_fail_closed(self, env, position, cfg: dict) -> None:
        """§24 — when the position cannot be broker-protected and the policy
        is fail-closed, exit it at market immediately (EMERGENCY_EXIT)."""
        if not bool(cfg.get("fail_closed", False)):
            return
        self.publish_event("sl_fail_closed_emergency_exit", {
            "position_id": position.position_id,
            "strategy_id": position.strategy_id,
            "instrument": position.instrument,
            "reason": "sl_protection_failed",
            "execution_mode": env.mode}, env_name=env.name)
        self._emergency_close_position(env, position, "sl_protection_failed")

    def _emergency_close_position(self, env, position, reason: str) -> None:
        """Market-close an unprotected position with a EMERGENCY_EXIT order."""
        if env.execution_engine is None:
            return
        if getattr(position, "sl_state", None) == "filled":
            return
        try:
            self.telegram.on_risk_alert({
                "severity": "CRITICAL",
                "type": "EMERGENCY_CLOSE",
                "message": f"Emergency market-close: {position.instrument} "
                           f"({position.strategy_id}) — {reason}",
                "strategy_id": position.strategy_id,
                "instrument": position.instrument,
            })
        except Exception:
            pass
        sig = Signal(
            signal_type=SignalType.LONG if position.is_short else SignalType.SHORT,
            instrument=position.instrument, strategy_id=position.strategy_id,
            timestamp=time.time(),
            trigger_price=(position.current_mark or position.average_entry or 0.0),
            stop_price=0.0, quantity=position.quantity,
            metadata={"exit": True, "exit_reason": "sl_protection_failed"},
        )
        sig.signal_id = (getattr(position, "entry_signal_id", None)
                         or f"EMG-{uuid.uuid4().hex[:8]}")
        sig.lifecycle_id = position.trade_id
        sig.parent_position_id = position.position_id
        sig.position_generation = position.position_generation
        try:
            order = env.execution_engine.create_order(
                sig, multiplier=position.multiplier, trade_id=position.trade_id)
            order.order_role = "EMERGENCY_EXIT"
            # C8 — an emergency close is a MARKET order, never the price-
            # planned LIMIT that a metadata-exit signal would otherwise get
            # (plan_for maps exits to LIMIT/system_exit).  With the mark or
            # average entry at 0.0 the planned LIMIT was priceless -> broker
            # rejection; degrade-to-market guarantees a fill attempt even when
            # no last price is known yet.
            order.order_type = "MARKET"
            order.planned_order_type = "MARKET"
            order.price = 0.0
            order.trigger_price = None
            order.requested_price = None
            # Persist the exit order BEFORE routing its fills: the fills table
            # trigger requires the order row to exist (§40 durability).
            try:
                self._persist_order(order, sig, env.name)
            except Exception as e:
                log.warning("[Engine] emergency order persist failed: %s", e)
            env.execution_engine.update_price(position.instrument,
                                              position.current_mark or position.average_entry or 0.0)
            before = len(getattr(env.execution_engine, "_fills", []))
            env.execution_engine.submit_order(order)
            new_fills = list(getattr(env.execution_engine, "_fills", [])[before:])
        except Exception as e:
            self._quarantine_event("emergency_exit_failed", {
                "position_id": position.position_id,
                "strategy_id": position.strategy_id,
                "instrument": position.instrument,
                "error": str(e), "execution_mode": env.mode}, persist=True)
            return
        strategy = (getattr(env, "strategies", {}) or {}).get(fill.strategy_id)
        if strategy is not None:
            strategy.position_quantity = int(remaining)
        if order.state.value != "filled" or not new_fills:
            self._quarantine_event("emergency_exit_not_filled", {
                "position_id": position.position_id,
                "order_id": order.order_id,
                "reason": order.reason,
                "execution_mode": env.mode}, persist=True)
            return
        router = env.broker_router
        for fill in new_fills:
            if router is not None:
                router.route_fill(
                    fill,
                    lambda f, es, ix: self._handle_fill(
                        f, es, is_exit=ix, env_name=env.name),
                    entry_signal_id="", is_exit=True)
            else:
                self._handle_fill(fill, "", is_exit=True, env_name=env.name)

    def emergency_exit_all(self, env_name: str = "live",
                           instrument: Optional[str] = None) -> dict:
        """Submit lifecycle-owned emergency exits through the LIVE gateway."""
        env = self._env_for(env_name)
        if not env.is_live or env.execution_engine is None:
            return {"closed": [], "errors": [{"error": "live execution unavailable"}]}
        closed, errors = [], []
        engine = env.execution_engine
        active = [o for o in list(getattr(engine, "_orders", {}).values())
                  if o.state.value in ("created", "submitted", "acknowledged",
                                       "partially_filled")]
        for pos in list(env.position_manager.open_positions):
            if instrument is not None and pos.instrument != instrument:
                continue
            pending_entries = [o for o in active
                               if o.strategy_id == pos.strategy_id
                               and o.instrument == pos.instrument
                               and str(getattr(o, "order_role", "")).upper()
                               in ("ENTRY", "REVERSAL_ENTRY", "FALLBACK_MARKET")]
            cancel_failed = []
            for pending in pending_entries:
                if not engine.cancel_order(pending.order_id):
                    cancel_failed.append(pending.order_id)
            if cancel_failed:
                errors.append({"position_id": pos.position_id,
                               "error": "entry_cancel_unconfirmed",
                               "order_ids": cancel_failed})
                continue
            # Do not stack a second exit onto an order already working for this
            # exact position. The poller remains the authority for its result.
            working = next((o for o in active
                            if getattr(o, "parent_position_id", None) == pos.position_id
                            and str(getattr(o, "order_role", "")).upper() in
                            ("EXIT", "STOP_LOSS", "REVERSAL_EXIT", "EMERGENCY_EXIT")), None)
            if working is not None:
                errors.append({"position_id": pos.position_id,
                               "error": "position_exit_already_working",
                               "order_id": working.order_id})
                continue
            if pos.sl_order_id and pos.sl_state in ("placed", "verified"):
                try:
                    if not engine.cancel_order(pos.sl_order_id):
                        raise RuntimeError("protective stop cancel unconfirmed")
                    pos.sl_state = "cancelled"
                except Exception as exc:
                    errors.append({"position_id": pos.position_id, "error": str(exc)})
                    continue
            signal = Signal(
                signal_type=SignalType.SHORT if pos.is_long else SignalType.LONG,
                instrument=pos.instrument, strategy_id=pos.strategy_id,
                timestamp=time.time(), trigger_price=pos.current_mark or pos.average_entry,
                stop_price=pos.stop_price or 0.0, quantity=pos.quantity,
                metadata={"exit": True, "exit_reason": "operator_emergency_exit"})
            signal.lifecycle_id = pos.trade_id
            signal.parent_position_id = pos.position_id
            signal.position_generation = pos.position_generation
            try:
                order = engine.create_order(
                    signal, multiplier=pos.multiplier, trade_id=pos.trade_id,
                    side="SELL" if pos.is_long else "BUY")
                order.order_role = "EMERGENCY_EXIT"
                order.order_type = "MARKET"
                order.price = 0.0
                order.trigger_price = None
                self._persist_order(order, signal, env.name)
                engine.update_price(pos.instrument, pos.current_mark or pos.average_entry)
                engine.submit_order(order)
                self._persist_order(order, signal, env.name)
                if order.state.value in ("submitted", "acknowledged", "partially_filled", "filled"):
                    pos.exit_started = True
                    self._persist_position(pos, env.name)
                    closed.append({"position_id": pos.position_id,
                                   "trade_id": pos.trade_id,
                                   "order_id": order.order_id,
                                   "broker_order_id": getattr(order, "_broker_order_id", None),
                                   "state": order.state.value})
                else:
                    errors.append({"position_id": pos.position_id,
                                   "order_id": order.order_id,
                                   "error": order.reason or "emergency exit rejected"})
                fills = [f for f in engine.get_fills(strategy_id=pos.strategy_id)
                         if f.fill_id in set(order.fill_ids)]
                for fill in fills:
                    env.broker_router.route_fill(
                        fill, lambda f, es, ix: self._handle_fill(
                            f, es, is_exit=ix, env_name=env.name),
                        entry_signal_id=signal.signal_id, is_exit=True)
            except Exception as exc:
                errors.append({"position_id": pos.position_id, "error": str(exc)})
        return {"closed": closed, "errors": errors}

    # ═══════════════════════════════════════════════════════════════════
    # SL RECOVERY — startup + unprotected position recovery
    # ═══════════════════════════════════════════════════════════════════

    def recover_missing_sl(self, env_name: Optional[str] = None) -> int:
        """Detect and recover protective SL for open positions that lack one.

        Called during startup reconciliation and periodically.  For each open
        position with sl_state in (None, 'failed') and no active SL order,
        recover the stop_price from the signal table and submit a new
        protective SL to Dhan.

        Returns the number of positions that were successfully protected.
        """
        env = self._env_for(env_name)
        if not env.is_live or env.mode != "LIVE":
            return 0
        if env.position_manager is None:
            return 0
        engine = env.execution_engine
        if engine is None or not hasattr(engine, "create_protective_sl"):
            return 0
        live_cfg = self.config.get("live") or {}
        cfg = live_cfg.get("broker_sl") or {}
        if not bool(cfg.get("enabled", False)):
            return 0
        max_attempts = int(cfg.get("retry_max_attempts", 3) or 3)
        recovered = 0
        for position in list(env.position_manager.open_positions):
            if not position.is_open:
                continue
            sl_state = getattr(position, "sl_state", None)
            if sl_state in ("placed", "verified", "filled"):
                continue
            if sl_state == "cancelled":
                continue
            # Check if there's an active SL order at the broker
            has_active_sl = False
            if hasattr(engine, "_orders"):
                for o in engine._orders.values():
                    if (getattr(o, "trade_id", None) == position.trade_id
                            and getattr(o, "order_role", "") == "STOP_LOSS"
                            and getattr(o, "state", "").value in ("submitted", "acknowledged")):
                        has_active_sl = True
                        break
            if has_active_sl:
                continue
            # Recover stop_price from signal table
            stop_price = self._recover_stop_price(env, position)
            if stop_price is None:
                log.warning("[Engine] SL recovery: no stop_price for %s/%s, "
                            "cannot create SL", position.strategy_id,
                            position.instrument)
                continue
            # For SHORT position, SL trigger must be ABOVE entry
            if position.is_short and stop_price <= position.average_entry:
                log.warning("[Engine] SL recovery: stop_price %.1f <= entry %.1f "
                            "for SHORT %s, skipping",
                            stop_price, position.average_entry, position.instrument)
                continue
            # For LONG position, SL trigger must be BELOW entry
            if position.is_long and stop_price >= position.average_entry:
                log.warning("[Engine] SL recovery: stop_price %.1f >= entry %.1f "
                            "for LONG %s, skipping",
                            stop_price, position.average_entry, position.instrument)
                continue
            log.info("[Engine] SL recovery: attempting for %s/%s stop=%.1f "
                     "entry=%.1f side=%s",
                     position.strategy_id, position.instrument,
                     stop_price, position.average_entry,
                     "SHORT" if position.is_short else "LONG")
            # Try to create and submit SL
            sl_order = None
            for attempt in range(1, max_attempts + 1):
                try:
                    sl_order = engine.create_protective_sl(
                        strategy_id=position.strategy_id,
                        instrument=position.instrument,
                        side="SELL" if position.is_long else "BUY",
                        quantity=position.quantity,
                        trigger_price=stop_price,
                        trade_id=position.trade_id or "",
                        entry_order_id="",
                        position_id=position.position_id,
                        position_generation=position.position_generation,
                        signal_id=position.entry_signal_id,
                    )
                except Exception as e:
                    log.warning("[Engine] SL recovery attempt %d failed: %s",
                                attempt, e)
                    if attempt < max_attempts:
                        time.sleep(0.5)
                    continue
                try:
                    engine.submit_order(sl_order)
                except Exception as e:
                    log.warning("[Engine] SL recovery submit attempt %d failed: %s",
                                attempt, e)
                    if attempt < max_attempts:
                        time.sleep(0.5)
                    continue
                if (sl_order.state.value in ("submitted", "acknowledged")
                        and getattr(sl_order, "_broker_order_id", None)):
                    position.sl_state = "placed"
                    position.sl_order_id = sl_order.order_id
                    position.sl_trigger_price = stop_price
                    position.sl_protected_at = time.time()
                    self._persist_position(position, env.name)
                    try:
                        self._persist_protective_order(
                            env, sl_order,
                            type("Trade", (), {"trade_id": position.trade_id})(),
                            position.entry_signal_id, stop_price)
                    except Exception:
                        pass
                    self.publish_event("sl_recovery_success", {
                        "position_id": position.position_id,
                        "trade_id": position.trade_id,
                        "strategy_id": position.strategy_id,
                        "instrument": position.instrument,
                        "sl_order_id": sl_order.order_id,
                        "sl_trigger_price": stop_price,
                        "stop_price": stop_price,
                        "attempt": attempt,
                        "execution_mode": env.mode}, env_name=env.name)
                    try:
                        self.telegram.on_info({
                            "component": "SL_RECOVERY",
                            "message": (f"SL recovered for {position.instrument} "
                                       f"({position.strategy_id}): trigger={stop_price:.1f}, "
                                       f"order={sl_order.order_id}"),
                        })
                    except Exception:
                        pass
                    recovered += 1
                    log.info("[Engine] SL recovery: SUCCESS for %s/%s "
                             "trigger=%.1f order=%s attempt=%d",
                             position.strategy_id, position.instrument,
                             stop_price, sl_order.order_id, attempt)
                    break
                # SL rejected by broker — log and retry
                reason = getattr(sl_order, "reason", "unknown")
                log.warning("[Engine] SL recovery attempt %d rejected: %s",
                            attempt, reason)
                if attempt < max_attempts:
                    time.sleep(0.5)
            else:
                # All attempts exhausted
                position.sl_state = "failed"
                self._persist_position(position, env.name)
                self.publish_event("sl_recovery_failed", {
                    "position_id": position.position_id,
                    "trade_id": position.trade_id,
                    "strategy_id": position.strategy_id,
                    "instrument": position.instrument,
                    "error": getattr(sl_order, "reason", "all_attempts_failed"),
                    "execution_mode": env.mode}, env_name=env.name)
                try:
                    self.telegram.on_error({
                        "component": "SL_RECOVERY",
                        "message": (f"SL recovery FAILED for {position.instrument} "
                                   f"({position.strategy_id}): "
                                   f"{getattr(sl_order, 'reason', 'all attempts failed')}"),
                    })
                except Exception:
                    pass
        return recovered

    def _recover_stop_price(self, env, position) -> Optional[float]:
        """Recover the intended stop_price for a position.

        Tries in order:
        1. position.stop_price (if already set)
        2. Strategy stop_price (if strategy has it)
        3. Signal table stop_price (from the entry signal)
        4. Candle HIGH/LOW based on position side
        """
        # 1. Position already has stop_price
        sp = getattr(position, "stop_price", None)
        if sp is not None and sp > 0:
            return sp
        # 2. Strategy stop_price
        strategy = env.strategies.get(position.strategy_id)
        if strategy is not None:
            sp = getattr(strategy, "stop_price", None)
            if sp is not None and sp > 0:
                return sp
        # 3. Signal table (C11 — lock-protected query_one API, not the raw
        # shared connection, so the read never observes a writer mid-tx).
        signal_id = getattr(position, "entry_signal_id", None)
        if signal_id and env.persistence is not None:
            try:
                row = env.persistence.query_one(
                    "SELECT stop_price, high, low FROM signals "
                    "WHERE signal_id=? AND execution_mode=?",
                    (signal_id, env.persistence.execution_mode)
                )
                if row:
                    sp = row.get("stop_price")
                    if sp is not None and sp > 0:
                        return float(sp)
                    # Fallback to candle high/low
                    if position.is_short and row.get("high") is not None:
                        return float(row["high"])
                    if position.is_long and row.get("low") is not None:
                        return float(row["low"])
            except Exception as e:
                log.warning("[Engine] SL recovery: signal lookup failed: %s", e)
        # 4. Trade table entry_signal_id -> signal
        if env.persistence is not None:
            try:
                trade = env.persistence.query_one(
                    "SELECT entry_signal_id FROM trades "
                    "WHERE trade_id=? AND execution_mode=?",
                    (position.trade_id, env.persistence.execution_mode)
                )
                if trade and trade.get("entry_signal_id"):
                    row = env.persistence.query_one(
                        "SELECT stop_price, high, low FROM signals "
                        "WHERE signal_id=? AND execution_mode=?",
                        (trade["entry_signal_id"], env.persistence.execution_mode)
                    )
                    if row:
                        sp = row.get("stop_price")
                        if sp is not None and sp > 0:
                            return float(sp)
                        if position.is_short and row.get("high") is not None:
                            return float(row["high"])
                        if position.is_long and row.get("low") is not None:
                            return float(row["low"])
            except Exception as e:
                log.warning("[Engine] SL recovery: trade/signal lookup failed: %s", e)
        return None

    def _entry_priority_blocker(self, strategy_id: str,
                                env_name: Optional[str] = None,
                                rec: Optional[Any] = None) -> Optional[int]:
        """Priority gate for the order watcher (§7).

        Returns the highest-priority reason an ENTRY must not proceed right
        now, or None when entries are clear.

        * P0  — an unprotected open position exists (never add into it).  The
                protective-SL bookkeeping (sl_state) only exists when the
                broker-side SL is enabled; in broker_sl-disabled mode (the
                deployed runtime) the position is protected by the system-side
                SL by design, so the SL-state P0/P1 never fires there — only a
                plain (non-reversal) ENTRY into a held position is still
                blocked P0.
        * P1  — the protective SL is in flight / not verified for an open pos
        * P2  — an exit leg is resting or reversing
        * P4  — a reversal leg is in flight for this strategy.  A reversal's
                OWN record is never locked by its own leg (TODO fixed): the
                order being decided is excluded so a resting REVERSAL_ENTRY can
                be recovered by the watcher (fallback) once its exit leg is gone.
        """
        env = self._env_for(env_name)
        if env is None or not getattr(env, "is_live", False):
            return None
        rec_role = str(getattr(rec, "order_role", "") or "").upper() if rec is not None else ""
        rec_oid = getattr(rec, "internal_order_id", None)
        live_cfg = self.config.get("live") or {}
        broker_sl_enabled = bool((live_cfg.get("broker_sl") or {}).get("enabled", False))
        # Open / open-strategy position check (P0/P1).
        pm = getattr(env, "position_manager", None)
        positions = []
        if pm is not None:
            try:
                positions = pm.get_positions_by_strategy(strategy_id) or []
            except Exception:
                positions = []
        open_pos = [p for p in positions if getattr(p, "is_open", False)]
        if broker_sl_enabled:
            # SL-protection lineage is tracked and enforceable in this mode.
            for pos in open_pos:
                sl_state = getattr(pos, "sl_state", None)
                if sl_state in (None, "", "failed"):
                    return 0
                if sl_state in ("pending", "placed"):
                    return 1
        elif open_pos and rec_role == "ENTRY":
            # broker_sl disabled: no sl_state bookkeeping exists, but a plain
            # (non-reversal) ENTRY must NEVER add into an already-held position.
            return 0
        # In-flight legs on the engine order book (P2/P4).
        engine = getattr(env, "execution_engine", None)
        if engine is not None:
            orders = getattr(engine, "_orders", None) or {}
            for o in orders.values():
                if getattr(o, "strategy_id", None) != strategy_id:
                    continue
                # Never lock an order on its OWN leg (self-lock fix).
                if rec_oid is not None and getattr(o, "order_id", None) == rec_oid:
                    continue
                role = str(getattr(o, "order_role", "") or "").upper()
                state_s = str(getattr(o, "state", "")).lower()
                if state_s in ("filled", "canceled", "cancelled", "rejected"):
                    continue
                if role in ("REVERSAL_EXIT", "REVERSAL_ENTRY"):
                    return 4
                if role in ("EXIT", "EMERGENCY_EXIT"):
                    return 2
        return None

    def _market_fallback_preflight(self, strategy_id: str, env_name: str,
                                   rec) -> Optional[str]:
        """Safety pre-flight for the watcher's MARKET fallback.

        Reuses the same invariant gates the normal live entry/exit path holds so
        a recovery MARKET can never bypass: strategy gate, kill switch / daily
        loss, market state / safe mode, and the position-vs-role invariant
        (an ENTRY must be flat; an EXIT must have a position to close).
        Returns an error string, or None when clear.
        """
        env = self._env_for(env_name)
        if env is None or not getattr(env, "is_live", False):
            return "env_not_live"
        if rec is None:
            return None
        # Strategy gate.
        strategies = getattr(env, "strategies", {}) or {}
        strat = strategies.get(strategy_id)
        if strat is not None and not getattr(strat, "enabled", True):
            return "strategy_disabled"
        # Risk gate (kill switch + daily loss, same as the normal path).
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
        # Market state / safe mode gate.
        ms = getattr(env, "market_status", None)
        if ms is not None:
            try:
                if not getattr(ms, "is_trading_allowed", True):
                    return "market_not_trading"
            except Exception:
                pass
        safe_mode = getattr(env, "safe_mode", None)
        if safe_mode is not None:
            try:
                if getattr(safe_mode, "is_active", False):
                    return "safe_mode_active"
            except Exception:
                pass
        # Position-vs-role invariant.
        role = str(getattr(rec, "order_role", "") or "").upper()
        pm = getattr(env, "position_manager", None)
        has_open = False
        if pm is not None:
            try:
                has_open = any(
                    getattr(p, "is_open", False)
                    for p in (pm.get_positions_by_strategy(strategy_id) or []))
            except Exception:
                has_open = False
        if role in ("ENTRY", "REVERSAL_ENTRY"):
            # A partial-fill continuation (filled_quantity > 0) is completing
            # the SAME order — the engine/strategy lifecycle already sized it —
            # so flatness is not required there.  A fresh entry (0 filled) must
            # never add into a held position.
            if has_open and int(getattr(rec, "filled_quantity", 0) or 0) <= 0:
                return "position_not_flat"
        elif role in ("EXIT", "REVERSAL_EXIT", "EMERGENCY_EXIT"):
            if not has_open:
                return "no_position_to_close"
        elif role == "STOP_LOSS":
            return "stop_loss_not_fall_backable"
        return None

    def _release_live_sl(self, env, position, closing_order_id: str,
                         closing_role: Optional[str]) -> None:
        """Teardown of a protective SL on a position exit.

        * The closing order IS the protective SLM fill  -> sl_state='filled'.
        * Any other exit is already happening            -> the resting SLM is
          cancelled FIRST so it can never fire against a later opposite
          position (§25 exit-first safety).  A failed cancel is remembered in
          ``_uncancelled_sl`` and blocks new entries until it is resolved.
        """
        if not env.is_live or env.mode != "LIVE":
            return
        sl_id = getattr(position, "sl_order_id", None)
        if not sl_id:
            return
        if sl_id == closing_order_id or closing_role == "STOP_LOSS":
            position.sl_state = "filled"
            try:
                self._persist_position(position, env.name)
            except Exception as e:
                log.debug("[Engine] sl filled persist skipped: %s", e)
            return
        engine = env.execution_engine
        if engine is None or not hasattr(engine, "cancel_order"):
            return
        key = (env.name, position.strategy_id, position.instrument)
        try:
            ok = bool(engine.cancel_order(sl_id))
        except Exception:
            ok = False
        if ok:
            self._uncancelled_sl.pop(key, None)
            position.sl_state = "cancelled"
            try:
                self._persist_position(position, env.name)
            except Exception as e:
                log.debug("[Engine] sl cancelled persist skipped: %s", e)
            try:
                # Durable lifecycle: the SLM order row must not stay 'submitted'
                # forever; a restart healing scan reads it as resolved.
                _co = engine.get_order(sl_id)
                env.persistence.save_order({
                    "order_id": sl_id,
                    "strategy_id": position.strategy_id,
                    "instrument": position.instrument,
                    "side": getattr(_co, "side", "SELL" if position.is_long else "BUY"),
                    "quantity": position.quantity,
                    "order_type": getattr(_co, "order_type", "STOP_LOSS_MARKET"),
                    "trade_id": position.trade_id,
                    "order_role": "STOP_LOSS",
                    "state": "canceled",
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                })
            except Exception as e:
                log.debug("[Engine] sl cancel order-row flip skipped: %s", e)
            self.publish_event("sl_protection_cancelled", {
                "position_id": position.position_id,
                "sl_order_id": sl_id,
                "strategy_id": position.strategy_id,
                "instrument": position.instrument,
                "execution_mode": env.mode}, env_name=env.name)
        else:
            self._uncancelled_sl[key] = sl_id
            position.sl_state = "failed"
            try:
                self._persist_position(position, env.name)
            except Exception as e:
                log.debug("[Engine] sl cancel-fail persist skipped: %s", e)
            self.publish_event("sl_protection_cancel_failed", {
                "position_id": position.position_id,
                "sl_order_id": sl_id,
                "strategy_id": position.strategy_id,
                "instrument": position.instrument,
                "execution_mode": env.mode}, env_name=env.name)
            # Durable orphan evidence: the protective SL could not be released.
            # A restart rebuilds the entry gate from these rows so the broker
            # SLM can never fire against a new opposite position (§108).
            try:
                _oo = engine.get_order(sl_id)
                _boid = getattr(_oo, "_broker_order_id", None) \
                    if _oo is not None else None
                import uuid as _uu
                env.persistence.save_execution_failure_event({
                    "event_id": f"SLX-{_uu.uuid4().hex}",
                    "event_type": "SL_PROTECTION_FAILED",
                    "strategy_id": position.strategy_id,
                    "trade_id": position.trade_id,
                    "order_id": sl_id,
                    "broker_order_id": _boid,
                    "instrument": position.instrument,
                    "error": "cancel_rejected",
                    "action": "cancel_rejected",
                    "final_state": "orphan_sl_blocking",
                    "details": {"position_id": position.position_id,
                                "sl_state": "failed"},
                })
            except Exception as e:
                log.warning("[Engine] SL cancel-fail audit write failed: %s", e)

    def _restore_uncancelled_sl(self, env) -> int:
        """Startup reconcile (§108 restart-safe): re-arm the orphan-SL entry
        gate from the durable cancel-rejection audit rows.

        After a crash mid-reversal the protective SLM may still rest at the
        broker while its trade is gone.  The in-memory ``_uncancelled_sl``
        gate is rebuilt from ``execution_failure_events`` records so a fresh
        boot blocks new opposite entries until the rogue SLM is resolved —
        fail closed, never double-entry.
        """
        if not getattr(env, "is_live", False):
            return 0
        pm = getattr(env, "persistence", None)
        if pm is None or not hasattr(pm, "get_execution_failure_events"):
            return 0
        try:
            rows = pm.get_execution_failure_events(limit=1000)
        except Exception as e:
            log.warning("[Engine] uncancelled-SL restore scan failed: %s", e)
            return 0
        latest: dict[tuple, tuple] = {}
        for r in rows or []:
            if str(r.get("action") or "") != "cancel_rejected":
                continue
            sid = r.get("strategy_id")
            inst = r.get("instrument")
            oid = r.get("order_id")
            if not sid or not inst or not oid:
                continue
            key_ = (env.name, sid, inst)
            rid = int(r.get("id") or 0)
            if key_ not in latest or rid > latest[key_][0]:
                latest[key_] = (rid, oid, r.get("broker_order_id"))
        restored = 0
        for key_, (rid, oid, boid) in latest.items():
            self._uncancelled_sl[key_] = oid
            restored += 1
            log.warning(
                "[Engine] startup reconcile: %s/%s blocked by uncancelled "
                "SL %s (audit #%d)", key_[1], key_[2], boid or oid, rid)
        if restored:
            log.warning("[Engine] startup reconcile: re-armed %d orphan-SL "
                        "entry block(s) for env %s", restored, env.name)
        return restored

    def _broker_flat_for_entry(self, env, signal) -> tuple[bool, dict]:
        """§25/§28/§108 — EXIT-FIRST gate: an entry is placed only when the
        broker shows the strategy's instrument FLAT.  Broker net position is
        cross-checked WITH any still-unresolved protective SL (``_uncancelled
        _sl``) that could re-fire against a new opposite position.  Not
        verifiable -> fail-closed (blocked + reconciliation surface)."""
        broker = getattr(env, "broker", None)
        if broker is None or not hasattr(broker, "positions"):
            return True, {}
        key = (env.name, signal.strategy_id, signal.instrument)
        if key in self._uncancelled_sl:
            return False, {"reason": "orphan_protective_sl",
                           "sl_order_id": self._uncancelled_sl[key]}
        net_qty = 0
        try:
            held = [p for p in (broker.positions() or [])
                    if (p.get("instrument") or "") == signal.instrument]
        except Exception:
            return False, {"reason": "positions_api_failed"}
        for p in held:
            side = str(p.get("side") or "").upper()
            qty = int(p.get("quantity") or 0)
            net_qty += qty if side in ("BUY", "LONG") else -qty
        if net_qty != 0:
            return False, {"reason": "broker_position_open", "net_qty": net_qty}
        return True, {}

    def _persist_fill(self, fill, trade_id: str, signal_id: str | None,
                      env_name: Optional[str] = None) -> None:
        env = self._env_for(env_name)
        if env.persistence:
            env.persistence.save_fill({"fill_id": fill.fill_id, "order_id": fill.order_id,
                "strategy_id": fill.strategy_id, "instrument": fill.instrument, "side": fill.side,
                "quantity": fill.quantity, "price": fill.price,
                "timestamp": datetime.fromtimestamp(fill.timestamp, tz=timezone.utc).isoformat(),
                "trade_id": trade_id, "entry_signal_id": signal_id,
                "broker_fill_id": getattr(fill, "broker_fill_id", None),
                "broker_order_id": getattr(fill, "broker_order_id", None),
                "broker_trade_id": getattr(fill, "broker_trade_id", None),
                "cumulative_filled_quantity": getattr(fill, "cumulative_filled_quantity", None),
                "position_id": getattr(fill, "position_id", None),
                "lifecycle_id": getattr(fill, "lifecycle_id", None) or trade_id,
                "position_generation": getattr(fill, "position_generation", None)})
            # Phase 9.7 — keep the per-order fill reconciliation ledger in lock
            # step with every persisted fill (entry path).
            self._update_fill_reconciliation(env, fill)

    def _reconcile_live_fill(self, env, fill) -> str:
        """Phase 9.7 — broker-authoritative fill admission for LIVE.

        Returns:
          'apply'          — the broker delta is not yet accounted; apply it.
          'duplicate'      — a fill with the same broker_fill_id is already
                             persisted (the same execution event re-delivered
                             through WS/REST/replay).
          'already_synced' — this broker order's cumulative quantity is already
                             fully accounted locally (restart re-poll of an
                             already-synced order).
          'divergence'     — the broker cumulative moved backwards vs the local
                             ledger; the fill is surfaced, never invented away.
        """
        broker_order_id = getattr(fill, "broker_order_id", None)
        if env.mode != "LIVE" or not broker_order_id or env.persistence is None:
            return "apply"
        bfid = getattr(fill, "broker_fill_id", None)
        if bfid:
            try:
                if env.persistence.fill_by_broker_fill_id(bfid):
                    return "duplicate"
            except Exception:
                pass
        broker_cum = getattr(fill, "cumulative_filled_quantity", None)
        if broker_cum is None:
            return "apply"
        broker_cum = int(broker_cum)
        try:
            ledger = env.persistence.get_fill_reconciliation(
                broker_order_id, execution_mode="LIVE")
        except Exception:
            ledger = None
        if ledger is None:
            return "apply"
        prior = int(ledger.get("broker_cumulative_qty") or 0)
        if broker_cum <= prior:
            return "already_synced"
        local_cum = int(ledger.get("local_cumulative_qty") or 0)
        if broker_cum < local_cum:
            self.publish_event("reconciliation_mismatch", {
                "broker_order_id": broker_order_id,
                "broker_cumulative": broker_cum,
                "local_cumulative": local_cum,
                "reason": "fill_ledger_broker_behind_local",
                "execution_mode": env.mode}, env_name=env.name)
            return "divergence"
        return "apply"

    def _update_fill_reconciliation(self, env, fill) -> None:
        """Mirror the per-order fill reconciliation ledger from persisted fills
        (Phase 9.7). The broker cumulative quantity is the authority; local
        cumulative mirrors the fills table; any gap is surfaced, never hidden."""
        broker_order_id = getattr(fill, "broker_order_id", None)
        if env.mode != "LIVE" or not broker_order_id or env.persistence is None:
            return
        try:
            local_cum = env.persistence.order_cumulative_filled(
                broker_order_id, execution_mode="LIVE")
            broker_cum = int(getattr(fill, "cumulative_filled_quantity",
                                     None) or local_cum)
            prior = env.persistence.get_fill_reconciliation(
                broker_order_id, execution_mode="LIVE")
            if prior is not None:
                broker_cum = max(broker_cum,
                                 int(prior.get("broker_cumulative_qty") or 0))
            env.persistence.save_fill_reconciliation({
                "broker_order_id": broker_order_id,
                "strategy_id": fill.strategy_id,
                "instrument": fill.instrument,
                "order_id": fill.order_id,
                "side": fill.side,
                "broker_cumulative_qty": broker_cum,
                "local_cumulative_qty": local_cum,
                "last_broker_fill_id": getattr(fill, "broker_fill_id", None),
                "broker_average_price": float(fill.price or 0.0),
            })
        except Exception as e:
            log.error("[Engine] fill reconciliation update failed for %s: %s",
                      broker_order_id, e)

    def _persist_position(self, position, env_name: Optional[str] = None) -> None:
        """Persist a position row into the canonical positions table.

        position_id is the row key; trade_id is the separate canonical trade
        identity (position_id != trade_id, enforced by the DB trigger).
        """
        env = self._env_for(env_name)
        if env.persistence is not None and hasattr(env.persistence, "save_position"):
            try:
                env.persistence.save_position(position)
            except Exception as e:
                log.error("[Engine] save_position failed for %s: %s",
                          getattr(position, "position_id", "?"), e)

    def _calculate_margin(self, instrument: str, price: float, quantity: int) -> float:
        model = self.config.instrument(instrument).get("margin_model", {})
        if model:
            return quantity * (model.get("slope", 0.0) * price + model.get("intercept", 0.0))
        return price * quantity * self.config.instrument(instrument).get("multiplier", 1.0) * 0.065

    def _reset_strategy_state(self, strategy_id: str, keep_pending: bool = False,
                              env_name: Optional[str] = None) -> None:
        env = self._env_for(env_name)
        strategy = env.strategies.get(strategy_id)
        if strategy:
            keep = keep_pending and strategy.pending_entry is not None
            if keep:
                pen = strategy.pending_entry
                pen.status = "pending"
                strategy.state = (StrategyState.PENDING_LONG if pen.side == "LONG"
                                  else StrategyState.PENDING_SHORT)
            else:
                strategy.state = StrategyState.FLAT
            strategy.position_side = strategy.stop_price = None
            strategy.current_position_id = None
            strategy.position_generation = None
            strategy.position_quantity = None
            # The stop-out re-fire guard lifts once the position actually
            # closes: the strategy is flat again, so later stop exits (new
            # trades) are evaluated normally.
            setattr(strategy, "stop_exit_submitted", False)
            # IMMEDIATE-LIMIT: a terminal reject/cancel must release the
            # signal lock so the strategy can detect a fresh signal again.
            setattr(strategy, "immediate_limit_sent", None)
            if not keep:
                strategy.pending_entry = None
            strategy.current_trade_id = None
        runtime = env.runtimes.get(strategy_id) if env.runtimes is not None else None
        if runtime is not None:
            runtime.current_trade_id = None

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
                "db_path", "data/db/trading.db"))
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
        # §108 restart-safe startup reconcile: re-arm the orphan-SL entry gate
        # from durable cancel-failure records BEFORE any signal can fire.
        for name, env in self._envs.items():
            if not env.is_live:
                continue
            try:
                self._restore_uncancelled_sl(env)
            except Exception as e:
                log.warning("[Engine] startup SL reconcile failed for %s: %s",
                            name, e)

        # ── SL RECOVERY: detect and protect unprotected positions ──
        # After restoring orphan-SL gates, check for open positions that
        # lack protective SL (e.g. from a crash/restart after entry but
        # before SL was placed, or when SL was rejected).
        for name, env in self._envs.items():
            if not env.is_live:
                continue
            try:
                recovered = self.recover_missing_sl(env_name=name)
                if recovered:
                    log.info("[Engine] startup SL recovery: protected %d "
                             "position(s) in env %s", recovered, name)
            except Exception as e:
                log.warning("[Engine] startup SL recovery failed for %s: %s",
                            name, e)

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
            print(f"  {name}: {strat.instrument} {strat.fast_timeframe}/{strat.mid_timeframe}/{strat.htf_timeframe}", flush=True)
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
            handle_fill = lambda fill, sid, is_exit, _env=env: self._handle_fill(
                fill, sid, is_exit=is_exit, env_name=_env.name)
            on_reconcile = lambda _env=env: self._reconcile_strategy_positions(
                _env.name)
            reset_strategy_fn = lambda sid, _env=env: self._reset_strategy_state(
                sid, env_name=_env.name)
            env.sync_service = BrokerSyncService(
                env,
                self.config,
                clock=time.time,
                wire_now=True,
                handle_fill=handle_fill,
                on_reconcile=on_reconcile,
                reset_strategy_fn=reset_strategy_fn,
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
                    on_failure_event=lambda ev: (
                        env.persistence.save_execution_failure_event(ev)
                        if env.persistence is not None else None),
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
                          "startup reconcile + recover_missing_sl are the "
                          "only protection", env_name, sids)

    def _warmup_from_rest(self, now_epoch: Optional[int] = None) -> None:
        """Warm up each strategy from Dhan REST historical data.

        PAPER strategies use the shared (engine-level) data adapter.
        LIVE strategies use their own per-env data adapter (LIVE independence).

        Contract (MCX + Dhan):
          - requested end = fetch boundary, NOT a market-close rule;
          - the returned dataset decides the latest available candle;
          - only genuinely completed candles feed the indicator streams
            (a forming candle can never poison DEMA/ATR state);
          - per (symbol, timeframe) forensics + latest-completed watermark.
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
            inst_cfg = self.config.get("instruments", {}).get(name, {})
            session_open = inst_cfg.get("session_open", "09:00")
            session_close = inst_cfg.get("session_close", "23:30")
            security_id = str(inst_cfg.get("security_id", ""))
            try:
                fast_id = {"5m": "5", "15m": "15"}.get(strategy.fast_timeframe, "5")
                fast_minutes = strategy._tf_to_minutes(strategy.fast_timeframe)
                accepted_fast, _ = self._fetch_warmup_candles(
                    name, strategy.instrument, fast_id, strategy.fast_timeframe,
                    fast_minutes, base_from, to_date, now_epoch,
                    session_open, session_close, security_id, last_days,
                    data_adapter=data_adapter)
                for _, row in accepted_fast.iterrows():
                    open_ts = float(row["timestamp"])
                    strategy.warmup_indicator(Bar(
                        instrument=strategy.instrument, timeframe=strategy.fast_timeframe,
                        start_ts=open_ts,
                        end_ts=open_ts + fast_minutes * 60,
                        open=row["open"], high=row["high"], low=row["low"], close=row["close"],
                        volume=int(row["volume"]),
                    ))
                for tf_id, tf_name in [("15", strategy.mid_timeframe), ("60", strategy.htf_timeframe)]:
                    try:
                        htf_min = int(tf_id)
                        accepted_htf, _ = self._fetch_warmup_candles(
                            name, strategy.instrument, tf_id, tf_name, htf_min,
                            base_from, to_date, now_epoch,
                            session_open, session_close, security_id, last_days,
                            data_adapter=data_adapter)
                        for _, row in accepted_htf.iterrows():
                            h_open_ts = float(row["timestamp"])
                            bar = Bar(instrument=strategy.instrument, timeframe=tf_name,
                                      start_ts=h_open_ts, end_ts=h_open_ts + htf_min * 60,
                                      open=row["open"], high=row["high"], low=row["low"], close=row["close"],
                                      volume=int(row["volume"]))
                            strategy.warmup_htf(bar)
                            strategy.warmup_indicator_htf(bar)
                    except Exception as e:
                        log.warning("[Engine] %s: HTF warmup failed: %s", name, e)
                log.info("[Engine] %s warmed (%s): fast=%d bars, mid_htf=%d, slow_htf=%d",
                         name, env.name, strategy.fast_indicator._count,
                         strategy.mid_htf_state.bar_count(), strategy.slow_htf_state.bar_count())
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
        # by +5:30 on non-IST hosts, mis-anchoring DEMA/ATR streams).
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

    def restore(self, saved_state: dict, env_name: str = "paper") -> None:
        """Restore ONE environment's engine state from a saved snapshot.

        Restores that environment's per-strategy indicator/HTF/pending state
        AND its per-strategy position managers (each position back into its
        OWN runtime). Account margin is then reconstituted from the restored
        open positions so the startup reconciliation margin check is exactly
        consistent. PAPER is the default target (legacy callers unchanged);
        the LIVE environment restores its own snapshot/state file.
        """
        if not saved_state:
            return
        env = self._env_for(env_name)
        strategies_state = saved_state.get("strategies", {})
        for name, strat in env.strategies.items():
            if name in strategies_state:
                try:
                    strat.restore(strategies_state[name])
                except Exception:
                    pass
        positions_state = saved_state.get("positions")
        if positions_state:
            try:
                env.position_manager.restore(positions_state)
            except Exception:
                pass
        # Arm the stop for every restored OPEN position.  Without this a
        # restarted strategy keeps stop_price=None and _check_stop_loss() bails
        # on its `stop_price is None` guard, so a live position restored across
        # a restart trades with NO stop at all.
        for sid, strat in env.strategies.items():
            try:
                open_pos = next((p for p in env.position_manager.get_positions_by_strategy(sid)
                                 if p.is_open), None)
                if open_pos is None:
                    continue
                if getattr(strat, "stop_price", None) is None:
                    _rec_stop = getattr(open_pos, "stop_price", None)
                    if _rec_stop is None:
                        _rec_stop = self._recover_stop_price(env, open_pos)
                    if _rec_stop is not None:
                        strat.stop_price = _rec_stop
                        if getattr(open_pos, "stop_price", None) is None:
                            open_pos.stop_price = _rec_stop
                    else:
                        log.error("[Engine] restore: %s has OPEN position %s with "
                                  "NO recoverable stop_price - UNPROTECTED",
                                  sid, getattr(open_pos, "position_id", "?"))
            except Exception as e:
                log.error("[Engine] restore: stop arming failed for %s: %s", sid, e)
        # §41b — reconcile orphaned exit states after a restart.  In-flight
        # paper-broker exit orders do not survive a restart (only the explicit
        # broker_order_id mapping does), so a strategy restored as
        # EXIT_ORDER_SUBMITTED with a still-open position has no live exit in
        # flight.  Re-arm the position state (and clear the stop re-fire
        # guard) so the now-guaranteed exit path can retry the stop-out on the
        # next evaluation; otherwise the position would stay open forever.
        for name, strat in env.strategies.items():
            if getattr(strat, "stop_exit_submitted", False):
                setattr(strat, "stop_exit_submitted", False)
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
                if getattr(env.execution_engine, "restore", None) is not None:
                    env.execution_engine.restore(exec_state)
                    router = getattr(env.execution_engine, "broker_router", None)
                    if router is not None and hasattr(router, "register_from_kwargs"):
                        for o in (getattr(env.execution_engine, "_orders", {})
                                  or {}).values():
                            bid = getattr(o, "_broker_order_id", None)
                            if bid and router.resolve(str(bid)) is None:
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
        # Per-strategy operator gates survive restart via the saved snapshot.
        gates_state = saved_state.get("strategy_gates")
        if gates_state:
            try:
                for sid, gdict in gates_state.items():
                    self._strategy_gates[sid] = StrategyGate.from_dict(gdict)
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

    @property
    def tick_signal_processing(self) -> bool:
        return getattr(self, '_tick_signal_processing', True)

    @tick_signal_processing.setter
    def tick_signal_processing(self, value: bool):
        self._tick_signal_processing = value

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
                    # An OPEN position must ALWAYS carry a stop, even when the
                    # strategy state was already restored as long/short: a
                    # missing stop_price makes _check_stop_loss() bail and
                    # leaves the position UNPROTECTED.  Recover it from the
                    # position, then the signals/trades rows.
                    if getattr(strategy, "stop_price", None) is None:
                        _rec_stop = self._recover_stop_price(env, open_pos)
                        if _rec_stop is not None:
                            strategy.stop_price = _rec_stop
                            if getattr(open_pos, "stop_price", None) is None:
                                open_pos.stop_price = _rec_stop
                        else:
                            log.error("[Engine] %s has an OPEN position %s with NO "
                                      "recoverable stop_price - UNPROTECTED",
                                      sid, open_pos.position_id)

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
        try:
            broker_rows = list(brokers_positions() or [])
        except Exception:
            broker_rows = []
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

    def _persist_recon_order(self, env, fill, order_type: str, trade_id: str,
                             signal_id) -> None:
        """Persist the synthetic reconciliation order row (fills/orders
        integrity triggers require an orders row before a fill can persist)."""
        if env.persistence is None:
            return
        try:
            now = datetime.fromtimestamp(fill.timestamp or time.time(),
                                         tz=timezone.utc).isoformat()
            env.persistence.save_order({
                "order_id": fill.order_id, "strategy_id": fill.strategy_id,
                "instrument": fill.instrument, "side": fill.side,
                "quantity": fill.quantity, "order_type": order_type,
                "price": fill.price, "planned_entry_price": fill.price,
                "planned_sl": None, "planned_order_type": order_type,
                "state": "filled", "filled_quantity": fill.quantity,
                "average_fill_price": fill.price,
                "created_at": now, "updated_at": now,
                "signal_id": signal_id, "trade_id": trade_id,
            })
        except Exception as e:
            log.error("[Engine] reconcile order persist failed for %s: %s",
                      fill.order_id, e)

    def _close_local_mirror(self, env, sid, position) -> None:
        """Broker reports FLAT while the local mirror holds an open position:
        mirror the position closed, audited and persisted (exits are always
        allowed, independent of the entry gate)."""
        close_manager = env.trade_close_manager or self._trade_close_manager
        if close_manager is None:
            return
        try:
            multiplier = getattr(position, "multiplier", 1.0) or 1.0
            fill = Fill(
                fill_id=f"LIVE-{uuid.uuid4().hex}",
                order_id=f"RECON-CLOSE-{uuid.uuid4().hex}",
                instrument=position.instrument,
                side="SELL" if position.is_long else "BUY",
                quantity=int(position.quantity),
                price=float(getattr(position, "average_entry", 0.0) or 0.0),
                timestamp=time.time(),
                strategy_id=sid,
                multiplier=multiplier,
                entry_signal_id=getattr(position, "entry_signal_id", None),
                trade_id=position.trade_id,
            )
            fill.broker_order_id = "RECON-CLOSE"
            fill.broker_fill_id = f"RECON-CLOSE:{fill.fill_id}"
            fill.cumulative_filled_quantity = int(position.quantity)
            # the persisted close references this synthetic order row (fills and
            # orders integrity triggers require it before save_fill)
            self._persist_recon_order(env, fill, "RECONCILE", position.trade_id,
                                      position.entry_signal_id)
            result = close_manager.close_position(
                fill, position, sid, multiplier,
                exit_reason="RECONCILE_BROKER_FLAT",
                exit_signal_id=None,
            )
            if result is False:
                return
            if env.persistence is not None and hasattr(env.persistence, "close_position_record"):
                try:
                    env.persistence.close_position_record(position)
                except Exception:
                    pass
            runtime = env.runtimes.require(sid) if env.runtimes is not None else None
            if runtime is not None and runtime.lifecycle is not None:
                runtime.lifecycle.register_exit_fill(
                    position.trade_id, fill.fill_id, fill.price, fill.timestamp,
                    "", exit_reason="RECONCILE_BROKER_FLAT")
                runtime.lifecycle.close_trade(
                    position.trade_id, result["gross_pnl"], result["charges"], result["net_pnl"])
            self.publish_event("reconciliation_resolved", {
                "strategy_id": sid, "instrument": position.instrument,
                "position_id": position.position_id, "direction": "broker_flat_close_local",
                "exit_reason": "RECONCILE_BROKER_FLAT",
                "execution_mode": env.mode}, env_name=env.name)
            self._reset_strategy_state(sid, env_name=env.name)
        except Exception as e:
            log.error("[Engine] reconcile close failed for %s: %s", sid, e)

    def _adopt_broker_position(self, env, live_cfg: dict, sid: str,
                               instrument: str, broker_row: dict) -> None:
        """Broker reports an open position the local mirror does not hold:
        adopt it into the local mirror only when the entry gate is ON, margin
        permits, and the strategy exists (audit + risk check per Phase 9.7)."""
        if not env.gate_enabled:
            self.publish_event("reconciliation_mismatch", {
                "strategy_id": sid, "instrument": instrument,
                "broker_side": str(broker_row.get("side") or "").upper(),
                "broker_quantity": int(broker_row.get("quantity") or 0),
                "reason": "broker_position_adoption_blocked_gate",
                "execution_mode": env.mode}, env_name=env.name)
            return
        if sid not in env.strategies:
            self.publish_event("reconciliation_mismatch", {
                "strategy_id": sid, "instrument": instrument,
                "broker_side": str(broker_row.get("side") or "").upper(),
                "broker_quantity": int(broker_row.get("quantity") or 0),
                "reason": "broker_position_adoption_unknown_strategy",
                "execution_mode": env.mode}, env_name=env.name)
            return
        side = str(broker_row.get("side") or "").upper()
        if side not in ("LONG", "SHORT"):
            return
        try:
            qty = int(broker_row.get("quantity") or 0)
            avg = float(broker_row.get("average_entry_price") or 0.0)
            if qty <= 0 or avg <= 0:
                self.publish_event("reconciliation_mismatch", {
                    "strategy_id": sid, "instrument": instrument,
                    "broker_side": side, "broker_quantity": qty,
                    "reason": "broker_position_adoption_missing_price",
                    "execution_mode": env.mode}, env_name=env.name)
                return
            strategy = env.strategies[sid]
            runtime = env.runtimes.require(sid) if env.runtimes is not None else None
            lifecycle = (runtime.lifecycle if runtime is not None
                         else self._lifecycle)
            if lifecycle is None:
                runtime = env.runtimes.require(sid)
                lifecycle = runtime.lifecycle
            inst_cfg = self.config.instrument(instrument) or {}
            multiplier = float(inst_cfg.get("multiplier", 1.0) or 1.0)
            signal = Signal(signal_type=SignalType.LONG if side == "LONG" else SignalType.SHORT,
                            instrument=instrument, strategy_id=sid,
                            timestamp=time.time(), trigger_price=avg,
                            stop_price=getattr(strategy, "stop_price", 0.0) or 0.0,
                            quantity=qty)
            signal.signal_id = f"RECON-{uuid.uuid4().hex[:12]}"
            if env.persistence is not None:
                try:
                    env.persistence.save_signal({
                        "signal_id": signal.signal_id,
                        "strategy_id": sid, "instrument": instrument,
                        "side": signal.signal_type.value,
                        "signal_type": "entry", "timestamp": signal.timestamp,
                        "trigger_price": avg, "stop_price": signal.stop_price,
                        "quantity": qty})
                except Exception:
                    pass
            trade = lifecycle.create_trade_from_signal(
                signal, sid, sid, instrument, qty, multiplier)
            if trade is None or not lifecycle.persist_trade(trade):
                return
            margin = self._calculate_margin(instrument, avg, qty)
            account = env.account_engines.get(sid)
            if account is None:
                return
            account_blocked = account.block_margin(margin)
            global_blocked = env.account_engine.block_margin(margin) if account_blocked else False
            if not (account_blocked and global_blocked):
                if account_blocked:
                    account.release_margin(margin)
                # This position ALREADY EXISTS at the broker (it came from the
                # reconcile snapshot): rejecting the adoption orphans a real
                # position with no tracking.  Over-allocate the margin and
                # surface a breach, then proceed so the position is tracked.
                account.block_margin(margin, force=True)
                env.account_engine.block_margin(margin, force=True)
                self.publish_event("reconciliation_mismatch", {
                    "strategy_id": sid, "instrument": instrument,
                    "reason": "broker_position_adoption_margin_breach",
                    "margin": margin, "execution_mode": env.mode},
                    env_name=env.name)
                try:
                    self.telegram.on_risk_alert({
                        "kind": "reconcile_margin_breach",
                        "message": (
                            f"RECONCILE MARGIN BREACH: broker position "
                            f"{sid} {instrument} x{qty} tracked despite margin "
                            f"over-limit ({margin:.2f})."),
                    })
                except Exception:
                    pass
            fill = Fill(
                fill_id=f"LIVE-{uuid.uuid4().hex}",
                order_id=f"RECON-ADOPT-{uuid.uuid4().hex}",
                instrument=instrument,
                side="BUY" if side == "LONG" else "SELL",
                quantity=qty, price=avg, timestamp=time.time(),
                strategy_id=sid, multiplier=multiplier,
                entry_signal_id=signal.signal_id, trade_id=trade.trade_id,
            )
            fill.broker_order_id = "RECON-ADOPT"
            fill.broker_fill_id = f"RECON-ADOPT:{fill.fill_id}"
            fill.cumulative_filled_quantity = qty
            position = env.position_manager.open_position(
                fill, multiplier=multiplier, margin=margin,
                stop_price=getattr(strategy, "stop_price", None),
                entry_signal_id=signal.signal_id, trade_id=trade.trade_id,
            )
            # the adopted position needs its synthetic order + fill persisted
            # (fills and orders integrity triggers require the order row first)
            if env.persistence is not None:
                self._persist_recon_order(env, fill, "RECONCILE",
                                          trade.trade_id, signal.signal_id)
                try:
                    env.persistence.save_fill({
                        "fill_id": fill.fill_id, "order_id": fill.order_id,
                        "strategy_id": fill.strategy_id, "instrument": fill.instrument,
                        "side": fill.side, "quantity": fill.quantity, "price": fill.price,
                        "timestamp": datetime.fromtimestamp(
                            fill.timestamp, tz=timezone.utc).isoformat(),
                        "trade_id": trade.trade_id,
                        "entry_signal_id": signal.signal_id,
                        "broker_order_id": None, "broker_fill_id": None,
                        "broker_trade_id": None,
                        "cumulative_filled_quantity": qty})
                except Exception:
                    pass
            self._persist_position(position, env_name=env.name)
            lifecycle.register_entry_fill(trade.trade_id, fill.fill_id, avg, fill.timestamp)
            lifecycle.register_position(trade.trade_id, position.position_id)
            strategy.current_trade_id = trade.trade_id
            strategy.state = (StrategyState.LONG_POSITION if side == "LONG"
                              else StrategyState.SHORT_POSITION)
            strategy.position_side = side
            if runtime is not None:
                runtime.current_trade_id = trade.trade_id
            self.publish_event("reconciliation_resolved", {
                "strategy_id": sid, "instrument": instrument,
                "position_id": position.position_id, "trade_id": trade.trade_id,
                "direction": "broker_open_adopt_local",
                "broker_side": side, "broker_quantity": qty,
                "broker_average_entry": avg,
                "execution_mode": env.mode}, env_name=env.name)
        except Exception as e:
            log.error("[Engine] reconcile adopt failed for %s/%s: %s",
                      sid, instrument, e)

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
