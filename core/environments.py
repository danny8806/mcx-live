"""Execution environment container for the LIVE + PAPER parallel architecture.

Each :class:`Environment` is a fully isolated execution context:

* its own strategy instance objects (same factories/logic, separate runtimes)
* its own execution engine / broker transport
* its own portfolio (position managers, pnl engines, account engines)
* its own risk engine
* its own persistence (separate DB file + state file)
* its own fill dedup / event store / trade ledger
* its own data infrastructure (data adapter, candle fetcher, event bus,
  candle distributor, candle router, indicator engine, market status, safe mode)

PAPER and LIVE environments are fully independent — each has its own Dhan
data adapter (REST + WebSocket), event bus, candle pipeline, indicator engine,
market status tracker, and safe mode manager.  This ensures LIVE can never
be disrupted by PAPER data issues and vice versa.

Symbols: both environments use the same strategy_id / instrument / signal_id
values by design (parity); the indicator streams are independent per-env
(but produce identical results for the same input), so feeding the same bar
through a paper strategy and a live strategy yields byte-identical indicator
snapshots.  Execution records are tagged with ``execution_mode``
('PAPER'/'LIVE') and live records are prefixed ``LIVE-``.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from strategies.instance import StrategyInstance
from strategies.runtime import StrategyRuntimeRegistry


class EnvironmentKind(Enum):
    """The two supported execution environments.

    A PAPER environment uses simulated fills and never contacts a broker.
    A LIVE environment uses a broker transport and honors the master gate.
    An environment's kind is immutable for the lifetime of the process and is
    stamped on every persisted execution record (``execution_mode``).
    """

    PAPER = "PAPER"
    LIVE = "LIVE"


# Master-gate states.  A config default may be OFF, ARMED or ON (ON is how a
# live-enabling config opts in; it requires live_trading_enabled=true).  The
# transitional/active states (CLOSE_ONLY, EMERGENCY_STOP, LOCKED) are reached
# operationally through the gate state machine (Phase 9.14).
GATE_STATES = ("OFF", "ARMED", "ON", "CLOSE_ONLY", "EMERGENCY_STOP", "LOCKED")
GATE_INITIAL = ("OFF", "ARMED", "ON")
VALID_ENV_NAMES = ("paper", "live")


def validate_environment_config(config) -> None:
    """Reject ambiguous environment configurations at load time.

    ``config`` may be a plain dict or a :class:`config.Config` instance.

    Enforced rules (no half-states):
      1. names must be a non-empty subset of {"paper", "live"}.
      2. a LIVE environment requires a ``live`` config section.
      3. ``live.broker`` must be a known transport when live is enabled.
      4. ``live.live_trading_enabled=True`` requires an initial gate of
         ARMED or ON (OFF would contradict the explicit intent to trade).
      5. an initial gate of ON requires ``live_trading_enabled=True``.
      6. CLOSE_ONLY / EMERGENCY_STOP / LOCKED are not valid config defaults;
         they are operational states only.
    """
    if not isinstance(config, dict):
        config = config.get()                      # Config object -> dict
    system = config.get("system", {})
    raw_names = system.get("environments", ["paper"])
    names = [str(e).strip().lower() for e in raw_names if str(e).strip()]
    if not names:
        raise RuntimeError("environment config: system.environments is empty")
    unknown = [n for n in names if n not in VALID_ENV_NAMES]
    if unknown:
        raise RuntimeError(
            f"environment config: unknown environment(s): {unknown}; "
            f"allowed: {list(VALID_ENV_NAMES)}")

    has_live = "live" in names
    if not has_live:
        return

    if "live" not in config:
        raise RuntimeError(
            "environment config: 'live' in system.environments requires a "
            "'live' config section")
    live = config["live"]

    broker = str(live.get("broker") or live.get("transport") or "stub").lower()
    if broker not in ("stub", "dhan", "dhan_rest"):
        raise RuntimeError(
            f"environment config: unknown live broker {broker!r}; "
            "allowed: stub, dhan, dhan_rest")

    gate = str(live.get("gate") or "OFF").upper()
    if gate not in GATE_STATES:
        raise RuntimeError(
            f"environment config: invalid live.gate {gate!r}; "
            f"allowed: {GATE_STATES}")
    if gate not in GATE_INITIAL:
        raise RuntimeError(
            f"environment config: {gate} is not a valid INITIAL live.gate; "
            f"initial state must be one of {GATE_INITIAL} "
            "(ON/CLOSE_ONLY/EMERGENCY_STOP/LOCKED are operational states)")

    enabled = bool(live.get("live_trading_enabled", False))
    if enabled and gate == "OFF":
        raise RuntimeError(
            "environment config: live.live_trading_enabled=true but "
            "live.gate=OFF (contradiction); initial gate must be ARMED or ON")
    if gate == "ON" and not enabled:
        raise RuntimeError(
            "environment config: live.gate=ON but live.live_trading_enabled "
            "is false (contradiction); set live_trading_enabled=true")

    pm = live.get("price_model") or {}
    if bool(pm.get("enabled", False)):
        if float(pm.get("entry_offset", 0.0)) < 0 or float(pm.get("sl_offset", 0.0)) < 0:
            raise RuntimeError(
                "environment config: live.price_model offsets must be >= 0")


@dataclass
class Environment:
    """One isolated execution environment (paper or live).

    Each environment carries its own fully independent data infrastructure
    (data adapter, candle fetcher, event bus, candle distributor/router,
    indicator engine, market status, safe mode) so LIVE and PAPER can never
    interfere with each other's market data or signal generation.
    """

    name: str                                        # "paper" | "live"
    mode: str                                        # "PAPER" | "LIVE"
    is_live: bool = False
    kind: EnvironmentKind = EnvironmentKind.PAPER
    gate_state: Optional[str] = None                 # live: master-gate initial state
    db_path: Optional[str] = None                    # canonical per-env DB file
    strategies: dict[str, StrategyInstance] = field(default_factory=dict)
    runtimes: Optional[StrategyRuntimeRegistry] = None
    execution_engine: Any = None
    order_manager: Any = None
    broker_router: Any = None
    position_manager: Any = None
    pnl_engines: dict = field(default_factory=dict)
    account_engines: dict = field(default_factory=dict)
    account_engine: Any = None
    risk_engine: Any = None
    persistence: Any = None
    fill_dedup: Any = None
    event_store: Any = None
    trade_ledger: Any = None
    trade_close_manager: Any = None
    indicators: dict = field(default_factory=dict)
    broker: Any = None                              # live broker client (live env)
    gate_enabled: bool = False                      # entries allowed (live: gate==ON)
    sync_service: Any = None                        # Phase 9.8 BrokerSyncService
    # ── Per-env data infrastructure (LIVE independence) ──
    data_adapter: Any = None                        # DhanDataAdapter (REST + WS)
    candle_fetcher: Any = None                      # CandleFetcher (REST polling)
    event_bus: Any = None                           # EventBus (per-env instance)
    candle_distributor: Any = None                  # NativeCandleDistributor
    candle_router: Any = None                       # NativeCandleRouter
    indicator_engine: Any = None                    # SharedNativeIndicatorEngine (per-env)
    market_status: Any = None                       # MarketStatus (per-env)
    safe_mode: Any = None                           # SafeModeManager (per-env)

    @property
    def identity(self) -> str:
        """Stable execution-identity stamp = the DB ``execution_mode`` value."""
        return self.kind.value

    def strategy_count(self) -> int:
        return len(self.strategies)