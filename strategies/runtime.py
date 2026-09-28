"""StrategyRuntime — fully isolated per-strategy runtime container.

The engine owns exactly four StrategyRuntime objects (one per strategy).
Every piece of mutable per-strategy state lives on the runtime:

    - strategy            StrategyInstance (indicators, HTF state, pending/SL)
    - lifecycle           TradeLifecycleManager (trade identity ONLY for this strategy)
    - order_manager       OrderManager (pending-signal dedup, active orders, fills slot)
    - position_manager    PositionManager (this strategy's positions)
    - trade_close_manager TradeCloseManager (this strategy's atomic close path)
    - current_trade_id    the strategy's current open trade (per-runtime mirror)

Nothing that varies per strategy is shared across StrategyRuntime objects.
Shared infrastructure (broker transport, EventBus, trading.db, market data,
clock) stays engine-level and is deliberately absent here.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, Optional


@dataclass
class StrategyRuntime:
    """Owns every piece of one strategy's mutable runtime state."""

    strategy_id: str
    strategy: Any = None                # StrategyInstance
    lifecycle: Any = None               # TradeLifecycleManager (per-strategy)
    order_manager: Any = None           # OrderManager (per-strategy)
    position_manager: Any = None        # PositionManager (per-strategy)
    trade_close_manager: Any = None     # TradeCloseManager (per-strategy)
    current_trade_id: Optional[str] = None

    # Per-strategy in-memory projections (order/fill/position views).
    orders: Dict[str, Any] = field(default_factory=dict)
    fills: Dict[str, Any] = field(default_factory=dict)
    positions: Dict[str, Any] = field(default_factory=dict)

    @property
    def execution(self) -> Any:
        """This strategy's execution-state owner (its OrderManager)."""
        return self.order_manager


class StrategyRuntimeRegistry:
    """Holds the per-strategy StrategyRuntime containers.

    Guards against duplicate registration and gives the engine a single,
    positionally-stable place to resolve a strategy's runtime.
    """

    def __init__(self) -> None:
        self._runtimes: Dict[str, StrategyRuntime] = {}

    def register(self, runtime: StrategyRuntime) -> None:
        if runtime.strategy_id in self._runtimes:
            raise ValueError(f"duplicate StrategyRuntime for {runtime.strategy_id}")
        self._runtimes[runtime.strategy_id] = runtime

    def register_or_replace(self, runtime: StrategyRuntime) -> None:
        self._runtimes[runtime.strategy_id] = runtime

    def get(self, strategy_id: str) -> Optional[StrategyRuntime]:
        return self._runtimes.get(strategy_id)

    def require(self, strategy_id: str) -> StrategyRuntime:
        runtime = self._runtimes.get(strategy_id)
        if runtime is None:
            raise KeyError(f"no StrategyRuntime registered for {strategy_id!r}")
        return runtime

    def __getitem__(self, strategy_id: str) -> StrategyRuntime:
        return self.require(strategy_id)

    def __iter__(self) -> Iterator[StrategyRuntime]:
        return iter(self._runtimes.values())

    def __len__(self) -> int:
        return len(self._runtimes)

    def all(self) -> list[StrategyRuntime]:
        return list(self._runtimes.values())

    @property
    def strategy_ids(self) -> list[str]:
        return list(self._runtimes.keys())

    def snapshot(self) -> dict:
        return {
            "strategy_ids": self.strategy_ids,
            "runtimes": {sid: rt.strategy_id for sid, rt in self._runtimes.items()},
        }