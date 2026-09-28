"""BrokerEventRouter — explicit broker event routing per order (§39–40).

Dhan (or the paper broker) may deliver events through one shared channel. This
router is the single entry point for those broker events. It NEVER decides
ownership by symbol, side, or "the latest order". Instead every broker event is
resolved through an EXPLICIT durable mapping:

    broker_order_id -> internal order_id -> trade_id -> strategy_id

Events that cannot be mapped safely are QUARANTINED (logged as ERROR, recorded,
never acted upon). The mapping survives process restart through the canonical
`broker_order_mapping` table in trading.db.
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

log = logging.getLogger(__name__)

_MAX_QUARANTINE = 500


@dataclass
class BrokerOrderMapping:
    """Explicit mapping for one broker order (mission §40)."""

    broker_order_id: str
    order_id: str
    trade_id: str
    strategy_id: str
    instrument: str = ""
    registered_at: float = field(default_factory=time.time)


class BrokerEventRouter:
    """Receives each broker event once, resolves it, routes it explicitly.

    The router holds the authoritative broker_order_id -> strategy identity
    mapping (restored from canonical persistence on restart). A broker event
    (order state change, or a fill) is routed to the owning strategy's
    OrderManager only when the mapping resolves AND agrees with the event's
    own identity. Anything else is quarantined — never guessed.
    """

    def __init__(self, persistence: Optional[Any] = None):
        self._persistence = persistence
        self._lock = threading.RLock()
        self._mappings: dict[str, BrokerOrderMapping] = {}
        self._quarantined_events: list[dict] = []
        self._routed_count: int = 0

    # ── registration ────────────────────────────────────────────────────

    def register_mapping(self, mapping: BrokerOrderMapping) -> None:
        """Record an explicit broker_order_id mapping (in-memory + durable)."""
        with self._lock:
            self._mappings[mapping.broker_order_id] = mapping
        if self._persistence is not None:
            try:
                self._persistence.save_broker_order_mapping({
                    "broker_order_id": mapping.broker_order_id,
                    "order_id": mapping.order_id,
                    "trade_id": mapping.trade_id,
                    "strategy_id": mapping.strategy_id,
                    "instrument": mapping.instrument,
                })
            except Exception as e:
                log.error("[BrokerRouter] mapping persist failed for %s: %s",
                          mapping.broker_order_id, e)

    def register_from_order(self, order) -> None:
        """Register a mapping from a broker Order object.

        broker_order_id == order.order_id in the paper model — the identity is
        explicit and unique per placed order.
        """
        mapping = BrokerOrderMapping(
            broker_order_id=order.order_id,
            order_id=order.order_id,
            trade_id=getattr(order, "trade_id", "") or "",
            strategy_id=order.strategy_id,
            instrument=getattr(order, "instrument", ""),
        )
        self.register_mapping(mapping)

    def register_from_kwargs(self, broker_order_id: str, order_id: str,
                             trade_id: str, strategy_id: str,
                             instrument: str = "") -> None:
        self.register_mapping(BrokerOrderMapping(
            broker_order_id=broker_order_id, order_id=order_id,
            trade_id=trade_id, strategy_id=strategy_id, instrument=instrument,
        ))

    # ── resolution ──────────────────────────────────────────────────────

    def resolve(self, broker_order_id: str) -> Optional[BrokerOrderMapping]:
        """Resolve the explicit mapping for a broker order id."""
        with self._lock:
            return self._mappings.get(broker_order_id)

    def resolve_strategy(self, broker_order_id: str) -> Optional[str]:
        mapping = self.resolve(broker_order_id)
        return mapping.strategy_id if mapping is not None else None

    # ── routing ─────────────────────────────────────────────────────────

    def route_fill(self, fill, on_fill: Callable, *,
                   entry_signal_id: Optional[str] = None,
                   is_exit: Optional[bool] = None) -> bool:
        """Route one broker fill event to its strategy via explicit mapping.

        Never uses symbol/side/"latest order": the owning strategy comes only
        from broker_order_id (= fill.order_id) -> strategy_id mapping. When the
        mapping is missing or disagrees with the fill's own strategy_id, the
        event is quarantined and on_fill is NOT called.

        Returns True if routed, False if quarantined (or fill not a fill).
        """
        broker_order_id = getattr(fill, "order_id", None)
        if not broker_order_id:
            self._quarantine("fill_without_order_id", str(getattr(fill, "fill_id", "?")),
                             {"fill_id": getattr(fill, "fill_id", None)})
            return False
        mapping = self.resolve(broker_order_id)
        if mapping is None:
            self._quarantine("unmappable_broker_order",
                             str(broker_order_id),
                             {"fill_id": getattr(fill, "fill_id", None)})
            return False
        if getattr(fill, "strategy_id", None) != mapping.strategy_id:
            self._quarantine(
                "fill_strategy_mismatch", str(fill.fill_id),
                {"broker_order_id": broker_order_id,
                 "mapped_strategy": mapping.strategy_id,
                 "fill_strategy": getattr(fill, "strategy_id", None)})
            return False
        with self._lock:
            self._routed_count += 1
        on_fill(fill, entry_signal_id, is_exit)
        return True

    def route_order_event(self, broker_order_id: str, on_order_event: Callable) -> bool:
        """Route a broker order state-change event by explicit mapping.

        on_order_event(mapping) is invoked only for a resolvable, in-scope
        mapping. Otherwise the event is quarantined.
        """
        mapping = self.resolve(broker_order_id)
        if mapping is None:
            self._quarantine("unmappable_broker_order", str(broker_order_id), {})
            return False
        with self._lock:
            self._routed_count += 1
        on_order_event(mapping)
        return True

    # ── quarantine ──────────────────────────────────────────────────────

    def _quarantine(self, reason: str, original_id: str, details: dict) -> None:
        """Record a rejected broker event. Never mutates lifecycle state."""
        record = {
            "reason": reason,
            "original_id": original_id,
            "details": details,
            "timestamp": time.time(),
        }
        log.error("[BrokerRouter] QUARANTINE %s id=%r details=%s",
                  reason, original_id, details)
        with self._lock:
            self._quarantined_events.append(record)
            if len(self._quarantined_events) > _MAX_QUARANTINE:
                self._quarantined_events = self._quarantined_events[-_MAX_QUARANTINE:]
        if self._persistence is not None:
            try:
                self._persistence.save_quarantine_record({
                    "original_type": "broker_event",
                    "original_id": str(original_id),
                    "reason": reason,
                    "payload": details,
                })
            except Exception as e:
                log.error("[BrokerRouter] quarantine persist failed: %s", e)

    # ── persistence / restart ───────────────────────────────────────────

    def set_persistence(self, persistence) -> None:
        self._persistence = persistence

    def restore(self) -> int:
        """Restore all durable broker mappings from canonical persistence.

        Survives restart (§40): broker_order_id -> order_id -> trade_id ->
        strategy_id is reloaded from trading.db so late broker events (a fill
        arriving after a crash) still route to the correct strategy.
        """
        if self._persistence is None:
            return 0
        try:
            rows = self._persistence.get_broker_order_mappings()
        except Exception as e:
            log.error("[BrokerRouter] restore failed: %s", e)
            return 0
        restored = 0
        for row in rows:
            self._mappings[row["broker_order_id"]] = BrokerOrderMapping(
                broker_order_id=row["broker_order_id"],
                order_id=row["order_id"],
                trade_id=row.get("trade_id") or "",
                strategy_id=row.get("strategy_id") or "",
                instrument=row.get("instrument") or "",
                registered_at=row.get("registered_at") or time.time(),
            )
            restored += 1
        if restored:
            log.info("[BrokerRouter] restored %d broker order mappings", restored)
        return restored

    # ── diagnostics ─────────────────────────────────────────────────────

    @property
    def mapping_count(self) -> int:
        with self._lock:
            return len(self._mappings)

    @property
    def routed_count(self) -> int:
        return self._routed_count

    @property
    def quarantine_count(self) -> int:
        with self._lock:
            return len(self._quarantined_events)

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "mappings": {
                    bid: {
                        "order_id": m.order_id,
                        "trade_id": m.trade_id,
                        "strategy_id": m.strategy_id,
                        "instrument": m.instrument,
                    }
                    for bid, m in self._mappings.items()
                },
                "routed_count": self._routed_count,
                "quarantined": list(self._quarantined_events),
            }