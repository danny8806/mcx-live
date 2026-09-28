"""Continuous order watcher + recovery engine for LIVE Dhan MCX.

The watcher owns the truth-transition from "a signal wants an entry" to "the
broker confirms a safe, protected position".  It combines three observations
into every decision:

* BROKER STATE  — what Dhan says about the order (WS fast path + REST verify)
* MARKET STATE  — LTP/ticks vs the strategy trigger (LONG: HIGH+offset,
                  SHORT: LOW-offset) from ticks, not candle-close polling
* INTENT STATE  — what the strategy still wants + risk validation

and then drives a SAFE recovery decision tree (WAIT / REPRICE / CANCEL / LOCK /
MARKET-fallback) under a strict P0..P6 priority model where an entry NEVER
blocks an exit or SL operation.

Anti-pattern this module is explicitly built against:

    LIMIT persisted -> timeout -> MARKET

There is NO blind ``LIMIT -> MARKET`` anywhere.  Every recovery action first
resolves the broker's authoritative state (order -> trades -> positions) and
the engine's ``apply_broker_statuses`` dedup guarantees a fill is never double
counted.  A MARKET fallback is only reachable when (a) explicitly configured,
(b) the resting LIMIT is confirmed cancelled, (c) the signal is still valid and
(d) the strategy position is flat.

The watcher is event-driven (WS ingest) + targeted-REST verify + periodic
reconciliation; it never creates a high-frequency poll loop.
"""
from __future__ import annotations

import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Callable, Optional

log = logging.getLogger(__name__)

# ── priority model (P0 = highest; an entry must never block higher) ──────
P0_EMERGENCY = 0
P1_STOP_LOSS = 1
P2_EXIT = 2
P3_REVERSAL_EXIT = 3
P4_REVERSAL_ENTRY = 4
P5_NORMAL_ENTRY = 5
P6_DASHBOARD = 6

_PRIORITY_RANK = {
    "EMERGENCY_EXIT": P0_EMERGENCY,
    "STOP_LOSS": P1_STOP_LOSS,
    "EXIT": P2_EXIT,
    "REVERSAL_EXIT": P3_REVERSAL_EXIT,
    "REVERSAL_ENTRY": P4_REVERSAL_ENTRY,
    "ENTRY": P5_NORMAL_ENTRY,
}

# ── missed-limit classification (A..K) ───────────────────────────────────
PENDING_BUT_VALID = "A_PENDING_BUT_VALID"
PENDING_AND_AGING = "B_PENDING_AND_AGING"
PENDING_MARKET_MOVED_AWAY = "C_PENDING_MARKET_MOVED_AWAY"
TRIGGER_CROSSED_NOT_FILLED = "D_TRIGGER_CROSSED_NOT_FILLED"
ORDER_REJECTED = "E_ORDER_REJECTED"
ORDER_CANCELLED = "F_ORDER_CANCELLED"
ORDER_EXPIRED = "G_ORDER_EXPIRED"
ORDER_UNKNOWN = "H_ORDER_UNKNOWN"
PARTIAL_FILL = "I_PARTIAL_FILL"
SIGNAL_EXPIRED = "J_SIGNAL_EXPIRED"
POSITION_CHANGED = "K_POSITION_CHANGED"

_FINAL_STATES = {"FILLED", "PARTIALLY_FILLED", "REJECTED", "CANCELED"}


def _cfg(config: dict, key: str, default=None):
    if not isinstance(config, dict):
        return default
    return config.get(key, default)


@dataclass
class OrderWatchRecord:
    """Per-order continuous tracking state (§5 of the watcher spec)."""
    internal_order_id: str
    broker_order_id: Optional[str] = None
    exchange_order_id: Optional[str] = None
    correlation_id: Optional[str] = None
    strategy_id: str = ""
    trade_id: Optional[str] = None
    lifecycle_id: Optional[str] = None
    signal_id: Optional[str] = None
    position_id: Optional[str] = None
    position_generation: Optional[int] = None
    original_order_id: Optional[str] = None
    reversal_id: Optional[str] = None
    order_role: str = "ENTRY"
    instrument: str = ""
    side: str = ""
    order_type: str = "LIMIT"
    submitted_at: float = 0.0
    last_event_at: float = 0.0
    last_rest_check_at: float = 0.0
    last_market_check_at: float = 0.0
    status: str = "CREATED"
    previous_status: str = ""
    requested_price: Optional[float] = None
    submitted_price: Optional[float] = None
    current_market_price: Optional[float] = None
    requested_quantity: int = 0
    filled_quantity: int = 0
    remaining_quantity: int = 0
    retry_count: int = 0
    reprice_count: int = 0
    timeout_count: int = 0
    last_error: Optional[str] = None
    last_error_code: Optional[str] = None
    # market-trigger state (§10-12)
    trigger_price: Optional[float] = None
    trigger_crossed: bool = False
    trigger_cross_time: float = 0.0
    trigger_cross_price: Optional[float] = None
    # classification + decision
    classification: Optional[str] = None
    decision: Optional[str] = None
    rest_verified: bool = False
    priority: int = P5_NORMAL_ENTRY
    extra: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items()}


class OrderWatcher:
    """Event-driven + REST-verified order state machine for one LIVE env.

    Safe, idempotent, rate-aware.  Re-entrant methods lock internally so the
    WS thread (ingest) and poller thread (scan/verify) can neither corrupt
    records nor double-fire broker actions.
    """

    def __init__(
        self,
        engine=None,
        broker=None,
        config: Optional[dict] = None,
        clock: Optional[Callable[[], float]] = None,
        quote_fn: Optional[Callable[[str], Optional[float]]] = None,
        on_event: Optional[Callable] = None,
        on_failure_event: Optional[Callable] = None,
        blocker_fn: Optional[Callable[[str], Optional[int]]] = None,
        strategy_lookup: Optional[Callable[[str], Any]] = None,
        position_lookup: Optional[Callable[[str, str], Any]] = None,
        preflight_fn: Optional[Callable[[str, "OrderWatchRecord"], Optional[str]]] = None,
        on_fills: Optional[Callable[[list], None]] = None,
    ):
        self._engine = engine
        self._broker = broker
        live_cfg = config.get("live", {}) if isinstance(config, dict) else {}
        self._cfg = live_cfg.get("order_watcher", {}) if isinstance(live_cfg, dict) else {}
        self._clock = clock or time.time
        self._quote_fn = quote_fn or self._default_quote
        self._on_event = on_event or (lambda *_a, **_k: None)
        self._on_failure_event = on_failure_event or (lambda *_a, **_k: None)
        self._lock = threading.RLock()
        self._records: dict[str, OrderWatchRecord] = {}
        self._verify_queue: dict[str, float] = {}  # order_id -> due
        self._blocker_fn = blocker_fn
        self._preflight_fn = preflight_fn
        self._strategy_lookup = strategy_lookup
        self._position_lookup = position_lookup
        self._on_fills = on_fills or (lambda _fills: None)
        self._rate_bucket_ts = 0.0
        self._rate_bucket_used = 0
        self._stats = {
            "ws_events": 0, "ws_applied": 0, "rest_verifies": 0,
            "recoveries": 0, "reprices": 0, "cancels": 0, "market_fallbacks": 0,
        }
        self._tick_cfg = {
            "verify_critical_after_ws_ms": _as_ms(
                _cfg(self._cfg, "verify_critical_after_ws_ms", 0.0), 0.0),
            "reconcile_verify_interval_ms": _as_ms(
                _cfg(self._cfg, "reconcile_verify_interval_ms", 5000.0), 5000.0),
            "trigger_detection": bool(_cfg(self._cfg, "trigger_detection_enabled", True)),
            "market_fallback_enabled": bool(_cfg(self._cfg, "market_fallback_enabled", False)),
            "market_fallback_timeout_ms": _as_ms(
                _cfg(self._cfg, "market_fallback_timeout_ms", 30000.0), 30000.0),
            "max_reprices": int(_cfg(self._cfg, "max_reprices", 3) or 3),
            "reprice_interval_ms": _as_ms(
                _cfg(self._cfg, "reprice_interval_ms", 5000.0), 5000.0),
            "max_order_age_ms": _as_ms(
                _cfg(self._cfg, "max_order_age_ms", 60000.0), 60000.0),
            "max_price_deviation_pct": float(
                _cfg(self._cfg, "max_price_deviation_pct", 0.5) or 0),
            "max_order_ops_per_sec": int(
                _cfg(self._cfg, "max_order_ops_per_sec", 10) or 10),
            "stale_after_ws_ms": _as_ms(
                _cfg(self._cfg, "stale_after_ws_ms", 15000.0), 15000.0),
            # Appendix I (I5) — LIMIT-skip policy: a resting immediate-limit
            # entry is marked for skip (cancel -> REST-verified -> MARKET for
            # the REMAINING qty) as soon as the broker truth is confirmed and
            # one of {age, deviation, trigger-crossed-unfilled} exceeds policy.
            "limit_skip_enabled": bool(_cfg(self._cfg, "limit_skip_policy", {}).get("enabled", False)),
            "skip_max_age_ms": _as_ms(
                _cfg(self._cfg, "limit_skip_policy", {}).get("max_age_ms", 15000.0), 15000.0),
            "skip_max_deviation_pct": float(
                _cfg(self._cfg, "limit_skip_policy", {}).get("max_deviation_pct", 0.5) or 0),
            "skip_trigger_crossed_unfilled_ms": _as_ms(
                _cfg(self._cfg, "limit_skip_policy", {}).get("trigger_crossed_unfilled_ms", 3000.0), 3000.0),
            "skip_cancel_check_ms": _as_ms(
                _cfg(self._cfg, "limit_skip_policy", {}).get("cancel_check_ms", 2000.0), 2000.0),
        }

    # ── helpers ──────────────────────────────────────────────────────────

    def _default_quote(self, instrument: str) -> Optional[float]:
        if self._engine is not None:
            prices = getattr(self._engine, "_current_prices", {}) or {}
            return prices.get(instrument)
        return None

    def _now(self) -> float:
        return self._clock()

    def _rate_allowed(self) -> bool:
        now = self._now()
        window = 1.0
        limit = self._tick_cfg["max_order_ops_per_sec"]
        if now - self._rate_bucket_ts >= window:
            self._rate_bucket_ts = now
            self._rate_bucket_used = 0
        if self._rate_bucket_used >= limit:
            return False
        self._rate_bucket_used += 1
        return True

    # ── record management ────────────────────────────────────────────────

    def register_from_order(self, order) -> OrderWatchRecord:
        oid = order.order_id
        with self._lock:
            rec = self._records.get(oid)
            if rec is None:
                state = getattr(order, "state", None)
                state_str = getattr(state, "value", None) or str(state or "").upper()
                rec = OrderWatchRecord(
                    internal_order_id=oid,
                    broker_order_id=getattr(order, "_broker_order_id", None),
                    correlation_id=getattr(order, "correlation_id", None),
                    strategy_id=getattr(order, "strategy_id", ""),
                    trade_id=getattr(order, "trade_id", None),
                    lifecycle_id=getattr(order, "lifecycle_id", None) or getattr(order, "trade_id", None),
                    signal_id=getattr(order, "entry_signal_id", None),
                    position_id=(getattr(order, "parent_position_id", None)
                                 or getattr(order, "position_id", None)),
                    position_generation=getattr(order, "position_generation", None),
                    original_order_id=getattr(order, "original_order_id", None),
                    reversal_id=getattr(order, "reversal_id", None),
                    order_role=str(getattr(order, "order_role", "ENTRY") or "ENTRY").upper(),
                    instrument=getattr(order, "instrument", ""),
                    side=str(getattr(order, "side", "") or "").upper(),
                    order_type=str(getattr(order, "order_type", "LIMIT") or "LIMIT").upper(),
                    submitted_at=getattr(order, "created_at", self._now()),
                    last_event_at=self._now(),
                    requested_price=getattr(order, "requested_price", None),
                    submitted_price=getattr(order, "price", None),
                    requested_quantity=int(getattr(order, "quantity", 0) or 0),
                    trigger_price=getattr(order, "planned_entry_price", None),
                    priority=_PRIORITY_RANK.get(
                        str(getattr(order, "order_role", "") or "").upper(),
                        P5_NORMAL_ENTRY),
                )
                plan_sl = getattr(order, "planned_sl", None)
                if plan_sl:
                    rec.extra["stop_price"] = float(plan_sl)
                if state_str:
                    rec.status = str(state_str).upper()
                self._records[oid] = rec
            return rec

    def register_from_engine(self) -> list:
        """Reconcile watcher records against the engine's order book (adopts
        orders placed before the watcher started — restart recovery)."""
        engine = self._engine
        if engine is None or not hasattr(engine, "get_fills"):
            return []
        orders = getattr(engine, "_orders", None)
        if not orders:
            return []
        adopted: list = []
        for order in list(orders.values()):
            rec = self.register_from_order(order)
            adopted.append(rec.internal_order_id)
        return adopted

    def record(self, order_id: str) -> Optional[OrderWatchRecord]:
        with self._lock:
            return self._records.get(order_id)

    def all_records(self) -> list:
        with self._lock:
            return [OrderWatchRecord(**{k: v for k, v in r.__dict__.items()})
                    for r in self._records.values()]

    # ── WS FAST PATH (broker_sync._on_ws_record hook) ─────────────────────

    def _sync_from_engine(self, order) -> Optional[OrderWatchRecord]:
        """Refresh a watcher record from the engine's authoritative Order
        object (fills/rejections discovered via REST polls or WS must land in
        the record even when no WS order-alert fires)."""
        oid = getattr(order, "order_id", None)
        if not oid:
            return None
        rec = self.record(oid)
        if rec is None:
            return None
        raw_state = getattr(order, "state", "") or ""
        state = getattr(raw_state, "value", None) or str(raw_state)
        state_upper = str(state).upper()
        broker_oid = getattr(order, "_broker_order_id", None)
        filled = int(getattr(order, "filled_quantity", 0) or 0)
        avg = float(getattr(order, "average_fill_price", 0.0) or 0.0)
        if broker_oid:
            rec.broker_order_id = broker_oid
        rec.filled_quantity = max(rec.filled_quantity, filled)
        rec.lifecycle_id = getattr(order, "lifecycle_id", None) or getattr(order, "trade_id", None)
        rec.position_id = (getattr(order, "parent_position_id", None)
                           or getattr(order, "position_id", None))
        rec.position_generation = getattr(order, "position_generation", None)
        rec.remaining_quantity = max(0, rec.requested_quantity - rec.filled_quantity)
        if avg > 0:
            rec.extra["last_fill_price"] = avg
        if state_upper and state_upper != rec.status:
            rec.previous_status = rec.status
            rec.status = state_upper
            rec.last_event_at = self._now()
            # A fill discovered through this path must still trigger a targeted
            # REST verification (positions/trades) for the audit trail.
            if state_upper in ("FILLED", "PARTIALLY_FILLED", "REJECTED",
                               "CANCELLED", "CANCELED", "EXPIRED"):
                self._queue_verify(oid, now=self._now())
        return rec

    def ingest_ws_record(self, record: dict) -> bool:
        """A WS order alert just arrived: update local state immediately.

        Never mints fills (the engine's authoritative fill path owns that);
        never acts on an unmappable broker order id.
        """
        self._stats["ws_events"] += 1
        bid = str(record.get("broker_order_id") or "")
        if not bid:
            return False
        with self._lock:
            rec = self._find_by_broker_id(bid)
            if rec is None:
                return False
            new_status = str(record.get("status") or "").upper()
            if new_status:
                rec.previous_status = rec.status
                rec.status = new_status if new_status != "" else rec.status
            rec.last_event_at = self._now()
            fq = record.get("filled_quantity")
            if isinstance(fq, int) and fq >= 0:
                rec.filled_quantity = max(rec.filled_quantity, int(fq))
                rec.remaining_quantity = max(
                    0, rec.requested_quantity - rec.filled_quantity)
            price = record.get("average_fill_price")
            if price:
                try:
                    rec.extra["last_fill_price"] = float(price)
                except (TypeError, ValueError):
                    pass
            reason = record.get("reason")
            if reason:
                rec.last_error = reason
            rec.extra["raw_status"] = record.get("raw_status")
            self._stats["ws_applied"] += 1
        # Critical events demand targeted REST verification (§4).
        if new_status in ("FILLED", "PARTIALLY_FILLED", "REJECTED",
                          "CANCELLED", "CANCELED", "EXPIRED", "TRADED"):
            self._queue_verify(rec.internal_order_id, now=self._now(),
                               delay_ms=self._tick_cfg["verify_critical_after_ws_ms"])
        return True

    def _find_by_broker_id(self, bid: str) -> Optional[OrderWatchRecord]:
        for rec in self._records.values():
            if rec.broker_order_id == bid:
                return rec
        return None

    def _queue_verify(self, order_id: str, now: float, delay_ms: float = 0.0) -> None:
        self._verify_queue[order_id] = now + (delay_ms / 1000.0)

    # ── MARKET TRIGGER MONITOR (§10-12) ─────────────────────────────────

    def feed_market(self, instrument: str, ltp: Optional[float]) -> None:
        """Tick/LTP update from the market feed.  Detects trigger crossings
        immediately (not on the next candle)."""
        if ltp is None:
            return
        with self._lock:
            now = self._now()
            for rec in self._records.values():
                if rec.instrument != instrument or not rec.trigger_price:
                    continue
                rec.current_market_price = ltp
                rec.last_market_check_at = now
                if self._tick_cfg["trigger_detection"] and not rec.trigger_crossed:
                    crossed = self._trigger_crossed(rec.side, ltp, rec.trigger_price)
                    if crossed:
                        rec.trigger_crossed = True
                        rec.trigger_cross_time = now
                        rec.trigger_cross_price = ltp

    @staticmethod
    def _trigger_crossed(side: str, ltp: float, trigger: float) -> bool:
        if side == "BUY":
            return ltp >= trigger
        if side == "SELL":
            return ltp <= trigger
        return False

    # ── TARGETED REST VERIFICATION (§4) ─────────────────────────────────

    def verify_order(self, order_id: str, force: bool = False) -> dict:
        """Authoritative check: GET /orders/{id} -> trades -> positions.

        Idempotent, dedup'd downstream by the engine's apply_broker_statuses.
        Returns a summary dict of what was learned.
        """
        rec = self.record(order_id)
        if rec is None:
            return {"verification": "not_tracked", "order_id": order_id}
        broker = self._broker
        engine = self._engine
        if broker is None or not hasattr(broker, "order_statuses"):
            return {"verification": "unsupported", "order_id": order_id}
        now = self._now()
        if not force:
            due = self._verify_queue.pop(order_id, None)
            if due is not None and now < due:
                self._verify_queue[order_id] = due
                return {"verification": "deferred", "order_id": order_id}
            if rec.status in _FINAL_STATES and rec.rest_verified:
                return {"verification": "already_final", "order_id": order_id}
        if not self._rate_allowed():
            return {"verification": "rate_limited", "order_id": order_id}
        self._stats["rest_verifies"] += 1
        summary: dict = {"order_id": order_id, "rest_verified": True}
        # 1) GET /orders/{id} (authoritative status + filled qty).
        try:
            statuses = broker.order_statuses()
            if isinstance(statuses, dict) and rec.broker_order_id in statuses:
                rec.extra["rest_status_body"] = statuses[rec.broker_order_id]
            applied = 0
            new_fills: list = []
            if engine is not None and hasattr(engine, "apply_broker_statuses") and statuses:
                new_fills = engine.apply_broker_statuses(statuses or {}) or []
                applied = len(new_fills)
            summary["fills_applied"] = applied
            # Route freshly-minted fills into the strategy lifecycle NOW so the
            # post-fill SL compare and position accounting run immediately, not
            # at the next 2 s order poll.  The engine fill path is idempotent
            # (broker-fill ledger dedup), so re-entrance from the WS thread is
            # safe.
            if new_fills:
                rec.extra["last_verify_fills"] = len(new_fills)
                try:
                    self._on_fills(new_fills)
                except Exception as e:
                    log.error("OrderWatcher on_fills route failed: %s", e)
            # A successful status read is authoritative evidence we queried the
            # broker (the §13 race gate: "send nothing until REST checked").
            rec.rest_verified = True
        except Exception as e:
            summary["order_status_error"] = str(e)
        # 2) GET /trades/{order-id} when execution is involved.
        if rec.filled_quantity > 0 and hasattr(broker, "order_trades"):
            try:
                trades = broker.order_trades(rec.broker_order_id) or []
                summary["trades"] = trades
                rec.extra["trade_rows"] = trades
            except Exception as e:
                summary["trades_error"] = str(e)
        # 3) GET /positions when this order could have hit.
        if rec.status in ("FILLED", "PARTIALLY_FILLED") and hasattr(broker, "positions"):
            try:
                positions = broker.positions() or []
                match = [p for p in positions
                         if p.get("strategy_id") == rec.strategy_id
                         and p.get("instrument") == rec.instrument]
                rec.extra["position_rows"] = match
                summary["positions"] = match
            except Exception as e:
                summary["positions_error"] = str(e)
        rec.last_rest_check_at = self._now()
        status = str(rec.status).upper()
        if status in _FINAL_STATES:
            rec.rest_verified = True
        return summary

    # ── CLASSIFICATION (§9 A..K) ────────────────────────────────────────

    def classify(self, order_id: str, now: float) -> str:
        """Map the observed broker + market + intent state into A..K."""
        rec = self.record(order_id)
        if rec is None:
            return ORDER_UNKNOWN
        status = str(rec.status).upper()
        age_ms = (now - rec.submitted_at) * 1000.0 if rec.submitted_at else 0.0
        # Appendix I (I5) — when the limit-skip policy is active the resting
        # entry is evaluated against the policy's age budget (faster skip)
        # rather than the generic max-order-age (which paces reconciliation).
        age_budget = (self._tick_cfg["skip_max_age_ms"]
                      if self._tick_cfg["limit_skip_enabled"]
                      else self._tick_cfg["max_order_age_ms"])
        if status == "REJECTED" or "REJECT" in status:
            return ORDER_REJECTED
        if status in ("CANCELLED", "CANCELED"):
            return ORDER_CANCELLED
        if status == "EXPIRED":
            return ORDER_EXPIRED
        if status == "PARTIALLY_FILLED" or (
                rec.filled_quantity > 0 and rec.remaining_quantity > 0):
            return PARTIAL_FILL
        if status == "FILLED" or (rec.filled_quantity > 0 and rec.remaining_quantity == 0):
            return "A_FILLED"
        if status in ("UNKNOWN", "CREATED") or (
                status == "SUBMITTED" and rec.broker_order_id is None):
            return ORDER_UNKNOWN
        # status == SUBMITTED (resting at broker) — classify pending.
        if rec.trigger_crossed:
            if age_ms > self._tick_cfg["max_order_age_ms"]:
                return SIGNAL_EXPIRED
            return TRIGGER_CROSSED_NOT_FILLED
        if age_ms > self._tick_cfg["max_order_age_ms"]:
            return PENDING_AND_AGING
        if age_ms > age_budget:
            # INSIDE the general reconcile age but PAST the skip-policy age:
            # the resting limit is a skip candidate (I5) once broker truth is
            # REST-verified in the decision step.
            return PENDING_AND_AGING
        market = rec.current_market_price
        requested = rec.requested_price or rec.submitted_price
        if market is not None and requested:
            deviation = abs(market - requested) / requested * 100.0
            if deviation > self._tick_cfg["max_price_deviation_pct"]:
                return PENDING_MARKET_MOVED_AWAY
        return PENDING_BUT_VALID

    # ── RECOVERY DECISION TREE (§14) + priorities (§7) ──────────────────

    def scan(self, now: Optional[float] = None) -> list:
        """One watcher pass.  Call from the poller's orders cycle AFTER
        apply_broker_statuses, or after any WS burst.

        Returns the list of recovery actions taken/decided.
        """
        now = now or self._now()
        if self._engine is None:
            return []
        self.register_from_engine()
        actions: list = []
        with self._lock:
            order_ids = list(self._records.keys())
        for order_id in order_ids:
            rec = self.record(order_id)
            if rec is None:
                continue
            # Sync authoritative engine Order state into the watch record first
            # so REST/WS discoveries (fills, rejections) are never stale.
            order_obj = (getattr(self._engine, "_orders", {}) or {}).get(order_id)
            if order_obj is not None:
                self._sync_from_engine(order_obj)
            # The watcher owns ENTRY/REVERSAL_ENTRY orders AND the exit family
            # (EXIT/REVERSAL_EXIT/EMERGENCY_EXIT) so a resting exit can be
            # REST-verified, re-priced and market-fallback-completed — a stale
            # exit must never leave a position open forever.  Protective
            # STOP_LOSS rests are owned by the broker + engine/poller path
            # (they are trigger orders, never to be re-driven here).
            if rec.order_role in ("EXIT", "REVERSAL_EXIT", "EMERGENCY_EXIT"):
                is_exit = True
            elif rec.order_role in ("ENTRY", "REVERSAL_ENTRY"):
                is_exit = False
            else:
                continue
            # Final orders are watched, not re-driven (no blind re-entry).
            if rec.status in ("REJECTED", "CANCELLED", "CANCELED", "EXPIRED"):
                continue
            # A fully-filled order is done: it was REST-verified once (WS/
            # engine-fill discovery + the first scan force-verify) and must
            # never be polled again — the continuous reconciliation loop
            # wastes one GET per settled order per scan otherwise.
            if rec.status == "FILLED" and rec.remaining_quantity <= 0 and rec.rest_verified:
                continue
            # Periodic REST verification (reconciliation; not a polling loop).
            if now - rec.last_rest_check_at > (self._tick_cfg[
                    "reconcile_verify_interval_ms"] / 1000.0):
                self.verify_order(order_id, force=True)
            classification = self.classify(order_id, now)
            rec.classification = classification
            decision = (self._decide_exit(rec, classification, now)
                        if is_exit else self._decide(rec, classification, now))
            rec.decision = decision
            if decision in ("VERIFY", "REPRICE", "CANCEL", "MARKET_FALLBACK", "LOCK"):
                action = self._apply_decision(rec, decision, now)
                actions.append(action)
        return actions

    def _decide(self, rec: OrderWatchRecord, classification: str, now: float) -> str:
        """§14 decision tree.  Returns WAIT / VERIFY / REPRICE / CANCEL / LOCK /
        MARKET_FALLBACK."""
        # P0..P4 gate: exits, SL and reversal leg must outrank an entry.
        blocker = self._highest_priority_blocker(rec)
        if blocker is not None:
            return "LOCK"
        if classification in (ORDER_REJECTED, ORDER_CANCELLED, ORDER_EXPIRED,
                              SIGNAL_EXPIRED):
            # Terminal state that must not be re-driven blindly (never a
            # blind LIMIT->MARKET duplicate).
            return "CANCEL"
        if classification == ORDER_UNKNOWN:
            return "VERIFY"
        if classification == PARTIAL_FILL:
            # A partial fill leaves a REMAINDER at the broker.  A broker-side
            # trigger (STOP_LOSS) keeps working untouched; anything else is
            # completed via the safe fallback (cancel -> verify -> MARKET for
            # the REMAINING qty only) once the fallback timeout has elapsed.
            if rec.order_type == "STOP_LOSS":
                return "WAIT"
            if not rec.rest_verified:
                return "VERIFY"
            if self._market_fallback_eligible(rec, now):
                return "MARKET_FALLBACK"
            return "WAIT"
        if classification == "A_FILLED":
            return "WAIT"  # fills flow through engine; nothing to do here
        if classification == PENDING_BUT_VALID:
            return "WAIT"
        # Broker-side trigger entries (STOP_LOSS stop-limits) REST at the broker
        # waiting for their breakout trigger.  Aging / deviation must NEVER
        # cancel them or escalate to MARKET — that would enter a position
        # BEFORE a breakout that may not come.  The broker's own trigger owns
        # activation; escalation only applies once the trigger has crossed and
        # the activated limit failed to fill (TRIGGER_CROSSED_NOT_FILLED).
        if rec.order_type == "STOP_LOSS":
            if classification == TRIGGER_CROSSED_NOT_FILLED:
                if not rec.rest_verified:
                    return "VERIFY"
                if rec.status in ("FILLED", "PARTIALLY_FILLED"):
                    return "WAIT"
                if self._market_fallback_eligible(rec, now):
                    return "MARKET_FALLBACK"
                if self._skip_eligible(rec, now, reason="trigger_crossed"):
                    return "MARKET_FALLBACK"
                # Neither market fallback nor skip is eligible but the trigger
                # has crossed and the order is still resting.  Cancel the
                # stale stop-limit so the strategy can re-arm on the next bar
                # instead of waiting indefinitely.
                return "CANCEL"
            return "WAIT"
        if classification == PENDING_AND_AGING:
            # §14 + Appendix I (I5) — before ANY escalation the broker truth
            # must be REST-verified (no blind LIMIT->MARKET).  Once verified,
            # the limit-skip policy escalates a resting entry that has aged
            # beyond policy to MARKET for the REMAINING qty.
            if not rec.rest_verified:
                return "VERIFY"
            if rec.status in ("FILLED", "PARTIALLY_FILLED"):
                return "WAIT"
            if self._skip_eligible(rec, now, reason="aging"):
                return "MARKET_FALLBACK"
            return "VERIFY"
        if classification == PENDING_MARKET_MOVED_AWAY:
            # §14 + Appendix I (I5) — deviation beyond policy + REST-verified:
            # skip the resting LIMIT (cancel -> verify -> MARKET remaining).
            if not rec.rest_verified:
                return "VERIFY"
            if rec.status in ("FILLED", "PARTIALLY_FILLED"):
                return "WAIT"
            if self._skip_eligible(rec, now, reason="deviation"):
                return "MARKET_FALLBACK"
            return "VERIFY"
        if classification == TRIGGER_CROSSED_NOT_FILLED:
            # §13 race: NEVER send MARKET because LTP crossed the trigger.
            # RESOLVE (REST) the broker order first; only when still PENDING
            # do we consider the decision tree below — never blind.
            if not rec.rest_verified:
                return "VERIFY"
            if rec.status in ("FILLED", "PARTIALLY_FILLED"):
                return "WAIT"
            if self._market_fallback_eligible(rec, now):
                return "MARKET_FALLBACK"
            if self._skip_eligible(rec, now, reason="trigger_crossed"):
                return "MARKET_FALLBACK"
            if self._can_reprice(rec):
                return "REPRICE"
            return "CANCEL"
        return "WAIT"

    def _decide_exit(self, rec: OrderWatchRecord, classification: str,
                     now: float) -> str:
        """Decision tree for exit-family orders (EXIT/REVERSAL_EXIT/
        EMERGENCY_EXIT).

        Exits outrank every entry (P0..P4 priority model) so they are NEVER
        LOCKed by the entry blocker — a stale exit must be completed, not
        deferred.  The tree keeps the same hard safety rules as entries:

        * REST-verify before any escalation (no blind LIMIT->MARKET).
        * MARKET fallback only after the REST-confirmed cancel of the resting
          remainder, for the REMAINING quantity, through the engine.
        * Terminal states (REJECTED/EXPIRED/CANCELLED) are never re-driven.
        * Protective STOP_LOSS trigger rests are never touched here.
        """
        if rec.order_type == "STOP_LOSS":
            return "WAIT"
        if classification in (ORDER_REJECTED, ORDER_CANCELLED, ORDER_EXPIRED,
                              SIGNAL_EXPIRED):
            # Terminal: engine/strategy lifecycle owns the aftermath.  Never a
            # blind re-close (a cancelled exit may already be effectively flat).
            return "WAIT"
        if classification == ORDER_UNKNOWN:
            return "VERIFY"
        if classification in (PARTIAL_FILL, "A_FILLED"):
            if not rec.rest_verified:
                return "VERIFY"
            if self._market_fallback_eligible(rec, now):
                return "MARKET_FALLBACK"
            return "WAIT"
        if classification == PENDING_BUT_VALID:
            return "VERIFY" if not rec.rest_verified else "WAIT"
        if classification in (PENDING_AND_AGING, PENDING_MARKET_MOVED_AWAY,
                              TRIGGER_CROSSED_NOT_FILLED):
            # A resting exit that aged past policy must complete the close:
            # escalate through cancel -> REST-verify -> MARKET remaining.
            if not rec.rest_verified:
                return "VERIFY"
            if rec.status in ("FILLED", "PARTIALLY_FILLED"):
                return "WAIT"
            if self._market_fallback_eligible(rec, now):
                return "MARKET_FALLBACK"
            if self._can_reprice(rec):
                return "REPRICE"
            return "VERIFY"
        return "WAIT"

    def _skip_eligible(self, rec: OrderWatchRecord, now: float,
                       reason: str) -> bool:
        """Appendix I (I5) — is this resting limit eligible for the skip
        (cancel -> verify -> MARKET remaining) policy?"""
        if not self._tick_cfg["limit_skip_enabled"]:
            return False
        if not self._tick_cfg["market_fallback_enabled"]:
            return False
        if rec.status in ("FILLED", "PARTIALLY_FILLED", "REJECTED", "CANCELLED",
                          "CANCELED", "EXPIRED"):
            return False
        if not rec.broker_order_id:
            return False
        age_ms = (now - rec.submitted_at) * 1000.0 if rec.submitted_at else 0.0
        if reason == "aging":
            if age_ms < self._tick_cfg["skip_max_age_ms"]:
                return False
            # §I5 — the aging branch must NOT outrun the global MARKET-fallback
            # timeout: a resting entry that has aged past the skip budget but
            # has NOT yet reached market_fallback_timeout_ms stays VERIFY
            # (re-checked), never escalates early.  Config defaults
            # (skip 15000ms < fallback 30000ms) keep the skip window meaningful
            # and the fallback window authoritative.
            if age_ms < self._tick_cfg["market_fallback_timeout_ms"]:
                return False
            return True
        if reason == "deviation":
            market = rec.current_market_price
            requested = rec.requested_price or rec.submitted_price
            if market is None or not requested:
                return False
            deviation = abs(market - requested) / requested * 100.0
            if deviation < self._tick_cfg["skip_max_deviation_pct"]:
                return False
            if age_ms < self._tick_cfg["skip_max_age_ms"]:
                return False
            return True
        if reason == "trigger_crossed":
            if not rec.trigger_crossed:
                return False
            crossed_at = rec.trigger_cross_time or now
            if (now - crossed_at) * 1000.0 < self._tick_cfg["skip_trigger_crossed_unfilled_ms"]:
                return False
            return True
        return False

    def _highest_priority_blocker(self, rec: OrderWatchRecord) -> Optional[int]:
        """Any P0-P4 condition must complete before this entry proceeds."""
        if self._blocker_fn is not None:
            try:
                return self._blocker_fn(rec.strategy_id, rec)
            except Exception:  # pragma: no cover - defensive
                pass
        engine = self._engine
        if engine is None:
            return None
        # In-flight exit / reversal / emergency legs for the same strategy.
        for oid, o in list(getattr(engine, "_orders", {}).items()):
            role = str(getattr(o, "order_role", "") or "").upper()
            if role not in ("EXIT", "REVERSAL_EXIT", "EMERGENCY_EXIT"):
                continue
            if getattr(o, "strategy_id", "") != rec.strategy_id:
                continue
            # Never lock an order on its OWN leg (self-lock fix).
            if getattr(o, "order_id", None) == rec.internal_order_id:
                continue
            state = str(getattr(o, "state", "") or "").lower()
            if state not in ("filled", "canceled", "cancelled", "rejected"):
                return _PRIORITY_RANK.get(role, P2_EXIT)
        return None

    def _can_reprice(self, rec: OrderWatchRecord) -> bool:
        if rec.reprice_count >= self._tick_cfg["max_reprices"]:
            return False
        if rec.order_type not in ("LIMIT",):
            return False
        if not rec.broker_order_id:
            return False
        return self._rate_allowed()

    def _reprice_interval_ok(self, rec: OrderWatchRecord, now: float) -> bool:
        if rec.extra.get("last_reprice_at"):
            if (now - rec.extra["last_reprice_at"]) * 1000.0 < \
                    self._tick_cfg["reprice_interval_ms"]:
                return False
        return True

    def _market_fallback_eligible(self, rec: OrderWatchRecord, now: float) -> bool:
        # broker_order_id is required so the fallback can cancel + verify before
        # placing MARKET; without it the fallback is blocked (I8/I9).
        return bool(
            self._tick_cfg["market_fallback_enabled"]
            and rec.broker_order_id
            and (now - rec.submitted_at) * 1000.0 >=
            self._tick_cfg["market_fallback_timeout_ms"]
        )

    def _apply_decision(self, rec: OrderWatchRecord, decision: str,
                        now: float) -> dict:
        if decision == "LOCK":
            result = {"order_id": rec.internal_order_id,
                      "decision": "LOCK",
                      "reason": "higher_priority_blocked"}
        elif decision == "VERIFY":
            if not self._rate_allowed():
                result = {"order_id": rec.internal_order_id,
                          "decision": "WAIT", "reason": "rate_limited"}
            else:
                summary = self.verify_order(rec.internal_order_id, force=True)
                result = {"order_id": rec.internal_order_id,
                          "decision": "VERIFY", **summary}
        elif decision == "REPRICE":
            result = self._do_reprice(rec, now)
        elif decision == "CANCEL":
            result = self._do_cancel(rec, now)
        elif decision == "MARKET_FALLBACK":
            result = self._do_market_fallback(rec, now)
        else:
            result = {"order_id": rec.internal_order_id, "decision": "WAIT"}
        self._stats["recoveries"] += 1
        return result

    # ── recovery actions ─────────────────────────────────────────────────

    def _do_reprice(self, rec: OrderWatchRecord, now: float) -> dict:
        """Modify the resting LIMIT toward the market (PUT /orders/{id})."""
        broker = self._broker
        if broker is None or not hasattr(broker, "modify_order"):
            return {"order_id": rec.internal_order_id, "decision": "REPRICE",
                    "ok": False, "error": "modify_unsupported"}
        if not self._reprice_interval_ok(rec, now):
            return {"order_id": rec.internal_order_id, "decision": "WAIT",
                    "reason": "reprice_interval"}
        side = rec.side
        market = rec.current_market_price or self._quote_fn(rec.instrument)
        current = rec.submitted_price or rec.requested_price
        if market is None or current is None:
            return {"order_id": rec.internal_order_id, "decision": "WAIT",
                    "reason": "no_market_or_price"}
        # A resting BUY limit is unmarketable when limit < market (raise it
        # toward the market); a SELL limit is unmarketable when limit > market
        # (lower it toward the market).  Aggressive-limit reprice targets the
        # market price itself — the order stays a LIMIT (never a MARKET type),
        # so an adverse fill cannot slip past the limit price.
        if side == "BUY":
            new_price = max(current, market) if market >= current else current
        elif side == "SELL":
            new_price = min(current, market) if market <= current else current
        else:
            new_price = market
        try:
            out = broker.modify_order(
                broker_order_id=rec.broker_order_id, order_type="LIMIT",
                price=new_price, quantity=0, validity="DAY")
        except Exception as e:
            rec.last_error = str(e)
            rec.last_error_code = getattr(e, "code", None)
            return {"order_id": rec.internal_order_id, "decision": "REPRICE",
                    "ok": False, "error": str(e)}
        rec.reprice_count += 1
        rec.submitted_price = new_price
        rec.extra["last_reprice_at"] = now
        rec.last_event_at = now
        self._stats["reprices"] += 1
        self._publish("entry_reprice", rec, out)
        if not out.get("ok"):
            self._fail_event("ENTRY_REPRICE_FAILED", rec, out)
        if rec.reprice_count >= self._tick_cfg["max_reprices"]:
            rec.decision = "MARKET_FALLBACK" if self._tick_cfg[
                "market_fallback_enabled"] else "CANCEL"
        return {"order_id": rec.internal_order_id, "decision": "REPRICE",
                "ok": bool(out.get("ok")), "price": new_price}

    def _do_cancel(self, rec: OrderWatchRecord, now: float) -> dict:
        """Cancel the resting order via the engine (DELETE /orders/{id}),
        keeping the engine book and broker state in sync."""
        engine = self._engine
        if engine is None or not hasattr(engine, "cancel_order"):
            return {"order_id": rec.internal_order_id, "decision": "CANCEL",
                    "ok": False, "error": "no_engine"}
        try:
            ok = bool(engine.cancel_order(rec.internal_order_id))
        except Exception as e:
            rec.last_error = str(e)
            return {"order_id": rec.internal_order_id, "decision": "CANCEL",
                    "ok": False, "error": str(e)}
        rec.last_event_at = now
        self._stats["cancels"] += 1
        self._publish("entry_cancel", rec, {"ok": ok})
        return {"order_id": rec.internal_order_id, "decision": "CANCEL",
                "ok": ok}

    def _do_market_fallback(self, rec: OrderWatchRecord, now: float) -> dict:
        """SAFE market fallback: CANCEL the resting LIMIT, verify the cancel
        landed with the broker (REST), THEN place a MARKET entry for the
        REMAINING quantity via the engine.  Only reachable through the
        decision tree when explicitly configured.

        Appendix I (I8/I9/I11): a MARKET may only be sent after the broker
        confirms CANCELLED (a confirmed fill cancels the fallback); ORDER_UNKNOWN
        blocks MARKET entirely; quantity = requested - filled, never the
        original requested amount."""
        engine = self._engine
        if engine is None:
            return {"order_id": rec.internal_order_id, "decision": "MARKET_FALLBACK",
                    "ok": False, "error": "no_engine"}
        # 0) If the LIMIT already filled in full there is nothing to fall back.
        remaining = max(0, rec.requested_quantity - rec.filled_quantity)
        if remaining <= 0:
            return {"order_id": rec.internal_order_id,
                    "decision": "MARKET_FALLBACK", "ok": False,
                    "error": "no_remaining_quantity"}
        # 0b) A MARKET may NEVER be placed for an order whose broker landing is
        # unconfirmed (no broker_order_id).  Without the id, the resting LIMIT
        # cannot be cancelled or REST-verified first, so placing a MARKET could
        # duplicate a broker-side fill.  Force VERIFY instead — no blind
        # LIMIT->MARKET even through the fallback path.
        if not rec.broker_order_id:
            return {"order_id": rec.internal_order_id,
                    "decision": "MARKET_FALLBACK", "ok": False,
                    "error": "missing_broker_order_id_blocks_market",
                    "status": str(rec.status)}
        # 1) cancel the resting LIMIT (must be confirmed before placing).
        try:
            cancelled = bool(engine.cancel_order(rec.internal_order_id))
        except Exception as e:
            return {"order_id": rec.internal_order_id,
                    "decision": "MARKET_FALLBACK", "ok": False, "error": str(e)}
        if not cancelled:
            return {"order_id": rec.internal_order_id,
                    "decision": "MARKET_FALLBACK", "ok": False,
                    "error": "resting_limit_cancel_failed"}
        # I8 — verify the cancel actually landed (authoritative REST state)
        # BEFORE placing the MARKET.  Race: the LIMIT may have filled
        # between the cancel request and the confirm; a confirmed fill
        # cancels the fallback and lets the fill flow through the engine.
        try:
            summary = self.verify_order(rec.internal_order_id, force=True)
        except Exception as e:
            summary = {"verification": "error", "order_status_error": str(e)}
        # verify_order refreshes the engine book; re-sync the record from it
        # so status/fills observed by the broker are authoritative here.
        order_obj = (getattr(engine, "_orders", {}) or {}).get(rec.internal_order_id)
        if order_obj is not None:
            self._sync_from_engine(order_obj)
        status = str(rec.status).upper()
        filled_now = int(rec.filled_quantity or 0)
        requested = int(rec.requested_quantity or 0)
        if status == "FILLED" or (filled_now > 0 and rec.remaining_quantity == 0):
            return {"order_id": rec.internal_order_id,
                    "decision": "MARKET_FALLBACK", "ok": False,
                    "error": "limit_filled_during_cancel",
                    **summary}
        if status == "PARTIALLY_FILLED":
            if filled_now >= requested:
                return {"order_id": rec.internal_order_id,
                        "decision": "MARKET_FALLBACK", "ok": False,
                        "error": "limit_filled_during_cancel",
                        **summary}
            # A partial fill with the remainder STILL WORKING at the broker
            # means the cancel did NOT land.  NEVER place a MARKET alongside a
            # working remainder (double-fill / overshoot).  Bounded retries let
            # the cancel land and the next scan complete the remainder; beyond
            # the cap the working remainder is left to fill (or expire) at the
            # broker and a failure event records the abandonment.
            attempts = int(rec.extra.get("partial_cancel_attempts", 0)) + 1
            rec.extra["partial_cancel_attempts"] = attempts
            if attempts > 3:
                rec.extra["remainder_abandoned"] = True
                self._fail_event("PARTIAL_REMAINDER_ABANDONED", rec, {
                    "error": "cancel_never_landed", "attempts": attempts,
                    "filled": filled_now, "requested": requested})
            return {"order_id": rec.internal_order_id,
                    "decision": "MARKET_FALLBACK", "ok": False,
                    "error": "partial_remainder_still_working",
                    "attempts": attempts, **summary}
        if status in ("REJECTED", "EXPIRED"):
            return {"order_id": rec.internal_order_id,
                    "decision": "MARKET_FALLBACK", "ok": False,
                    "error": f"limit_{status.lower()}_during_cancel",
                    **summary}
        # I9 — UNKNOWN blocks MARKET: the broker truth is unresolved, a
        # MARKET could duplicate a broker-side fill.
        if status in ("UNKNOWN", "CREATED", "SUBMITTED"):
            return {"order_id": rec.internal_order_id,
                    "decision": "MARKET_FALLBACK", "ok": False,
                    "error": "cancel_unverified_blocks_market",
                    "status": status, **summary}
        # Broker confirms cancelled: proceed with the REMAINING qty.
        remaining = max(0, rec.requested_quantity - rec.filled_quantity)
        if remaining <= 0:
            return {"order_id": rec.internal_order_id,
                    "decision": "MARKET_FALLBACK", "ok": False,
                    "error": "no_remaining_quantity_after_verify",
                    **summary}
        # 2) Pre-flight safety gate (risk / market state / position-vs-role)
        #    wired by the trading engine so a recovery MARKET can never bypass
        #    the checks the normal entry/exit path enforces.
        preflight = None
        if self._preflight_fn is not None:
            try:
                preflight = self._preflight_fn(rec.strategy_id, rec)
            except Exception as e:  # pragma: no cover - defensive
                preflight = f"preflight_error: {e}"
        if preflight:
            return {"order_id": rec.internal_order_id,
                    "decision": "MARKET_FALLBACK", "ok": False,
                    "error": "preflight_blocked",
                    "reason": str(preflight), **summary}
        # 3) place a fresh MARKET entry (remaining qty) through the engine.
        # Safety: verify strategy gate, risk gate, and market status before
        # placing the fallback order to prevent bypassing safety checks.
        strategy = None
        if self._strategy_lookup is not None:
            strategy = self._strategy_lookup(rec.strategy_id)
        elif self._engine is not None:
            strategy = (getattr(self._engine, "_strategies", {})
                        or getattr(getattr(self._engine, "_env_for",
                                           lambda e: None)(rec.strategy_id),
                                   "strategies", {})).get(rec.strategy_id)
        if strategy is not None:
            if not getattr(strategy, "enabled", True):
                return {"order_id": rec.internal_order_id,
                        "decision": "MARKET_FALLBACK", "ok": False,
                        "error": "strategy_disabled", **summary}
        try:
            if self._position_lookup is not None:
                current_position = self._position_lookup(rec.strategy_id, rec.instrument)
                if current_position is not None:
                    if getattr(current_position, "trade_id", None) != rec.trade_id:
                        return {"order_id": rec.internal_order_id,
                                "decision": "MARKET_FALLBACK", "ok": False,
                                "error": "lifecycle_changed_before_fallback", **summary}
                    rec.position_id = current_position.position_id
                    rec.position_generation = current_position.position_generation
            order = self._fresh_market_entry(rec, quantity=remaining)
            new_order = engine.create_order(
                signal=order, multiplier=1.0, trade_id=rec.trade_id or "")
            new_order.order_type = "MARKET"
            new_order.order_role = "FALLBACK_MARKET"
            new_order.lifecycle_id = rec.lifecycle_id or rec.trade_id
            new_order.parent_signal_id = rec.signal_id
            new_order.parent_position_id = rec.position_id
            new_order.position_id = rec.position_id
            new_order.position_generation = rec.position_generation
            new_order.original_order_id = rec.internal_order_id
            engine.submit_order(new_order)
        except Exception as e:
            rec.last_error = str(e)
            self._fail_event("ENTRY_MARKET_FALLBACK_FAILED", rec, {"error": str(e)})
            return {"order_id": rec.internal_order_id,
                    "decision": "MARKET_FALLBACK", "ok": False, "error": str(e)}
        self._stats["market_fallbacks"] += 1
        self._publish("entry_market_fallback", rec, {
            "new_order_id": new_order.order_id,
            "correlation_id": getattr(new_order, "correlation_id", None),
            "quantity": remaining, "prev_filled": int(rec.filled_quantity or 0)})
        return {"order_id": rec.internal_order_id,
                "decision": "MARKET_FALLBACK", "ok": True,
                "market_order_id": new_order.order_id, "quantity": remaining}

    def _fresh_market_entry(self, rec: OrderWatchRecord, quantity: Optional[int] = None):
        """Signal clone carrying the exact strategy intent for a fresh entry.
        I11 — the fallback always uses the REMAINING quantity
        (requested - already-filled), never the original requested amount."""
        from strategies.types import Signal, SignalType
        s_type = SignalType.LONG if rec.side == "BUY" else SignalType.SHORT
        qty = quantity if quantity is not None else max(
            0, rec.requested_quantity - rec.filled_quantity)
        signal = Signal(
            signal_type=s_type,
            instrument=rec.instrument,
            strategy_id=rec.strategy_id,
            timestamp=self._now(),
            trigger_price=rec.trigger_price,
            stop_price=rec.extra.get("stop_price", 0.0) or 0.0,
            quantity=qty,
            metadata={
                "market_fallback": True,
                "prev_order_id": rec.internal_order_id,
                "prev_correlation_id": rec.correlation_id,
                "original_order_id": rec.internal_order_id,
            },
        )
        signal.signal_id = rec.signal_id or signal.signal_id
        signal.lifecycle_id = rec.lifecycle_id or rec.trade_id
        signal.parent_position_id = rec.position_id
        signal.position_generation = rec.position_generation
        return signal

    # ── events / durability ──────────────────────────────────────────────

    def _publish(self, kind: str, rec: OrderWatchRecord, details: dict) -> None:
        try:
            self._on_event(kind, {
                "order_id": rec.internal_order_id,
                "strategy_id": rec.strategy_id,
                "instrument": rec.instrument,
                "broker_order_id": rec.broker_order_id,
                "details": details,
            })
        except Exception:  # pragma: no cover - defensive
            pass

    def _fail_event(self, kind: str, rec: OrderWatchRecord, details: dict) -> None:
        try:
            self._on_failure_event({
                "event_id": f"WAT-{uuid.uuid4().hex[:12]}",
                "event_type": kind,
                "strategy_id": rec.strategy_id,
                "trade_id": rec.trade_id,
                "order_id": rec.internal_order_id,
                "broker_order_id": rec.broker_order_id,
                "instrument": rec.instrument,
                "error": str(details.get("error") or details.get("outcome")),
                "action": kind.lower(),
                "final_state": rec.status,
                "details": details,
            })
        except Exception:  # pragma: no cover - defensive
            pass

    # ── diagnostics ──────────────────────────────────────────────────────

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "enabled": bool(self._tick_cfg["trigger_detection"]),
                "stats": dict(self._stats),
                "tick": dict(self._tick_cfg),
                "orders": [r.to_dict() for r in self._records.values()],
                "verify_queue": dict(self._verify_queue),
            }

    def stats(self) -> dict:
        with self._lock:
            return dict(self._stats)


def _as_ms(value, default_ms: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default_ms
