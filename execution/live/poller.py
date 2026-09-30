"""Phase 2 — LIVE broker poller + reconciliation worker.

One background worker per LIVE environment.  It bridges the broker's truth into
the live engine without ever trusting a simulated price::

    order poll     (order_poll_interval_seconds)    reconcile in-flight broker
                                                   order statuses -> engine
                                                   fills, routed through the
                                                   env broker router into the
                                                   strategy lifecycle.
    position poll  (position_poll_interval_seconds) pull authoritative broker
                                                   positions; surface and record
                                                   internal-vs-broker side/qty
                                                   mismatches (never opens or
                                                   reverses anything on poller
                                                   authority).
    account poll   (pnl_poll_interval_seconds)      snapshot broker account
                                                   status for the dashboard /
                                                   reconcile surface.
    reconcile      (reconcile_interval_seconds)     run the engine's strategy-
                                                   state vs position reconcile
                                                   (heals the restart desync).

Every cycle is idempotent, fully exception-contained (a broker hiccup must never
kill the worker), and safe to call directly from tests via :meth:`poll_once`.
"""
from __future__ import annotations

import logging
import inspect
import threading
import time
from contextlib import nullcontext
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from config import as_dict
from strategies.types import StrategyState

log = logging.getLogger(__name__)

_TASKS = ("orders", "positions", "account", "reconcile", "tradebook")
_DEFAULT_INTERVAL = 0.5

# Exit-family order roles: an order whose whole purpose is to REDUCE or close
# exposure.  These are the only roles a MARKET order is ever allowed to serve
# (a MARKET *entry* stays banned at the engine).
_EXIT_ROLES = ("EXIT", "REVERSAL_EXIT", "EMERGENCY_EXIT")
# Terminal broker states that mean "this order is finished at the broker".
_TERMINAL_ORDER_STATES = ("rejected", "canceled", "cancelled", "expired")


def _reset_strategy_if_owned(reset_fn, strategy_id, *, signal_id=None,
                             trade_id=None) -> None:
    """Call ownership-aware reset callbacks without masking callback errors."""
    try:
        inspect.signature(reset_fn).bind(
            strategy_id, signal_id=signal_id, trade_id=trade_id)
    except (TypeError, ValueError):
        reset_fn(strategy_id)
    else:
        reset_fn(strategy_id, signal_id=signal_id, trade_id=trade_id)


def _terminal_entry_matches_pending(strategy, order) -> bool:
    """Whether a terminal entry order owns the strategy's current trigger.

    Historical rejected orders remain in the restored execution book. Their
    cleanup must never reset a newer, separately armed or just-fired signal.
    ``on_tick`` clears ``pending_entry`` before SignalFlow submits the fired
    signal, so a missing pending entry is not evidence that every historical
    terminal order owns the strategy.
    """
    pending = getattr(strategy, "pending_entry", None)
    order_signal_id = (getattr(order, "entry_signal_id", None)
                       or getattr(order, "parent_signal_id", None))
    if pending is not None:
        pending_signal_id = getattr(
            getattr(pending, "signal", None), "signal_id", None)
        return bool(pending_signal_id and order_signal_id
                    and str(pending_signal_id) == str(order_signal_id))

    # The trigger has fired and pending_entry is intentionally None during
    # the tick -> order-submit handoff. Keep ownership on the fired signal id
    # so a 500 ms poll of an older rejected order cannot clear its permit.
    fired_signal_id = getattr(strategy, "_last_fired_trigger_signal_id", None)
    if fired_signal_id:
        return bool(order_signal_id
                    and str(order_signal_id) == str(fired_signal_id))

    # After restart, the fired-id cache may not be present. The current trade
    # id is the durable fallback for an entry already being tracked; never
    # treat missing lineage as ownership.
    current_trade_id = getattr(strategy, "current_trade_id", None)
    order_trade_id = getattr(order, "trade_id", None)
    return bool(current_trade_id and order_trade_id
                and str(current_trade_id) == str(order_trade_id))


def _has_active_market_fallback_child(engine_orders: dict, order) -> bool:
    """A canceled entry LIMIT is not terminal while its MARKET child is live."""
    parent_id = getattr(order, "order_id", None)
    if not parent_id:
        return False
    active = {"created", "submitted", "acknowledged", "partially_filled"}
    return any(
        getattr(candidate, "original_order_id", None) == parent_id
        and str(getattr(candidate, "order_type", "") or "").upper() == "MARKET"
        and str(getattr(candidate, "order_role", "") or "").upper()
            in {"FALLBACK_MARKET", "ENTRY", "REVERSAL_ENTRY"}
        and str(getattr(getattr(candidate, "state", None), "value",
                        getattr(candidate, "state", "")) or "").lower() in active
        for candidate in (engine_orders or {}).values())


def _has_market_fallback_child(engine_orders: dict, order) -> bool:
    """A canceled parent LIMIT is not a failed exit if its fallback exists."""
    parent_id = getattr(order, "order_id", None)
    if not parent_id:
        return False
    return any(
        getattr(candidate, "original_order_id", None) == parent_id
        and str(getattr(candidate, "order_type", "") or "").upper() == "MARKET"
        and str(getattr(candidate, "order_role", "") or "").upper()
            in {"EXIT", "REVERSAL_EXIT", "EMERGENCY_EXIT", "FALLBACK_MARKET"}
        for candidate in (engine_orders or {}).values())


def _is_live(env) -> bool:
    """True when the env is a LIVE broker-backed environment."""
    try:
        return bool(env.is_live) or str(getattr(env, "mode", "") or "").upper() == "LIVE"
    except Exception:
        return False


def _push_broker_account(env, acct: dict) -> None:
    """Adopt broker-reported totals into the env's reporting account engine
    (F1 remediation).  Never touches the per-strategy risk account engines."""
    try:
        ae = getattr(env, "account_engine", None)
        if ae is None or not hasattr(ae, "set_broker_reported"):
            return
        if not acct.get("equity") and not acct.get("available_margin"):
            return
        ae.set_broker_reported(
            equity=acct.get("equity"),
            used_margin=acct.get("used_margin"),
            available_margin=acct.get("available_margin"),
        )
    except Exception:
        pass  # account reporting must never break the polling cycle


def _interval(config: dict, key: str, default: float = _DEFAULT_INTERVAL) -> float:
    try:
        return max(0.25, float(config.get(key, default)))
    except (TypeError, ValueError):
        return default


class LiveBrokerPoller:
    """Interval-driven poller for one LIVE environment."""

    def __init__(
        self,
        env,
        config: Optional[dict] = None,
        clock: Optional[callable] = None,
        handle_fill: Optional[Callable] = None,
        on_reconcile: Optional[Callable] = None,
        reset_strategy_fn: Optional[Callable] = None,
        state_lock=None,
        wire_now: bool = False,
    ):
        self.env = env
        self.config = config or {}
        self._clock = clock or time.time
        # handle_fill(fill, signal_id, is_exit) — env-scoped lifecycle handler
        # (the engine's _handle_fill wrapped with the env name).
        self._handle_fill = handle_fill
        # on_reconcile() — engine.strategy-state vs position reconcile.
        self._on_reconcile = on_reconcile
        # reset_strategy_fn(strategy_id) — resets strategy state to FLAT
        # (TradingEngine._reset_strategy_state wrapped with env lock).
        self._reset_strategy_fn = reset_strategy_fn
        self._state_lock = state_lock or threading.RLock()

        live_cfg = as_dict(config).get("live", {}) or {}
        self.intervals = {
            "orders": _interval(live_cfg, "order_poll_interval_seconds", 0.5),
            "positions": _interval(live_cfg, "position_poll_interval_seconds", _DEFAULT_INTERVAL),
            "account": _interval(live_cfg, "pnl_poll_interval_seconds", _DEFAULT_INTERVAL),
            # Broker-flat reconciliation also releases a manually closed
            # position's strategy state and any reversal waiting for flat.
            # Keep this faster than the market/account snapshots; 0.5s stays
            # well inside Dhan's documented non-trading REST request limit.
            "reconcile": _interval(live_cfg, "reconcile_interval_seconds", 0.5),
            # Daily execution reconciliation is read-only and deliberately
            # slower than order/position polling to stay within broker limits.
            "tradebook": _interval(
                live_cfg, "tradebook_reconcile_interval_seconds", 30.0),
        }
        self.max_retries = int(live_cfg.get("max_retries", 3) or 3)
        self.retry_backoff = float(live_cfg.get("retry_backoff_seconds", 1.0) or 1.0)

        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()
        self._next_run: dict[str, float] = {t: self._clock() for t in _TASKS}
        self._last_run: dict[str, float] = {}
        self._errors: dict[str, int] = {t: 0 for t in _TASKS}
        self._errors.setdefault("sl_verify", 0)
        self._stats: dict[str, int] = {
            "orders_polled": 0, "positions_polled": 0, "accounts_polled": 0,
            "reconciles_run": 0, "tradebook_polls": 0,
            "fills_created": 0, "fills_routed": 0,
            "order_persist_errors": 0, "sl_reconciled": 0,
            "pending_terminalized": 0,
        }
        self._last_broker_positions: list[dict] = []
        self._last_position_report: list[dict] = []
        self._last_account: dict = {}
        self._last_tradebook_reconciliation: dict = {
            "status": "NOT_CHECKED", "mismatch_count": 0, "mismatches": []}

        if wire_now:
            # Prime every task for immediate execution on start (tests and
            # restart bootstrap want a first pass without waiting an interval).
            for t in _TASKS:
                self._next_run[t] = 0.0

    # ── lifecycle ─────────────────────────────────────────────────────

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run, name=f"live-poller-{self.env.name}",
                                        daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 3.0) -> None:
        self._running = False
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=timeout)
        self._thread = None

    @property
    def running(self) -> bool:
        return self._running

    # ── main loop ─────────────────────────────────────────────────────

    def _run(self) -> None:
        while self._running:
            now = self._clock()
            for task in _TASKS:
                # Avoid an extra 50 ms sleep when binary float rounding puts
                # an otherwise-due half-second poll infinitesimally ahead of
                # the clock value.
                if now + 1e-9 >= self._next_run.get(task, 0.0):
                    try:
                        self._run_task(task)
                    except Exception as e:  # pragma: no cover - defensive
                        self._errors[task] += 1
                        log.error("[LivePoller:%s] %s cycle failed: %s",
                                  self.env.name, task, e)
                    self._last_run[task] = self._clock()
                    self._next_run[task] = self._last_run[task] + self.intervals[task]
            # Sleep in small slices so stop() responds quickly.
            deadline = self._clock() + 0.25
            while self._running and self._clock() < deadline:
                remaining = deadline - self._clock()
                time.sleep(min(0.05, remaining))

    def _run_task(self, task: str):
        if task == "orders":
            return self.poll_orders()
        if task == "positions":
            return self.poll_positions()
        if task == "account":
            return self.poll_account()
        if task == "reconcile":
            return self.poll_reconcile()
        if task == "tradebook":
            return self.poll_tradebook_reconciliation()
        return None

    def poll_tradebook_reconciliation(self) -> dict:
        """Compare broker-confirmed day executions with durable local fills.

        This is diagnostic only: an unmatched fill is surfaced as a mismatch,
        never silently adopted into an invented trade/signal lineage.
        """
        from execution.live.tradebook_reconciliation import compare_tradebook

        self._stats["tradebook_polls"] += 1
        broker = getattr(self.env, "broker", None)
        transport = getattr(broker, "_transport", None) or broker
        order_reader = getattr(transport, "day_order_book", None)
        trade_reader = getattr(transport, "tradebook", None)
        if not callable(order_reader) or not callable(trade_reader):
            self._last_tradebook_reconciliation = {
                "status": "UNSUPPORTED", "mismatch_count": 0,
                "mismatches": [], "checked_at": self._clock()}
            return self._last_tradebook_reconciliation
        try:
            broker_orders = list(order_reader() or [])
            broker_trades = list(trade_reader() or [])
            persistence = getattr(self.env, "persistence", None)
            local_orders = (persistence.get_orders() if persistence is not None
                            and hasattr(persistence, "get_orders") else [])
            local_fills = (persistence.get_fills() if persistence is not None
                           and hasattr(persistence, "get_fills") else [])
            report = compare_tradebook(
                broker_orders, broker_trades, local_orders, local_fills)
            report["checked_at"] = self._clock()
            report["broker_order_rows"] = len(broker_orders)
            report["broker_trade_rows"] = len(broker_trades)
            self._last_tradebook_reconciliation = report
            self._stats["tradebook_mismatches"] = report["mismatch_count"]
            if report["mismatch_count"]:
                log.error("[LivePoller:%s] broker tradebook/local fill mismatch: %s",
                          self.env.name, report["mismatches"])
            return report
        except Exception as e:
            self._errors["tradebook"] += 1
            self._last_tradebook_reconciliation = {
                "status": "ERROR", "error": str(e), "mismatch_count": 0,
                "mismatches": [], "checked_at": self._clock()}
            log.error("[LivePoller:%s] tradebook reconciliation failed: %s",
                      self.env.name, e)
            return self._last_tradebook_reconciliation

    def _engine_order_snapshot(self, engine) -> dict:
        """Copy the mutable order book under the engine's state lock."""
        with self._state_lock:
            return dict(getattr(engine, "_orders", {}) or {})

    def _position_book_fingerprint(self, position_manager) -> tuple:
        """Fingerprint local ownership around a slow broker positions read."""
        with self._state_lock:
            rows = []
            for position in list(getattr(
                    position_manager, "open_positions", []) or []):
                rows.append((
                    str(getattr(position, "position_id", "")),
                    str(getattr(position, "trade_id", "")),
                    bool(getattr(position, "is_open", False)),
                    str(getattr(position, "instrument", "")),
                    bool(getattr(position, "is_long", False)),
                    int(getattr(position, "quantity", 0) or 0),
                    str(getattr(position, "position_generation", "")),
                ))
            return tuple(sorted(rows))

    # ── single cycles (idempotent; callable directly in tests) ────────

    def poll_orders(self) -> list:
        """Reconcile broker order statuses into engine fills + lifecycle.

        New fills are routed through the environment's broker router with the
        SAME explicit-mapping semantics as the synchronous path; unmappable or
        conflicting events are quarantined by the router, never applied.
        Returns the list of newly routed fills.
        """
        self._stats["orders_polled"] += 1
        broker = getattr(self.env, "broker", None)
        engine = getattr(self.env, "execution_engine", None)
        router = getattr(self.env, "broker_router", None)
        if broker is None or engine is None:
            return []
        if not hasattr(broker, "order_statuses"):
            return []
        try:
            statuses = broker.order_statuses() or {}
        except Exception as e:
            self._errors["orders"] += 1
            log.error("[LivePoller:%s] order_statuses failed: %s", self.env.name, e)
            return []
        # F2 — terminalize durable pending rows whose entry the broker ended
        # without a fill (rejected/cancelled).  Entry-role orders only; exit /
        # reversal-exit orders never own a pending row.  Idempotent: rows
        # already terminal are skipped.  Runs BEFORE the empty-statuses guard
        # because the in-memory broker book is empty right after a restart,
        # and the sweep actively probes each stranded row's broker order.
        engine_orders = self._engine_order_snapshot(engine)
        self._terminalize_pending_from_broker(statuses)
        # F3 — an exit the broker ended WITHOUT closing the position must
        # re-arm the local SL.  Runs BEFORE the empty-statuses guard for the
        # same reason as above: right after a restart the broker status map is
        # empty, yet a DAY-expired exit from the previous session is exactly the
        # order that left a position unprotected.
        self._release_terminal_exits()
        if not statuses:
            return []
        applied: list = []
        # Broker status application mutates Order objects and can create fills;
        # serialize this local state transition with concurrent submit/fill
        # callbacks. Network I/O above remains outside the lock.
        # Keep broker-order state application and the resulting fill-to-position
        # transition in one critical section. Releasing the lock between these
        # steps exposes an intermediate state (broker order FILLED, local
        # position not yet created/closed) to candle and SL callbacks.
        fills_to_persist = []
        with self._state_lock:
            broker_fills = engine.apply_broker_statuses(statuses)
            for fill in broker_fills:
                self._stats["fills_created"] += 1
                routed = False
                if router is not None:
                    try:
                        routed = bool(router.route_fill(
                            fill,
                            lambda f, es, ix: self._route_fill(f, es, is_exit=ix),
                            entry_signal_id=getattr(fill, "entry_signal_id", None),
                        ))
                    except Exception as e:
                        self._errors["orders"] += 1
                        log.error("[LivePoller:%s] fill route failed for %s: %s",
                                  self.env.name, fill.fill_id, e)
                if routed:
                    self._stats["fills_routed"] += 1
                    applied.append(fill)
                fills_to_persist.append(fill)
        for fill in fills_to_persist:
            self._upgrade_dbs_order_row(fill)
        # Phase 6 — persist any order whose state flipped to REJECTED, CANCELLED
        # or EXPIRED during apply_broker_statuses (no fill is produced for
        # those, so _upgrade_dbs_order_row is never reached).  EXPIRED matters:
        # a DAY-validity exit that never filled is the classic "SL order died
        # and the position is still open" case.
        # Dhan-linked state: when an ENTRY order is rejected/cancelled by the
        # broker, reset the strategy state back to FLAT so it is never stuck
        # in ENTRY_TRIGGERED with no Dhan fill to confirm the position.
        engine_orders = self._engine_order_snapshot(engine)
        strategies = getattr(self.env, "strategies", {}) or {}
        for order in list(engine_orders.values()):
            if order.state.value in ("rejected", "canceled", "cancelled", "expired"):
                self._persist_order_state(order)
                role = (getattr(order, "order_role", "") or "").upper()
                if (role in ("ENTRY", "REVERSAL_ENTRY")
                        and _has_active_market_fallback_child(engine_orders, order)):
                    # The LIMIT was cancelled as the first half of the
                    # cancel-confirm-MARKET fallback. It no longer owns the
                    # strategy reset while its MARKET child is still active.
                    continue
                if role in ("EXIT", "STOP_LOSS", "REVERSAL_EXIT", "EMERGENCY_EXIT"):
                    if _has_market_fallback_child(engine_orders, order):
                        # The watcher canceled this LIMIT as the first half of
                        # cancel-confirm-MARKET. Do not release the SL or cancel
                        # the reversal entry while its child MARKET owns exit.
                        continue
                    position_id = getattr(order, "parent_position_id", None)
                    with self._state_lock:
                        position = next((p for p in getattr(
                            self.env.position_manager, "open_positions", [])
                            if p.position_id == position_id
                            and p.trade_id == (getattr(order, "lifecycle_id", None)
                                               or getattr(order, "trade_id", None))
                            and p.position_generation == getattr(
                                order, "position_generation", None)), None)
                        if position is not None:
                            position.exit_started = False
                            strat = strategies.get(order.strategy_id)
                            if strat is not None:
                                strat.stop_exit_submitted = False
                    if position is not None:
                        try:
                            persistence = getattr(self.env, "persistence", None)
                            if persistence is not None:
                                persistence.save_position(position)
                        except Exception:
                            pass
                    if role == "REVERSAL_EXIT":
                        self._settle_reversal_terminal(order, engine_orders)
                        # Reversal cleanup can invalidate a just-fired paired
                        # entry. Serialize it with tick -> SignalFlow handling,
                        # so either the entry submits first or the terminal old
                        # exit retires it before it can fire; no half-interleave.
                        with self._state_lock:
                            strat = strategies.get(order.strategy_id)
                            if strat is not None:
                                pending = getattr(strat, "pending_entry", None)
                                pending_signal = getattr(pending, "signal", None)
                                pending_signal_id = getattr(pending_signal, "signal_id", None)
                                if pending_signal_id:
                                    persistence = getattr(self.env, "persistence", None)
                                    if persistence is not None:
                                        try:
                                            persistence.terminalize_pending_order(
                                                str(pending_signal_id), status="resolved",
                                                reason="reversal_exit_ended_without_close")
                                        except Exception as exc:
                                            log.error("[LivePoller:%s] failed to retire reversal "
                                                      "entry %s: %s", self.env.name,
                                                      pending_signal_id, exc)
                                    registry = getattr(self.env, "pending_triggers", None)
                                    if registry is not None:
                                        registry.remove_signal(str(pending_signal_id))
                                strat.pending_entry = None
                                strat.pending_exit_trigger = None
                                if position is not None:
                                    strat.position_side = (
                                        "LONG" if getattr(position, "is_long", False)
                                        else "SHORT")
                                    strat.state = (StrategyState.LONG_POSITION
                                                   if position.is_long
                                                   else StrategyState.SHORT_POSITION)
                                strat._last_fired_trigger_signal_id = None
                                if hasattr(strat, "_fired_trigger_signal_ids"):
                                    strat._fired_trigger_signal_ids.clear()
                                strat.stop_exit_submitted = False
                if role.startswith("ENTRY") or role in ("REVERSAL_ENTRY", "FALLBACK_MARKET"):
                    # The POST may have been accepted as PENDING and rejected
                    # only on a later broker status poll. Settle the canonical
                    # trade here too; SignalFlow's synchronous rejection path
                    # cannot handle this delayed terminal response.
                    self._settle_terminal_entry_lifecycle(order)
                    if role in ("REVERSAL_ENTRY", "FALLBACK_MARKET"):
                        self._settle_reversal_terminal(order, engine_orders)
                    # C5 — a terminal entry reset must fire ONCE and only for
                    # the strategy's CURRENT, pre-position entry.  Previously
                    # every terminal ENTRY order left in the book (up to 500)
                    # re-triggered a full reset every 2s poll; when the watcher
                    # had cancelled the resting entry LIMIT and the MARKET
                    # fallback then opened the position, that permanent reset
                    # wiped position_side/stop_price and disarmed the system
                    # stop while the position was OPEN.
                    strat = strategies.get(order.strategy_id)
                    if strat is None:
                        continue
                    if not _terminal_entry_matches_pending(strat, order):
                        pending = getattr(strat, "pending_entry", None)
                        pending_id = getattr(getattr(pending, "signal", None),
                                             "signal_id", None)
                        order_signal_id = (getattr(order, "entry_signal_id", None)
                                           or getattr(order, "parent_signal_id", None))
                        log.info("[LivePoller:%s] ignoring terminal entry %s "
                                 "(signal=%s); strategy %s has newer pending %s",
                                 self.env.name, order.order_id, order_signal_id,
                                 order.strategy_id, pending_id)
                        continue
                    state_v = getattr(strat, "state", None)
                    if hasattr(state_v, "value"):
                        state_v = state_v.value
                    if isinstance(state_v, str) and \
                            state_v.lower() in ("", "flat"):
                        continue  # already flat: nothing left to release
                    pos_side = getattr(strat, "position_side", None)
                    cur_trade = getattr(strat, "current_trade_id", None)
                    order_trade = getattr(order, "trade_id", None)
                    # C5 — a live position that owns this entry must never be
                    # disarmed by the terminal-entry reset.
                    if isinstance(pos_side, str) and isinstance(order_trade, str) \
                            and pos_side.lower() not in ("", "flat", "none") \
                            and order_trade and order_trade == cur_trade:
                        continue
                    if isinstance(cur_trade, str) and isinstance(order_trade, str) \
                            and cur_trade and order_trade \
                            and order_trade != cur_trade:
                        continue  # stale terminal order from an older trade
                    try:
                        if self._reset_strategy_fn is not None:
                            _reset_strategy_if_owned(
                                self._reset_strategy_fn, order.strategy_id,
                                signal_id=(getattr(order, "entry_signal_id", None)
                                           or getattr(order, "parent_signal_id", None)),
                                trade_id=getattr(order, "trade_id", None))
                    except Exception as e:
                        log.error("[LivePoller:%s] reset_strategy_state failed for %s: %s",
                                  self.env.name, order.strategy_id, e)
        self._terminalize_pending_entries(tuple(engine_orders.values()), statuses)
        self._terminalize_pending_from_broker(statuses)
        # Order Watcher — continuous broker+market+intent observation with
        # priority-safe recovery (WAIT/REPRICE/CANCEL/LOCK/MARKET-fallback).
        # Runs AFTER broker truth is applied so it never acts on assumptions.
        watcher = getattr(self, "_order_watcher", None) or \
            getattr(self.env, "order_watcher", None)
        if watcher is not None:
            try:
                watcher.scan()
            except Exception as e:
                log.error("[LivePoller:%s] order watcher scan failed: %s",
                          self.env.name, e)
        return applied

    def _settle_reversal_terminal(self, order, engine_orders=None) -> None:
        """Persist broker-confirmed terminal reversal order outcomes."""
        persistence = getattr(self.env, "persistence", None)
        if persistence is None:
            return
        state = str(getattr(getattr(order, "state", None), "value",
                            getattr(order, "state", ""))).lower()
        if state not in _TERMINAL_ORDER_STATES:
            return
        role = str(getattr(order, "order_role", "") or "").upper()
        status = "CANCELLED" if state in ("cancelled", "canceled", "expired") else "REJECTED"
        if role == "REVERSAL_EXIT":
            signal_id = (getattr(order, "entry_signal_id", None)
                         or getattr(order, "parent_signal_id", None))
            fields = {"old_exit_broker_status": status,
                      "status": f"EXIT_{status}",
                      "failure_reason": getattr(order, "reason", None) or state}
        elif role in ("REVERSAL_ENTRY", "FALLBACK_MARKET"):
            parent = getattr(order, "reversal_parent_signal_id", None)
            if not parent and getattr(order, "original_order_id", None):
                parent_order = (engine_orders or {}).get(order.original_order_id)
                if str(getattr(parent_order, "order_role", "")).upper() == "REVERSAL_ENTRY":
                    parent = getattr(parent_order, "reversal_parent_signal_id", None)
            signal_id = parent or getattr(order, "entry_signal_id", None) or getattr(order, "parent_signal_id", None)
            fields = {"new_entry_broker_status": status,
                      "status": f"ENTRY_{status}",
                      "failure_reason": getattr(order, "reason", None) or state}
        else:
            return
        try:
            reversal = persistence.get_reversal_by_signal_id(str(signal_id or ""))
            if reversal:
                persistence.update_reversal(reversal["reversal_id"], fields)
        except Exception as exc:
            log.error("[LivePoller:%s] reversal terminal persistence failed for %s: %s",
                      self.env.name, getattr(order, "order_id", ""), exc)

    def _settle_terminal_entry_lifecycle(self, order) -> bool:
        """Settle a broker-terminal entry whose fill quantity is still zero."""
        if int(getattr(order, "filled_quantity", 0) or 0) > 0:
            return False
        state = str(getattr(getattr(order, "state", None), "value",
                            getattr(order, "state", ""))).lower()
        if state not in ("rejected", "canceled", "cancelled", "expired"):
            return False
        runtimes = getattr(self.env, "runtimes", None)
        runtime = runtimes.get(getattr(order, "strategy_id", None)) if runtimes else None
        lifecycle = getattr(runtime, "lifecycle", None)
        if lifecycle is None:
            return False
        trade_id = getattr(order, "trade_id", None)
        trade = lifecycle.get_trade(trade_id) if trade_id else None
        if trade is None:
            signal_id = (getattr(order, "entry_signal_id", None)
                         or getattr(order, "parent_signal_id", None))
            trade = lifecycle.resolve_trade_from_signal(signal_id) if signal_id else None
        if trade is None:
            return False
        return lifecycle.reject_unfilled_entry(
            trade.trade_id,
            reason=getattr(order, "reason", None)
            or f"broker entry terminal status: {state}",
            order_id=getattr(order, "order_id", ""),
            status=("CANCELLED" if state in ("canceled", "cancelled", "expired")
                    else "REJECTED"),
        )


    def _route_fill(self, fill, signal_id, is_exit=None) -> None:
        if self._handle_fill is not None:
            self._handle_fill(fill, signal_id, is_exit=is_exit)

    def _upgrade_dbs_order_row(self, fill) -> None:
        """Flip the persisted order row SUBMITTED -> FILLED once the broker
        status confirms the fill (fixes live DB order rows stuck 'submitted').
        Phase 6 — also writes broker_order_id and broker_fill_id into the DB."""
        persistence = getattr(self.env, "persistence", None)
        engine = getattr(self.env, "execution_engine", None)
        if persistence is None or engine is None:
            return
        order = engine.get_order(fill.order_id)
        if order is None:
            return
        try:
            persistence.save_order({
                "order_id": order.order_id,
                "strategy_id": order.strategy_id,
                "instrument": order.instrument,
                "side": order.side,
                "quantity": order.quantity,
                "order_type": getattr(order, "order_type", "MARKET"),
                "price": getattr(order, "price", None) or 0.0,
                "trigger_price": getattr(order, "trigger_price", None),
                "planned_entry_price": getattr(order, "planned_entry_price", None),
                "planned_sl": getattr(order, "planned_sl", None),
                "planned_order_type": getattr(order, "planned_order_type", None),
                "order_role": getattr(order, "order_role", None),
                "protected_order_id": getattr(order, "protected_order_id", None),
                "correlation_id": getattr(order, "correlation_id", None),
                "state": order.state.value,
                "filled_quantity": order.filled_quantity,
                "average_fill_price": order.average_fill_price,
                "created_at": datetime.fromtimestamp(
                    getattr(order, "created_at", time.time()), tz=timezone.utc).isoformat(),
                "updated_at": datetime.fromtimestamp(
                    getattr(order, "updated_at", time.time()), tz=timezone.utc).isoformat(),
                "signal_id": order.entry_signal_id,
                "trade_id": order.trade_id,
                "broker_order_id": getattr(fill, "broker_order_id", None)
                    or getattr(order, "_broker_order_id", None),
            })
            persistence.save_fill({
                "fill_id": fill.fill_id,
                "order_id": fill.order_id,
                "strategy_id": fill.strategy_id,
                "instrument": fill.instrument,
                "side": fill.side,
                "quantity": fill.quantity,
                "price": fill.price,
                "timestamp": datetime.fromtimestamp(
                    fill.timestamp, tz=timezone.utc).isoformat(),
                "trade_id": fill.trade_id,
                "entry_signal_id": fill.entry_signal_id,
                "broker_fill_id": getattr(fill, "broker_fill_id", None),
                "broker_order_id": getattr(fill, "broker_order_id", None)
                    or getattr(order, "_broker_order_id", None),
                "broker_trade_id": getattr(fill, "broker_trade_id", None),
                "cumulative_filled_quantity": getattr(
                    fill, "cumulative_filled_quantity", None),
                "position_id": getattr(fill, "position_id", None),
                "lifecycle_id": getattr(fill, "lifecycle_id", None)
                    or getattr(order, "lifecycle_id", None)
                    or getattr(order, "trade_id", None),
                "position_generation": getattr(fill, "position_generation", None),
            })
        except Exception as e:
            self._stats["order_persist_errors"] += 1
            log.error("[LivePoller:%s] order row upgrade failed for %s: %s",
                      self.env.name, order.order_id, e)

    def _terminalize_pending_entries(self, orders, statuses) -> int:
        """F2 — move durable pending rows to RESOLVED when the broker ended the
        entry order without a fill.  Only entry-role orders are considered; the
        pending row is keyed by the order's entry signal id.  Never re-arms and
        never touches rows already terminal."""
        persistence = getattr(self.env, "persistence", None)
        if persistence is None or not hasattr(persistence, "terminalize_pending_order"):
            return 0
        changed = 0
        terminal = {"rejected", "canceled", "cancelled", "expired"}
        for order in orders:
            if order.state.value not in terminal:
                continue
            role = (getattr(order, "order_role", "") or "").upper()
            if role and not (role.startswith("ENTRY") or role == "REVERSAL_ENTRY"):
                continue  # exit-side orders never own a durable pending row
            sig_id = getattr(order, "entry_signal_id", None)
            if not sig_id:
                continue
            reason = f"broker_{order.state.value}"
            broker_id = getattr(order, "_broker_order_id", None)
            if broker_id:
                rec = (statuses or {}).get(broker_id) or {}
                message = str(rec.get("message") or rec.get("reject_reason") or "")
                if message:
                    reason = f"broker_{order.state.value}: {message[:200]}"
            try:
                if persistence.terminalize_pending_order(sig_id, reason=reason):
                    changed += 1
            except Exception as e:  # never let terminalization break the poll
                self._errors.setdefault("pending_terminalize", 0)
                self._errors["pending_terminalize"] += 1
                log.error("[LivePoller:%s] pending terminalize failed %s: %s",
                          self.env.name, sig_id, e)
        if changed:
            self._stats["pending_terminalized"] += changed
        return changed

    def _release_terminal_exits(self) -> int:
        # Recovery mutates the same position stop/exit latches as live ticks.
        # The routine only performs local reads and persistence writes, so keep
        # its ownership check and re-arm atomic with tick/exit callbacks.
        with getattr(self, "_state_lock", nullcontext()):
            worker = getattr(self, "_release_terminal_exits_locked", None)
            if callable(worker):
                return worker()
            # A few unit fixtures bind this method directly to a SimpleNamespace
            # rather than constructing LiveBrokerPoller; retain that supported
            # test seam while production instances use the bound method above.
            return LiveBrokerPoller._release_terminal_exits_locked(self)

    def _release_terminal_exits_locked(self) -> int:
        """Re-arm the position-owned SL for exits the broker ended unclosed.

        An exit order that reaches REJECTED / CANCELLED / EXPIRED while its
        position is still open is the dangerous case: there is no broker-side
        stop backing the position any more, and the local monitor is latched in
        EXITING, so ``evaluate`` can never fire it again.  The position would
        sit open and unprotected until the next restart.

        Releasing the latch makes the very next tick re-evaluate that
        position's OWN stop and mint a FRESH exit, which is the only correct
        recovery now that no resting stop exists at the broker.

        Safety properties:

        * one-shot per latch — a position is only touched while its SL is
          actually latched, so the 2 s poll cannot churn the book;
        * scoped to ``open_positions`` and requires the SAME ``trade_id`` and
          ``position_generation`` as the order, so a position that really did
          close — or that a reversal already superseded — can never be
          resurrected here;
        * a position whose SL is NONE / SL_UNAVAILABLE (never armed) is left
          alone rather than being falsely reported as ARMED.
        """
        engine = getattr(self.env, "execution_engine", None)
        monitor = getattr(self.env, "sl_monitor", None)
        if engine is None or monitor is None:
            return 0
        if not hasattr(monitor, "release_exit") or not hasattr(monitor, "state_of"):
            return 0
        strategies = getattr(self.env, "strategies", {}) or {}
        open_positions = list(getattr(
            getattr(self.env, "position_manager", None), "open_positions", []) or [])
        if not open_positions:
            return 0
        persistence = getattr(self.env, "persistence", None)
        released = 0
        for order in list((getattr(engine, "_orders", {}) or {}).values()):
            state_v = str(getattr(order.state, "value", order.state) or "").lower()
            if state_v not in _TERMINAL_ORDER_STATES:
                continue
            role = str(getattr(order, "order_role", "") or "").upper()
            if role not in _EXIT_ROLES:
                continue
            if _has_market_fallback_child(
                    getattr(engine, "_orders", {}) or {}, order):
                continue
            position_id = getattr(order, "parent_position_id", None)
            if not position_id:
                continue
            position = next((
                p for p in open_positions
                if str(getattr(p, "position_id", "") or "") == str(position_id)
                and getattr(p, "trade_id", None) == (
                    getattr(order, "lifecycle_id", None)
                    or getattr(order, "trade_id", None))
                and getattr(p, "position_generation", None) == getattr(
                    order, "position_generation", None)), None)
            if position is None:
                continue
            pid = str(getattr(position, "position_id", "") or "")
            # One-shot: only act while the SL is genuinely latched on a dead
            # exit.  Already-ARMED / NONE / SL_UNAVAILABLE positions are left
            # exactly as they are.
            if monitor.state_of(pid) not in ("EXITING", "TRIGGERED"):
                continue
            monitor.release_exit(pid)
            if monitor.state_of(pid) != "ARMED":
                continue
            position.exit_started = False
            position.exit_order_id = None
            position.sl_state = "ARMED"
            strat = strategies.get(getattr(order, "strategy_id", None))
            if strat is not None:
                strat.stop_exit_submitted = False
            try:
                if persistence is not None:
                    persistence.save_position(position)
            except Exception as e:
                log.debug("[LivePoller:%s] SL re-arm persist failed %s: %s",
                          self.env.name, pid, e)
            released += 1
            log.warning(
                "[LivePoller:%s] SL RE-ARMED after %s exit %s (%s) ended %s "
                "without closing — position %s still open, next tick "
                "re-evaluates its own stop",
                self.env.name, role, getattr(order, "order_id", "?"),
                getattr(position, "instrument", "?"), state_v, pid)
        if released:
            self._stats["sl_rearmed"] = self._stats.get("sl_rearmed", 0) + released
        return released

    def _terminalize_pending_from_broker(self, statuses: dict) -> int:
        """F2 — self-heal stale ENTRY_SENT pending rows against broker truth by
        broker_order_id (independent of in-memory orders, so it also cleans rows
        orphaned by a restart where the engine no longer holds the order).

        Rows whose broker order is NOT already in the polled status cache are
        actively re-queried via :meth:`broker.order_status` (a read-only probe)
        so a cold restarted transport can still resolve them."""
        persistence = getattr(self.env, "persistence", None)
        if persistence is None:
            return 0
        broker = getattr(self.env, "broker", None)
        cold_probe = hasattr(broker, "order_status")
        try:
            rows = persistence.get_pending_orders(
                status="entry_sent", execution_mode="LIVE")
        except Exception as e:
            self._errors.setdefault("pending_terminalize", 0)
            self._errors["pending_terminalize"] += 1
            log.error("[LivePoller:%s] pending load failed: %s", self.env.name, e)
            return 0
        if not rows:
            return 0
        terminal = {"rejected", "canceled", "cancelled", "expired"}
        changed = 0
        for row in rows:
            broker_id = row.get("broker_order_id") or ""
            if not broker_id:
                # An order whose POST response AND correlation lookup both failed
                # was parked as ENTRY_SENT with NO broker_order_id, so the
                # by-id self-heal below can never see it. Resolve it by the
                # correlation id we recorded instead of leaving it unresolved
                # forever (a permanently "pending" row that may hide a fill).
                self._self_heal_by_correlation(row, terminal)
                continue
            rec = (statuses or {}).get(broker_id) or {}
            already_terminal = str(rec.get("status") or "").lower() in terminal
            reason = ""
            if not already_terminal and cold_probe:
                # Cold path 1: transport's in-memory book does not know this
                # order (e.g. fresh restart); ask the broker for one order.
                try:
                    rec = broker.order_status(broker_id) or {}
                except Exception as e:
                    self._errors.setdefault("pending_terminalize", 0)
                    self._errors["pending_terminalize"] += 1
                    log.error("[LivePoller:%s] broker order_status failed %s: %s",
                              self.env.name, broker_id, e)
                    rec = {}
                st2 = str(rec.get("status") or "").lower()
                if st2 not in terminal:
                    # Cold path 2 — the broker no longer answers for this order
                    # (empty reply, e.g. it left the day order book).  The
                    # durable LOCAL order lifecycle is broker-derived truth: a
                    # rejected/cancelled entry with zero fills ended without
                    # ever filling.  Label the source explicitly.
                    try:
                        orec = persistence.get_order_by_broker_order_id(broker_id)
                    except Exception:
                        orec = None
                    if orec and str(orec.get("state") or "").lower() in terminal \
                            and int(orec.get("filled_quantity") or 0) == 0:
                        st2 = str(orec["state"]).lower()
                        reason = (f"local_lifecycle: order "
                                  f"{orec.get('order_id')} state={st2}")
            if not already_terminal and not reason and \
                    str(rec.get("status") or "").lower() not in terminal:
                continue
            message = str(rec.get("message") or rec.get("reason") or "")
            if not reason:
                reason = f"broker_terminal: {message[:200]}" \
                    if message else "broker_terminal"
            try:
                if persistence.terminalize_pending_order(
                        row.get("signal_id") or row.get("pending_order_id"),
                        reason=reason):
                    changed += 1
            except Exception as e:
                self._errors.setdefault("pending_terminalize", 0)
                self._errors["pending_terminalize"] += 1
                log.error("[LivePoller:%s] broker pending terminalize failed: %s",
                          self.env.name, row.get("pending_order_id"), e)
        if changed:
            self._stats["pending_terminalized"] += changed
        return changed

    def _self_heal_by_correlation(self, row, terminal) -> bool:
        """F2 — resolve an ENTRY_SENT row that has no ``broker_order_id``.

        Such a row exists only when both the placement POST and its correlation
        lookup failed, i.e. the broker outcome was genuinely UNKNOWN. The
        correlation id is the only handle we have.

        A *failed* lookup is NOT a rejection: the row is left ENTRY_SENT so the
        next cycle tries again, because the order may be live at the exchange.
        Only an authoritative "no such order" terminalizes the row.
        """
        persistence = getattr(self.env, "persistence", None)
        broker = getattr(self.env, "broker", None)
        if persistence is None or broker is None:
            return False
        resolver = getattr(broker, "order_by_correlation_id", None)
        if not callable(resolver):
            return False
        corr = row.get("correlation_id") or ""
        if not corr:
            return False
        try:
            rec = resolver(corr) or {}
        except Exception as e:
            self._errors.setdefault("pending_terminalize", 0)
            self._errors["pending_terminalize"] += 1
            log.error("[LivePoller:%s] correlation resolve failed %s: %s",
                      self.env.name, corr, e)
            return False
        status = str(rec.get("status") or "").lower()
        if status == "unresolved":
            # Broker did not answer. Keep the row pending; never assume flat.
            log.warning("[LivePoller:%s] pending order %s still UNRESOLVED at "
                        "broker (correlation=%s)", self.env.name,
                        row.get("pending_order_id"), corr)
            return False
        broker_id = rec.get("broker_order_id")
        if broker_id:
            # The order DID reach the exchange and we only just learned its id.
            # Link it (save_pending_order upserts and keeps the existing status)
            # so the normal by-id self-heal and fill reconciliation can take over.
            pend_id = row.get("pending_order_id") or row.get("signal_id")
            try:
                update = dict(row)
                update["pending_order_id"] = pend_id
                update["broker_order_id"] = str(broker_id)
                update["correlation_id"] = corr
                persistence.save_pending_order(update)
            except Exception as e:
                log.error("[LivePoller:%s] linking broker id for %s failed: %s",
                          self.env.name, row.get("pending_order_id"), e)
                return False
            log.warning("[LivePoller:%s] recovered broker order %s for pending "
                        "%s via correlation %s", self.env.name, broker_id,
                        row.get("pending_order_id"), corr)
            return False
        if status not in terminal and status not in ("not_found",):
            return False
        # For a lost POST response, a definitive correlation miss means the
        # order never entered Dhan's book. Heal every local owner of that
        # attempt, otherwise the persisted order/trade and strategy remain
        # SUBMITTED/PENDING forever even though the pending row is resolved.
        if status == "not_found":
            signal_id = str(row.get("signal_id") or row.get("pending_order_id") or "")
            reason = f"broker_confirmed_not_found: correlation={corr}"
            engine = getattr(self.env, "execution_engine", None)
            orders = getattr(engine, "_orders", {}) if engine is not None else {}
            matched = [o for o in orders.values()
                       if str(getattr(o, "correlation_id", "") or "") == str(corr)]
            for order in matched:
                if int(getattr(order, "filled_quantity", 0) or 0) > 0:
                    continue
                state = getattr(order, "state", None)
                state_value = str(getattr(state, "value", state)).lower()
                if state_value not in ("rejected", "cancelled", "canceled", "expired"):
                    try:
                        from execution.models import OrderState
                        order.state = OrderState.REJECTED
                    except Exception:
                        order.state = "rejected"
                order.reason = reason
                order.updated_at = time.time()
                self._persist_order_state(order)
            runtimes = getattr(self.env, "runtimes", None)
            runtime = runtimes.get(row.get("strategy_id")) if runtimes is not None else None
            lifecycle = getattr(runtime, "lifecycle", None)
            trade_settled = False
            if lifecycle is not None and signal_id:
                trade = lifecycle.resolve_trade_from_signal(signal_id)
                if trade is not None:
                    trade_settled = lifecycle.reject_unfilled_entry(
                        trade.trade_id, reason=reason,
                        order_id=(matched[0].order_id if matched else ""),
                        status="REJECTED")
            strategy = getattr(self.env, "strategies", {}).get(row.get("strategy_id"))
            pending = getattr(strategy, "pending_entry", None)
            pending_signal_id = getattr(getattr(pending, "signal", None), "signal_id", None)
            matching_pending = bool(pending_signal_id and str(pending_signal_id) == signal_id)
            has_position = bool(getattr(strategy, "position_side", None))
            if (self._reset_strategy_fn is not None and not has_position
                    and (trade_settled or matching_pending)):
                try:
                    _reset_strategy_if_owned(
                        self._reset_strategy_fn, row.get("strategy_id"),
                        signal_id=signal_id)
                except Exception as exc:
                    log.error("[LivePoller:%s] reset after not-found failed: %s",
                              self.env.name, exc)
        try:
            if persistence.terminalize_pending_order(
                    row.get("signal_id") or row.get("pending_order_id"),
                    reason=f"broker_no_such_order: correlation={corr}"):
                self._stats["pending_terminalized"] = \
                    self._stats.get("pending_terminalized", 0) + 1
                return True
        except Exception as e:
            self._errors.setdefault("pending_terminalize", 0)
            self._errors["pending_terminalize"] += 1
            log.error("[LivePoller:%s] correlation terminalize failed %s: %s",
                      self.env.name, row.get("pending_order_id"), e)
        return False

    def _persist_order_state(self, order) -> None:
        """Persist an order's current state to the DB without a fill (covers
        REJECTED/CANCELLED transitions surfaced by broker status polls)."""
        persistence = getattr(self.env, "persistence", None)
        if persistence is None:
            return
        try:
            persistence.save_order({
                "order_id": order.order_id,
                "strategy_id": order.strategy_id,
                "instrument": order.instrument,
                "side": order.side,
                "quantity": order.quantity,
                "order_type": getattr(order, "order_type", "MARKET"),
                "price": getattr(order, "price", None) or 0.0,
                "trigger_price": getattr(order, "trigger_price", None),
                "planned_entry_price": getattr(order, "planned_entry_price", None),
                "planned_sl": getattr(order, "planned_sl", None),
                "planned_order_type": getattr(order, "planned_order_type", None),
                "order_role": getattr(order, "order_role", None),
                "protected_order_id": getattr(order, "protected_order_id", None),
                "correlation_id": getattr(order, "correlation_id", None),
                "state": order.state.value,
                "filled_quantity": order.filled_quantity,
                "average_fill_price": order.average_fill_price,
                "created_at": datetime.fromtimestamp(
                    getattr(order, "created_at", time.time()), tz=timezone.utc).isoformat(),
                "updated_at": datetime.fromtimestamp(
                    getattr(order, "updated_at", time.time()), tz=timezone.utc).isoformat(),
                "signal_id": order.entry_signal_id,
                "trade_id": order.trade_id,
                "broker_order_id": getattr(order, "_broker_order_id", None),
            })
        except Exception as e:
            self._stats["order_persist_errors"] += 1
            log.error("[LivePoller:%s] order state persist failed for %s: %s",
                      self.env.name, order.order_id, e)

    def poll_positions(self) -> list:
        """Pull authoritative broker positions; surface internal-vs-broker
        mismatches in the report.  The poller NEVER opens or reverses a
        position — it only observes and reports broker truth."""
        self._stats["positions_polled"] += 1
        broker = getattr(self.env, "broker", None)
        if broker is None or not hasattr(broker, "positions"):
            return []
        pm = getattr(self.env, "position_manager", None)
        before_fingerprint = self._position_book_fingerprint(pm)
        try:
            bpos = list(broker.positions() or [])
        except Exception as e:
            self._errors["positions"] += 1
            log.error("[LivePoller:%s] positions failed: %s", self.env.name, e)
            return []
        report: list[dict] = []
        # Dhan positions are instrument-level, but DhanRestTransport expands
        # each net row to every configured strategy on that instrument. Compare
        # one broker net to the aggregate local net once; per-strategy
        # comparison falsely reports every non-owning strategy as MISSING_LOCAL.
        broker_by_instrument: dict[str, set[tuple[str, int]]] = {}
        for row in bpos:
            instrument = str(row.get("instrument") or "")
            quantity = int(row.get("quantity") or 0)
            if not instrument or quantity <= 0:
                continue
            side = str(row.get("side") or "").upper()
            signed = quantity if side in ("LONG", "BUY") else -quantity
            broker_by_instrument.setdefault(instrument, set()).add((side, signed))

        local_by_instrument: dict[str, list[dict]] = {}
        if pm is not None:
            # Freeze one coherent local-net view. The broker call above remains
            # concurrent; this lock only covers the inexpensive in-memory copy.
            with self._state_lock:
                if self._position_book_fingerprint(pm) != before_fingerprint:
                    # The broker result spans a local fill/exit transition. It
                    # cannot be compared meaningfully with the newer book, so
                    # retain the last coherent diagnostic until the next poll.
                    self._stats["position_snapshots_discarded"] = (
                        self._stats.get("position_snapshots_discarded", 0) + 1)
                    return list(self._last_position_report)
                self._last_broker_positions = bpos
                try:
                    open_positions = list(pm.open_positions)
                except Exception:
                    open_positions = []
                for position in open_positions:
                    if not getattr(position, "is_open", False):
                        continue
                    instrument = str(getattr(position, "instrument", "") or "")
                    quantity = int(getattr(position, "quantity", 0) or 0)
                    if not instrument or quantity <= 0:
                        continue
                    signed = quantity if getattr(position, "is_long", False) else -quantity
                    local_by_instrument.setdefault(instrument, []).append({
                        "strategy_id": getattr(position, "strategy_id", None),
                        "side": "LONG" if signed > 0 else "SHORT",
                        "quantity": quantity,
                        "signed_quantity": signed,
                    })
        else:
            self._last_broker_positions = bpos

        for instrument in sorted(set(broker_by_instrument) | set(local_by_instrument)):
            broker_rows = broker_by_instrument.get(instrument, set())
            local_rows = local_by_instrument.get(instrument, [])
            local_net = sum(row["signed_quantity"] for row in local_rows)
            broker_nets = {signed for _side, signed in broker_rows}
            conflicting = len(broker_nets) > 1
            broker_net = next(iter(broker_nets)) if len(broker_nets) == 1 else 0
            if not conflicting and broker_net == local_net:
                continue
            report.append({
                "strategy_id": (local_rows[0]["strategy_id"]
                                if len(local_rows) == 1 else None),
                "instrument": instrument,
                "reason": "BROKER_ROWS_CONFLICT" if conflicting else "INSTRUMENT_NET_MISMATCH",
                "broker_side": "LONG" if broker_net > 0 else "SHORT" if broker_net < 0 else None,
                "broker_qty": abs(broker_net),
                "memory_side": "LONG" if local_net > 0 else "SHORT" if local_net < 0 else None,
                "memory_qty": abs(local_net),
                "local_owners": local_rows,
            })
        self._last_position_report = report
        return report

    def poll_account(self) -> dict:
        """Snapshot broker account status (read-only; never overwrites the
        lifecycle-owned account engines with broker numbers)."""
        self._stats["accounts_polled"] += 1
        broker = getattr(self.env, "broker", None)
        if broker is None or not hasattr(broker, "account_status"):
            return {}
        try:
            acct = broker.account_status() or {}
        except Exception as e:
            self._errors["account"] += 1
            log.error("[LivePoller:%s] account_status failed: %s", self.env.name, e)
            return {}
        if isinstance(acct, dict):
            self._last_account = acct
            # F1 remediation — LIVE account snapshots must show broker truth.
            # Adopt the broker-reported totals into the environment's reporting
            # account engine so persisted account_snapshots reflect the real
            # exchange balance instead of the locally derived starting-capital
            # base.  Per-strategy account_engines (which drive the risk gate)
            # are deliberately left untouched.
            if _is_live(self.env):
                _push_broker_account(self.env, acct)
        return self._last_account

    def poll_reconcile(self) -> None:
        """Run the engine's strategy-state vs position reconcile for this env
        (heals the crash/REST restart desync: strategy FLAT vs open position)."""
        self._stats["reconciles_run"] += 1
        if self._on_reconcile is None:
            return
        try:
            self._on_reconcile()
        except Exception as e:
            self._errors["reconcile"] += 1
            log.error("[LivePoller:%s] reconcile failed: %s", self.env.name, e)

    # ── diagnostics ───────────────────────────────────────────────────

    def stats(self) -> dict:
        with self._lock:
            return {
                "intervals": dict(self.intervals),
                "last_run": dict(self._last_run),
                "errors": dict(self._errors),
                "cycle": dict(self._stats),
                "last_position_report": list(self._last_position_report),
                "tradebook_reconciliation": dict(
                    self._last_tradebook_reconciliation),
                "broker_positions_count": len(self._last_broker_positions),
                "has_broker_account": bool(self._last_account),
            }

    def snapshot(self) -> dict:
        return {
            "name": self.env.name,
            "running": self._running,
            "accounts": dict(self._last_account),
            "positions": list(self._last_broker_positions),
            "position_mismatches": list(self._last_position_report),
            "stats": self.stats(),
        }

    # ── one-shot for tests / bootstrap ────────────────────────────────

    def poll_once(self, task: str) -> Any:
        """Run ONE cycle synchronously (default 'orders').  Test-facing."""
        if task not in _TASKS:
            raise ValueError(f"unknown cycle {task!r}; expected one of {_TASKS}")
        try:
            return self._run_task(task)
        except Exception as e:  # pragma: no cover - defensive
            self._errors[task] += 1
            log.error("[LivePoller:%s] %s cycle failed: %s", self.env.name, task, e)
            return None
