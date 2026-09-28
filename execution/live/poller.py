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
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Optional

log = logging.getLogger(__name__)

_TASKS = ("orders", "positions", "account", "reconcile")
_DEFAULT_INTERVAL = 5.0


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

        live_cfg = config.get("live", {}) if isinstance(config, dict) else {}
        self.intervals = {
            "orders": _interval(live_cfg, "order_poll_interval_seconds", 2.0),
            "positions": _interval(live_cfg, "position_poll_interval_seconds", _DEFAULT_INTERVAL),
            "account": _interval(live_cfg, "pnl_poll_interval_seconds", _DEFAULT_INTERVAL),
            "reconcile": _interval(live_cfg, "reconcile_interval_seconds", _DEFAULT_INTERVAL),
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
            "reconciles_run": 0, "fills_created": 0, "fills_routed": 0,
            "order_persist_errors": 0, "sl_verified": 0, "sl_failures": 0,
            "pending_terminalized": 0,
        }
        self._last_broker_positions: list[dict] = []
        self._last_position_report: list[dict] = []
        self._last_account: dict = {}

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
                if now >= self._next_run.get(task, 0.0):
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
                time.sleep(0.05)

    def _run_task(self, task: str):
        if task == "orders":
            return self.poll_orders()
        if task == "positions":
            return self.poll_positions()
        if task == "account":
            return self.poll_account()
        if task == "reconcile":
            return self.poll_reconcile()
        return None

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
        engine_orders = getattr(engine, "_orders", {})
        self._terminalize_pending_from_broker(statuses)
        if not statuses:
            return []
        applied: list = []
        for fill in engine.apply_broker_statuses(statuses):
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
            self._upgrade_dbs_order_row(fill)
        # Phase 6 — persist any order whose state flipped to REJECTED or
        # CANCELLED during apply_broker_statuses (no fill is produced for
        # those, so _upgrade_dbs_order_row is never reached).
        # Dhan-linked state: when an ENTRY order is rejected/cancelled by the
        # broker, reset the strategy state back to FLAT so it is never stuck
        # in ENTRY_TRIGGERED with no Dhan fill to confirm the position.
        engine_orders = getattr(engine, "_orders", {})
        strategies = getattr(self.env, "strategies", {}) or {}
        for order in list(engine_orders.values()):
            if order.state.value in ("rejected", "canceled"):
                self._persist_order_state(order)
                role = (getattr(order, "order_role", "") or "").upper()
                if role in ("EXIT", "STOP_LOSS", "REVERSAL_EXIT", "EMERGENCY_EXIT"):
                    position_id = getattr(order, "parent_position_id", None)
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
                        try:
                            persistence = getattr(self.env, "persistence", None)
                            if persistence is not None:
                                persistence.save_position(position)
                        except Exception:
                            pass
                    if role == "REVERSAL_EXIT":
                        strat = strategies.get(order.strategy_id)
                        if strat is not None:
                            strat.pending_entry = None
                            strat.stop_exit_submitted = False
                if role.startswith("ENTRY") or role == "REVERSAL_ENTRY":
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
                            self._reset_strategy_fn(order.strategy_id)
                    except Exception as e:
                        log.error("[LivePoller:%s] reset_strategy_state failed for %s: %s",
                                  self.env.name, order.strategy_id, e)
        self._terminalize_pending_entries(engine_orders.values(), statuses)
        self._terminalize_pending_from_broker(statuses)
        # V4 — verify resting protective-SL placements (sl_state placed ->
        # verified/failed) once the broker confirms acceptance.
        self.verify_sl_protections(statuses)
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

    def verify_sl_protections(self, statuses: dict) -> list:
        """V4 — spec §22-24 SL verification: confirm resting broker-side
        protective STOP_LOSS_MARKET orders are accepted.

        * broker shows the SLM accepted/submitted/open -> sl_state 'verified'
          (protection is live; kept for the exit-first gate).
        * broker REJECTED/CANCELLED/EXPIRED the SLM               -> sl_state
          'failed' + durable SL_PROTECTION_FAILED audit event (the position's
          LOCAL stop stays armed as the safety net; broker_sl.fail_closed is
          evaluated at placement time, not here).

        Idempotent: only positions whose sl_state is exactly 'placed' are
        visited; every other state is left untouched.
        """
        pm = getattr(self.env, "position_manager", None)
        engine = getattr(self.env, "execution_engine", None)
        persistence = getattr(self.env, "persistence", None)
        if pm is None or engine is None or persistence is None:
            return []
        changed: list = []
        strategies = getattr(self.env, "strategies", {}) or {}
        accept = {"submitted", "open", "accepted", "pending", "open_pending", "pendingnew", "triggered"}
        # C10 — 'unknown' is deliberately NOT in the fail set: a single missed
        # poll or the first cycle after restart was previously escalated to a
        # permanent sl_state='failed' (removing the position from every future
        # verify pass while the protective SL may actually be live).  Unknown
        # now keeps the position in 'placed' and bumps a bounded attempt count;
        # only after max_unknown_polls consecutive unknowns is it failed.
        fail = {"rejected", "cancelled", "canceled", "expired"}
        sl_cfg = (self.config.get("live") or {}).get("broker_sl") or {}
        max_unknown = max(1, int(sl_cfg.get("max_unknown_polls", 5) or 5))
        for sid in list(strategies.keys()):
            positions = pm.get_positions_by_strategy(sid)
            for position in positions:
                if getattr(position, "sl_state", None) != "placed":
                    continue
                sl_id = getattr(position, "sl_order_id", None)
                if not sl_id:
                    continue
                order = engine.get_order(sl_id)
                broker_oid = getattr(order, "_broker_order_id", None) \
                    if order is not None else None
                st = None
                if broker_oid:
                    rec = (statuses or {}).get(broker_oid) or {}
                    st = str(rec.get("status") or "").lower()
                base_status = st or "unknown"
                unknown_attempts = 0
                if base_status in accept:
                    position.sl_state = "verified"
                    position.sl_protected_at = time.time()
                    position.sl_verify_attempts = 0
                    try:
                        persistence.save_position(position)
                    except Exception as e:
                        self._errors["sl_verify"] += 1
                        log.error("[LivePoller:%s] SL verify persist failed %s: %s",
                                  self.env.name, sl_id, e)
                    self._stats["sl_verified"] += 1
                    changed.append({"sl_order_id": sl_id, "sl_state": "verified"})
                elif base_status == "unknown":
                    # C10 — transient/non-confirmed broker state: keep the
                    # position pending verification ('placed') and requery next
                    # cycle.  Escalate to 'failed' only after the configured
                    # number of consecutive unknown polls.
                    unknown_attempts = int(getattr(
                        position, "sl_verify_attempts", 0) or 0) + 1
                    position.sl_verify_attempts = unknown_attempts
                    try:
                        persistence.save_position(position)
                    except Exception as e:
                        self._errors["sl_verify"] += 1
                        log.error(
                            "[LivePoller:%s] SL unknown-persist failed %s: %s",
                            self.env.name, sl_id, e)
                    if unknown_attempts < max_unknown:
                        changed.append({
                            "sl_order_id": sl_id, "sl_state": "placed",
                            "note": f"status_unknown_requery_{unknown_attempts}"})
                        continue
                    # Sustained unknown -> treat as failed (local stop stays
                    # armed as the safety net).

                elif base_status in fail or (
                        base_status == "unknown"
                        and unknown_attempts >= max_unknown):
                    # SL retry policy: re-place the protective SLM at the same
                    # trigger (retry_same_price) up to the configured limit
                    # before declaring the protection failed.  A re-place only
                    # happens while the position is still open; if retries are
                    # disabled/exhausted the state flips to 'failed' and the
                    # fail_closed policy decides the emergency exit.
                    sl_cfg = (self.config.get("live") or {}).get("broker_sl") or {}
                    if bool(sl_cfg.get("retry_enabled", False)):
                        retry_max = int(sl_cfg.get("retry_max_attempts", 1) or 1)
                        attempts = int(getattr(position, "sl_retry_count", 0) or 0)
                        recreate = hasattr(engine, "create_protective_sl")
                        if attempts < retry_max and recreate and \
                                getattr(position, "is_open", False):
                            theme = getattr(position, "sl_trigger_price", 0.0) or 0.0
                            if theme:
                                try:
                                    sl2 = engine.create_protective_sl(
                                        strategy_id=position.strategy_id,
                                        instrument=position.instrument,
                                        side="SELL" if getattr(
                                            position, "is_long", False) else "BUY",
                                        quantity=getattr(position, "quantity", 0),
                                        trigger_price=theme,
                                        trade_id=str(getattr(position, "trade_id", "") or ""),
                                        entry_order_id=str(getattr(
                                            position, "entry_signal_id", "") or ""),
                                    )
                                    engine.submit_order(sl2)
                                except Exception as e:
                                    log.warning("[LivePoller:%s] SL re-place failed: %s",
                                                self.env.name, e)
                                    sl2 = None
                                if sl2 is not None and \
                                        getattr(sl2, "_broker_order_id", None):
                                    position.sl_state = "placed"
                                    position.sl_order_id = sl2.order_id
                                    position.sl_retry_count = attempts + 1
                                    try:
                                        persistence.save_position(position)
                                    except Exception as e:
                                        self._errors["sl_verify"] += 1
                                        log.error(
                                            "[LivePoller:%s] SL re-place persist failed: %s",
                                            self.env.name, e)
                                    changed.append({
                                        "sl_order_id": sl2.order_id,
                                        "sl_state": "placed",
                                        "retry": True,
                                        "retry_count": attempts + 1})
                                    continue
                        # Retries disabled or exhausted -> protection failed.
                    position.sl_state = "failed"
                    try:
                        persistence.save_position(position)
                    except Exception as e:
                        self._errors["sl_verify"] += 1
                        log.error("[LivePoller:%s] SL fail persist failed %s: %s",
                                  self.env.name, sl_id, e)
                    try:
                        import uuid as _uu
                        persistence.save_execution_failure_event({
                            "event_id": f"SLF-{_uu.uuid4().hex}",
                            "event_type": "SL_PROTECTION_FAILED",
                            "strategy_id": position.strategy_id,
                            "trade_id": position.trade_id,
                            "order_id": sl_id,
                            "broker_order_id": broker_oid,
                            "instrument": position.instrument,
                            "error": f"broker_status_{base_status}",
                            "action": "verify_failed",
                            "final_state": "position_local_stop_armed",
                            "details": {
                                "sl_state": "placed", "status": "verify",
                                "broker_status": base_status,
                            },
                        })
                    except Exception as e:
                        self._errors["sl_verify"] += 1
                        log.error("[LivePoller:%s] SL fail audit write failed: %s",
                                  self.env.name, e)
                    self._stats["sl_failures"] += 1
                    changed.append({"sl_order_id": sl_id, "sl_state": "failed"})
        return changed

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
        try:
            bpos = list(broker.positions() or [])
        except Exception as e:
            self._errors["positions"] += 1
            log.error("[LivePoller:%s] positions failed: %s", self.env.name, e)
            return []
        self._last_broker_positions = bpos
        report: list[dict] = []
        pm = getattr(self.env, "position_manager", None)
        for p in bpos:
            sid = p.get("strategy_id") or ""
            inst = p.get("instrument") or ""
            bside = (p.get("side") or "").upper()
            bqty = int(p.get("quantity") or 0)
            mem = None
            if pm is not None:
                mem = next((
                    x for x in pm.get_positions_by_strategy(sid)
                    if getattr(x, "instrument", None) == inst
                    and getattr(x, "is_open", False)), None)
            mside = ("LONG" if getattr(mem, "is_long", False) else "SHORT") if mem else None
            mqty = getattr(mem, "quantity", 0) if mem else 0
            if (mside, mqty) != (bside, bqty):
                report.append({
                    "strategy_id": sid, "instrument": inst,
                    "broker_side": bside, "broker_qty": bqty,
                    "memory_side": mside, "memory_qty": mqty,
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
