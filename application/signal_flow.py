"""Signal-to-order application flow for TradingEngine.

The strategy emits intent; this coordinator applies operator/risk gates, binds
that intent to an owned trade/position lifecycle, then delegates placement to
the execution gateway.
"""
from __future__ import annotations

import logging
import time
from typing import Optional

from core.lifecycle import PendingOrderState
from strategies.types import PendingEntry, StrategyState, resolve_order_role

log = logging.getLogger("trading_engine")


def _strategy_positions_for_risk(signal_type, open_positions) -> int:
    """Count positions that consume this strategy's position cap."""
    open_held = [p for p in open_positions if getattr(p, "is_open", False)]
    holds_short = any(getattr(p, "is_short", False) for p in open_held)
    holds_long = any(getattr(p, "is_long", False) for p in open_held)
    if signal_type.name == "LONG" and holds_short:
        return max(0, len(open_held) - 1)
    if signal_type.name == "SHORT" and holds_long:
        return max(0, len(open_held) - 1)
    return len(open_held)


class SignalFlowMixin:
    def _preserve_position_after_exit_block(self, signal, env, *, cancel_reversal=False,
                                            stop_exit_blocked=False) -> None:
        """Keep exposure and its local SL state when an exit is gated off."""
        strategy = (getattr(env, "strategies", {}) or {}).get(signal.strategy_id)
        if strategy is None or strategy.position_side not in ("LONG", "SHORT"):
            return
        if cancel_reversal:
            strategy._cancel_trigger(getattr(strategy, "pending_exit_trigger", None))
            strategy._cancel_trigger(getattr(strategy, "pending_entry", None))
            strategy.pending_exit_trigger = None
            strategy.pending_entry = None
            strategy._last_fired_trigger_signal_id = None
            strategy._fired_trigger_signal_ids.clear()
        strategy.state = (StrategyState.LONG_POSITION
                          if strategy.position_side == "LONG"
                          else StrategyState.SHORT_POSITION)
        # Prevent duplicate stop signals while the operator's SL gate is off.
        # A later explicit lifecycle action can still close the owned position.
        if stop_exit_blocked:
            strategy.stop_exit_submitted = True

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
        # During the narrowly scoped live canary, only its loopback-triggered
        # lifecycle may create entries. Normal strategy exits remain enabled
        # so risk controls can still reduce exposure during the test.
        config = getattr(self, "config", None)
        test_cfg = (config.get("live_test_order_cycle", {}) or {}) if config else {}
        if (env.is_live and test_cfg.get("enabled")
                and signal.strategy_id == str(test_cfg.get("strategy_id", ""))
                and signal.instrument == str(test_cfg.get("instrument", ""))
                and not is_exit and not bool(metadata.get("test_cycle"))):
            self.publish_event("live_test_cycle_signal_blocked", {
                "signal_id": signal.signal_id,
                "strategy_id": signal.strategy_id,
                "instrument": signal.instrument,
                "reason": "canary_only_entry_mode",
                "execution_mode": env.mode,
            }, env_name=env.name)
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
                exe = env.execution_engine
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
                            if not exe.cancel_order(o.order_id):
                                # The old order may have filled during cancel
                                # or its broker state may be unknown. Drop the
                                # replacement trigger until reconciliation.
                                self._reset_strategy_state(
                                    signal.strategy_id, env_name=env.name)
                                self.publish_event("pending_replacement_blocked", {
                                    "signal_id": signal.signal_id,
                                    "old_order_id": o.order_id,
                                    "reason": "old_order_cancel_unconfirmed",
                                    "execution_mode": env.mode,
                                }, env_name=env.name)
                                return
                            break
            # Terminalize the old durable pending row (Phase 9.6) so the
            # superseded pending order doesn't stay ARMED forever in the DB.
            # Use old_pending_id from metadata if available, otherwise fall
            # back to _last_armed_pending_id.
            old_pending_id = (metadata.get("old_pending_id")
                              or getattr(strategy, "_last_armed_pending_id", None))
            if old_pending_id is not None and env.persistence is not None:
                try:
                    expired = metadata.get("pending_termination") == "expired"
                    env.persistence.terminalize_pending_order(
                        old_pending_id,
                        status="expired" if expired else "cancelled_by_reversal",
                        reason="pending_trigger_timed_out" if expired
                        else "opposite_crossover_superseded")
                except Exception as e:
                    log.warning("[Engine] cancel_inflight: failed to "
                                "terminalize pending %s: %s", old_pending_id, e)
            keep_current_trigger = bool(
                strategy is not None
                and getattr(strategy, "pending_entry", None) is not None
                and getattr(strategy.pending_entry.signal, "signal_id", None)
                    == signal.signal_id)
            self._reset_strategy_state(signal.strategy_id,
                                       keep_pending=keep_current_trigger,
                                       env_name=env.name)
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
            self._preserve_position_after_exit_block(
                signal, env, cancel_reversal=True)
            return
        if reversal_sig and not self._reversal_under_cap(signal):
            self._publish_gate_block(signal, "reversal_daily_cap",
                                     {"gate": gates.to_dict()}, env)
            self._preserve_position_after_exit_block(
                signal, env, cancel_reversal=True)
            return
        if is_exit:
            exit_reason_md = str((metadata or {}).get("exit_reason") or "").lower()
            is_sl_exit = ("stop_loss" in exit_reason_md
                          or resolve_order_role(signal) == "STOP_LOSS")
            if is_sl_exit:
                if not gates.sl_enabled:
                    self._publish_gate_block(signal, "sl_disabled",
                                             {"gate": gates.to_dict()}, env)
                    self._preserve_position_after_exit_block(
                        signal, env, stop_exit_blocked=True)
                    return
            elif not gates.exit_enabled:
                self._publish_gate_block(signal, "exit_disabled",
                                         {"gate": gates.to_dict()}, env)
                self._preserve_position_after_exit_block(signal, env)
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
            # §38 — never open exposure on a feed we already know is dead.
            # The stop is evaluated locally from market data, so an entry
            # taken on a stale feed is an unprotected entry from the moment it
            # fills.  Refusing here (rather than auto-flatting) is deliberate:
            # exiting on stale data is a second failure, not a remedy.
            # §35 — never open exposure on an UNRECONCILED book.  If startup
            # could not establish the broker's real position state we do not
            # know what is already open, so a new entry could stack onto a
            # position the local book has no record of.
            reconciled = getattr(self, "_reconciled_envs", None)
            if (env.is_live and reconciled is not None
                    and env.name not in reconciled):
                self._publish_gate_block(
                    signal, "startup_reconciliation_failed",
                    {"env": env.name}, env)
                log.error("[Engine] entry BLOCKED: env %s has no confirmed "
                          "broker position state", env.name)
                self._reset_strategy_state(signal.strategy_id, env_name=env.name)
                return
            health = getattr(self, "market_data_health", None)
            if health is not None and not health.is_healthy(signal.instrument):
                age = health.age(signal.instrument)
                self._publish_gate_block(
                    signal, "market_data_unhealthy",
                    {"instrument": signal.instrument,
                     "last_tick_age_seconds": age,
                     "stale_after_seconds": health.stale_after}, env)
                log.error("[Engine] entry BLOCKED: market data unhealthy for %s "
                          "(age=%s)", signal.instrument, age)
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
                reversal_entry = metadata.get("reversal_entry_signal")
                if (metadata.get("pending_trigger_kind") == "REVERSAL_EXIT"
                        and reversal_entry is not None):
                    self._persist_signal(reversal_entry, "entry", env.name)
                    self._arm_live_pending(reversal_entry, env)
                elif not is_exit:
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
            # There is no broker-side protective SL, so a local SL exit is
            # never suppressed here.  §9/§10 duplicate + race protection lives
            # in the position-owned SL monitor's state machine (TRIGGERED ->
            # EXITING), which is consulted immediately before the order is
            # minted.  A second exit for an already-exiting position is blocked
            # further down by validate_live_order_ownership.
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
            # EVERY exit (local-SL fire, reversal, manual, emergency) moves the
            # position-owned SL to EXITING before the order is minted, so a
            # concurrent tick cannot race a second exit (§10: one authoritative
            # exit state per position).
            self._mark_sl_exiting(env, position)

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
                # The exit never reached the broker: re-arm the position-owned
                # SL so a later tick can still protect this position.
                self._release_sl_after_failed_exit(env, position,
                                                  reason="exit_not_submitted")
                if reversal_sig:
                    strategy.pending_entry = None
            else:
                self._reset_strategy_state(signal.strategy_id, env_name=env.name)
            return
        if is_exit and position is not None and order.state.value in (
                "submitted", "acknowledged", "partially_filled", "filled"):
            position.exit_started = True
            self._mark_sl_exiting(env, position, order.order_id,
                                  getattr(order, "_broker_order_id", None))
        elif is_exit and position is not None:
            # Order exists but was rejected/cancelled by the engine or broker.
            self._release_sl_after_failed_exit(env, position,
                                              reason=(order.reason or "exit_rejected"))
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
