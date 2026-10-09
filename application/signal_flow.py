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
    def _park_reversal_entry_for_retry(self, signal, env, reason: str) -> bool:
        """Keep a fired reversal entry durable when a recoverable safety gate closes.

        The paired entry is retried by broker reconciliation after flatness is
        re-confirmed. This never bypasses the gate and never promises a broker
        acceptance or fill.
        """
        metadata = getattr(signal, "metadata", None) or {}
        if (not env.is_live or not metadata.get("is_reversal_entry")
                or str(metadata.get("trigger_state", "")).upper() != "FIRED"):
            return False
        strategy = (getattr(env, "strategies", {}) or {}).get(signal.strategy_id)
        if strategy is None:
            return False
        from strategies.types import PendingEntry
        original_trigger = metadata.get(
            "reversal_entry_trigger_level", signal.trigger_price)
        try:
            original_trigger = float(original_trigger)
        except (TypeError, ValueError):
            original_trigger = float(signal.trigger_price or 0.0)
        metadata.update(
            pending=True, triggered=False, trigger_state="ARMED",
            pending_gate_wait_reason=str(reason),
            reversal_entry_trigger_level=original_trigger,
        )
        signal.metadata = metadata
        signal.trigger_price = original_trigger
        current = getattr(strategy, "pending_entry", None)
        if getattr(getattr(current, "signal", None), "signal_id", None) != signal.signal_id:
            strategy.pending_entry = PendingEntry(
                signal=signal, trigger_price=original_trigger,
                side=str(signal.side or signal.signal_type.value).upper(),
                status="waiting_for_flat", created_at=time.time())
        else:
            current.status = "waiting_for_flat"
            current.trigger_price = original_trigger
        strategy.state = StrategyState.EXIT_ORDER_SUBMITTED
        registry = getattr(env, "pending_triggers", None)
        if registry is not None:
            registry.sync_strategy(strategy)
        persistence = getattr(env, "persistence", None)
        if persistence is not None:
            try:
                self._persist_signal(signal, "entry", env.name)
                persistence.save_pending_order({
                    "pending_order_id": signal.signal_id,
                    "signal_id": signal.signal_id,
                    "trade_id": None,
                    "status": PendingOrderState.ARMED.value,
                    "strategy_id": signal.strategy_id,
                    "instrument": signal.instrument,
                    "direction": str(signal.side or signal.signal_type.value).upper(),
                    "trigger_price": original_trigger,
                    "trigger_state": "ARMED",
                    "trigger_generation": metadata.get("trigger_generation"),
                    "trigger_source": f"reversal_entry_wait:{reason}",
                    "signal_timestamp": signal.timestamp,
                    "quantity": signal.quantity,
                })
                registry = getattr(env, "pending_triggers", None)
                if registry is not None:
                    registry.update_live_row(signal.signal_id, {
                        "status": PendingOrderState.ARMED.value,
                        "trigger_state": "ARMED",
                        "trigger_source": f"reversal_entry_wait:{reason}",
                    })
            except Exception as exc:
                log.exception("[Engine] could not persist parked reversal entry %s",
                              signal.signal_id)
                self.publish_event("reversal_entry_park_persist_failed", {
                    "signal_id": signal.signal_id,
                    "strategy_id": signal.strategy_id,
                    "reason": reason,
                    "error": str(exc),
                    "execution_mode": env.mode,
                }, env_name=env.name)
                return False
        self.publish_event("reversal_entry_waiting_for_entry_gate", {
            "signal_id": signal.signal_id,
            "strategy_id": signal.strategy_id,
            "instrument": signal.instrument,
            "reason": reason,
            "execution_mode": env.mode,
        }, env_name=env.name)
        return True

    def _retire_fired_live_pending(self, signal, env, reason: str) -> None:
        """Do not restore an already-fired trigger after a local gate rejects it."""
        metadata = getattr(signal, "metadata", None) or {}
        if (not env.is_live or bool(metadata.get("exit"))
                or str(metadata.get("trigger_state", "")).upper() != "FIRED"):
            return
        signal_id = str(getattr(signal, "signal_id", "") or "")
        if not signal_id:
            return
        persistence = getattr(env, "persistence", None)
        if persistence is not None:
            try:
                persistence.terminalize_pending_order(
                    signal_id, status="resolved", reason=reason)
            except Exception as exc:
                log.error("[Engine] fired pending trigger cleanup failed for %s: %s",
                          signal_id, exc)
                self.publish_event("pending_trigger_cleanup_failed", {
                    "signal_id": signal_id, "strategy_id": signal.strategy_id,
                    "reason": reason, "error": str(exc),
                    "execution_mode": env.mode,
                }, env_name=env.name)
        registry = getattr(env, "pending_triggers", None)
        if registry is not None:
            registry.remove_signal(signal_id)

    def _preserve_position_after_exit_block(self, signal, env, *, cancel_reversal=False) -> None:
        """Keep broker-owned exposure and strategy direction after a blocked exit."""
        strategy = (getattr(env, "strategies", {}) or {}).get(signal.strategy_id)
        if strategy is None or strategy.position_side not in ("LONG", "SHORT"):
            return
        if cancel_reversal:
            strategy._cancel_trigger(getattr(strategy, "pending_exit_trigger", None))
            strategy._cancel_trigger(getattr(strategy, "pending_entry", None))
            strategy.pending_exit_trigger = None
            strategy.pending_entry = None
            registry = getattr(env, "pending_triggers", None)
            if registry is not None:
                registry.sync_strategy(strategy)
            strategy._last_fired_trigger_signal_id = None
            strategy._fired_trigger_signal_ids.clear()
        strategy.state = (StrategyState.LONG_POSITION
                          if strategy.position_side == "LONG"
                          else StrategyState.SHORT_POSITION)

    def _process_signal(self, signal, env_name: Optional[str] = None) -> None:
        """Move one strategy signal through the explicit durable lifecycle.

        A completed 15-minute candle is the signal and execution decision
        point. Entries and exits use market orders; no candle-breakout trigger
        or local stop order is evaluated here.

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
        config = getattr(self, "config", None)
        runtime_policy = ((config.get("strategy_runtime", {}) or {})
                          if config is not None else {})
        if runtime_policy.get("exclusive"):
            allowed = (runtime_policy.get("allowlist", {}) or {}).get(
                signal.strategy_id)
            paired_entry = metadata.get("reversal_entry_signal")
            entry_intent = (not is_exit or paired_entry is not None
                            or bool(metadata.get("is_reversal_entry")))
            expected_type = (allowed or {}).get("strategy_type")
            expected_instrument = (allowed or {}).get("instrument")
            expected_timeframe = (allowed or {}).get("timeframe")
            mismatch = (allowed is None
                        or signal.instrument != expected_instrument
                        or (expected_type and getattr(strategy, "strategy_type", None)
                            != expected_type)
                        or (expected_timeframe and getattr(
                            strategy, "signal_timeframe", None) != expected_timeframe))
            if entry_intent and mismatch:
                self.publish_event("strategy_runtime_policy_blocked", {
                    "signal_id": signal.signal_id,
                    "strategy_id": signal.strategy_id,
                    "instrument": signal.instrument,
                    "reason": "exclusive_strategy_allowlist",
                    "execution_mode": env.mode,
                }, env_name=env.name)
                log.error("[Engine] exclusive runtime policy blocked entry signal "
                          "%s from %s (%s)", signal.signal_id,
                          signal.strategy_id, signal.instrument)
                return
        if env.is_live and is_exit:
            position_id = (metadata.get("position_id")
                           or metadata.get("parent_position_id")
                           or getattr(strategy, "current_position_id", None))
            position_manager = getattr(env, "position_manager", None)
            position = (position_manager.get_position(str(position_id))
                        if position_id and position_manager is not None else None)
        # Entries and exits in this runtime come only from the completed-candle
        # SuperTrend flip. There is no canary-only signal interception or tick
        # trigger path.
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

        # §66 — IDEMPOTENT REPLAY: a signal whose own trade already executed
        # its entry (entry_fill recorded => a real position exists) must never
        # be executed again. Replaying the same entry signal upstream (crash
        # replay, WS+REST double delivery, operator retry, backfill) must not
        # mint another economic attempt. A definitive rejection/cancellation
        # is also terminal for that immutable signal; a fresh signal is needed
        # to submit a new order.
        if not is_exit and not is_pending:
            prior_trade = lifecycle.resolve_trade_from_signal(signal.signal_id)
            prior_status = str(getattr(prior_trade, "status", "") or "").upper()
            if (prior_trade is not None
                    and (getattr(prior_trade, "entry_fill_id", None)
                         or prior_status in ("REJECTED", "CANCELLED"))):
                self.publish_event("signal_replayed_ignored", {
                    "signal_id": signal.signal_id,
                    "trade_id": prior_trade.trade_id,
                    "strategy_id": signal.strategy_id,
                    "already_filled": getattr(prior_trade, "entry_fill_id", None),
                    "terminal_status": prior_status,
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
            if not gates.exit_enabled:
                self._publish_gate_block(signal, "exit_disabled",
                                         {"gate": gates.to_dict()}, env)
                self._preserve_position_after_exit_block(signal, env)
                return
        else:
            # Contract validation / rollover: entries stay blocked whenever
            # the configured contract cannot be verified or is inside its
            # rollover window. This supersedes every other entry gate.
            if (env.name, signal.instrument) in self._rollover_blocked:
                self._publish_gate_block(
                    signal, "contract_rollover_blocked",
                    {"rollover": "contract_unverified_or_rollover_window"}, env)
                try:
                    self.telegram.on_risk_alert({
                        "severity": "WARNING",
                        "type": "contract_rollover_blocked",
                        "message": f"{signal.instrument}: entries blocked because "
                                   "the active contract is unverified or in its "
                                   "rollover window",
                        "strategy_id": signal.strategy_id,
                        "instrument": signal.instrument,
                    })
                except Exception as e:
                    log.warning("[Engine] rollover telegraph failed: %s", e)
                self._retire_fired_live_pending(
                    signal, env, "entry_blocked_contract_rollover")
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
                self._retire_fired_live_pending(signal, env, reason)
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
                if not self._park_reversal_entry_for_retry(
                        signal, env, "startup_reconciliation_failed"):
                    self._retire_fired_live_pending(
                        signal, env, "startup_reconciliation_failed")
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
                if not self._park_reversal_entry_for_retry(
                        signal, env, "market_data_unhealthy"):
                    self._retire_fired_live_pending(signal, env, "market_data_unhealthy")
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
                self._retire_fired_live_pending(signal, env, reject_reason)
                self._reset_strategy_state(signal.strategy_id, env_name=env.name)
                return

        try:
            self._persist_signal(signal, "exit" if is_exit else "entry", env_name)
        except Exception as exc:  # noqa: BLE001
            if is_pending and env.mode == "LIVE":
                self._discard_unjournaled_live_pending(
                    strategy, env, f"signal persistence failed: {exc}")
                self.publish_event("pending_order_blocked", {
                    "signal_id": signal.signal_id,
                    "reason": "live_signal_persistence_failed",
                    "execution_mode": env.mode,
                }, env_name=env.name)
                log.exception("[Engine] refusing RAM-only LIVE trigger %s",
                              signal.signal_id)
                return
            raise

        # A SuperTrend reversal submits its old-position market exit first,
        # then submits the opposite market entry only after Dhan confirms
        # flatness. Journal that paired entry before the exit reaches Dhan:
        # the later post-flat handoff requires an ARMED durable row, and a
        # missing row would otherwise silently block the reversal entry.
        paired_reversal_entry = metadata.get("reversal_entry_signal")
        if (env.mode == "LIVE" and is_exit
                and metadata.get("market_on_signal_close")
                and metadata.get("is_reversal")
                and paired_reversal_entry is not None):
            try:
                self._persist_signal(paired_reversal_entry, "entry", env_name)
                self._arm_live_pending(paired_reversal_entry, env)
            except Exception as exc:  # noqa: BLE001
                self.publish_event("reversal_entry_journal_failed", {
                    "signal_id": paired_reversal_entry.signal_id,
                    "exit_signal_id": signal.signal_id,
                    "strategy_id": signal.strategy_id,
                    "instrument": signal.instrument,
                    "reason": "opposite_entry_not_durable_before_exit",
                    "error": str(exc),
                    "execution_mode": env.mode,
                }, env_name=env.name)
                safe_mode = getattr(env, "safe_mode", None)
                if safe_mode is not None:
                    safe_mode.enter_safe_mode(
                        "reversal_entry_journal_failed",
                        f"Could not durably arm paired reversal entry "
                        f"{paired_reversal_entry.signal_id}")
                # Keep the existing broker position authoritative if the
                # paired entry cannot survive restart/reconciliation.
                self._preserve_position_after_exit_block(
                    signal, env, cancel_reversal=True)
                log.exception("[Engine] refusing reversal exit without a "
                              "durable opposite-entry record")
                return
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
                try:
                    if (metadata.get("pending_trigger_kind") == "REVERSAL_EXIT"
                            and reversal_entry is not None):
                        self._persist_signal(reversal_entry, "entry", env.name)
                        self._arm_live_pending(reversal_entry, env)
                    elif not is_exit:
                        self._arm_live_pending(signal, env)
                except Exception as exc:  # noqa: BLE001
                    self._discard_unjournaled_live_pending(
                        strategy, env, f"pending trigger persistence failed: {exc}")
                    self.publish_event("pending_order_blocked", {
                        "signal_id": getattr(reversal_entry, "signal_id", None)
                        if reversal_entry is not None else signal.signal_id,
                        "reason": "live_pending_persistence_failed",
                        "execution_mode": env.mode,
                    }, env_name=env.name)
                    log.exception("[Engine] refusing LIVE trigger without durable row")
            return

        # Immediate-after-close strategies still journal an ARMED durable
        # signal before order creation. This preserves the LIVE no-RAM-only
        # invariant while allowing the completed candle to submit directly.
        if (env.mode == "LIVE"
                and metadata.get("market_on_signal_close")
                and not is_exit):
            self._arm_live_pending(signal, env)

        # Exits reduce risk and remain available during a safety halt. Entries
        # must pass both the session/data gate and the (environment's) risk gate.
        if not is_exit:
            safe_mode = env.safe_mode if env.safe_mode is not None else self.safe_mode
            market_status = env.market_status if env.market_status is not None else self.market_status
            if safe_mode.is_active or not market_status.is_trading_allowed:
                if not self._park_reversal_entry_for_retry(
                        signal, env, "safe_mode_or_market_closed"):
                    self._retire_fired_live_pending(
                        signal, env, "safe_mode_or_market_closed")
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
                    self._retire_fired_live_pending(signal, env, reason)
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
        if env.mode == "LIVE" and not is_exit and not is_pending:
            live_pending = self._live_pending_row(env, signal)
            pend_status = (live_pending.get("status") or "").lower() \
                if live_pending is not None else ""
            if live_pending is None or pend_status != PendingOrderState.ARMED.value:
                self.publish_event("pending_order_blocked", {
                    "signal_id": signal.signal_id,
                    "pending_order_id": (live_pending or {}).get("pending_order_id"),
                    "state": pend_status,
                    "reason": "durable_pending_not_armed"
                                if pend_status else "durable_pending_missing",
                    "execution_mode": env.mode}, env_name=env.name)
                # ENTRY_SENT is a duplicate/replay while its original Dhan
                # order owns the signal. Preserve that lifecycle; a missing
                # or terminal row cannot safely create a fresh broker order.
                if pend_status != PendingOrderState.ENTRY_SENT.value:
                    self._retire_fired_live_pending(
                        signal, env, "durable_pending_not_armed")
                    self._reset_strategy_state(
                        signal.strategy_id, env_name=env.name)
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
                                side=side, status="waiting_for_flat",
                                created_at=time.time())
                            md = signal.metadata or {}
                            md.update(pending=True, triggered=False,
                                      trigger_state="ARMED")
                            signal.metadata = md
                            strategy.state = StrategyState.EXIT_ORDER_SUBMITTED
                            strategy._fired_trigger_signal_ids.pop(
                                signal.signal_id, None)
                            if strategy._last_fired_trigger_signal_id == signal.signal_id:
                                strategy._last_fired_trigger_signal_id = None
                            registry = getattr(env, "pending_triggers", None)
                            if registry is not None:
                                registry.sync_strategy(strategy)
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

        order_submit_started = time.perf_counter()
        order = order_manager.submit_signal(
            signal, multiplier=multiplier, trade_id=trade.trade_id, side=exit_side,
        )
        submit_ms = (time.perf_counter() - order_submit_started) * 1000
        log.info("[ExecutionTiming] env=%s strategy=%s instrument=%s signal=%s "
                 "submit_response_ms=%.1f result=%s indicator_ms=%s",
                 env.name, signal.strategy_id, signal.instrument,
                 signal.signal_id, submit_ms,
                 (getattr(getattr(order, "state", None), "value", None)
                  if order is not None else "deduplicated_or_not_created"),
                 (metadata.get("indicator_calculation_ms")
                  if metadata else None))
        if order is None:
            if is_exit and position is not None:
                position.exit_started = False
                strategy.position_side = "LONG" if position.is_long else "SHORT"
                strategy.state = (StrategyState.LONG_POSITION if position.is_long
                                  else StrategyState.SHORT_POSITION)
                if reversal_sig:
                    strategy.pending_entry = None
                    registry = getattr(env, "pending_triggers", None)
                    if registry is not None:
                        registry.sync_strategy(strategy)
            else:
                self._retire_fired_live_pending(
                    signal, env, "entry_order_not_created")
                self._reset_strategy_state(signal.strategy_id, env_name=env.name)
            return
        if is_exit and position is not None and order.state.value in (
                "submitted", "acknowledged", "partially_filled", "filled"):
            position.exit_started = True
        elif is_exit and position is not None:
            # Order exists but was rejected/cancelled by the engine or broker.
            position.exit_started = False
        order_role = resolve_order_role(signal)
        lifecycle.register_order(trade.trade_id, order.order_id,
                                 order_role or ("EXIT" if is_exit else "ENTRY"))
        # REVERSAL — SAME TRIGGER, EXIT FIRST, ENTRY SECOND: a REVERSAL_EXIT
        # opens a durable reversal lifecycle record (old trade/position + the
        # old exit order); the OPPOSITE REVERSAL_ENTRY order later merges the
        # new trade/order onto the SAME record via the shared signal id.  The
        # record is complete only after the old exit and new entry are
        # broker-confirmed.
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
            "submission_outcome": getattr(order, "submission_outcome", "NOT_SENT"),
            "reason": getattr(order, "reason", None),
            "execution_mode": env.mode}, env_name=env.name)
        self.publish_event("order_submission_outcome", {
            "trade_id": trade.trade_id, "order_id": order.order_id,
            "signal_id": signal.signal_id, "strategy_id": signal.strategy_id,
            "instrument": signal.instrument, "state": order.state.value,
            "submission_outcome": getattr(order, "submission_outcome", "NOT_SENT"),
            "submission_attempt_count": getattr(order, "submission_attempt_count", 0),
            "rejection_retry_count": getattr(order, "rejection_retry_count", 0),
            "submission_attempts": getattr(order, "submission_attempts", []),
            "broker_order_id": getattr(order, "_broker_order_id", None),
            "reason": getattr(order, "reason", None),
            "execution_mode": env.mode,
        }, env_name=env.name)
        # Phase 9.6 — the LIVE pending order whose trigger fired is now sent to
        # the broker: record ENTRY_SENT with the broker correlation tie-back.
        if (live_pending is not None
                and getattr(order, "submission_outcome", "NOT_SENT")
                != "NOT_SENT"):
            self._mark_live_pending_entry_sent(env, signal, trade, order)
        # A broker placement rejection is terminal even when Dhan returned no
        # broker order id. Settle the canonical trade and pending trigger now;
        # otherwise the signal remains PENDING/ENTRY_SENT and permanently
        # occupies the strategy after a definitive no-fill response.
        terminal_entry = (order_role in ("ENTRY", "REVERSAL_ENTRY")
                          and str(getattr(order.state, "value", order.state)).lower()
                          in ("rejected", "cancelled", "canceled", "expired")
                          and int(getattr(order, "filled_quantity", 0) or 0) == 0)
        if terminal_entry:
            reason = getattr(order, "reason", None) or "broker rejected entry without fill"
            lifecycle.reject_unfilled_entry(
                trade.trade_id, reason=reason, order_id=order.order_id,
                status=("CANCELLED" if str(getattr(order.state, "value", order.state)).lower()
                        in ("cancelled", "canceled", "expired") else "REJECTED"))
            if env.persistence is not None:
                try:
                    env.persistence.terminalize_pending_order(
                        signal.signal_id, reason=str(reason))
                except Exception as exc:
                    log.error("[Engine] failed to terminalize rejected entry %s: %s",
                              signal.signal_id, exc)
            if order_role == "REVERSAL_ENTRY" and env.mode == "LIVE":
                self._update_reversal_entry_rejected(
                    env, (signal.metadata or {}).get("reversal_parent_signal_id")
                    or signal.signal_id, reason)
            pending = getattr(strategy, "pending_entry", None)
            pending_sid = getattr(getattr(pending, "signal", None), "signal_id", None)
            if pending is None or str(pending_sid) == str(signal.signal_id):
                self._reset_strategy_state(signal.strategy_id, env_name=env.name)
            self.publish_event("entry_order_terminal_without_fill", {
                "trade_id": trade.trade_id, "order_id": order.order_id,
                "signal_id": signal.signal_id, "strategy_id": signal.strategy_id,
                "instrument": signal.instrument, "state": str(getattr(order.state, "value", order.state)),
                "reason": str(reason), "execution_mode": env.mode,
            }, env_name=env.name)
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
