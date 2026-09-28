"""Position exits, reversal persistence, and live protective stop lifecycle."""
from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from strategies.types import Signal, SignalType

log = logging.getLogger("trading_engine")


class LivePositionFlowMixin:
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
