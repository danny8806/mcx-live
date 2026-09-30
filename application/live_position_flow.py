"""Position exits, reversal persistence, and the position-owned SL lifecycle.

There is NO broker-side protective stop-loss in this module.  The only stop is
the local, position-owned SL monitor (see ``execution/live/sl_monitor.py`` and
``application/sl_flow.py``); it mints an ordinary direct exit order when a live
tick crosses ``position.stop_price``.
"""
from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from execution.live.sl_monitor import SLState
from strategies.types import Signal, SignalType

log = logging.getLogger("trading_engine")


class LivePositionFlowMixin:
    def _record_reversal_open(self, env, signal, trade, position, order) -> None:
        """Open a durable reversal lifecycle record on REVERSAL_EXIT submit.

        The exit ALWAYS carries the SAME trigger price the OPPOSITE entry will
        use: captured from the still-armed strategy reversal pending_entry
        (candle HIGH for a LONG->SHORT reversal / LOW for SHORT->LONG), falling
        back to the signal's own trigger.  The old position + its exit order are
        stamped so the chain has a start.
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
            env.persistence.save_reversal({
                "reversal_id": reversal_id,
                "signal_id": signal.signal_id,
                "strategy_id": signal.strategy_id,
                "instrument": signal.instrument,
                "old_trade_id": getattr(trade, "trade_id", None),
                "old_position_id": getattr(position, "position_id", None),
                "old_exit_order_id": getattr(order, "order_id", None),
                "old_broker_order_id": getattr(order, "_broker_order_id", None),
                "old_sl_order_id": None,
                "old_sl_state": getattr(position, "sl_state", None),
                "reversal_trigger_price": trigger,
                "old_exit_broker_status": str(getattr(
                    getattr(order, "state", None), "value",
                    getattr(order, "state", "submitted"))).upper(),
                "failure_reason": (getattr(order, "reason", None)
                                   if str(getattr(getattr(order, "state", None),
                                                  "value", "")).lower()
                                   in ("rejected", "cancelled", "canceled", "expired")
                                   else None),
                "status": ("EXIT_REJECTED" if str(getattr(
                    getattr(order, "state", None), "value",
                    getattr(order, "state", ""))).lower() == "rejected"
                    else "EXIT_CANCELLED" if str(getattr(
                    getattr(order, "state", None), "value",
                    getattr(order, "state", ""))).lower() in
                    ("cancelled", "canceled", "expired") else "PENDING_EXIT"),
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
                "old_sl_state": "CLOSED",
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

    def _update_reversal_entry_rejected(self, env, signal_id: Optional[str],
                                        reason: str) -> None:
        """Record that the opposite entry definitively failed without a fill."""
        rev = self._find_reversal(env, signal_id)
        if rev is None:
            return
        try:
            env.persistence.update_reversal(rev["reversal_id"], {
                "new_entry_broker_status": "REJECTED",
                "status": "ENTRY_REJECTED",
                "failure_reason": str(reason),
            })
        except Exception as e:
            log.warning("[Engine] reversal entry-reject stamp failed for %s: %s",
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
            filled_order = (env.execution_engine.get_order(fill.order_id)
                            if getattr(env, "execution_engine", None) is not None
                            else None)
            is_fallback = str(getattr(filled_order, "order_role", "")).upper() == "FALLBACK_MARKET"
            # The NEW position's stop is the local position-owned monitor, so
            # "protected" is simply "this position is ARMED (or explicitly
            # unavailable) with a stop level".  There is no broker SL id.
            sl_state = getattr(position, "sl_state", None) or "NONE"
            stop_price = getattr(position, "stop_price", None)
            sl_armed = (sl_state == "ARMED" and stop_price is not None
                        and float(stop_price) > 0)
            fields = {
                "new_entry_order_id": getattr(fill, "order_id", None),
                "new_entry_fill_price": fill.price,
                "new_entry_filled_quantity": fill.quantity,
                "new_entry_broker_status": "FILLED",
                "new_position_id": getattr(position, "position_id", None),
                "new_sl_state": "ARMED" if sl_armed else sl_state,
                "entry_fill_confirmed_at": datetime.now(timezone.utc).isoformat(),
                "status": "COMPLETE" if sl_armed else "AWAITING_LOCAL_SL",
            }
            broker_order_id = getattr(filled_order, "_broker_order_id", None)
            if broker_order_id:
                fields["new_broker_order_id"] = broker_order_id
            if is_fallback:
                fields.update({"fallback_used": 1, "fallback_status": "FILLED"})
            env.persistence.update_reversal(rev["reversal_id"], fields)
        except Exception as e:
            log.warning("[Engine] reversal entry-fill stamp failed for %s: %s",
                        rev["reversal_id"], e)
    def _emergency_close_position(self, env, position, reason: str) -> None:
        """Market-close an unprotected position with an EMERGENCY_EXIT order."""
        if env.execution_engine is None:
            return
        if getattr(position, "sl_state", None) in ("CLOSED", "EXITING"):
            # Already closed, or an exit is already the authoritative state.
            return
        if not getattr(position, "is_open", False):
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
            metadata={"exit": True, "exit_reason": "emergency_exit",
                      "trigger_state": "FIRED",
                      "trigger_source": "emergency_action"},
        )
        sig.signal_id = (getattr(position, "entry_signal_id", None)
                         or f"EMG-{uuid.uuid4().hex[:8]}")
        sig.lifecycle_id = position.trade_id
        sig.parent_position_id = position.position_id
        sig.position_generation = position.position_generation
        # INVARIANT 4 — one authoritative exit state: latch the position's SL
        # as EXITING before the order leaves, so a concurrent tick cannot mint
        # a second exit.
        self._mark_sl_exiting(env, position, None)
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
                            ("EXIT", "REVERSAL_EXIT", "EMERGENCY_EXIT")), None)
            if working is not None:
                errors.append({"position_id": pos.position_id,
                               "error": "position_exit_already_working",
                               "order_id": working.order_id})
                continue
            # INVARIANT 4 — latch the position's local SL as EXITING before the
            # order is minted so no tick can race a second exit.
            self._mark_sl_exiting(env, pos, None)
            signal = Signal(
                signal_type=SignalType.SHORT if pos.is_long else SignalType.LONG,
                instrument=pos.instrument, strategy_id=pos.strategy_id,
                timestamp=time.time(), trigger_price=pos.current_mark or pos.average_entry,
                stop_price=pos.stop_price or 0.0, quantity=pos.quantity,
                metadata={"exit": True, "exit_reason": "operator_emergency_exit",
                          "trigger_state": "FIRED",
                          "trigger_source": "operator_action"})
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
                try:
                    runtime = env.runtimes.require(pos.strategy_id)
                    if runtime is not None and runtime.lifecycle is not None:
                        runtime.lifecycle.register_order(
                            pos.trade_id, order.order_id, "EMERGENCY_EXIT")
                except (KeyError, ValueError):
                    # The broker flatten remains available during degraded
                    # recovery even if the strategy runtime cache is absent.
                    pass
                self._persist_order(order, signal, env.name)
                engine.update_price(pos.instrument, pos.current_mark or pos.average_entry)
                engine.submit_order(order)
                self._persist_order(order, signal, env.name)
                if order.state.value in ("submitted", "acknowledged", "partially_filled", "filled"):
                    self._mark_sl_exiting(
                        env, pos, order.order_id,
                        getattr(order, "_broker_order_id", None))
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
    def _entry_priority_blocker(self, strategy_id: str,
                                env_name: Optional[str] = None,
                                rec: Optional[Any] = None) -> Optional[int]:
        """Priority gate for the order watcher (§7).

        Returns the highest-priority reason an ENTRY must not proceed right
        now, or None when entries are clear.

        * P0  — an unprotected open position exists (never add into it): either
                the position has no usable stop_price (SL_UNAVAILABLE) or no
                local SL is armed for it.
        * P1  — the position's local SL is mid-transition (TRIGGERED/EXITING).
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
        # Open / open-strategy position check (P0/P1) against the POSITION-OWNED
        # local SL — never against a broker-side protective order.
        pm = getattr(env, "position_manager", None)
        positions = []
        if pm is not None:
            try:
                positions = pm.get_positions_by_strategy(strategy_id) or []
            except Exception:
                positions = []
        open_pos = [p for p in positions if getattr(p, "is_open", False)]
        monitor = self._sl_monitor(env)
        for pos in open_pos:
            state = str(getattr(pos, "sl_state", None) or "NONE")
            if state in ("NONE", "SL_UNAVAILABLE"):
                return 0
            if state in ("TRIGGERED", "EXITING"):
                return 1
            if not monitor.active_for(getattr(pos, "position_id", "")):
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
            # Fail-closed tripwire: the retired broker-side role is never
            # fall-back-able and can never be submitted.
            return "stop_loss_not_fall_backable"
        return None

    def _release_live_sl(self, env, position, closing_order_id: str,
                         closing_role: Optional[str]) -> None:
        """Position-owned SL teardown when the position closes.

        There is no broker-side protective order to cancel any more: the SL
        lives in this process.  Clearing the monitor record IS the
        cancellation, and it happens on the broker-confirmed close, so the
        old SL can never fire against a later opposite position (INVARIANT 5).
        """
        if position is None:
            return
        self._clear_position_sl(env, position,
                                final_state=SLState.CLOSED.value,
                                reason="closed_by_%s" % (closing_role or "exit"))

    def _broker_flat_for_entry(self, env, signal) -> tuple[bool, dict]:
        """§25/§28 — EXIT-FIRST gate: an entry is placed only when the broker
        shows the strategy's instrument FLAT.  No orphan-protective-order check
        is needed any more (there is no broker SL), but the broker flat check
        stays authoritative.  Not verifiable -> fail-closed."""
        broker = getattr(env, "broker", None)
        positions_fn = getattr(broker, "positions", None)
        if not callable(positions_fn):
            return False, {"reason": "positions_api_unavailable"}
        try:
            rows = positions_fn()
        except Exception:
            return False, {"reason": "positions_api_failed"}
        if not isinstance(rows, (list, tuple)):
            return False, {"reason": "positions_api_invalid"}
        for p in rows:
            if not isinstance(p, dict):
                return False, {"reason": "positions_row_invalid"}
            if (p.get("instrument") or "") != signal.instrument:
                continue
            side = str(p.get("side") or "").upper()
            if side not in ("BUY", "LONG", "SELL", "SHORT"):
                return False, {"reason": "broker_position_side_invalid"}
            try:
                qty = int(p.get("quantity") or 0)
            except (TypeError, ValueError):
                return False, {"reason": "broker_position_quantity_invalid"}
            if qty != 0:
                # A broker row may be fanned out for multiple strategies.
                # Never net opposite rows to zero and mistake exposure for flat.
                return False, {"reason": "broker_position_open",
                               "side": side, "quantity": qty}
        return True, {}
