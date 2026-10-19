"""Position exits, reversal persistence, and broker-confirmed ownership."""
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

    @staticmethod
    def _reversal_parent_signal_id(env, order) -> Optional[str]:
        """Resolve reversal lineage through a confirmed fallback child order."""
        parent_signal_id = getattr(order, "reversal_parent_signal_id", None)
        if parent_signal_id or not getattr(order, "original_order_id", None):
            return parent_signal_id
        engine = getattr(env, "execution_engine", None)
        root = engine.get_order(order.original_order_id) if engine else None
        if str(getattr(root, "order_role", "")).upper() == "REVERSAL_ENTRY":
            return getattr(root, "reversal_parent_signal_id", None)
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
                                    fill, position) -> Optional[dict]:
        """Stamp the broker-confirmed NEW entry fill ONLY on a new position.

        The record stays EXIT_FILLED (never COMPLETE) until this broker
        confirmation arrives; the reverse directional order's fill always
        creates/occupies the NEW position row.
        """
        rev = self._find_reversal(env, signal_id)
        if rev is None:
            return None
        try:
            previous_status = str(rev.get("status") or "").upper()
            filled_order = (env.execution_engine.get_order(fill.order_id)
                            if getattr(env, "execution_engine", None) is not None
                            else None)
            is_fallback = str(getattr(filled_order, "order_role", "")).upper() == "FALLBACK_MARKET"
            # The current strategy has no separate stop order or local stop
            # monitor. The reversal completes after the broker confirms the
            # new entry fill.
            fields = {
                "new_entry_order_id": getattr(fill, "order_id", None),
                "new_entry_fill_price": getattr(
                    position, "average_entry", None) or fill.price,
                "new_entry_filled_quantity": int(
                    getattr(position, "quantity", None) or fill.quantity),
                "new_entry_broker_status": "FILLED",
                "new_position_id": getattr(position, "position_id", None),
                "entry_fill_confirmed_at": datetime.now(timezone.utc).isoformat(),
                "status": "COMPLETE",
            }
            broker_order_id = getattr(filled_order, "_broker_order_id", None)
            if broker_order_id:
                fields["new_broker_order_id"] = broker_order_id
            if is_fallback:
                fields.update({"fallback_used": 1, "fallback_status": "FILLED"})
            env.persistence.update_reversal(rev["reversal_id"], fields)
            updated = self._find_reversal(env, signal_id) or {**rev, **fields}
            if (str(updated.get("status") or "").upper() == "COMPLETE"
                    and previous_status != "COMPLETE"):
                self._notify_reversal_complete(env, updated, position=position)
            return updated
        except Exception as e:
            log.warning("[Engine] reversal entry-fill stamp failed for %s: %s",
                        rev["reversal_id"], e)
            return None

    def _notify_reversal_complete(self, env, reversal: dict,
                                  position=None) -> None:
        """Send one summary when both broker-confirmed reversal legs complete."""
        if str((reversal or {}).get("status") or "").upper() != "COMPLETE":
            return
        notifier = getattr(getattr(self, "telegram", None),
                            "on_reversal_complete", None)
        if not callable(notifier):
            return
        new_side = ("LONG" if getattr(position, "is_long", False) else "SHORT") \
            if position is not None else str(
                (reversal or {}).get("new_side") or "").upper()
        old_side = {"LONG": "SHORT", "SHORT": "LONG"}.get(new_side, "?")
        try:
            notifier({
                "reversal_id": reversal.get("reversal_id"),
                "signal_id": reversal.get("signal_id"),
                "strategy_id": reversal.get("strategy_id"),
                "instrument": reversal.get("instrument"),
                "old_side": old_side,
                "new_side": new_side or "?",
                "quantity": reversal.get("new_entry_filled_quantity"),
                "old_exit_fill_price": reversal.get("old_exit_fill_price"),
                "new_entry_fill_price": reversal.get("new_entry_fill_price"),
                "old_exit_order_id": reversal.get("old_exit_order_id"),
                "new_broker_order_id": reversal.get("new_broker_order_id"),
                "execution_mode": getattr(env, "mode", None),
                "status": "COMPLETE",
            })
        except Exception as e:
            log.warning("[Telegram] reversal completion alert failed for %s: %s",
                        reversal.get("reversal_id"), e)
    def emergency_exit_all(self, env_name: str = "live",
                           instrument: Optional[str] = None,
                           exit_reason: str = "operator_emergency_exit") -> dict:
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
            pos.exit_started = True
            signal = Signal(
                signal_type=SignalType.SHORT if pos.is_long else SignalType.LONG,
                instrument=pos.instrument, strategy_id=pos.strategy_id,
                timestamp=time.time(), trigger_price=pos.current_mark or pos.average_entry,
                stop_price=0.0, quantity=pos.quantity,
                metadata={"exit": True, "strategy_type": "supertrend",
                          "market_on_signal_close": True,
                          "exit_reason": str(exit_reason or "operator_emergency_exit"),
                          "trigger_state": "FIRED",
                          "trigger_source": "operator_action",
                          "operator_action": True})
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
                    pos.exit_started = True
                    pos.exit_order_id = order.order_id
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

        * P0  — an open position exists; never add exposure to it.
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
        # The open-position check prevents adding exposure to a running trade.
        pm = getattr(env, "position_manager", None)
        if pm is None:
            if rec is not None:
                rec.extra["priority_blocker_error"] = "position_manager_unavailable"
            return 0
        try:
            positions = pm.get_positions_by_strategy(strategy_id) or []
        except Exception as exc:
            if rec is not None:
                rec.extra["priority_blocker_error"] = (
                    f"position_lookup_failed: {exc}")
            return 0
        open_pos = [p for p in positions if getattr(p, "is_open", False)]
        if open_pos:
            if rec is not None:
                rec.extra.pop("priority_blocker_error", None)
            return 0
        if rec is not None:
            rec.extra.pop("priority_blocker_error", None)
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
                raw_state = getattr(o, "state", "") or ""
                # OrderState is an Enum in the engine.  str(enum) renders as
                # e.g. "OrderState.FILLED", which used to leave completed
                # exit/reversal legs looking active here and LOCK the entry
                # watcher (including its safe cancel-confirm-MARKET fallback).
                # Match OrderWatcher._highest_priority_blocker by comparing
                # the normalized value instead.
                state_s = str(getattr(raw_state, "value", raw_state)).lower()
                if state_s in ("filled", "canceled", "cancelled", "rejected",
                               "expired"):
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
        return None


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
