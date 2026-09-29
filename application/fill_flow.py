"""Broker fill admission and trade/position lifecycle application."""
from __future__ import annotations

import logging
import math
from typing import Optional

from execution.models import Fill
from strategies.types import StrategyState

log = logging.getLogger("trading_engine")


class FillFlowMixin:
    def _handle_fill(self, fill, signal_id: str | None, is_exit: bool | None = None,
                     env_name: Optional[str] = None) -> None:
        """Apply a fill exactly once, using explicit IDs throughout.

        All mutable state is resolved from the fill's OWN environment so a live
        fill can never mutate a paper position / ledger / dedup state and vice
        versa.
        """
        env = self._env_for(env_name)
        # A broker exit can be quarantined during the brief interval after the
        # position monitor has already removed its in-memory owner, while the
        # durable position/trade are still open.  If the exact broker fill is
        # persisted, its exact owned order is FILLED, and the still-open
        # position is EXITING, safely replay only that full-closing fill.
        replay_stale_exit = (env.mode == "LIVE"
                             and self._is_replayable_stale_exit(env, fill))
        # C1 — atomic in-process claim (check + hold in one critical section).
        # is_duplicate()+note_processed() was two separate lock acquisitions and
        # two racing callers could both pass the check before either claimed.
        if env.fill_dedup.is_duplicate(fill.fill_id) and not replay_stale_exit:
            return
        if replay_stale_exit:
            env.fill_dedup.unmark_processed(fill.fill_id)
        if not env.fill_dedup.claim(fill.fill_id):
            return
        # Phase 9.7 — broker-authoritative fill admission for LIVE: a fill
        # whose broker execution identity is already persisted, or whose broker
        # cumulative quantity is already fully accounted, is never applied a
        # second time (restart re-poll, WS+REST duplicate, replay).
        if env.mode == "LIVE" and not replay_stale_exit:
            verdict = self._reconcile_live_fill(env, fill)
            if verdict in ("duplicate", "already_synced", "divergence"):
                env.fill_dedup.mark_processed(fill.fill_id)
                return
        if fill.price <= 0 or (isinstance(fill.price, float) and not math.isfinite(fill.price)):
            env.fill_dedup.mark_processed(fill.fill_id)
            return

        # §34 — validate fill strategy identity via its owning runtime.
        # Unknown strategy ids are quarantined: the fill is never applied.
        runtime = None
        if env.runtimes is not None:
            try:
                runtime = env.runtimes.require(fill.strategy_id)
            except (KeyError, ValueError):
                runtime = None
        if runtime is None:
            self._quarantine_event("fill_unknown_strategy", {
                "fill_id": fill.fill_id, "order_id": fill.order_id,
                "strategy_id": fill.strategy_id, "trade_id": getattr(fill, "trade_id", ""),
                "execution_mode": env.mode})
            env.fill_dedup.mark_processed(fill.fill_id)
            return
        lifecycle = runtime.lifecycle
        position_manager = runtime.position_manager
        current = next((p for p in position_manager.get_positions_by_strategy(fill.strategy_id)
                        if p.instrument == fill.instrument and p.is_open), None)
        if env.mode == "LIVE":
            source_order = (env.execution_engine.get_order(fill.order_id)
                            if env.execution_engine is not None else None)
            source_role = str(getattr(source_order, "order_role", "") or "").upper()
            if source_role in ("EXIT", "STOP_LOSS", "REVERSAL_EXIT", "EMERGENCY_EXIT"):
                owner_pos = getattr(source_order, "parent_position_id", None)
                owner_trade = getattr(source_order, "lifecycle_id", None) or getattr(source_order, "trade_id", None)
                owner_gen = getattr(source_order, "position_generation", None)
                missing_lineage_recovered = False
                if replay_stale_exit and current is not None:
                    # Older persisted execution snapshots may omit position id
                    # and generation even though the canonical open position,
                    # pending reversal, exact exit order and broker fill all
                    # bind this fill to the same lifecycle.  The recovery
                    # validator has already checked those durable identities.
                    reversal = env.persistence.get_reversal_by_signal_id(
                        signal_id or "") if env.persistence is not None else None
                    missing_lineage_recovered = bool(
                        reversal
                        and reversal.get("old_exit_order_id") == fill.order_id
                        and reversal.get("old_position_id") == current.position_id
                        and reversal.get("old_trade_id") == current.trade_id
                        and str(reversal.get("status", "")).upper() == "PENDING_EXIT"
                        and (not owner_pos or owner_pos == current.position_id)
                        and (not owner_trade or owner_trade == current.trade_id)
                        and (owner_gen is None
                             or owner_gen == current.position_generation))
                if (current is None
                        or (not missing_lineage_recovered
                            and (not owner_pos
                                 or current.position_id != owner_pos
                                 or current.trade_id != owner_trade
                                 or current.position_generation != owner_gen))):
                    self._quarantine_event("stale_lifecycle_fill_rejected", {
                        "fill_id": fill.fill_id, "order_id": fill.order_id,
                        "strategy_id": fill.strategy_id,
                        "instrument": fill.instrument,
                        "parent_position_id": owner_pos,
                        "current_position_id": current.position_id if current else None,
                        "lifecycle_id": owner_trade,
                        "position_generation": owner_gen,
                        "execution_mode": env.mode}, persist=True)
                    if env.safe_mode is not None:
                        env.safe_mode.enter_safe_mode("position_mismatch",
                                                      "stale lifecycle fill")
                    env.fill_dedup.mark_processed(fill.fill_id)
                    return
        # Phase 9.7 — a PARTIAL-fill leg from the broker must never be
        # misread as an exit: direction is decided by side-vs-position for
        # broker fills, so same-side legs aggregate onto the open position
        # (one logical trade/position, multiple broker fills). Paper and
        # legacy paths keep the historic position-presence rule.
        broker_live = env.mode == "LIVE" and bool(getattr(fill, "broker_fill_id", None))
        same_side = (current is not None
                     and ((current.is_long and fill.side == "BUY")
                          or (not current.is_long and fill.side == "SELL")))
        if broker_live:
            is_exit = current is not None and not same_side
        else:
            is_exit = bool(is_exit) if is_exit is not None else current is not None
        if not is_exit:
            trade = lifecycle.get_trade(fill.trade_id) or lifecycle.resolve_trade_from_signal(signal_id)
            # §34 — entry fill must have an explicit trade reference in this
            # strategy's scope. A fill that resolves to a cross-strategy trade
            # (different strategy_id) is quarantined — never applied.
            if trade is None or trade.strategy_id != fill.strategy_id:
                self._quarantine_event("entry_fill_no_trade_or_mismatch", {
                    "fill_id": fill.fill_id, "order_id": fill.order_id,
                    "trade_id": getattr(fill, "trade_id", None),
                    "resolved_trade_id": getattr(trade, "trade_id", None) if trade else None,
                    "fill_strategy_id": fill.strategy_id,
                    "trade_strategy_id": getattr(trade, "strategy_id", None) if trade else None,
                    "signal_id": signal_id, "execution_mode": env.mode})
                env.fill_dedup.mark_processed(fill.fill_id)
                return
            account = env.account_engines[fill.strategy_id]
            margin = self._calculate_margin(fill.instrument, fill.price, fill.quantity)
            account_blocked = account.block_margin(margin)
            # Only block the global account if the per-strategy block
            # succeeded (avoids a double-release of the same margin).
            global_blocked = env.account_engine.block_margin(margin) if account_blocked else False
            if not (account_blocked and global_blocked):
                if account_blocked:
                    account.release_margin(margin)
                # The broker has ALREADY executed this entry (LIVE): refusing
                # the fill orphans a REAL position with no tracking and no
                # protective stop.  Over-allocate the margin (the position
                # already exists at the broker) and surface a margin-breach
                # alert, then let the normal booking below proceed so the
                # entry is tracked and _guard_live_position arms its stop.
                # Paper has no broker side, so a reset + drop stays correct.
                if env.mode == "LIVE":
                    account.block_margin(margin, force=True)
                    env.account_engine.block_margin(margin, force=True)
                    self.publish_event("margin_breach_entry", {
                        "fill_id": fill.fill_id, "trade_id": trade.trade_id,
                        "strategy_id": fill.strategy_id,
                        "instrument": fill.instrument,
                        "quantity": fill.quantity, "price": fill.price,
                        "margin": margin, "execution_mode": env.mode},
                        env_name=env.name)
                    try:
                        self.telegram.on_risk_alert({
                            "kind": "margin_breach_entry",
                            "message": (
                                f"MARGIN BREACH: broker-confirmed entry "
                                f"{fill.fill_id} {fill.instrument} "
                                f"x{fill.quantity} @ {fill.price} — margin "
                                f"{margin:.2f} over limit; position tracked "
                                f"and will be protected."),
                        })
                    except Exception:
                        pass
                else:
                    self._reset_strategy_state(fill.strategy_id, env_name=env.name)
                    return
            if broker_live and same_side:
                # Phase 9.7 — aggregate the leg onto the open position (one
                # logical trade, multiple broker fills) and persist the leg.
                old_qty = int(current.quantity)
                old_avg = float(current.average_entry or 0.0)
                new_qty = old_qty + int(fill.quantity)
                new_avg = ((old_avg * old_qty + float(fill.price) * int(fill.quantity))
                           / new_qty) if new_qty else old_avg
                current.quantity = new_qty
                current.average_entry = round(new_avg, 4)
                current.entry_fill_ids.append(fill.fill_id)
                current.margin = float(getattr(current, "margin", 0.0) or 0.0) + margin
                self._persist_fill(fill, trade.trade_id, signal_id, env_name)
                lifecycle.register_entry_fill(trade.trade_id, fill.fill_id,
                                              current.average_entry, fill.timestamp)
                if env.trade_ledger is not None:
                    try:
                        env.trade_ledger.record_fill(
                            trade_id=trade.trade_id,
                            fill_id=fill.fill_id,
                            order_id=fill.order_id,
                            side="BUY" if current.is_long else "SELL",
                            quantity=fill.quantity,
                            price=fill.price,
                            timestamp=fill.timestamp,
                            is_entry=True,
                        )
                    except Exception as e:
                        log.error("[Engine] ledger projection write failed for %s: %s",
                                  trade.trade_id, e)
                self.publish_event("position_augmented", {
                    "trade_id": trade.trade_id,
                    "position_id": current.position_id,
                    "fill_id": fill.fill_id,
                    "strategy_id": fill.strategy_id,
                    "instrument": fill.instrument,
                    "quantity": new_qty, "average_entry": current.average_entry,
                    "execution_mode": env.mode}, env_name=env.name)
                # Position-owned SL: arm the local monitor on THIS position's
                # own stop_price.  No broker-side protective order exists.
                self._arm_position_sl(env, current, source="entry_fill_augment")
                self._notify_entry_fill(fill, current, env, signal_id)
                self._sync_strategy_on_entry_fill(
                    env, fill.strategy_id, "LONG" if current.is_long else "SHORT",
                    current)
            else:
                # stop_price: prefer the entry order's own execution plan
                # (planned_sl).  For a REVERSAL_ENTRY that is the NEW opposite
                # side's stop computed from the entry signal (instance.py
                # plan_for/create_order), never the stale pre-reversal stop
                # still held by strategy.state / the shared signals row.
                entry_order = (env.execution_engine.get_order(fill.order_id)
                               if env.execution_engine is not None else None)
                _stop = (getattr(entry_order, "planned_sl", None)
                         if entry_order is not None else None)
                if _stop is None:
                    _stop = getattr(env.strategies.get(fill.strategy_id),
                                    "stop_price", None)
                if _stop is None and signal_id and env.persistence is not None:
                    try:
                        _sig = env.persistence.query_one(
                            "SELECT stop_price FROM signals WHERE signal_id=?",
                            (signal_id,))
                        if _sig and _sig.get("stop_price") is not None:
                            _stop = float(_sig["stop_price"])
                            # Restore strategy stop_price for future use
                            env.strategies[fill.strategy_id].stop_price = _stop
                    except Exception:
                        log.error("[Engine] SL recovery failed for signal %s: %s",
                                  signal_id, exc_info=True)
                if _stop is None and signal_id:
                    # A broker-confirmed entry fill with no recoverable stop is
                    # an UNPROTECTED live position: the local tick stop monitor
                    # has no stop level, so the exit-on-stop path is dead.
                    # Never let that pass silently.
                    log.error("[Engine] entry fill %s has NO stop price "
                              "(planned_sl/strategy/signal all empty) - "
                              "position will be UNPROTECTED",
                              fill.fill_id)
                    self.publish_event("entry_stop_missing", {
                        "fill_id": fill.fill_id, "signal_id": signal_id,
                        "strategy_id": fill.strategy_id,
                        "instrument": fill.instrument,
                        "reason": "no_stop_price_recoverable",
                        "execution_mode": env.mode}, env_name=env.name)
                position = position_manager.open_position(
                    fill, multiplier=fill.multiplier, margin=margin,
                    stop_price=_stop,
                    entry_signal_id=signal_id, trade_id=trade.trade_id,
                    position_generation=(getattr(entry_order, "position_generation", None)
                                         if entry_order is not None else None),
                    entry_order_id=(getattr(entry_order, "order_id", None)
                                    or fill.order_id),
                )
                fill.position_id = position.position_id
                fill.lifecycle_id = trade.trade_id
                fill.position_generation = position.position_generation
                self._persist_fill(fill, trade.trade_id, signal_id, env_name)
                self._persist_position(position, env_name)
                lifecycle.register_entry_fill(trade.trade_id, fill.fill_id,
                                              fill.price, fill.timestamp)
                lifecycle.register_position(trade.trade_id, position.position_id)
                # Keep the analytics read-model (trade ledger) in lock-step at
                # entry: the OPEN projection must exist as soon as the position
                # opens so a crash/restart never has a position without a
                # ledger trade.
                if env.trade_ledger is not None:
                    try:
                        if env.trade_ledger.get_trade(trade.trade_id) is None:
                            env.trade_ledger.create_trade(
                                strategy_id=fill.strategy_id,
                                instrument=fill.instrument,
                                side="LONG" if position.is_long else "SHORT",
                                entry_quantity=position.quantity,
                                signal_time=fill.timestamp,
                                trigger_price=position.average_entry,
                                stop_price=getattr(position, "stop_price", None) or 0.0,
                                multiplier=fill.multiplier,
                                entry_reason="signal",
                                trade_id=trade.trade_id,
                                position_id=position.position_id,
                            )
                        env.trade_ledger.record_fill(
                            trade_id=trade.trade_id,
                            fill_id=fill.fill_id,
                            order_id=fill.order_id,
                            side="BUY" if position.is_long else "SELL",
                            quantity=fill.quantity,
                            price=fill.price,
                            timestamp=fill.timestamp,
                            is_entry=True,
                        )
                    except Exception as e:
                        log.error("[Engine] ledger projection write failed for %s: %s",
                                  trade.trade_id, e)
                self.publish_event("position_opened", {"trade_id": trade.trade_id,
                    "position_id": position.position_id, "fill_id": fill.fill_id,
                    "strategy_id": fill.strategy_id, "instrument": fill.instrument,
                    "execution_mode": env.mode}, env_name=env.name)
                # Position-owned SL: the broker-confirmed entry fill opened THIS
                # position, so its stop is armed now.  No broker SL order.
                self._arm_position_sl(env, position, source="entry_fill")
                # REVERSAL — the broker-confirmed NEW opposite entry creates
                # the NEW position; record its fill + SL on the reversal.
                if getattr(entry_order, "order_role", "") == "REVERSAL_ENTRY":
                    self._update_reversal_entry_fill(
                        env, getattr(entry_order, "reversal_parent_signal_id", None)
                        or signal_id, fill,
                                                     position)
                self._notify_entry_fill(fill, position, env, signal_id)
                self._sync_strategy_on_entry_fill(
                    env, fill.strategy_id, "LONG" if position.is_long else "SHORT",
                    position)
        else:
            if current is None or not current.trade_id:
                self._quarantine_event("exit_fill_no_position", {
                    "fill_id": fill.fill_id, "order_id": fill.order_id,
                    "strategy_id": fill.strategy_id,
                    "instrument": fill.instrument, "trade_id": getattr(fill, "trade_id", ""),
                    "execution_mode": env.mode})
                env.fill_dedup.mark_processed(fill.fill_id)
                return
            close_manager = env.trade_close_manager or self._trade_close_manager
            if close_manager is None:
                raise RuntimeError("trade close manager is not initialized")
            # §27 — stop-loss closes carry NO strategy signal: exit_signal_id
            # must stay NULL and exit_reason must be the canonical STOP_LOSS.
            # Reversal/signal exits keep their explicit exit signal id.
            raw_reason = (env.strategies[fill.strategy_id].last_exit_reason
                          or "signal_exit")
            is_stop_loss = (raw_reason or "").lower() in ("stop_loss_hit", "stop_loss")
            # Defensive: a STOP_LOSS-role order can no longer be created (the
            # live engine rejects it outright), but if one ever appeared its
            # fill is still a stop-loss regardless of stale strategy state.
            sl_order = env.execution_engine.get_order(fill.order_id) \
                if env.execution_engine is not None else None
            sl_role = getattr(sl_order, "order_role", None)
            if sl_role == "STOP_LOSS":
                is_stop_loss = True
            exit_reason = "STOP_LOSS" if is_stop_loss else raw_reason
            exit_signal_id = "" if is_stop_loss else (signal_id or "")
            if sl_role == "EMERGENCY_EXIT":
                # Emergency market close: canonical EMERGENCY_EXIT reason, no
                # strategy signal is ever attached.
                exit_reason = "EMERGENCY_EXIT"
                exit_signal_id = ""
            current.exit_order_id = fill.order_id
            # A PARTIAL exit leaves the position OPEN on the remaining
            # quantity; the local SL and strategy position state must stay
            # armed (at the new, smaller quantity) until the position is
            # fully gone (§12).
            exit_qty = int(fill.quantity or 0)
            pos_qty = int(current.quantity or 0)
            if 0 < exit_qty < pos_qty:
                self._handle_partial_exit(
                    env, fill, current, trade, signal_id,
                    exit_reason, exit_signal_id)
                env.fill_dedup.mark_processed(fill.fill_id)
                return
            if exit_qty > pos_qty:
                self._quarantine_event("exit_fill_exceeds_position", {
                    "fill_id": fill.fill_id, "order_id": fill.order_id,
                    "position_id": current.position_id,
                    "position_quantity": pos_qty, "fill_quantity": exit_qty,
                    "execution_mode": env.mode}, persist=True)
                if env.safe_mode is not None:
                    env.safe_mode.enter_safe_mode("position_mismatch",
                                                  "exit fill exceeds local position")
                env.fill_dedup.mark_processed(fill.fill_id)
                return
            self._release_live_sl(env, current, fill.order_id, sl_role)
            result = close_manager.close_position(
                fill, current, fill.strategy_id, fill.multiplier,
                exit_reason=exit_reason,
                exit_signal_id=exit_signal_id or None,
            )
            if result is False:
                return
            if env.persistence is not None and hasattr(env.persistence, "close_position_record"):
                try:
                    env.persistence.close_position_record(current)
                except Exception as e:
                    log.error("[Engine] close_position_record failed for %s: %s",
                              current.position_id, e)
            lifecycle.register_exit_fill(current.trade_id, fill.fill_id, fill.price,
                fill.timestamp, exit_signal_id, exit_reason=exit_reason)
            lifecycle.close_trade(current.trade_id, result["gross_pnl"], result["charges"], result["net_pnl"])
            # Phase 9.7 — mirror the exit fill in the reconciliation ledger too.
            self._update_fill_reconciliation(env, fill)
            # REVERSAL — the broker-confirmed old-position exit is the
            # transition point: record the exit fill on the reversal record.
            # Only when it is provably flat does the new opposite entry flow.
            if sl_role == "REVERSAL_EXIT" or (exit_reason
                                              and "reversal" in exit_reason.lower()):
                self._update_reversal_exit_fill(env, signal_id, fill)
            # Reversal exits arm an OPPOSITE pending breakout entry which must
            # survive the close: keep it if the strategy has one armed.
            pending_armed = env.strategies[fill.strategy_id].pending_entry is not None
            self._reset_strategy_state(fill.strategy_id, keep_pending=pending_armed,
                                       env_name=env.name)
        env.fill_dedup.mark_processed(fill.fill_id)

    def _is_replayable_stale_exit(self, env, fill) -> bool:
        """Prove a previously quarantined broker exit is still unapplied."""
        try:
            if not getattr(fill, "broker_fill_id", None):
                return False
            pm = getattr(env, "position_manager", None)
            engine = getattr(env, "execution_engine", None)
            persistence = getattr(env, "persistence", None)
            if pm is None or engine is None or persistence is None:
                return False
            persisted = persistence.fill_by_broker_fill_id(fill.broker_fill_id)
            if not persisted:
                return False
            order = engine.get_order(fill.order_id)
            if order is None:
                return False
            role = str(getattr(order, "order_role", "") or "").upper()
            state_obj = getattr(order, "state", "")
            state = str(getattr(state_obj, "value", state_obj)).lower()
            if state != "filled":
                return False
            if int(getattr(fill, "quantity", 0) or 0) < int(
                    getattr(order, "quantity", 0) or 0):
                return False
            positions = pm.get_positions_by_strategy(fill.strategy_id) or []
            current = next((p for p in positions
                            if p.instrument == fill.instrument and p.is_open), None)
            if current is None:
                # After restart, the saved engine snapshot may already contain
                # the stale CLOSED/zero-quantity snapshot even though the
                # canonical DB position/trade stayed OPEN.  Reconstruct only
                # when the exact persisted reversal exit and broker-flat state
                # jointly prove this is an unbooked close.
                db_positions = persistence.get_open_positions(fill.strategy_id)
                reversals = persistence.get_reversals(fill.strategy_id, limit=100)
                reversal = next((r for r in reversals
                                 if r.get("old_exit_order_id") == fill.order_id
                                 and str(r.get("status", "")).upper()
                                     == "PENDING_EXIT"), None)
                owner_id = (getattr(order, "parent_position_id", None)
                            or getattr(order, "position_id", None)
                            or (reversal or {}).get("old_position_id"))
                owner_row = next((p for p in db_positions
                                  if p.get("position_id") == owner_id
                                  and p.get("instrument") == fill.instrument
                                  and p.get("exit_order_id") == fill.order_id), None)
                if owner_row is None:
                    return False
                reversal = next((r for r in reversals
                                 if r.get("old_position_id") == owner_id
                                 and r.get("old_trade_id") == owner_row.get("trade_id")
                                 and r.get("old_exit_order_id") == fill.order_id
                                 and str(r.get("status", "")).upper()
                                     == "PENDING_EXIT"), None)
                if reversal is None or (role not in {
                        "EXIT", "STOP_LOSS", "REVERSAL_EXIT", "EMERGENCY_EXIT"}
                        and role):
                    return False
                broker_rows = env.broker.positions()
                for row in broker_rows or []:
                    if row.get("instrument") != fill.instrument:
                        continue
                    qty = int(row.get("quantity") or 0)
                    side = str(row.get("side") or "").upper()
                    signed = qty if side in ("BUY", "LONG") else -qty
                    if signed:
                        return False
                from datetime import datetime
                from portfolio.position_manager import Position, PositionSide
                entry_time = owner_row.get("entry_time")
                if isinstance(entry_time, str):
                    entry_time = datetime.fromisoformat(
                        entry_time.replace("Z", "+00:00")).timestamp()
                trade_row = next((t for t in persistence.get_trades()
                                  if t.get("trade_id") == owner_row.get("trade_id")
                                  and str(t.get("status", "")).upper() == "OPEN"), None)
                if trade_row is None:
                    return False
                entry_fill_id = trade_row.get("entry_fill_id")
                from portfolio.position_manager import PositionStatus
                current = Position(
                    position_id=owner_id,
                    strategy_id=fill.strategy_id,
                    instrument=fill.instrument,
                    side=PositionSide(str(owner_row.get("side", "LONG")).upper()),
                    quantity=int(owner_row.get("quantity") or 0),
                    average_entry=float(owner_row.get("average_entry_price") or 0),
                    entry_timestamp=float(entry_time or 0),
                    entry_fill_ids=[entry_fill_id] if entry_fill_id else [],
                    stop_price=owner_row.get("stop_price"),
                    trade_id=owner_row.get("trade_id"),
                    status=PositionStatus.OPEN,
                    sl_state="EXITING",
                    sl_protected_at=(float(owner_row["sl_protected_at"])
                                     if owner_row.get("sl_protected_at") else None),
                    entry_order_id=owner_row.get("entry_order_id"),
                    exit_order_id=fill.order_id,
                    position_generation=int(owner_row.get("position_generation") or 0),
                    exit_started=True,
                    lifecycle_id=owner_row.get("lifecycle_id")
                        or owner_row.get("trade_id"),
                )
                if (current.quantity != int(fill.quantity or 0)
                        or (current.is_long and fill.side != "SELL")
                        or (not current.is_long and fill.side != "BUY")):
                    return False
                pm.restore_open_position(current)
            if str(getattr(current, "sl_state", "")).upper() != "EXITING":
                return False
            if (getattr(order, "parent_position_id", None)
                    and getattr(order, "parent_position_id") != current.position_id):
                return False
            if ((getattr(order, "lifecycle_id", None)
                    or getattr(order, "trade_id", None))
                    and (getattr(order, "lifecycle_id", None)
                         or getattr(order, "trade_id", None)) != current.trade_id):
                return False
            if (getattr(order, "position_generation", None) is not None
                    and getattr(order, "position_generation")
                        != current.position_generation):
                return False
            trade = env.runtimes.require(fill.strategy_id).lifecycle.get_trade(
                current.trade_id)
            if trade is None or str(getattr(trade, "status", "")).upper() not in (
                    "OPEN", "ACTIVE"):
                return False
            return True
        except Exception:
            log.exception("[Engine] stale exit fill recovery validation failed")
            return False
    def _exit_order_still_working(self, env, position) -> bool:
        """True when a live exit order still has unfilled quantity for this
        position.  Used to decide between "SL stays EXITING" and "SL re-arms"
        after a partial fill (§12 / §10)."""
        engine = getattr(env, "execution_engine", None)
        if engine is None or position is None:
            return False
        oid = getattr(position, "exit_order_id", None)
        if not oid:
            return False
        order = engine.get_order(oid)
        if order is None:
            return False
        if str(getattr(order.state, "value", order.state)).lower() in (
                "filled", "canceled", "cancelled", "rejected"):
            return False
        filled = sum(int(getattr(f, "quantity", 0) or 0)
                     for f in (getattr(order, "fills", None) or []))
        return filled < int(getattr(order, "quantity", 0) or 0)

    def _handle_partial_exit(self, env, fill, position, trade, signal_id: Optional[str],
                             exit_reason: str, exit_signal_id: str) -> None:
        """C2 — apply a partial exit fill (exit qty < held qty).

        Books realized P&L ONLY on the portion that exited, releases margin
        proportionally, records the leg (persistence, reconciliation ledger,
        lifecycle exit-fill) and reduces the open position — without closing
        the trade, the position or the protective SL.  The strategy position
        state stays armed for the remaining quantity.
        """
        runtime = None
        if getattr(env, "runtimes", None) is not None:
            try:
                runtime = env.runtimes.get(fill.strategy_id) or \
                    env.runtimes[fill.strategy_id]
            except Exception:
                runtime = None
        pm = getattr(runtime, "position_manager", None) or \
            getattr(env, "position_manager", None)
        if pm is None:
            self._quarantine_event("partial_exit_no_position_manager", {
                "fill_id": fill.fill_id, "strategy_id": fill.strategy_id,
                "instrument": fill.instrument,
                "execution_mode": env.mode}, persist=True)
            return
        try:
            remaining = pm.reduce_position(
                position.position_id, fill, reason=exit_reason,
                exit_signal_id=exit_signal_id or None)
        except Exception as e:
            self._quarantine_event("partial_exit_reduce_failed", {
                "fill_id": fill.fill_id, "strategy_id": fill.strategy_id,
                "instrument": fill.instrument, "error": str(e),
                "execution_mode": env.mode}, persist=True)
            return
        # P&L on the exiting quantity only (entry side == position side).
        pnl_engine = env.pnl_engines.get(fill.strategy_id)
        if pnl_engine is not None:
            entry_fill = Fill(
                fill_id=(position.entry_fill_ids[0]
                         if position.entry_fill_ids else ""),
                order_id="",
                instrument=position.instrument,
                side="BUY" if position.is_long else "SELL",
                quantity=int(fill.quantity),
                price=float(position.average_entry or 0.0),
                timestamp=float(position.entry_timestamp or 0.0),
                strategy_id=position.strategy_id,
                multiplier=fill.multiplier,
            )
            gross_pnl, charges, net_pnl = pnl_engine.calculate_realized_pnl(
                entry_fill=entry_fill, exit_fill=fill, multiplier=fill.multiplier)
        else:
            gross_pnl, charges, net_pnl = 0.0, 0.0, 0.0
        try:
            self._persist_fill(fill, trade.trade_id, signal_id, env.name)
        except Exception as e:
            log.warning("[Engine] partial exit fill persist failed: %s", e)
        try:
            self._persist_position(position, env.name)
        except Exception as e:
            log.warning("[Engine] partial exit position persist failed: %s", e)
        # Account: book the realized leg, release the exited margin share (the
        # position's margin was already reduced by reduce_position).
        strat_account = env.account_engines.get(fill.strategy_id)
        try:
            released_expected = max(
                0.0, float(position.margin) * int(fill.quantity)
                / int(remaining)) if int(remaining) > 0 else 0.0
            if strat_account is not None:
                strat_account.update_realized_pnl(net_pnl, charges)
                strat_account.release_margin(released_expected)
            if env.account_engine is not None:
                env.account_engine.update_realized_pnl(net_pnl, charges)
                env.account_engine.release_margin(released_expected)
        except Exception as e:
            log.warning("[Engine] partial exit account update failed: %s", e)
        try:
            if env.risk_engine is not None:
                env.risk_engine.update_daily_pnl(net_pnl)
        except Exception as e:
            log.warning("[Engine] partial exit risk update failed: %s", e)
        # Reconciliation ledger mirror leg (position still open: no close_trade).
        if env.trade_ledger is not None:
            try:
                env.trade_ledger.record_fill(
                    trade_id=trade.trade_id, fill_id=fill.fill_id,
                    order_id=fill.order_id, side=fill.side,
                    quantity=int(fill.quantity), price=fill.price,
                    timestamp=fill.timestamp, is_entry=False)
            except Exception as e:
                log.error("[Engine] partial exit ledger write failed for %s: %s",
                          trade.trade_id, e)
        try:
            self._update_fill_reconciliation(env, fill)
        except Exception as e:
            log.debug("[Engine] partial exit reconcile mirror skipped: %s", e)
        try:
            lifecycle = getattr(runtime, "lifecycle", None)
            if lifecycle is not None:
                lifecycle.register_exit_fill(
                    trade.trade_id, fill.fill_id, fill.price, fill.timestamp,
                    exit_signal_id, exit_reason=exit_reason)
        except Exception as e:
            log.warning("[Engine] partial exit lifecycle update failed: %s", e)
        self.publish_event("position_partially_exited", {
            "trade_id": trade.trade_id,
            "position_id": position.position_id,
            "fill_id": fill.fill_id,
            "strategy_id": fill.strategy_id,
            "instrument": fill.instrument,
            "exited_quantity": int(fill.quantity),
            "remaining_quantity": int(remaining),
            "gross_pnl": gross_pnl, "charges": charges, "net_pnl": net_pnl,
            "execution_mode": env.mode}, env_name=env.name)
        # §12 — the position stays OPEN on the remainder.  If the exit order
        # still has working quantity the SL stays EXITING (one authoritative
        # exit state); otherwise the local SL re-arms at the REMAINING quantity.
        self._rearm_sl_after_partial_exit(
            env, position,
            exit_still_working=self._exit_order_still_working(env, position))
    def _live_account_snapshot(self, env) -> dict:
        """Best-effort REAL Dhan account snapshot for LIVE notifications.

        Prefers the poller's cached /fundlimit snapshot (updated every few
        seconds, zero cost); falls back to one fresh broker call bounded to
        5s via a daemon thread so a hung REST call can never stall the caller.
        Returns {} when the account cannot be read.
        """
        if not getattr(env, "is_live", False):
            return {}
        poller = getattr(env, "poller", None)
        if poller is not None:
            try:
                cached = getattr(poller, "_last_account", {}) or {}
            except Exception:
                cached = {}
            if isinstance(cached, dict) and cached.get("equity"):
                return cached
        broker = getattr(env, "broker", None)
        if broker is None or not hasattr(broker, "account_status"):
            return {}
        import threading
        result: dict = {}

        def _fetch():
            try:
                acct = broker.account_status() or {}
            except Exception:  # noqa: BLE001
                acct = {}
            if isinstance(acct, dict) and acct.get("equity"):
                result.update(acct)

        t = threading.Thread(target=_fetch, daemon=True)
        t.start()
        t.join(timeout=5.0)
        return result
    def _sync_strategy_on_entry_fill(self, env, strategy_id: str, side: str,
                                     position) -> None:
        """Settle an entry fill into the strategy's execution state.

        The pending-breakout model transitions the strategy optimistically
        inside ``_tick_entry_trigger`` before the order is even submitted
        (position_side/state set at trigger-cross).  The Appendix I
        local-trigger model does NO strategy state transition at signal
        time — the LIMIT intentionally rests flat at the broker — so the fill
        arriving from the broker is the moment the strategy becomes an open
        position.  This mirror-step is idempotent, so it is safe for both.
        """
        strat = env.strategies.get(strategy_id)
        if strat is None:
            return
        strat.position_side = side
        strat.current_position_id = position.position_id
        strat.position_generation = position.position_generation
        strat.position_quantity = position.quantity
        strat.current_trade_id = position.trade_id
        strat.state = (StrategyState.LONG_POSITION if side == "LONG"
                       else StrategyState.SHORT_POSITION)
        if getattr(position, "stop_price", None) is not None:
            strat.stop_price = position.stop_price
        strat.just_entered = True
        strat.pending_entry = None
        strat.pending_exit_trigger = None
        setattr(strat, "stop_exit_submitted", False)
    def _notify_entry_fill(self, fill, position, env, signal_id: Optional[str]) -> None:
        try:
            strat_obj = env.strategies.get(fill.strategy_id)
            account = env.account_engines.get(fill.strategy_id)
            strategy_dict = {
                "entry_value": fill.price * fill.quantity * fill.multiplier,
                "stop_price": getattr(position, "stop_price", None) or 0.0,
                "htf_dema_atr": getattr(strat_obj, 'htf_dema_atr', 0) if strat_obj else 0,
            }
            account_dict = {
                "equity": account.equity if account else 0.0,
                "used_margin": account.used_margin if account else 0.0,
                "source": "engine",
            }
            # LIVE fills must show the REAL Dhan account, never the configured
            # starting capital.
            if env.is_live:
                broker_acct = self._live_account_snapshot(env)
                if broker_acct:
                    account_dict = {
                        "equity": broker_acct.get("equity", 0.0),
                        "used_margin": broker_acct.get("used_margin", 0.0),
                        "available_margin": broker_acct.get("available_margin", 0.0),
                        "realized_pnl": broker_acct.get("realized_pnl", 0.0),
                        "unrealized_pnl": broker_acct.get("unrealized_pnl", 0.0),
                        "net_pnl": broker_acct.get("net_pnl", 0.0),
                        "dhan_client_id": broker_acct.get("dhan_client_id", ""),
                        "source": "dhan",
                    }
            self.telegram.on_fill({
                "side": "BUY" if position.is_long else "SELL",
                "instrument": fill.instrument,
                "strategy_id": fill.strategy_id,
                "price": fill.price,
                "quantity": fill.quantity,
                "multiplier": fill.multiplier,
                "order_id": fill.order_id,
                "execution_mode": env.mode,
            }, strategy_dict, account_dict)
        except Exception as e:
            log.warning("[Engine] telegram fill notify failed: %s", e)
