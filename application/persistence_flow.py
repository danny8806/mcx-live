"""Durable persistence and strategy state transitions for TradingEngine."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from core.lifecycle import PendingOrderState, transition_pending_state
from strategies.types import StrategyState

log = logging.getLogger("trading_engine")


class PersistenceFlowMixin:
    def _persist_signal(self, signal, signal_type: str,
                        env_name: Optional[str] = None) -> None:
        env = self._env_for(env_name)
        if not env.persistence:
            return
        # F3 — persist the full frozen signal-candle snapshot (Phase 4 signal
        # context) into the signals row: dedicated OHLC / indicator columns plus
        # the JSON blobs.  Previously only the base identity fields were written
        # and later lifecycle writes were dropped by INSERT-OR-IGNORE, leaving
        # every candle column NULL and the blobs absent.
        ctx = getattr(signal, "context", None)
        metadata = getattr(signal, "metadata", None) or {}
        candle_blob = None
        indicator_blob = None
        if ctx is not None:
            candle_blob = {
                "timestamp": ctx.timestamp, "open": ctx.open, "high": ctx.high,
                "low": ctx.low, "close": ctx.close,
            }
            indicator_blob = {
                "fast_dema": ctx.dema, "fast_atr": ctx.atr,
                "htf_value": ctx.htf_value, "mid_value": ctx.mid_value,
            }
        env.persistence.save_signal({
            "signal_id": signal.signal_id, "strategy_id": signal.strategy_id,
            "instrument": signal.instrument, "side": signal.signal_type.value,
            "signal_type": signal_type, "timestamp": signal.timestamp,
            "trigger_price": signal.trigger_price, "stop_price": signal.stop_price,
            "quantity": signal.quantity,
            "candle_timestamp": ctx.timestamp if ctx is not None else None,
            "open": ctx.open if ctx is not None else None,
            "high": ctx.high if ctx is not None else None,
            "low": ctx.low if ctx is not None else None,
            "close": ctx.close if ctx is not None else None,
            "htf_value": ctx.htf_value if ctx is not None else None,
            "mid_value": ctx.mid_value if ctx is not None else None,
            "fast_dema": ctx.dema if ctx is not None else None,
            "fast_atr": ctx.atr if ctx is not None else None,
            "signal_reason": metadata.get("reason") or metadata.get("trigger_reason"),
            "candle_data": candle_blob,
            "indicator_data": indicator_blob,
        })
    def _persist_order(self, order, signal,
                       env_name: Optional[str] = None) -> None:
        env = self._env_for(env_name)
        if env.persistence:
            env.persistence.save_order({
                "order_id": order.order_id, "strategy_id": order.strategy_id,
                "instrument": order.instrument, "side": order.side, "quantity": order.quantity,
                "order_type": order.order_type,
                "price": order.price if order.price is not None else signal.trigger_price,
                "trigger_price": getattr(order, "trigger_price", None),
                "planned_entry_price": getattr(order, "planned_entry_price", None),
                "planned_sl": getattr(order, "planned_sl", None),
                "planned_order_type": getattr(order, "planned_order_type", None),
                "order_role": (getattr(order, "order_role", None)
                               or resolve_order_role(signal)),
                "protected_order_id": getattr(order, "protected_order_id", None),
                "correlation_id": getattr(order, "correlation_id", None),
                "state": order.state.value, "filled_quantity": order.filled_quantity,
                "average_fill_price": order.average_fill_price,
                "created_at": datetime.fromtimestamp(order.created_at, tz=timezone.utc).isoformat(),
                "updated_at": datetime.fromtimestamp(order.updated_at, tz=timezone.utc).isoformat(),
                "signal_id": signal.signal_id, "trade_id": order.trade_id,
                "lifecycle_id": getattr(order, "lifecycle_id", None) or order.trade_id,
                "parent_signal_id": getattr(order, "parent_signal_id", None) or signal.signal_id,
                "position_id": getattr(order, "position_id", None),
                "parent_position_id": getattr(order, "parent_position_id", None),
                "position_generation": getattr(order, "position_generation", None),
                "original_order_id": getattr(order, "original_order_id", None),
            })
    def _live_pending_row(self, env, signal) -> Optional[dict]:
        """The durable pending-order row for a signal (LIVE env only)."""
        if env.persistence is None:
            return None
        rows = env.persistence.get_pending_orders(execution_mode="LIVE")
        return next((r for r in rows if r.get("pending_order_id") == signal.signal_id), None)
    def _arm_live_pending(self, signal, env) -> None:
        """Register a LIVE pending breakout durably: PENDING -> ARMED.

        The row is written PENDING first (the moment the strategy armed the
        breakout) then immediately promoted to ARMED (accepted into LIVE
        execution).  A replayed pending signal for an already-ARMED or
        terminal row is a no-op (idempotent)."""
        pend_id = signal.signal_id
        base = {
            "pending_order_id": pend_id,
            "signal_id": pend_id,
            "side": (signal.side or getattr(signal.signal_type, "value", "LONG")).upper(),
            "order_type": "LIMIT",
            "trigger_price": signal.trigger_price,
            "quantity": signal.quantity,
            "trade_id": None,
        }
        existing = self._live_pending_row(env, signal)
        if existing is not None:
            try:
                transition_pending_state(
                    existing.get("status"), PendingOrderState.ARMED.value, pend_id)
            except ValueError:
                return  # terminal / already ENTRY_SENT: replay must not re-arm
            row = dict(existing)
            row.update(base)
            row["status"] = PendingOrderState.ARMED.value
            row["armed_at"] = datetime.now(timezone.utc).isoformat()
            env.persistence.save_pending_order(row)
            return
        row = dict(base)
        row["status"] = PendingOrderState.PENDING.value
        env.persistence.save_pending_order(row)
        self.publish_event("pending_order_created", {
            "pending_order_id": pend_id, "signal_id": pend_id,
            "state": row["status"], "execution_mode": env.mode}, env_name=env.name)
        row["status"] = transition_pending_state(
            row["status"], PendingOrderState.ARMED.value, pend_id)
        row["armed_at"] = datetime.now(timezone.utc).isoformat()
        env.persistence.save_pending_order(row)
        self.publish_event("pending_order_armed", {
            "pending_order_id": pend_id, "signal_id": pend_id,
            "state": row["status"], "execution_mode": env.mode}, env_name=env.name)
    def _mark_live_pending_entry_sent(self, env, signal, trade, order) -> None:
        """Record ENTRY_SENT on the durable pending order once the entry was
        placed at the broker, tying the broker correlation back to the row."""
        row = self._live_pending_row(env, signal)
        if row is None:
            self.publish_event("pending_order_blocked", {
                "signal_id": signal.signal_id,
                "reason": "durable_pending_missing_at_entry",
                "execution_mode": env.mode}, env_name=env.name)
            return
        state_value = getattr(order.state, "value", "")
        if state_value not in ("submitted", "filled"):
            # The entry never reached the broker (e.g. gate closed / broker
            # unavailable): ENTRY_SENT is reserved for actual placement. The
            # pending row stays ARMED and may be retried.
            return
        pend_id = row.get("pending_order_id") or signal.signal_id
        try:
            status = transition_pending_state(
                row.get("status"), PendingOrderState.ENTRY_SENT.value, pend_id)
        except ValueError:
            return
        env.persistence.save_pending_order({
            "pending_order_id": pend_id,
            "signal_id": pend_id,
            "trade_id": trade.trade_id,
            "status": status,
            "correlation_id": getattr(order, "correlation_id", None),
            "broker_order_id": getattr(order, "_broker_order_id", None),
        })
        self.publish_event("pending_order_entry_sent", {
            "pending_order_id": pend_id, "signal_id": pend_id,
            "trade_id": trade.trade_id, "order_id": order.order_id,
            "correlation_id": getattr(order, "correlation_id", None),
            "broker_order_id": getattr(order, "_broker_order_id", None),
            "execution_mode": env.mode}, env_name=env.name)
    def _persist_fill(self, fill, trade_id: str, signal_id: str | None,
                      env_name: Optional[str] = None) -> None:
        env = self._env_for(env_name)
        if env.persistence:
            env.persistence.save_fill({"fill_id": fill.fill_id, "order_id": fill.order_id,
                "strategy_id": fill.strategy_id, "instrument": fill.instrument, "side": fill.side,
                "quantity": fill.quantity, "price": fill.price,
                "timestamp": datetime.fromtimestamp(fill.timestamp, tz=timezone.utc).isoformat(),
                "trade_id": trade_id, "entry_signal_id": signal_id,
                "broker_fill_id": getattr(fill, "broker_fill_id", None),
                "broker_order_id": getattr(fill, "broker_order_id", None),
                "broker_trade_id": getattr(fill, "broker_trade_id", None),
                "cumulative_filled_quantity": getattr(fill, "cumulative_filled_quantity", None),
                "position_id": getattr(fill, "position_id", None),
                "lifecycle_id": getattr(fill, "lifecycle_id", None) or trade_id,
                "position_generation": getattr(fill, "position_generation", None)})
            # Phase 9.7 — keep the per-order fill reconciliation ledger in lock
            # step with every persisted fill (entry path).
            self._update_fill_reconciliation(env, fill)
    def _reconcile_live_fill(self, env, fill) -> str:
        """Phase 9.7 — broker-authoritative fill admission for LIVE.

        Returns:
          'apply'          — the broker delta is not yet accounted; apply it.
          'duplicate'      — a fill with the same broker_fill_id is already
                             persisted (the same execution event re-delivered
                             through WS/REST/replay).
          'already_synced' — this broker order's cumulative quantity is already
                             fully accounted locally (restart re-poll of an
                             already-synced order).
          'divergence'     — the broker cumulative moved backwards vs the local
                             ledger; the fill is surfaced, never invented away.
        """
        broker_order_id = getattr(fill, "broker_order_id", None)
        if env.mode != "LIVE" or not broker_order_id or env.persistence is None:
            return "apply"
        bfid = getattr(fill, "broker_fill_id", None)
        if bfid:
            try:
                if env.persistence.fill_by_broker_fill_id(bfid):
                    return "duplicate"
            except Exception:
                pass
        broker_cum = getattr(fill, "cumulative_filled_quantity", None)
        if broker_cum is None:
            return "apply"
        broker_cum = int(broker_cum)
        try:
            ledger = env.persistence.get_fill_reconciliation(
                broker_order_id, execution_mode="LIVE")
        except Exception:
            ledger = None
        if ledger is None:
            return "apply"
        prior = int(ledger.get("broker_cumulative_qty") or 0)
        if broker_cum <= prior:
            return "already_synced"
        local_cum = int(ledger.get("local_cumulative_qty") or 0)
        if broker_cum < local_cum:
            self.publish_event("reconciliation_mismatch", {
                "broker_order_id": broker_order_id,
                "broker_cumulative": broker_cum,
                "local_cumulative": local_cum,
                "reason": "fill_ledger_broker_behind_local",
                "execution_mode": env.mode}, env_name=env.name)
            return "divergence"
        return "apply"
    def _update_fill_reconciliation(self, env, fill) -> None:
        """Mirror the per-order fill reconciliation ledger from persisted fills
        (Phase 9.7). The broker cumulative quantity is the authority; local
        cumulative mirrors the fills table; any gap is surfaced, never hidden."""
        broker_order_id = getattr(fill, "broker_order_id", None)
        if env.mode != "LIVE" or not broker_order_id or env.persistence is None:
            return
        try:
            local_cum = env.persistence.order_cumulative_filled(
                broker_order_id, execution_mode="LIVE")
            broker_cum = int(getattr(fill, "cumulative_filled_quantity",
                                     None) or local_cum)
            prior = env.persistence.get_fill_reconciliation(
                broker_order_id, execution_mode="LIVE")
            if prior is not None:
                broker_cum = max(broker_cum,
                                 int(prior.get("broker_cumulative_qty") or 0))
            env.persistence.save_fill_reconciliation({
                "broker_order_id": broker_order_id,
                "strategy_id": fill.strategy_id,
                "instrument": fill.instrument,
                "order_id": fill.order_id,
                "side": fill.side,
                "broker_cumulative_qty": broker_cum,
                "local_cumulative_qty": local_cum,
                "last_broker_fill_id": getattr(fill, "broker_fill_id", None),
                "broker_average_price": float(fill.price or 0.0),
            })
        except Exception as e:
            log.error("[Engine] fill reconciliation update failed for %s: %s",
                      broker_order_id, e)
    def _persist_position(self, position, env_name: Optional[str] = None) -> None:
        """Persist a position row into the canonical positions table.

        position_id is the row key; trade_id is the separate canonical trade
        identity (position_id != trade_id, enforced by the DB trigger).
        """
        env = self._env_for(env_name)
        if env.persistence is not None and hasattr(env.persistence, "save_position"):
            try:
                env.persistence.save_position(position)
            except Exception as e:
                log.error("[Engine] save_position failed for %s: %s",
                          getattr(position, "position_id", "?"), e)
    def _calculate_margin(self, instrument: str, price: float, quantity: int) -> float:
        model = self.config.instrument(instrument).get("margin_model", {})
        if model:
            return quantity * (model.get("slope", 0.0) * price + model.get("intercept", 0.0))
        return price * quantity * self.config.instrument(instrument).get("multiplier", 1.0) * 0.065
    def _reset_strategy_state(self, strategy_id: str, keep_pending: bool = False,
                              env_name: Optional[str] = None) -> None:
        env = self._env_for(env_name)
        strategy = env.strategies.get(strategy_id)
        if strategy:
            keep = keep_pending and strategy.pending_entry is not None
            if keep:
                pen = strategy.pending_entry
                pen.status = "pending"
                strategy.state = (StrategyState.PENDING_LONG if pen.side == "LONG"
                                  else StrategyState.PENDING_SHORT)
            else:
                strategy.state = StrategyState.FLAT
            strategy.position_side = strategy.stop_price = None
            strategy.current_position_id = None
            strategy.position_generation = None
            strategy.position_quantity = None
            # The stop-out re-fire guard lifts once the position actually
            # closes: the strategy is flat again, so later stop exits (new
            # trades) are evaluated normally.
            setattr(strategy, "stop_exit_submitted", False)
            # IMMEDIATE-LIMIT: a terminal reject/cancel must release the
            # signal lock so the strategy can detect a fresh signal again.
            setattr(strategy, "immediate_limit_sent", None)
            if not keep:
                strategy.pending_entry = None
            strategy.current_trade_id = None
        runtime = env.runtimes.get(strategy_id) if env.runtimes is not None else None
        if runtime is not None:
            runtime.current_trade_id = None
