"""Durable persistence and strategy state transitions for TradingEngine."""
from __future__ import annotations

import logging
import time
from contextlib import nullcontext
from datetime import datetime, timezone
from typing import Optional

from core.lifecycle import PendingOrderState, transition_pending_state
from strategies.types import (
    StrategyState, resolve_order_role, restored_trigger_metadata,
)

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
        # Persist metadata needed to reconstruct reversal and breakout triggers.
        # Nested Signal objects are represented by their ids; their full rows
        # are saved separately by this same signal flow.
        saved_metadata = {}
        for key, value in metadata.items():
            if hasattr(value, "signal_id"):
                saved_metadata[f"{key}_signal_id"] = value.signal_id
            elif isinstance(value, (str, int, float, bool)) or value is None:
                saved_metadata[key] = value
            elif isinstance(value, (list, tuple)) and all(
                    isinstance(item, (str, int, float, bool)) or item is None
                    for item in value):
                saved_metadata[key] = list(value)
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
            "signal_metadata": saved_metadata,
        })
    def _persist_order(self, order, signal,
                       env_name: Optional[str] = None) -> None:
        env = self._env_for(env_name)
        if env.persistence:
            signal_id = (getattr(signal, "signal_id", None)
                         or getattr(order, "entry_signal_id", None))
            role = (getattr(order, "order_role", None)
                    or (resolve_order_role(signal) if signal is not None else None))
            env.persistence.save_order({
                "order_id": order.order_id, "strategy_id": order.strategy_id,
                "instrument": order.instrument, "side": order.side, "quantity": order.quantity,
                "order_type": order.order_type,
                "price": (order.price if order.price is not None else
                          getattr(signal, "trigger_price", None)
                          if signal is not None else
                          getattr(order, "trigger_price", None)),
                "trigger_price": getattr(order, "trigger_price", None),
                "planned_entry_price": getattr(order, "planned_entry_price", None),
                "planned_sl": getattr(order, "planned_sl", None),
                "planned_order_type": getattr(order, "planned_order_type", None),
                "order_role": role,
                "protected_order_id": getattr(order, "protected_order_id", None),
                "correlation_id": getattr(order, "correlation_id", None),
                "broker_order_id": getattr(order, "_broker_order_id", None),
                "state": order.state.value, "filled_quantity": order.filled_quantity,
                "average_fill_price": order.average_fill_price,
                "created_at": datetime.fromtimestamp(order.created_at, tz=timezone.utc).isoformat(),
                "updated_at": datetime.fromtimestamp(order.updated_at, tz=timezone.utc).isoformat(),
                "signal_id": signal_id, "trade_id": order.trade_id,
                "lifecycle_id": getattr(order, "lifecycle_id", None) or order.trade_id,
                "parent_signal_id": getattr(order, "parent_signal_id", None) or signal_id,
                "position_id": getattr(order, "position_id", None),
                "parent_position_id": getattr(order, "parent_position_id", None),
                "position_generation": getattr(order, "position_generation", None),
                "original_order_id": getattr(order, "original_order_id", None),
                "reversal_parent_signal_id": getattr(order, "reversal_parent_signal_id", None),
                "trigger_state": getattr(order, "trigger_state", None),
                "trigger_generation": getattr(order, "trigger_generation", None),
                "trigger_source": getattr(order, "trigger_source", None),
            })
    def _live_pending_row(self, env, signal) -> Optional[dict]:
        """The durable pending-order row for a signal (LIVE env only)."""
        if env.persistence is None:
            return None
        registry = getattr(env, "pending_triggers", None)
        if registry is not None:
            # The tick-to-order path uses the in-memory ARMED row; it must not
            # scan SQLite while handling a WebSocket tick.
            return registry.live_row(signal.signal_id)
        rows = env.persistence.get_pending_orders(execution_mode="LIVE")
        return next((r for r in rows if r.get("pending_order_id") == signal.signal_id), None)
    def _discard_unjournaled_live_pending(self, strategy, env, reason: str,
                                          signal_id: str | None = None) -> None:
        """Fail closed when a pending trigger cannot be confirmed in SQLite.

        A JSON strategy snapshot is useful for restoring indicators/positions,
        but it is not authoritative for an executable LIVE trigger.  Keep the
        local trigger and O(1) index from firing if its durable row is absent,
        terminal, or could not be read/written.
        """
        pending = getattr(strategy, "pending_entry", None)
        pending_signal = getattr(pending, "signal", None)
        pending_id = getattr(pending_signal, "signal_id", None)
        entry_matches = (pending is not None and (signal_id is None
                          or str(pending_id) == str(signal_id)))
        exit_pending = getattr(strategy, "pending_exit_trigger", None)
        exit_signal = getattr(exit_pending, "signal", None)
        exit_id = getattr(exit_signal, "signal_id", None)
        exit_matches = (exit_pending is not None and signal_id is not None
                        and str(exit_id) == str(signal_id))
        if not entry_matches and not exit_matches:
            return
        metadata = getattr(pending_signal, "metadata", None) or {}
        paired_exit_id = metadata.get("reversal_parent_signal_id")
        if (entry_matches and paired_exit_id and exit_pending is not None
                and str(getattr(exit_signal, "signal_id", "")) == str(paired_exit_id)):
            strategy._cancel_trigger(exit_pending)
            strategy.pending_exit_trigger = None
        elif exit_matches:
            strategy._cancel_trigger(exit_pending)
            strategy.pending_exit_trigger = None
        if entry_matches:
            strategy._cancel_trigger(pending)
            strategy.pending_entry = None
        if getattr(strategy, "position_side", None) == "LONG":
            strategy.state = StrategyState.LONG_POSITION
        elif getattr(strategy, "position_side", None) == "SHORT":
            strategy.state = StrategyState.SHORT_POSITION
        else:
            strategy.state = StrategyState.FLAT
        registry = getattr(env, "pending_triggers", None)
        if registry is not None:
            if entry_matches and pending_id:
                registry.remove_signal(str(pending_id))
            if exit_matches and exit_id:
                registry.remove_signal(str(exit_id))
            registry.sync_strategy(strategy)
        discarded_id = pending_id if entry_matches else exit_id
        log.error("[Engine] discarded unjournaled LIVE trigger %s for %s: %s",
                  discarded_id, getattr(strategy, "strategy_id", "?"), reason)

    def _arm_live_pending(self, signal, env) -> None:
        """Persist and index a LIVE trigger, disarming RAM state on DB failure."""
        try:
            self._arm_live_pending_durable(signal, env)
        except Exception as exc:  # noqa: BLE001
            strategy = (getattr(env, "strategies", {}) or {}).get(signal.strategy_id)
            if strategy is not None:
                self._discard_unjournaled_live_pending(
                    strategy, env, f"database arm failed: {exc}",
                    signal_id=signal.signal_id)
            raise

    def _arm_live_pending_durable(self, signal, env) -> None:
        """Register a LIVE pending breakout durably: PENDING -> ARMED.

        The row is written PENDING first (the moment the strategy armed the
        breakout) then immediately promoted to ARMED (accepted into LIVE
        execution).  A replayed pending signal for an already-ARMED or
        terminal row is a no-op (idempotent)."""
        pend_id = signal.signal_id
        base = {
            "pending_order_id": pend_id,
            "signal_id": pend_id,
            "strategy_id": signal.strategy_id,
            "instrument": signal.instrument,
            "side": (signal.side or getattr(signal.signal_type, "value", "LONG")).upper(),
            "direction": (signal.side or getattr(signal.signal_type, "value", "LONG")).upper(),
            "order_type": "LIMIT",
            "trigger_price": signal.trigger_price,
            "trigger_state": (signal.metadata or {}).get("trigger_state", "ARMED"),
            "trigger_generation": (signal.metadata or {}).get("trigger_generation"),
            "trigger_source": (signal.metadata or {}).get("trigger_source"),
            "signal_timestamp": signal.timestamp,
            "quantity": signal.quantity,
            "trade_id": None,
        }
        existing = self._live_pending_row(env, signal)
        # Arming happens on the completed-candle path, outside the WebSocket
        # hot path, so a cache miss may consult the durable journal here.
        if existing is None and env.persistence is not None:
            rows = env.persistence.get_pending_orders(execution_mode="LIVE")
            existing = next((r for r in rows
                             if r.get("pending_order_id") == pend_id), None)
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
            registry = getattr(env, "pending_triggers", None)
            if registry is not None:
                registry.cache_live_row(row)
            self._sync_live_pending_strategy(signal, env, registry)
            return
        row = dict(base)
        row["status"] = PendingOrderState.PENDING.value
        env.persistence.save_pending_order(row)
        registry = getattr(env, "pending_triggers", None)
        if registry is not None:
            registry.cache_live_row(row)
        self.publish_event("pending_order_created", {
            "pending_order_id": pend_id, "signal_id": pend_id,
            "state": row["status"], "execution_mode": env.mode}, env_name=env.name)
        row["status"] = transition_pending_state(
            row["status"], PendingOrderState.ARMED.value, pend_id)
        row["armed_at"] = datetime.now(timezone.utc).isoformat()
        env.persistence.save_pending_order(row)
        if registry is not None:
            registry.cache_live_row(row)
        self._sync_live_pending_strategy(signal, env, registry)
        self.publish_event("pending_order_armed", {
            "pending_order_id": pend_id, "signal_id": pend_id,
            "state": row["status"], "execution_mode": env.mode}, env_name=env.name)

    def _sync_live_pending_strategy(self, signal, env, registry=None) -> None:
        """Keep fresh signal, durable row, strategy RAM, and tick index aligned.

        The strategy normally set ``pending_entry`` before emitting the
        pending signal. Rebuild it from that same immutable signal if a
        callback/race cleared the reference between candle evaluation and DB
        arming; otherwise the DB says ARMED while the WebSocket path has no
        executable trigger.
        """
        strategy = (getattr(env, "strategies", {}) or {}).get(signal.strategy_id)
        if strategy is None:
            return
        pend_id = str(signal.signal_id)
        current = getattr(strategy, "pending_entry", None)
        current_id = getattr(getattr(current, "signal", None), "signal_id", None)
        metadata = signal.metadata or {}
        side = str(signal.side or getattr(signal.signal_type, "value", "LONG")).upper()
        if current_id != pend_id:
            from strategies.types import PendingEntry, StrategyState
            status = ("waiting_for_flat"
                      if metadata.get("is_reversal_entry")
                      and getattr(strategy, "position_side", None) else "pending")
            strategy.pending_entry = PendingEntry(
                signal=signal, trigger_price=float(signal.trigger_price),
                side=side, created_at=time.time(), status=status)
            if not getattr(strategy, "position_side", None):
                strategy.state = (StrategyState.PENDING_LONG if side == "LONG"
                                  else StrategyState.PENDING_SHORT)
            generation = metadata.get("trigger_generation")
            if generation is not None:
                strategy._trigger_generation = max(
                    int(getattr(strategy, "_trigger_generation", 0) or 0),
                    int(generation))
            log.warning("[Engine] rebuilt missing in-memory trigger %s for %s "
                        "while arming its LIVE DB row", pend_id,
                        signal.strategy_id)
        strategy._last_armed_pending_id = pend_id
        if registry is not None:
            registry.sync_strategy(strategy)
            indexed = registry.entry_for(signal.strategy_id)
            if getattr(getattr(indexed, "signal", None), "signal_id", None) != pend_id:
                log.error("[Engine] LIVE DB trigger %s did not enter the "
                          "WebSocket registry for %s", pend_id,
                          signal.strategy_id)
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
            "strategy_id": signal.strategy_id,
            "instrument": signal.instrument,
            "direction": (signal.side or getattr(signal.signal_type, "value", "LONG")).upper(),
            "trigger_price": signal.trigger_price,
            "trigger_state": (signal.metadata or {}).get("trigger_state", "FIRED"),
            "trigger_generation": (signal.metadata or {}).get("trigger_generation"),
            "trigger_source": (signal.metadata or {}).get("trigger_source"),
            "signal_timestamp": signal.timestamp,
            "correlation_id": getattr(order, "correlation_id", None),
            "broker_order_id": getattr(order, "_broker_order_id", None),
        })
        registry = getattr(env, "pending_triggers", None)
        if registry is not None:
            registry.update_live_row(pend_id, {
                "status": status, "trade_id": trade.trade_id,
                "correlation_id": getattr(order, "correlation_id", None),
                "broker_order_id": getattr(order, "_broker_order_id", None),
            })

    def _restore_live_pending_triggers(self, env) -> int:
        """Warm the in-memory trigger index from the LIVE DB before WS starts.

        Only executable ARMED rows are loaded into the trigger cache; terminal
        history remains in SQLite and is queried by signal id only when needed
        to invalidate a stale snapshot trigger.
        """
        registry = getattr(env, "pending_triggers", None)
        persistence = getattr(env, "persistence", None)
        if registry is None or persistence is None or env.mode != "LIVE":
            return 0
        from strategies.types import (PendingEntry, Signal, SignalType,
                                      StrategyState, freeze_signal_context)
        try:
            if hasattr(persistence, "get_pending_order"):
                # Production restores only executable rows. A single-row
                # lookup below still catches a terminalized stale RAM signal.
                rows = persistence.get_pending_orders(
                    status=PendingOrderState.ARMED.value,
                    execution_mode="LIVE")
            else:
                # Lightweight adapters/test doubles may expose only the
                # original API; production never loads all historical rows.
                rows = persistence.get_pending_orders(execution_mode="LIVE")
        except Exception as exc:
            log.error("[Engine] pending trigger restore failed: %s", exc)
            # The JSON snapshot is not sufficient authority to execute a LIVE
            # entry. If SQLite cannot be read at startup, remove snapshot-only
            # entries before the WebSocket adapter starts.
            for strategy in (getattr(env, "strategies", {}) or {}).values():
                self._discard_unjournaled_live_pending(
                    strategy, env, f"database restore failed: {exc}")
            return 0
        restored = 0
        rows_by_signal = {
            str(row.get("signal_id") or row.get("pending_order_id") or ""): row
            for row in rows
        }
        # A persisted engine snapshot can still contain an entry trigger that
        # has since been terminalized in the canonical DB. Do not resurrect it
        # from RAM on restart (for example after an operator expires stale
        # signals before a fresh live session).
        for sid, strategy in (getattr(env, "strategies", {}) or {}).items():
            current = getattr(strategy, "pending_entry", None)
            signal_id = getattr(getattr(current, "signal", None), "signal_id", None)
            row = rows_by_signal.get(str(signal_id or ""))
            if (row is None and signal_id and
                    callable(getattr(persistence, "get_pending_order", None))):
                try:
                    row = persistence.get_pending_order(
                        str(signal_id), execution_mode="LIVE")
                except Exception as exc:
                    log.warning("[Engine] pending state lookup failed for %s: %s",
                                signal_id, exc)
            status = str((row or {}).get("status", "")).lower()
            if current is not None and status != PendingOrderState.ARMED.value:
                self._discard_unjournaled_live_pending(
                    strategy, env,
                    f"durable status is {status or 'missing'}, expected armed",
                    signal_id=str(signal_id) if signal_id else None)
                log.info("[Engine] discarded non-armed startup trigger %s for %s",
                         signal_id or "?", sid)
            elif (status == PendingOrderState.ARMED.value and current is not None
                  and getattr(current.signal, "signal_id", None) == signal_id):
                current.signal.metadata = restored_trigger_metadata(
                    current.signal.metadata, active=True)
                current.status = ("waiting_for_flat"
                                  if current.status == "waiting_for_flat"
                                  else "pending")
        for row in rows:
            if str(row.get("status", "")).lower() != PendingOrderState.ARMED.value:
                continue
            registry.cache_live_row(row)
            sid = str(row.get("strategy_id") or "")
            strategy = (getattr(env, "strategies", {}) or {}).get(sid)
            signal_id = str(row.get("signal_id") or row.get("pending_order_id") or "")
            if strategy is None or not signal_id or row.get("trigger_price") is None:
                log.error("[Engine] armed pending %s cannot be restored; missing strategy/id/trigger",
                          signal_id or "?")
                continue
            current = strategy.pending_entry
            if current is not None and getattr(current.signal, "signal_id", None) == signal_id:
                registry.sync_strategy(strategy)
                restored += 1
                continue
            try:
                saved = persistence.get_signal(signal_id)
                if not saved:
                    raise ValueError("signal row is missing")
                side = str(row.get("direction") or row.get("side") or saved.get("side") or "").upper()
                signal_type = SignalType(side)
                timestamp = float(
                    row.get("signal_timestamp")
                    or saved.get("signal_timestamp")
                    or saved.get("candle_timestamp")
                    or 0.0)
                metadata = {
                    "pending": True, "triggered": False, "trigger_state": "ARMED",
                    "trigger_generation": int(row.get("trigger_generation") or 0),
                    "trigger_source": row.get("trigger_source") or "market_websocket_ltp",
                    "signal_candle_start": saved.get("candle_timestamp"),
                    "signal_candle_open": saved.get("open"),
                    "signal_candle_high": saved.get("high"),
                    "signal_candle_low": saved.get("low"),
                    "signal_candle_close": saved.get("close"),
                    "signal_htf_dema_atr": saved.get("htf_value"),
                    "signal_mid_dema_atr": saved.get("mid_value"),
                    "signal_fast_dema_atr": saved.get("fast_dema"),
                }
                try:
                    import json
                    metadata.update(json.loads(saved.get("signal_metadata") or "{}"))
                except (TypeError, ValueError):
                    pass
                if current is not None and getattr(current.signal, "signal_id", None) == signal_id:
                    metadata.update(current.signal.metadata or {})
                metadata = restored_trigger_metadata(metadata, active=True)
                signal = Signal(
                    signal_type=signal_type, instrument=str(row.get("instrument") or saved.get("instrument")),
                    strategy_id=sid, timestamp=timestamp,
                    trigger_price=float(row["trigger_price"]),
                    stop_price=float(saved.get("stop_price") or 0.0),
                    quantity=int(row.get("quantity") or saved.get("quantity") or strategy.quantity),
                    side=side, metadata=metadata,
                )
                signal.signal_id = signal_id
                freeze_signal_context(
                    signal, close=saved.get("close"), high=saved.get("high"),
                    low=saved.get("low"), timestamp=saved.get("candle_timestamp") or timestamp,
                    open_=saved.get("open"), dema=saved.get("fast_dema"),
                    atr=saved.get("fast_atr"), htf_value=saved.get("htf_value"),
                    mid_value=saved.get("mid_value"),
                )
                created_at = time.time()
                try:
                    from datetime import datetime
                    armed_at = row.get("armed_at")
                    if armed_at:
                        created_at = datetime.fromisoformat(armed_at).timestamp()
                except (TypeError, ValueError):
                    pass
                entry_status = ("waiting_for_flat"
                                if metadata.get("is_reversal_entry")
                                and strategy.position_side is not None else "pending")
                pending = PendingEntry(signal=signal, trigger_price=float(row["trigger_price"]),
                                       side=side, status=entry_status, created_at=created_at)
                strategy.pending_entry = pending
                strategy._trigger_generation = max(
                    int(getattr(strategy, "_trigger_generation", 0)),
                    int(row.get("trigger_generation") or 0))
                strategy._last_armed_pending_id = signal_id
                if strategy.position_side is None:
                    strategy.state = (StrategyState.PENDING_LONG if side == "LONG"
                                      else StrategyState.PENDING_SHORT)
                # Rebuild the opposite-position exit trigger for a reversal.
                # The entry row points back to its parent exit signal id.
                reversal_exit_id = metadata.get("reversal_parent_signal_id")
                reversal_record = None
                get_reversal = getattr(persistence, "get_reversal_by_signal_id", None)
                if reversal_exit_id and callable(get_reversal):
                    try:
                        reversal_record = get_reversal(str(reversal_exit_id))
                    except Exception:
                        reversal_record = None
                reversal_status = str((reversal_record or {}).get("status") or "").upper()
                if reversal_status in {"EXIT_REJECTED", "EXIT_CANCELLED"}:
                    # Canonical reversal state wins over a stale ARMED trigger
                    # row. Do not resurrect the paired entry after its old-side
                    # exit definitively failed or was cancelled.
                    try:
                        persistence.terminalize_pending_order(
                            signal_id, status="resolved",
                            reason=f"reversal_exit_terminal:{reversal_status.lower()}")
                    except Exception:
                        pass
                    registry.remove_signal(signal_id)
                    strategy.pending_entry = None
                    strategy.pending_exit_trigger = None
                    continue

                orders = list((getattr(
                    getattr(env, "execution_engine", None), "_orders", {}) or {}).values())
                active_reversal_exit = next((order for order in orders
                    if str(getattr(order, "order_role", "")).upper() == "REVERSAL_EXIT"
                    and str(getattr(getattr(order, "state", None), "value",
                                    getattr(order, "state", ""))).lower()
                        in ("created", "submitted", "acknowledged", "partially_filled")
                    and str(getattr(order, "entry_signal_id", None)
                            or getattr(order, "parent_signal_id", None) or "")
                        == str(reversal_exit_id or "")), None)
                if reversal_exit_id and active_reversal_exit is not None:
                    # The exit trigger already fired and its broker order is
                    # still live. Recreating the candle trigger after restart
                    # could submit a duplicate exit. Keep the opposite entry
                    # parked until this exact order is confirmed terminal/flat.
                    pending.status = "waiting_for_flat"
                    strategy.pending_exit_trigger = None
                    strategy.stop_exit_submitted = True
                    strategy.state = StrategyState.EXIT_ORDER_SUBMITTED
                    position = next((p for p in getattr(
                        getattr(env, "position_manager", None), "open_positions", [])
                        if p.position_id == getattr(
                            active_reversal_exit, "parent_position_id", None)
                        and p.is_open), None)
                    if position is not None:
                        position.exit_started = True
                        position.exit_order_id = active_reversal_exit.order_id
                        position.sl_state = "EXITING"
                        monitor_factory = getattr(self, "_sl_monitor", None)
                        if callable(monitor_factory):
                            monitor = monitor_factory(env)
                            monitor.arm(position)
                            monitor.mark_exiting(position.position_id,
                                                 active_reversal_exit.order_id)
                        try:
                            persistence.save_position(position)
                        except Exception:
                            pass
                    registry.sync_strategy(strategy)
                    restored += 1
                    continue

                if reversal_exit_id and strategy.pending_exit_trigger is None:
                    exit_saved = persistence.get_signal(str(reversal_exit_id))
                    if exit_saved is not None:
                        try:
                            exit_meta = json.loads(exit_saved.get("signal_metadata") or "{}")
                        except (TypeError, ValueError):
                            exit_meta = {}
                        exit_meta = restored_trigger_metadata(exit_meta, active=True)
                        exit_side = str(exit_saved.get("side") or "").upper()
                        exit_signal = Signal(
                            signal_type=SignalType(exit_side), instrument=str(exit_saved.get("instrument") or strategy.instrument),
                            strategy_id=sid,
                            timestamp=float(exit_saved.get("signal_timestamp") or timestamp),
                            trigger_price=float(exit_saved.get("trigger_price") or 0.0),
                            stop_price=float(exit_saved.get("stop_price") or 0.0),
                            quantity=int(exit_saved.get("quantity") or strategy.quantity),
                            side=exit_side, metadata=exit_meta,
                        )
                        exit_signal.signal_id = str(reversal_exit_id)
                        strategy.pending_exit_trigger = PendingEntry(
                            signal=exit_signal, trigger_price=exit_signal.trigger_price,
                            side=exit_side, status="pending", created_at=created_at)
                if strategy.pending_exit_trigger is not None:
                    strategy.state = StrategyState.EXIT_PENDING
                registry.sync_strategy(strategy)
                restored += 1
                log.warning("[Engine] restored armed trigger %s for %s from durable state",
                            signal_id, sid)
            except Exception as exc:
                log.error("[Engine] armed pending %s cannot be safely restored: %s",
                          signal_id, exc)
        return restored

    def _invalidate_warmed_pending_triggers(self, env) -> int:
        """Retire restored triggers against the latest completed hourly line.

        Startup restores SQLite rows before the REST warmup. Run this check
        after warmup and before connecting the market feed, so a gap-open tick
        cannot fire a trigger from an older hourly indicator context.
        """
        invalidated_count = 0
        with getattr(self, "_lock", nullcontext()):
            for strategy in (getattr(env, "strategies", {}) or {}).values():
                pending = (getattr(strategy, "pending_entry", None)
                           or getattr(strategy, "pending_exit_trigger", None))
                old_signal = getattr(pending, "signal", None)
                if old_signal is None:
                    continue
                slow_state = getattr(strategy, "slow_htf_state", None)
                current_value = getattr(slow_state, "last_value", None)
                invalidated = strategy._invalidate_changed_dema_triggers(
                    current_value)
                if not invalidated:
                    continue
                registry = getattr(env, "pending_triggers", None)
                if registry is not None:
                    registry.sync_strategy(strategy)
                control = strategy._dema_cancel_control_signal(
                    time.time(), float(old_signal.trigger_price), invalidated)
                self._process_signal(control, env.name)
                invalidated_count += 1
                log.warning(
                    "[Engine] retired restored trigger %s for %s: hourly "
                    "DEMA-ATR %r -> %r",
                    invalidated["old_pending_id"], strategy.strategy_id,
                    invalidated["trigger_htf_dema_atr"],
                    invalidated["current_htf_dema_atr"])
        return invalidated_count
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

    def _queue_position_close_persist(self, env, position, source: str,
                                      error: Exception) -> None:
        """Retain a broker-confirmed close until its canonical DB write succeeds."""
        position_id = str(getattr(position, "position_id", "") or "")
        if not position_id:
            return
        queue = getattr(env, "pending_position_close_persist", None)
        if queue is None:
            queue = {}
            env.pending_position_close_persist = queue
        queue[position_id] = position
        reconciled = getattr(self, "_reconciled_envs", None)
        if reconciled is not None:
            reconciled.discard(getattr(env, "name", ""))
        log.critical("[Engine] position %s is closed at broker but DB close "
                     "write is pending (%s): %s", position_id, source, error)
        try:
            self.publish_event("position_close_persistence_failed", {
                "position_id": position_id,
                "trade_id": getattr(position, "trade_id", None),
                "strategy_id": getattr(position, "strategy_id", None),
                "instrument": getattr(position, "instrument", None),
                "source": source, "error": str(error),
                "severity": "CRITICAL",
                "execution_mode": getattr(env, "mode", None),
            }, env_name=getattr(env, "name", None))
        except Exception:
            pass

    def _retry_position_close_persistence(self, env) -> list[str]:
        """Retry failed close writes from the periodic broker reconciliation path."""
        queue = getattr(env, "pending_position_close_persist", None) or {}
        persistence = getattr(env, "persistence", None)
        failed = []
        if queue and persistence is not None:
            for position_id, position in list(queue.items()):
                try:
                    persistence.close_position_record(position)
                except Exception as exc:
                    failed.append(str(position_id))
                    log.error("[Engine] retry close persistence failed for %s: %s",
                              position_id, exc)
                    continue
                queue.pop(position_id, None)
                try:
                    self.publish_event("position_close_persistence_recovered", {
                        "position_id": str(position_id),
                        "execution_mode": getattr(env, "mode", None),
                    }, env_name=getattr(env, "name", None))
                except Exception:
                    pass
        elif queue:
            failed.extend(str(key) for key in queue)
        env.pending_position_close_persist = queue
        failed.extend(self._retry_trade_close_persistence(env))
        return failed

    def _queue_trade_close_persist(self, env, lifecycle, trade_id: str,
                                   reason: str) -> None:
        queue = getattr(env, "pending_trade_close_persist", None)
        if queue is None:
            queue = {}
            env.pending_trade_close_persist = queue
        queue[str(trade_id)] = (lifecycle, reason)
        reconciled = getattr(self, "_reconciled_envs", None)
        if reconciled is not None:
            reconciled.discard(getattr(env, "name", ""))
        try:
            self.publish_event("trade_close_persistence_failed", {
                "trade_id": str(trade_id), "reason": reason,
                "severity": "CRITICAL",
                "execution_mode": getattr(env, "mode", None),
            }, env_name=getattr(env, "name", None))
        except Exception:
            pass

    def _retry_trade_close_persistence(self, env) -> list[str]:
        queue = getattr(env, "pending_trade_close_persist", None) or {}
        failed = []
        for trade_id, (lifecycle, reason) in list(queue.items()):
            try:
                if not lifecycle.close_trade_from_broker_reconciliation(
                        trade_id, reason):
                    failed.append(str(trade_id))
                    continue
            except Exception as exc:
                failed.append(str(trade_id))
                log.error("[Engine] retry trade close persistence failed for %s: %s",
                          trade_id, exc)
                continue
            queue.pop(trade_id, None)
        env.pending_trade_close_persist = queue
        return failed
    def _calculate_margin(self, instrument: str, price: float, quantity: int) -> float:
        model = self.config.instrument(instrument).get("margin_model", {})
        if model:
            return quantity * (model.get("slope", 0.0) * price + model.get("intercept", 0.0))
        return price * quantity * self.config.instrument(instrument).get("multiplier", 1.0) * 0.065
    def _reset_strategy_state(self, strategy_id: str, keep_pending: bool = False,
                              env_name: Optional[str] = None) -> None:
        # This helper is reached by market callbacks, broker fills, and
        # reconciliation. Make clearing a fired permit atomic with trigger
        # evaluation/submission regardless of the caller's thread.
        with getattr(self, "_lock", nullcontext()):
            env = self._env_for(env_name)
            strategy = env.strategies.get(strategy_id)
            if strategy:
                keep = keep_pending and strategy.pending_entry is not None
                strategy._cancel_trigger(getattr(strategy, "pending_exit_trigger", None))
                strategy.pending_exit_trigger = None
                if keep:
                    pen = strategy.pending_entry
                    pen.status = "pending"
                    if pen.signal is not None:
                        md = pen.signal.metadata or {}
                        md.update(pending=True, triggered=False, trigger_state="ARMED")
                        pen.signal.metadata = md
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
                strategy._last_fired_trigger_signal_id = None
                strategy._fired_trigger_signal_ids.clear()
                if not keep:
                    strategy._cancel_trigger(strategy.pending_entry)
                    strategy.pending_entry = None
                strategy.current_trade_id = None
                registry = getattr(env, "pending_triggers", None)
                if registry is not None:
                    registry.sync_strategy(strategy)
            runtime = env.runtimes.get(strategy_id) if env.runtimes is not None else None
            if runtime is not None:
                runtime.current_trade_id = None
