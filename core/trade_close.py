"""Atomic trade close with guaranteed consistency.

Order of operations:
1. Calculate P&L (pure calculation, no side effects)
2. Persist trade to database FIRST (before memory update)
3. Persist fill to database
4. Close position in memory
5. Update account P&L
6. Update risk engine
7. Record event
8. Send Telegram (notification only, after persistence)

If step 2-3 fails: return False, do NOT update memory.
If step 4-8 fails: state is recoverable from database on restart.
"""
from __future__ import annotations

import math
import time
from datetime import datetime, timezone
from typing import Any, Optional

from execution.models import Fill


class TradeCloseManager:
    """Atomic trade close with guaranteed consistency."""

    def __init__(
        self,
        position_manager,
        pnl_engines: dict[str, Any],
        account_engines: dict[str, Any],
        global_account: Any,
        risk_engine,
        persistence,
        event_store,
        telegram=None,
        event_callback=None,
        trade_ledger=None,
    ):
        self._position_manager = position_manager
        self._pnl_engines = pnl_engines
        self._account_engines = account_engines
        self._global_account = global_account
        self._risk_engine = risk_engine
        self._persistence = persistence
        self._event_store = event_store
        self._telegram = telegram
        self._event_callback = event_callback
        self._trade_ledger = trade_ledger

    def close_position(self, fill: Fill, position, strategy_id: str, multiplier: float, exit_reason: str = "signal_exit", exit_signal_id: Optional[str] = None):
        """Atomically close a position.

        Args:
            fill: The exit fill.
            position: The open Position object.
            strategy_id: Strategy identifier.
            multiplier: Contract multiplier.
            exit_reason: Reason for close (e.g. "signal_exit", "stop_loss").
            exit_signal_id: Signal ID that triggered the exit (if signal-based).

        Returns:
            dict with gross_pnl, charges, net_pnl on success, False on failure.
        """
        # ── Step 0: Reject a close at a non-positive / non-finite exit price ──
        # This is the last line of defence against a `-1` no-data sentinel (or
        # NaN/inf) ever booking a realistic trade into the ledger/account/risk.
        # Entry fills are validated at order time; guard every close regardless.
        if (fill is None or fill.price is None
                or (isinstance(fill.price, float) and (math.isnan(fill.price) or math.isinf(fill.price)))
                or fill.price <= 0.0):
            print(
                f"[TradeClose] REFUSED close for {getattr(position, 'position_id', '?')}: "
                f"invalid exit price={getattr(fill, 'price', None)}",
                flush=True,
            )
            return False

        # ── Step 1: Calculate P&L (pure calculation, no side effects) ──
        pnl_engine = self._pnl_engines.get(strategy_id)
        if pnl_engine:
            entry_fill = Fill(
                fill_id=position.entry_fill_ids[0] if position.entry_fill_ids else "",
                order_id="",
                instrument=position.instrument,
                side="BUY" if position.is_long else "SELL",
                quantity=position.quantity,
                price=position.average_entry,
                timestamp=position.entry_timestamp,
                strategy_id=position.strategy_id,
                multiplier=multiplier,
            )
            gross_pnl, charges, net_pnl = pnl_engine.calculate_realized_pnl(
                entry_fill=entry_fill,
                exit_fill=fill,
                multiplier=multiplier,
            )
        else:
            gross_pnl, charges, net_pnl = 0.0, 0.0, 0.0

        side = "LONG" if position.is_long else "SHORT"
        entry_ts = (
            datetime.fromtimestamp(position.entry_timestamp, tz=timezone.utc).isoformat()
            if position.entry_timestamp else None
        )
        exit_ts = (
            datetime.fromtimestamp(fill.timestamp, tz=timezone.utc).isoformat()
            if fill.timestamp else None
        )
        exit_reason_final = position.exit_reason or exit_reason

        # ── Resolve effective exit_signal_id ──
        effective_exit_signal_id = exit_signal_id or getattr(position, 'exit_signal_id', None)
        trade_id = getattr(position, "trade_id", None)
        if not trade_id:
            print(
                f"[TradeClose] CRITICAL: position {position.position_id} "
                "has no explicit trade_id",
                flush=True,
            )
            return False

        # ── Steps 2-3: Persist trade and exit fill in one transaction ──
        if self._persistence:
            try:
                trade_record = {
                    "trade_id": trade_id,
                    "strategy_id": strategy_id,
                    "instrument": fill.instrument,
                    "side": side,
                    "entry_timestamp": entry_ts,
                    "entry_price": position.average_entry,
                    "exit_timestamp": exit_ts,
                    "exit_price": fill.price,
                    "quantity": position.quantity,
                    "multiplier": multiplier,
                    "gross_pnl": gross_pnl,
                    "charges": charges,
                    "net_pnl": net_pnl,
                    "exit_reason": exit_reason_final,
                    "status": "closed",
                    "entry_signal_id": getattr(position, 'entry_signal_id', None),
                    "exit_signal_id": effective_exit_signal_id,
                    # ── durable lineage (§15) ──
                    "entry_fill_id": (
                        position.entry_fill_ids[0]
                        if getattr(position, "entry_fill_ids", None) else None),
                    "exit_fill_id": fill.fill_id,
                    "entry_order_id": getattr(position, "entry_order_id", None),
                    "exit_order_id": getattr(fill, "order_id", None),
                    "position_id": getattr(position, "position_id", None),
                    "realized_pnl": net_pnl,
                    "exit_type": (
                        exit_reason_final if isinstance(exit_reason_final, str)
                        else getattr(exit_reason_final, "value", str(exit_reason_final))),
                }
                fill_record = {
                    "fill_id": fill.fill_id, "order_id": fill.order_id,
                    "strategy_id": fill.strategy_id, "instrument": fill.instrument,
                    "side": fill.side, "quantity": fill.quantity, "price": fill.price,
                    "timestamp": datetime.fromtimestamp(fill.timestamp or time.time(), tz=timezone.utc).isoformat(),
                    "entry_signal_id": getattr(position, 'entry_signal_id', None),
                    "trade_id": trade_id,
                    # Phase 9.7 — persist the broker execution identity with the
                    # close so the fill ledger/reconciliation stays authoritative.
                    "broker_order_id": getattr(fill, "broker_order_id", None),
                    "broker_fill_id": getattr(fill, "broker_fill_id", None),
                    "broker_trade_id": getattr(fill, "broker_trade_id", None),
                    "cumulative_filled_quantity": getattr(fill, "cumulative_filled_quantity", None),
                }
                if hasattr(self._persistence, "save_trade_and_fill"):
                    self._persistence.save_trade_and_fill(trade_record, fill_record)
                else:
                    self._persistence.save_trade(trade_record)
                    self._persistence.save_fill(fill_record)
            except Exception as e:
                print(f"[TradeClose] CRITICAL: Failed to persist close: {e}", flush=True)
                return False
        # Record P&L in engine AFTER successful persistence
        if pnl_engine:
            pnl_engine.record_trade(gross_pnl, charges, net_pnl)

        # ── Steps 4-8: Update in-memory state (recoverable from DB on restart) ──

        # Step 4: Close position in memory
        try:
            self._position_manager.close_position(
                position_id=position.position_id,
                fill=fill,
                reason=exit_reason_final,
                exit_signal_id=effective_exit_signal_id,
            )
        except Exception as e:
            print(f"[TradeClose] CRITICAL: close_position memory update failed: {e}", flush=True)
            # Do NOT return True — the position is still open in memory while
            # the DB says closed.  Returning True would let the strategy state
            # be updated (position_side=None) while the position manager still
            # holds the position, causing a desync on next restart.
            return False

        # Step 5: Update account P&L
        strat_account = self._account_engines.get(strategy_id)
        try:
            if strat_account:
                strat_account.update_realized_pnl(net_pnl, charges)
                strat_account.release_margin(position.margin)
            if self._global_account:
                self._global_account.update_realized_pnl(net_pnl, charges)
                self._global_account.release_margin(position.margin)
        except Exception as e:
            print(f"[TradeClose] WARNING: account update failed: {e}", flush=True)

        # Step 6: Update risk engine
        try:
            if self._risk_engine:
                self._risk_engine.update_daily_pnl(net_pnl)
                # Update peak equity for drawdown tracking after trade close
                if self._global_account and hasattr(self._global_account, 'equity'):
                    self._risk_engine.update_peak_equity(self._global_account.equity)
        except Exception as e:
            print(f"[TradeClose] WARNING: risk engine update failed: {e}", flush=True)

        # Step 6b: Close the derived ledger projection for the explicit trade.
        if self._trade_ledger:
            try:
                # Record the exit fill leg on the trade linked to this position
                # (trade_id == position_id), then sync authoritative P&L.
                trade = self._trade_ledger.get_trade(trade_id)
                if trade:
                    self._trade_ledger.record_fill(
                        trade_id=trade_id,
                        fill_id=fill.fill_id,
                        order_id=fill.order_id,
                        side=fill.side,
                        quantity=fill.quantity,
                        price=fill.price,
                        timestamp=fill.timestamp,
                        is_entry=False,
                    )
                    self._trade_ledger.close_trade(
                        trade_id, exit_reason=exit_reason_final,
                        gross_pnl=gross_pnl, net_pnl=net_pnl, fees=charges,
                    )
                else:
                    # GUARDED projection heal: the derived read-model is missing
                    # but the canonical trade already exists (persisted in
                    # Step 2-3). Rebuild the projection entirely from canonical
                    # data -- never invent a trade_id, never use position/analytics
                    # as an authority for identity. Failure to heal is logged
                    # (and classified CRITICAL below), never fatal to the close.
                    entry_fill_id = (
                        position.entry_fill_ids[0]
                        if getattr(position, "entry_fill_ids", None)
                        else ""
                    )
                    if self._trade_ledger.create_trade(
                        strategy_id=strategy_id,
                        instrument=fill.instrument,
                        side=side,
                        entry_quantity=position.quantity,
                        signal_time=position.entry_timestamp,
                        trigger_price=position.average_entry,
                        stop_price=getattr(position, "stop_price", None) or 0.0,
                        multiplier=multiplier,
                        entry_reason=exit_reason_final,  # projection bookkeeping only
                        trade_id=trade_id,
                        position_id=position.position_id,
                    ):
                        self._trade_ledger.record_fill(
                            trade_id=trade_id,
                            fill_id=entry_fill_id,
                            order_id="",
                            side="BUY" if position.is_long else "SELL",
                            quantity=position.quantity,
                            price=position.average_entry,
                            timestamp=position.entry_timestamp,
                            is_entry=True,
                        )
                        self._trade_ledger.record_fill(
                            trade_id=trade_id,
                            fill_id=fill.fill_id,
                            order_id=fill.order_id,
                            side=fill.side,
                            quantity=fill.quantity,
                            price=fill.price,
                            timestamp=fill.timestamp,
                            is_entry=False,
                        )
                        self._trade_ledger.close_trade(
                            trade_id, exit_reason=exit_reason_final,
                            gross_pnl=gross_pnl, net_pnl=net_pnl, fees=charges,
                        )
                    else:
                        raise ValueError(
                            f"failed to rebuild derived projection for canonical trade {trade_id}"
                        )
            except Exception as e:
                print(f"[TradeClose] CRITICAL: analytics ledger close failed for {position.position_id}: {e}", flush=True)

        # Step 7: Record event
        if self._event_store:
            try:
                self._event_store.record(
                    trade_id=trade_id,
                    strategy_id=strategy_id,
                    instrument=fill.instrument,
                    event_type="TRADE_CLOSED",
                    payload={
                        "gross_pnl": gross_pnl,
                        "charges": charges,
                        "net_pnl": net_pnl,
                        "exit_reason": exit_reason_final,
                        "entry_price": position.average_entry,
                        "exit_price": fill.price,
                        "entry_signal_id": getattr(position, 'entry_signal_id', None),
                        "exit_signal_id": effective_exit_signal_id,
                    },
                )
            except Exception:
                pass

        # Step 7b: Publish to dashboard EventBus + persistence
        if self._event_callback:
            try:
                self._event_callback("trade_closed", {
                    "trade_id": trade_id,
                    "strategy_id": strategy_id,
                    "instrument": fill.instrument,
                    "side": side,
                    "entry_price": position.average_entry,
                    "exit_price": fill.price,
                    "quantity": position.quantity,
                    "gross_pnl": gross_pnl,
                    "charges": charges,
                    "net_pnl": net_pnl,
                    "exit_reason": exit_reason_final,
                })
            except Exception:
                pass
        if self._persistence:
            try:
                self._persistence.save_event({
                    "event_type": "trade_closed",
                    "strategy_id": strategy_id,
                    "instrument": fill.instrument,
                    "details": {
                        "trade_id": trade_id,
                        "side": side,
                        "net_pnl": net_pnl,
                    },
                })
            except Exception:
                pass

        # Step 8: Send Telegram notification
        if self._telegram:
            try:
                entry_ts = position.entry_timestamp if hasattr(position, 'entry_timestamp') else 0
                duration_s = fill.timestamp - entry_ts if entry_ts else 0
                hrs, rem = divmod(int(duration_s), 3600)
                mins = rem // 60
                duration_str = f"{hrs}h {mins}m" if hrs else f"{mins}m"
                self._telegram.on_trade_close({
                    "instrument": fill.instrument,
                    "strategy_id": strategy_id,
                    "side": side,
                    "entry_price": position.average_entry,
                    "exit_price": fill.price,
                    "net_pnl": net_pnl,
                    "exit_reason": exit_reason,
                    "duration": duration_str,
                })
            except Exception:
                pass

        print(f"[TradeClose] Closed: strategy={strategy_id} P&L={net_pnl:.2f}", flush=True)
        return {"gross_pnl": gross_pnl, "charges": charges, "net_pnl": net_pnl}
