"""Reconciliation engine for verifying consistency across all system components.

Compares orders, fills, positions, trades, P&L, and account state
to detect discrepancies after startup, reconnection, or restart.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from persistence.manager import PersistenceManager
from persistence.database import Database
from portfolio.position_manager import PositionManager
from portfolio.pnl import PNLEngine
from portfolio.account import AccountEngine
from execution.order_manager import OrderManager


@dataclass
class ReconciliationResult:
    """Structured output from a reconciliation run."""
    is_consistent: bool = True
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    stats: dict[str, Any] = field(default_factory=dict)
    phase: str = "startup"
    timestamp: str = ""

    def add_error(self, msg: str) -> None:
        self.errors.append(msg)
        self.is_consistent = False

    def add_warning(self, msg: str) -> None:
        self.warnings.append(msg)

    def summary(self) -> str:
        lines = [
            f"=== Reconciliation Report ({self.phase}) ===",
            f"Timestamp: {self.timestamp}",
            f"Consistent: {self.is_consistent}",
            f"Errors: {len(self.errors)}",
            f"Warnings: {len(self.warnings)}",
        ]
        for e in self.errors:
            lines.append(f"  [ERROR] {e}")
        for w in self.warnings:
            lines.append(f"  [WARN]  {w}")
        if self.stats:
            lines.append("Stats:")
            for k, v in self.stats.items():
                lines.append(f"  {k}: {v}")
        return "\n".join(lines)


class ReconciliationEngine:
    """Cross-validates all trading system components.

    Pulls authoritative state from SQLite (orders, fills, trades)
    and compares it against in-memory manager state to detect:
    - Missing or extra orders/fills/trades
    - Duplicate fills or orders
    - Position mismatches
    - P&L inconsistencies
    - Account margin mismatches
    """

    TOLERANCE = 1e-6

    def __init__(
        self,
        persistence: PersistenceManager,
        position_manager: PositionManager,
        pnl_engines: dict[str, PNLEngine],
        account_engines: dict[str, AccountEngine],
        strategies: dict[str, Any],
        order_manager: OrderManager,
    ):
        self.persistence = persistence
        self.position_manager = position_manager
        self.pnl_engines = pnl_engines
        self.account_engines = account_engines
        self.strategies = strategies
        self.order_manager = order_manager

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def reconcile(self, phase: str = "startup") -> ReconciliationResult:
        """Run full reconciliation across all components.

        Args:
            phase: One of "startup", "reconnect", "restart".

        Returns:
            ReconciliationResult with errors, warnings, and stats.
        """
        result = ReconciliationResult(
            phase=phase,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )

        # Use the shared Database connection for the read-only check.  This
        # instance's reference is released as soon as the rows are materialised:
        # Database shares ONE process-wide connection and only closes it when the
        # LAST reference is released, so leaking a ref here pins the connection
        # open for the life of the process.
        db = Database(self.persistence.db_path)
        try:
            db_orders = self._load_db_orders(db)
            db_fills = self._load_db_fills(db)
            db_trades = self._load_db_trades(db)
        finally:
            db.close()

        # Collect in-memory state
        mem_orders = self._load_mem_orders()
        mem_fills = self._load_mem_fills()
        mem_positions_open = self.position_manager.open_positions
        mem_positions_closed = self.position_manager.closed_positions

        # Stats
        result.stats = {
            "db_orders": len(db_orders),
            "db_fills": len(db_fills),
            "db_trades": len(db_trades),
            "mem_orders": len(mem_orders),
            "mem_fills": len(mem_fills),
            "open_positions": len(mem_positions_open),
            "closed_positions": len(mem_positions_closed),
        }

        # Run all checks
        for check in [
            lambda: self._check_orders_vs_fills(db_orders, db_fills, result),
            lambda: self._check_fills_vs_positions(db_fills, db_trades, mem_positions_open, mem_positions_closed, result),
            lambda: self._check_positions_vs_trades(mem_positions_open, mem_positions_closed, db_trades, result),
            lambda: self._check_trades_vs_pnl(db_trades, result),
            lambda: self._check_accounts_vs_positions(mem_positions_open, result),
            lambda: self._check_duplicate_fills(db_fills, result),
            lambda: self._check_duplicate_orders(db_orders, result),
            lambda: self._check_db_vs_memory_orders(db_orders, mem_orders, result),
            lambda: self._check_db_vs_memory_fills(db_fills, mem_fills, result),
            lambda: self._check_price_sanity(db_fills, db_trades, result),
        ]:
            try:
                check()
            except Exception as exc:
                result.add_warning(f"Check failed with exception: {exc}")

        return result

    # ------------------------------------------------------------------
    # Database loaders
    # ------------------------------------------------------------------

    @staticmethod
    def _load_db_orders(conn: sqlite3.Connection) -> list[dict]:
        rows = conn.execute("SELECT * FROM orders ORDER BY id").fetchall()
        return [dict(r) for r in rows]

    @staticmethod
    def _load_db_fills(conn: sqlite3.Connection) -> list[dict]:
        rows = conn.execute("SELECT * FROM fills ORDER BY id").fetchall()
        return [dict(r) for r in rows]

    @staticmethod
    def _load_db_trades(conn: sqlite3.Connection) -> list[dict]:
        rows = conn.execute("SELECT * FROM trades ORDER BY id").fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # In-memory loaders
    # ------------------------------------------------------------------

    def _load_mem_orders(self) -> dict[str, dict]:
        """Build a dict of order_id -> order info from the execution engine."""
        orders: dict[str, dict] = {}
        exec_engine: Any = self.order_manager.execution_engine
        for oid, order in exec_engine._orders.items():
            orders[oid] = {
                "order_id": order.order_id,
                "strategy_id": order.strategy_id,
                "instrument": order.instrument,
                "side": order.side,
                "quantity": order.quantity,
                "state": order.state.value,
                "fill_ids": list(order.fill_ids),
                "filled_quantity": order.filled_quantity,
                "average_fill_price": order.average_fill_price,
            }
        return orders

    def _load_mem_fills(self) -> dict[str, dict]:
        """Build a dict of fill_id -> fill info from the execution engine."""
        fills: dict[str, dict] = {}
        exec_engine: Any = self.order_manager.execution_engine
        for fill in exec_engine._fills:
            fills[fill.fill_id] = {
                "fill_id": fill.fill_id,
                "order_id": fill.order_id,
                "instrument": fill.instrument,
                "side": fill.side,
                "quantity": fill.quantity,
                "price": fill.price,
                "strategy_id": fill.strategy_id,
                "multiplier": fill.multiplier,
            }
        return fills

    # ------------------------------------------------------------------
    # Individual checks
    # ------------------------------------------------------------------

    def _check_orders_vs_fills(
        self,
        db_orders: list[dict],
        db_fills: list[dict],
        result: ReconciliationResult,
    ) -> None:
        """Every FILLED order must have at least one fill. Fills must reference valid orders."""
        fills_by_order: dict[str, list[dict]] = {}
        for f in db_fills:
            oid = f.get("order_id", "")
            fills_by_order.setdefault(oid, []).append(f)

        for order in db_orders:
            state = order.get("state", "")
            oid = order.get("order_id", "")
            recorded_fills = fills_by_order.get(oid, [])

            if state == "filled":
                if not recorded_fills:
                    result.add_error(
                        f"Order {oid} is FILLED in DB but has zero fills"
                    )
            elif state in ("created", "submitted", "partially_filled", "acknowledged"):
                # Active orders with fills is suspicious
                if recorded_fills:
                    result.add_warning(
                        f"Order {oid} is in state '{state}' but has {len(recorded_fills)} fill(s)"
                    )
            # rejected/canceled orders: fills are not expected

        # Check fills referencing non-existent orders
        order_ids = {o.get("order_id") for o in db_orders}
        for f in db_fills:
            oid = f.get("order_id", "")
            if oid and oid not in order_ids:
                result.add_error(
                    f"Fill {f.get('fill_id')} references non-existent order {oid}"
                )

    def _check_fills_vs_positions(
        self,
        db_fills: list[dict],
        db_trades: list[dict],
        open_positions: list,
        closed_positions: list,
        result: ReconciliationResult,
    ) -> None:
        """Every entry fill should correspond to a position. Exit fills should close one."""
        # Build sets of known fill IDs from positions
        entry_fill_ids: set[str] = set()
        exit_fill_ids: set[str] = set()

        position_by_trade: dict[str, Any] = {}
        for pos in open_positions + closed_positions:
            if pos.trade_id:
                position_by_trade.setdefault(pos.trade_id, []).append(pos)
            entry_fill_ids.update(pos.entry_fill_ids)
            for ef in pos.exit_fills:
                exit_fill_ids.add(ef.fill_id)

        db_fill_ids = {f.get("fill_id") for f in db_fills}
        # ### OBSERVATION (restored-position entry fills)
        # Positions restored from the in-memory state carry entry_fill_ids that
        # were never persisted for trades whose fills predate the durable
        # linkage (gold's historical fills).  Flagging those as a hard ERROR
        # every restart is misleading — they cannot be repaired by
        # reconciliation and are simply pre-durability history.  Only trades
        # that DO have a canonical DB row are expected to have their fills in
        # the DB; the rest are downgraded to a warning.
        db_trade_ids = {t.get("trade_id") for t in db_trades if t.get("trade_id")}

        # DB fills not tracked by any position
        orphan_fills = db_fill_ids - entry_fill_ids - exit_fill_ids
        if orphan_fills:
            result.add_warning(
                f"{len(orphan_fills)} fill(s) in DB not linked to any position: "
                f"{list(orphan_fills)[:5]}{'...' if len(orphan_fills) > 5 else ''}"
            )

        # Position entry fill IDs missing from DB
        missing_entry = entry_fill_ids - db_fill_ids
        if missing_entry:
            # Group by whether the owning trade has a canonical DB row.
            missing_db_linked: set[str] = set()
            missing_memory_only: set[str] = set()
            for fid in missing_entry:
                owning_trade_ids = [
                    tid for tid, poses in position_by_trade.items()
                    if any(fid in (p.entry_fill_ids or []) for p in poses)
                ]
                if owning_trade_ids and any(t in db_trade_ids for t in owning_trade_ids):
                    missing_db_linked.add(fid)
                else:
                    missing_memory_only.add(fid)
            if missing_db_linked:
                result.add_error(
                    f"{len(missing_db_linked)} position entry fill(s) not in DB: "
                    f"{list(missing_db_linked)[:5]}"
                )
            if missing_memory_only:
                result.add_warning(
                    f"{len(missing_memory_only)} restored position entry fill(s) not in DB "
                    f"(pre-durability history, trade has no canonical row): "
                    f"{list(missing_memory_only)[:5]}"
                )

    def _check_positions_vs_trades(
        self,
        open_positions: list,
        closed_positions: list,
        db_trades: list[dict],
        result: ReconciliationResult,
    ) -> None:
        """Compare positions and trades through the explicit child link.

        Position IDs are never used as trade identity. A missing link is an
        integrity error and cannot be repaired by a latest-object lookup.
        """
        open_trade_ids = {p.trade_id for p in open_positions if p.trade_id}
        db_trade_ids = {t.get("trade_id") for t in db_trades if t.get("trade_id")}

        # A persisted "closed" trade row whose trade_id is still an open position
        # in memory means the close was written but the in-memory position was
        # not closed (e.g. crash between persist and memory update).
        for trade in db_trades:
            tid = trade.get("trade_id")
            if tid and trade.get("status") == "closed" and tid in open_trade_ids:
                result.add_error(
                    f"Trade {tid} is closed in DB but position is still open in memory "
                    f"for {trade.get('instrument')} ({trade.get('strategy_id')})"
                )

        # A recently closed in-memory position with no DB trade row means the
        # close was never persisted. trade_close persists BEFORE closing in
        # memory, so a closed in-memory position must always have its row.
        missing = [p.position_id for p in closed_positions if p.trade_id not in db_trade_ids]
        if missing:
            result.add_error(
                f"{len(missing)} closed position(s) have no trade row in DB: "
                f"{missing[:5]}{'...' if len(missing) > 5 else ''}"
            )

    def _check_trades_vs_pnl(
        self,
        db_trades: list[dict],
        result: ReconciliationResult,
    ) -> None:
        """Sum of trade net_pnl in DB should match PNLEngine realized_net per strategy."""
        trade_pnl_by_strategy: dict[str, float] = {}
        trade_count_by_strategy: dict[str, int] = {}

        for trade in db_trades:
            strat = trade.get("strategy_id", "")
            net = trade.get("net_pnl") or 0.0
            trade_pnl_by_strategy[strat] = trade_pnl_by_strategy.get(strat, 0.0) + net
            # Only count closed trades (PNLEngine only tracks realized/closed)
            status = (trade.get("status") or "").lower()
            if status in ("closed",):
                trade_count_by_strategy[strat] = trade_count_by_strategy.get(strat, 0) + 1

        for strat_id, engine in self.pnl_engines.items():
            db_pnl = trade_pnl_by_strategy.get(strat_id, 0.0)
            mem_pnl = engine.realized_net
            if abs(db_pnl - mem_pnl) > self.TOLERANCE:
                result.add_error(
                    f"P&L mismatch for {strat_id}: DB trade sum = {db_pnl:.2f}, "
                    f"PNLEngine realized_net = {mem_pnl:.2f}"
                )

            db_count = trade_count_by_strategy.get(strat_id, 0)
            mem_count = engine.trade_count
            if db_count != mem_count:
                result.add_error(
                    f"Trade count mismatch for {strat_id}: DB = {db_count}, "
                    f"PNLEngine = {mem_count}"
                )

    def _check_accounts_vs_positions(
        self,
        open_positions: list,
        result: ReconciliationResult,
    ) -> None:
        """Account used_margin should approximately equal sum of position margins."""
        for strat_id, account in self.account_engines.items():
            mem_margin = account.used_margin
            calc_margin = sum(
                p.margin for p in open_positions if p.strategy_id == strat_id
            )
            if abs(mem_margin - calc_margin) > self.TOLERANCE:
                result.add_error(
                    f"Margin mismatch for {strat_id}: AccountEngine used_margin = "
                    f"{mem_margin:.2f}, sum of position margins = {calc_margin:.2f}"
                )

    def _check_duplicate_fills(
        self,
        db_fills: list[dict],
        result: ReconciliationResult,
    ) -> None:
        """No duplicate fill_id in the database."""
        seen: dict[str, int] = {}
        for f in db_fills:
            fid = f.get("fill_id", "")
            seen[fid] = seen.get(fid, 0) + 1
        dupes = {k: v for k, v in seen.items() if v > 1}
        if dupes:
            for fid, count in dupes.items():
                result.add_error(
                    f"Duplicate fill_id '{fid}' found {count} times in DB"
                )

    def _check_duplicate_orders(
        self,
        db_orders: list[dict],
        result: ReconciliationResult,
    ) -> None:
        """No duplicate order_id in the database."""
        seen: dict[str, int] = {}
        for o in db_orders:
            oid = o.get("order_id", "")
            seen[oid] = seen.get(oid, 0) + 1
        dupes = {k: v for k, v in seen.items() if v > 1}
        if dupes:
            for oid, count in dupes.items():
                result.add_error(
                    f"Duplicate order_id '{oid}' found {count} times in DB"
                )

    def _check_db_vs_memory_orders(
        self,
        db_orders: list[dict],
        mem_orders: dict[str, dict],
        result: ReconciliationResult,
    ) -> None:
        """Compare database orders against in-memory orders."""
        db_ids = {o.get("order_id") for o in db_orders}
        mem_ids = set(mem_orders.keys())

        # The in-memory engine restores working orders, not the complete
        # historical order book. A terminal DB row missing from memory is
        # therefore expected after restart; active orders missing from memory
        # remain a reconciliation warning.
        terminal = {"filled", "rejected", "cancelled", "canceled", "expired"}
        missing_in_mem = {
            order.get("order_id") for order in db_orders
            if order.get("order_id") not in mem_ids
            and str(order.get("state") or "").strip().lower() not in terminal
        }
        extra_in_mem = mem_ids - db_ids

        if missing_in_mem:
            result.add_warning(
                f"{len(missing_in_mem)} nonterminal order(s) in DB but not in memory: "
                f"{list(missing_in_mem)[:5]}{'...' if len(missing_in_mem) > 5 else ''}"
            )

        if extra_in_mem:
            result.add_warning(
                f"{len(extra_in_mem)} order(s) in memory but not in DB: "
                f"{list(extra_in_mem)[:5]}{'...' if len(extra_in_mem) > 5 else ''}"
            )

        # For orders present in both, verify state consistency
        for oid in db_ids & mem_ids:
            db_order = next(o for o in db_orders if o.get("order_id") == oid)
            mem_order = mem_orders[oid]
            db_state = db_order.get("state", "")
            mem_state = mem_order.get("state", "")
            if db_state != mem_state:
                result.add_error(
                    f"Order {oid} state mismatch: DB='{db_state}', memory='{mem_state}'"
                )

    def _check_db_vs_memory_fills(
        self,
        db_fills: list[dict],
        mem_fills: dict[str, dict],
        result: ReconciliationResult,
    ) -> None:
        """Compare database fills against in-memory fills."""
        db_ids = {f.get("fill_id") for f in db_fills}
        mem_ids = set(mem_fills.keys())

        missing_in_mem = db_ids - mem_ids
        extra_in_mem = mem_ids - db_ids

        if missing_in_mem:
            result.add_warning(
                f"{len(missing_in_mem)} fill(s) in DB but not in memory: "
                f"{list(missing_in_mem)[:5]}{'...' if len(missing_in_mem) > 5 else ''}"
            )

        if extra_in_mem:
            result.add_warning(
                f"{len(extra_in_mem)} fill(s) in memory but not in DB: "
                f"{list(extra_in_mem)[:5]}{'...' if len(extra_in_mem) > 5 else ''}"
            )

        # For fills present in both, verify price consistency
        for fid in db_ids & mem_ids:
            db_fill = next(f for f in db_fills if f.get("fill_id") == fid)
            mem_fill = mem_fills[fid]
            db_price = db_fill.get("price", 0.0)
            mem_price = mem_fill.get("price", 0.0)
            if abs(db_price - mem_price) > self.TOLERANCE:
                result.add_error(
                    f"Fill {fid} price mismatch: DB={db_price}, memory={mem_price}"
                )
            db_qty = db_fill.get("quantity", 0)
            mem_qty = mem_fill.get("quantity", 0)
            if db_qty != mem_qty:
                result.add_error(
                    f"Fill {fid} quantity mismatch: DB={db_qty}, memory={mem_qty}"
                )

    def _check_price_sanity(
        self,
        db_fills: list[dict],
        db_trades: list[dict],
        result: ReconciliationResult,
    ) -> None:
        """Reject non-positive / non-finite prices anywhere in the book.

        This is the guard that would have caught the `-1` no-data sentinel
        corruption: a trade/fill booked at `-1` is nonsense regardless of the
        rest of the DB<->memory consistency. Both sources can carry the same
        poison yet still 'agree', so this is intentionally independent of the
        cross-source checks.
        """
        bad_fills = []
        for f in db_fills:
            price = f.get("price")
            if price is None or price <= 0.0 or (
                isinstance(price, float) and (price != price or abs(price) == float("inf"))
            ):
                bad_fills.append((f.get("fill_id"), price))
        if bad_fills:
            result.add_error(
                f"{len(bad_fills)} fill(s) with invalid price (<=0/NaN/inf): "
                f"{bad_fills[:5]}{'...' if len(bad_fills) > 5 else ''}"
            )

        bad_trades = []
        for t in db_trades:
            status = str(t.get("status", "")).lower()
            # A zero entry price is expected for an unfilled pending,
            # rejected, or cancelled entry. Only trades with confirmed
            # exposure (or a completed lifecycle) must have a valid entry.
            entry_required = (status in ("open", "exit_pending", "closed")
                              or bool(t.get("entry_fill_id")))
            keys = ["entry_price"] if entry_required else []
            if status == "closed":
                keys.append("exit_price")
            for key in keys:
                price = t.get(key)
                if price is None or price <= 0.0 or (
                    isinstance(price, float) and (price != price or abs(price) == float("inf"))
                ):
                    bad_trades.append((t.get("trade_id"), key, price))
        if bad_trades:
            result.add_error(
                f"{len(bad_trades)} trade price(s) invalid (<=0/NaN/inf): "
                f"{bad_trades[:5]}{'...' if len(bad_trades) > 5 else ''}"
            )
