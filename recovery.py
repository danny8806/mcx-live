"""Canonical startup recovery and safe-mode checks."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from persistence.database import Database


@dataclass
class RecoveryReport:
    healthy: bool
    integrity_errors: list[str]
    foreign_key_errors: list[dict]
    missing_lineage: list[dict]
    open_trade_ids: list[str]


class TradeRecoveryManager:
    """Validate and reconstruct runtime trade state from trading.db."""

    def __init__(self, db_path: str | Path):
        self.database = Database(db_path)

    def verify(self) -> RecoveryReport:
        integrity_errors = [e for e in self.database.integrity_check() if e != "ok"]
        foreign_key_errors = self.database.foreign_key_check()
        missing_lineage = self.database.query(
            "SELECT trade_id, 'missing_entry_signal' AS reason FROM trades "
            "WHERE entry_signal_id IS NULL OR entry_signal_id = '' "
            "UNION ALL SELECT order_id, 'missing_trade' FROM orders "
            "WHERE trade_id IS NULL OR trade_id = '' "
            "UNION ALL SELECT fill_id, 'missing_trade_or_order' FROM fills "
            "WHERE trade_id IS NULL OR trade_id = '' OR order_id IS NULL OR order_id = ''"
        )
        open_trade_ids = [
            row["trade_id"]
            for row in self.database.query(
                "SELECT trade_id FROM trades WHERE status IN "
                "('OPEN', 'PENDING', 'PENDING_ENTRY', 'EXIT_PENDING', 'OPEN')"
            )
        ]
        return RecoveryReport(
            healthy=not integrity_errors and not foreign_key_errors and not missing_lineage,
            integrity_errors=integrity_errors,
            foreign_key_errors=foreign_key_errors,
            missing_lineage=missing_lineage,
            open_trade_ids=open_trade_ids,
        )

    def restore_lifecycle(self, lifecycle_manager) -> RecoveryReport:
        report = self.verify()
        if not report.healthy:
            raise RuntimeError(
                "canonical database recovery failed: "
                f"integrity={report.integrity_errors}, "
                f"foreign_keys={report.foreign_key_errors}, "
                f"lineage={report.missing_lineage}"
            )
        lifecycle_manager.restore_from_db()
        return report
