"""Canonical append-only lifecycle event store backed by trading.db."""
from __future__ import annotations

import json
import threading
import time
import uuid
from typing import Optional

from persistence.database import Database


class EventStore:
    """Append-only lifecycle events in the canonical database."""

    EVENT_TYPES = [
        "SIGNAL_CREATED",
        "TRADE_CREATED",
        "TRIGGER_PENDING",
        "TRIGGERED",
        "ORDER_CREATED",
        "ORDER_SUBMITTED",
        "ORDER_ACKNOWLEDGED",
        "ORDER_REJECTED",
        "PARTIALLY_FILLED",
        "POSITION_OPENED",
        "POSITION_UPDATED",
        "STOP_UPDATED",
        "EXIT_SIGNAL",
        "EXIT_ORDER",
        "EXIT_FILL",
        "POSITION_CLOSED",
        "TRADE_CLOSED",
        "FILL",
        "SNAPSHOT",
    ]

    def __init__(self, db_path: str = "trading.db", execution_mode: str = "PAPER"):
        self._db = Database(db_path)
        self._lock = threading.Lock()
        self._sequence = 0
        self.execution_mode = str(execution_mode).upper()

    def record(
        self,
        trade_id: str,
        strategy_id: str,
        instrument: str,
        event_type: str,
        payload: Optional[dict] = None,
        source: str = "system",
        timestamp: Optional[float] = None,
    ) -> str:
        """Record one event atomically and return its immutable event ID."""
        if event_type not in self.EVENT_TYPES:
            raise ValueError(f"Invalid event_type: {event_type}")

        event_id = str(uuid.uuid4())
        event_timestamp = timestamp or time.time()
        with self._lock, self._db.transaction() as conn:
            sequence_no = int(
                conn.execute(
                    "SELECT COALESCE(MAX(sequence_no), 0) + 1 "
                    "FROM trade_events WHERE trade_id = ?",
                    (trade_id,),
                ).fetchone()[0]
            )
            conn.execute(
                """INSERT INTO trade_events
                   (event_id, trade_id, strategy_id, instrument, timestamp,
                    event_type, event_version, payload_json, sequence_no,
                    execution_mode)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    event_id,
                    trade_id,
                    strategy_id,
                    instrument,
                    event_timestamp,
                    event_type,
                    1,
                    json.dumps({"source": source, **(payload or {})}),
                    sequence_no,
                    self.execution_mode,
                ),
            )
        return event_id

    def get_events_for_trade(self, trade_id: str) -> list[dict]:
        return self._db.query(
            "SELECT * FROM trade_events WHERE trade_id = ? ORDER BY sequence_no",
            (trade_id,),
        )

    def get_events_for_strategy(
        self,
        strategy_id: str,
        event_type: Optional[str] = None,
        limit: int = 1000,
    ) -> list[dict]:
        if event_type:
            return self._db.query(
                "SELECT * FROM trade_events "
                "WHERE strategy_id = ? AND event_type = ? "
                "ORDER BY sequence_no DESC LIMIT ?",
                (strategy_id, event_type, limit),
            )
        return self._db.query(
            "SELECT * FROM trade_events WHERE strategy_id = ? "
            "ORDER BY sequence_no DESC LIMIT ?",
            (strategy_id, limit),
        )

    def get_events_in_range(
        self,
        start_time: float,
        end_time: float,
        event_type: Optional[str] = None,
    ) -> list[dict]:
        if event_type:
            return self._db.query(
                "SELECT * FROM trade_events WHERE timestamp BETWEEN ? AND ? "
                "AND event_type = ? ORDER BY timestamp",
                (start_time, end_time, event_type),
            )
        return self._db.query(
            "SELECT * FROM trade_events WHERE timestamp BETWEEN ? AND ? "
            "ORDER BY timestamp",
            (start_time, end_time),
        )

    def count_events(self, strategy_id: Optional[str] = None) -> int:
        if strategy_id:
            return int(
                self._db.scalar(
                    "SELECT COUNT(*) FROM trade_events WHERE strategy_id = ?",
                    (strategy_id,),
                )
            )
        return int(self._db.scalar("SELECT COUNT(*) FROM trade_events"))

    def get_event_by_id(self, event_id: str) -> Optional[dict]:
        return self._db.query_one(
            "SELECT * FROM trade_events WHERE event_id = ?", (event_id,)
        )
