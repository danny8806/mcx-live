"""Fill deduplication to prevent duplicate fill processing.

Fills are identified by fill_id (UUID). This module provides:
- In-memory set for fast lookups
- Database-backed persistence for crash recovery
- Atomic check-and-mark to prevent race conditions
"""
from __future__ import annotations

from persistence.database import Database

import threading
from typing import Optional


class FillDeduplicator:
    """Prevents duplicate fill processing."""

    def __init__(self, db_path: str = "trading.db"):
        self._db = Database(db_path)
        self._processed_fills: set[str] = set()
        self._lock = threading.Lock()
        self._init_db()

    def _init_db(self) -> None:
        """Create the processed_fills tracking table if it doesn't exist."""
        with self._db.transaction() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS processed_fills (
                    fill_id TEXT PRIMARY KEY,
                    processed_at TEXT DEFAULT (datetime('now'))
                )
            """)

    def load_from_database(self) -> int:
        """Load processed fill IDs from database on startup.

        Returns:
            Number of processed fills loaded.
        """
        rows = self._db.query("SELECT fill_id FROM processed_fills")
        with self._lock:
            self._processed_fills = {row["fill_id"] for row in rows}
        return len(self._processed_fills)

    def is_duplicate(self, fill_id: str) -> bool:
        """Check if this fill was already processed.

        Checks in-memory set first (fast path), then falls back to database.
        """
        # Fast path: check in-memory set
        with self._lock:
            if fill_id in self._processed_fills:
                return True

        # Slow path: check database
        row = self._db.query_one(
            "SELECT 1 FROM processed_fills WHERE fill_id = ?", (fill_id,)
        )
        if row is not None:
            # Found in DB but not in memory -- populate memory cache
            with self._lock:
                self._processed_fills.add(fill_id)
            return True
        return False

    def mark_processed(self, fill_id: str) -> bool:
        """Mark a fill as processed.

        Atomically inserts into database and adds to in-memory set.
        Returns True if newly marked, False if already existed.

        Raises:
            sqlite3.IntegrityError: If fill_id already exists (caller should
                catch and treat as duplicate).
        """
        from sqlite3 import IntegrityError as _IE
        try:
            with self._db.transaction() as conn:
                conn.execute(
                    "INSERT INTO processed_fills (fill_id) VALUES (?)",
                    (fill_id,),
                )
        except Exception as e:
            # Already in database -- treat as duplicate
            if "UNIQUE" in str(e).upper() or "INTEGRITY" in str(e).upper():
                return False
            raise

        with self._lock:
            self._processed_fills.add(fill_id)
        return True

    def note_processed(self, fill_id: str) -> None:
        """Add a fill to the in-memory dedup set only (no DB write).

        Used to hold the in-process duplicate lock the moment a fill is handed
        to the engine, while the durable DB mark (mark_processed) happens only
        AFTER all financial effects are applied -- closing the crash window in
        which a fill was indelibly marked before its rows were written.
        """
        with self._lock:
            self._processed_fills.add(fill_id)

    def unmark_processed(self, fill_id: str) -> None:
        """Remove a durable mark when a fill was quarantined before booking.

        Callers must first prove that the corresponding financial lifecycle
        effects are still absent.  This is intentionally not a general retry
        mechanism: it exists for narrowly validated recovery paths only.
        """
        with self._lock:
            self._processed_fills.discard(fill_id)
            with self._db.transaction() as conn:
                conn.execute("DELETE FROM processed_fills WHERE fill_id = ?",
                             (fill_id,))

    def claim(self, fill_id: str) -> bool:
        """Atomically check-and-claim a fill in the in-memory set.

        Returns True if this caller is the FIRST to claim the fill (safe to
        process), False if another caller already claimed it (duplicate).
        Unlike ``is_duplicate`` + ``note_processed`` -- two separate lock
        acquisitions -- this single critical section closes the TOCTOU window
        in which two threads both observed 'not duplicate' and both proceeded
        to apply the fill.
        """
        with self._lock:
            if fill_id in self._processed_fills:
                return False
            self._processed_fills.add(fill_id)
            return True

    def is_processed(self, fill_id: str) -> bool:
        """Alias for is_duplicate -- checks if fill was already processed."""
        return self.is_duplicate(fill_id)

    def cleanup_old(self, days: int = 30) -> int:
        """Remove processed fill records older than N days.

        Returns:
            Number of records deleted.
        """
        with self._db.transaction() as conn:
            cursor = conn.execute(
                "DELETE FROM processed_fills WHERE processed_at < datetime('now', ?)",
                (f"-{days} days",),
            )
            deleted = cursor.rowcount

        if deleted > 0:
            # Rebuild in-memory set from database
            self.load_from_database()

        return deleted

    @property
    def count(self) -> int:
        """Number of processed fills tracked."""
        with self._lock:
            return len(self._processed_fills)

    def close(self) -> None:
        """Release the shared database connection reference."""
        if hasattr(self, "_db") and self._db:
            self._db.close()
            self._db = None
