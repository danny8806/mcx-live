"""LiveEngine — the LIVE application's own engine graph.

Wraps :class:`trading_engine.TradingEngine` in ``live_only`` mode and attaches
the LIVE app's own persistence (``live/data/db/live_trading.db``), so the
entire runtime (EventBus, market data, indicators, strategies, risk,
execution, positions, P&L, persistence) belongs to the LIVE app alone — never
shared with DEMO.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

from persistence.manager import PersistenceManager
from trading_engine import TradingEngine
from live.paths import APP_ROOT

log = logging.getLogger("live.engine")

DEFAULT_CONFIG = "config/live_settings.json"


def _app_path(value: str) -> Path:
    """Resolve a config path against the LIVE app folder when it is relative."""
    p = Path(value)
    return (APP_ROOT / p) if not p.is_absolute() else p


def _resolve_live_config(config_path) -> Path:
    """Materialize an APP_ROOT-anchored copy of the LIVE config.

    The shared library's ``Config.resolve_path`` anchors every relative path
    to the library root (MCX-TRADER), not to this app folder.  To make the
    LIVE app own its DB/state/credentials dirs wherever it runs (local folder
    or container), the LIVE paths are rewritten to be absolute against this
    app folder BEFORE the engine builds its environments.  The rest of the
    config (instruments, strategies) stays untouched and portable.
    """
    raw = json.loads(Path(config_path).read_text(encoding="utf-8"))
    system = raw.setdefault("system", {})
    system["live_db_path"] = str(_app_path(
        system.get("live_db_path", "live/data/db/live_trading.db")))
    system["live_state_path"] = str(_app_path(
        system.get("live_state_path", "live/data/db/live_system_state.json")))
    if "token_file" in raw.get("dhan", {}):
        raw["dhan"]["token_file"] = str(_app_path(raw["dhan"]["token_file"]))
    resolved = APP_ROOT / "config" / "live_settings.resolved.json"
    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(json.dumps(raw, indent=2), encoding="utf-8")
    return resolved


class LiveEngine:
    """Own the LIVE app's engine + persistence and its lifecycle."""

    def __init__(self, config_path: str = DEFAULT_CONFIG) -> None:
        self.config_path = str(config_path)
        resolved = _resolve_live_config(self.config_path)
        self.engine: TradingEngine = TradingEngine(
            config_path=str(resolved), live_only=True)
        if not self.engine._live_only:
            raise RuntimeError("LiveEngine requires a live-only engine build")
        if self.engine.paper is not None:
            raise RuntimeError("LiveEngine must not own a PAPER runtime")

        db = _app_path(self.engine.config.get(
            "system", {}).get("live_db_path", "live/data/db/live_trading.db"))
        state = _app_path(self.engine.config.get(
            "system", {}).get("live_state_path", "live/data/db/live_system_state.json"))
        Path(db).parent.mkdir(parents=True, exist_ok=True)

        self.persistence = PersistenceManager(state_path=str(state), db_path=str(db))
        self.engine.set_persistence(self.persistence, env_name="live")

    # ── convenience surface (LIVE is the pivot env in live_only mode) ──────
    @property
    def env(self):
        return self.engine.live

    @property
    def event_bus(self):
        return self.engine.event_bus

    @property
    def config(self):
        return self.engine.config

    @property
    def broker(self):
        return self.env.broker

    @property
    def db_path(self) -> str:
        return str(self.persistence.db_path)

    # ── lifecycle ──────────────────────────────────────────────────────────
    def restore(self) -> Optional[dict]:
        """Restore the LIVE engine from its own durable snapshot (if any).
        Returns the loaded state or None."""
        saved = self.persistence.load_state()
        if saved:
            try:
                self.engine.restore(saved, env_name="live")
            except Exception as e:  # noqa: BLE001
                log.error("[LiveEngine] restore failed: %s", e)
        return saved

    def start(self) -> None:
        self.engine.start()

    def stop(self) -> None:
        try:
            state = self.engine.snapshot("live")
            if state:
                self.persistence.save_state(state)
                try:
                    self.persistence.save_account_snapshot_from_state(state)
                except Exception:  # noqa: BLE001
                    pass
        except Exception as e:  # noqa: BLE001
            log.warning("[LiveEngine] final snapshot save failed: %s", e)
        try:
            self.engine.stop()
        finally:
            try:
                self.persistence.close()
            except Exception:  # noqa: BLE001
                pass

    def snapshot(self) -> dict:
        return self.engine.snapshot("live")

    def __repr__(self) -> str:
        return (f"LiveEngine(engine=<{type(self.engine).__name__} live_only>, "
                f"db={self.db_path})")