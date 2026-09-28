"""Headless LIVE trader entry point (no UI required for trading).

A single daemon process that owns the LIVE engine graph, restores its own
durable state, connects directly to Dhan (market WS + REST + order WS) and
trades without any frontend/browser dependency.
"""
from __future__ import annotations

import os
import signal
import sys
import time
from pathlib import Path

_PKG_DIR = Path(__file__).resolve().parent
if str(_PKG_DIR) not in sys.path:
    sys.path.insert(0, str(_PKG_DIR))

import paths as _paths  # noqa: E402
_paths.bootstrap()

from config import Config  # noqa: E402


def _default_config_path() -> str:
    root_cfg = _PKG_DIR.parent / "config" / "live_settings.json"
    if root_cfg.exists():
        return str(root_cfg)
    return str(_PKG_DIR / "config" / "live_settings.json")


def _build() -> "LiveEngine":
    from live.engine import LiveEngine
    config_path = os.getenv("LIVE_CONFIG", _default_config_path())
    return LiveEngine(config_path=config_path)


def main() -> None:
    App = _build()
    Config.validate_live_security()
    App.restore()
    App.start()

    _halting = {"stop": False}

    def _shutdown(signum, _frame):
        if _halting["stop"]:
            return
        _halting["stop"] = True
        print(f"[LiveApp] signal {signum}: stopping", file=sys.stderr, flush=True)
        try:
            App.stop()
        except Exception as e:  # noqa: BLE001
            print(f"[LiveApp] shutdown error: {e}", file=sys.stderr, flush=True)
        sys.exit(0)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _shutdown)
        except (ValueError, OSError):
            pass

    print(f"[LiveApp] LIVE engine started (db={App.db_path})", file=sys.stderr, flush=True)
    while True:
        time.sleep(60)
        if _halting["stop"]:
            break
        try:
            state = App.engine.snapshot("live")
            if state:
                App.persistence.save_state(state)
                App.persistence.save_account_snapshot_from_state(state)
        except Exception as e:  # noqa: BLE001
            print(f"[LiveApp] periodic save failed: {e}", file=sys.stderr, flush=True)


if __name__ == "__main__":
    main()