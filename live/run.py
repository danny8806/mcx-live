"""Uvicorn entry point for the LIVE application (own process, own port).

The LIVE container runs this as PID 1: it owns the LIVE engine graph, the
LIVE database, the LIVE API and the LIVE workers.  Default port 8001
(``LIVE_PORT``), host ``LIVE_HOST`` (default 0.0.0.0 — container friendly).
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

_PKG_DIR = Path(__file__).resolve().parent
if str(_PKG_DIR) not in sys.path:
    sys.path.insert(0, str(_PKG_DIR))

import paths as _paths  # noqa: E402
_paths.bootstrap()

import uvicorn  # noqa: E402


def main() -> None:
    from services.safety import assert_runtime_mode
    assert_runtime_mode("LIVE", real_order_execution_required=True)
    from live.engine import LiveEngine
    from live.api import create_live_app

    root_cfg = _PKG_DIR.parent / "config" / "live_settings.json"
    default_cfg = str(root_cfg if root_cfg.exists() else _PKG_DIR / "config" / "live_settings.json")
    config_path = os.getenv("LIVE_CONFIG", default_cfg)
    app = create_live_app(LiveEngine(config_path=config_path))
    host = os.getenv("LIVE_HOST", "0.0.0.0")
    port = int(os.getenv("LIVE_PORT", "8001"))
    uvicorn.run(app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()