"""Resolve the shared trading library root for the LIVE app.

The LIVE app is vendored INSIDE the ``MCX-TRADER`` repo (``live/`` package),
so the shared library root IS this repo.  Resolution order:
  1. ``MCX_LIB_ROOT`` environment variable (overrides, if set).
  2. The repository root containing this package (default).
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

APP_ROOT = Path(__file__).resolve().parent.parent
_LIB_ROOT = Path(os.getenv("MCX_LIB_ROOT", "") or APP_ROOT).resolve()


def library_root() -> Path:
    """The shared trading library (``MCX-TRADER``) root directory."""
    return _LIB_ROOT


def bootstrap() -> Path:
    """Place this app root and the shared library root first on ``sys.path``."""
    for p in (APP_ROOT, _LIB_ROOT):
        if str(p) not in sys.path:
            sys.path.insert(0, str(p))
    return _LIB_ROOT