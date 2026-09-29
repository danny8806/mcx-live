"""Load VPS credentials from the process environment or ignored local seed."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Mapping


def load_vps_password(
    seed_path: str | Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> str:
    """Return VPS_PASS without embedding credentials in source or images."""
    env = os.environ if environ is None else environ
    password = (env.get("VPS_PASS") or "").strip()
    if password:
        return password

    path = Path(seed_path) if seed_path else Path(__file__).with_name("mcx-trader.env")
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            key, separator, value = line.partition("=")
            if separator and key.strip() == "VPS_PASS":
                password = value.strip().strip("\"'")
                if password:
                    return password
    except OSError:
        pass

    raise RuntimeError("VPS_PASS is required in the environment or mcx-trader.env")
