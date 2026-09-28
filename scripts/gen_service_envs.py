"""Generate the env file used by docker-compose (live-only deployment).

Reads the gitignored credential seed (mcx-trader.env) and writes the single
gitignored .env.live.  Values are placeholders when the seed is missing so the
file can also serve as a template.

Usage:
    python scripts/gen_service_envs.py [--env-file mcx-trader.env]
"""
from __future__ import annotations

import argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def load_env(path: Path) -> dict:
    seed = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            seed[k.strip()] = v.strip().strip('"').strip("'")
    return seed


def block_env(name: str, entries: dict) -> str:
    lines = [f"# --- {name} ---"]
    for k, v in entries.items():
        lines.append(f"{k}={v}" if v else f"{k}=")
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env-file", default=str(ROOT / "mcx-trader.env"))
    args = ap.parse_args()
    seed = load_env(Path(args.env_file))

    def ph(key: str) -> str:
        return seed.get(key, "CHANGE_ME")

    # SPA runs same-origin (empty APP_API_BASE / APP_WS_BASE) — nginx proxies
    # /api/*, /ws, and / straight to the single live-mcx upstream.
    env = {
        ".env.live": {
            "LIVE_HOST": "0.0.0.0", "LIVE_PORT": "8001",
            "LIVE_CONFIG": "config/live_settings.json",
            "TRADING_MODE": "LIVE", "REAL_ORDER_EXECUTION": "1",
            "BROKER_ENABLED": "1",
            "DHAN_CLIENT_ID": ph("DHAN_CLIENT_ID"),
            "DHAN_ACCESS_TOKEN": seed.get("DHAN_ACCESS_TOKEN", ""),
            "TRADING_PIN": ph("TRADING_PIN"),
            "TOTP_SECRET": ph("TOTP_SECRET"),
            "TELEGRAM_BOT_TOKEN": seed.get("TELEGRAM_BOT_TOKEN", ""),
            "TELEGRAM_CHAT_ID": seed.get("TELEGRAM_CHAT_ID", ""),
            "APP_API_BASE": "", "APP_WS_BASE": "",
            "TZ": "Asia/Kolkata",
        },
    }
    (ROOT / ".env.live").write_text(
        block_env(".env.live", env[".env.live"]) + "\n", encoding="utf-8", newline="\n"
    )
    print(f"wrote {ROOT / '.env.live'} (gitignored *.env)")


if __name__ == "__main__":
    main()