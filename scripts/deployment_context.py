"""Filters for files that must never enter a VPS deployment context."""

SKIP_DIRS = {
    ".git", "__pycache__", ".pytest_cache", "node_modules", "reports",
    "replay_output", "tests", "tools", "scripts", "data/db", "live/data/db",
    "dashboard-ui/dist", ".vscode", ".idea", "logs", "nginx/certs",
}
SKIP_ROOT_PREFIXES = ("p6_", "live_verify", "vps_", "verify_", "check_")
SKIP_ROOT_NAMES = {"deploy_restart.py"}
SKIP_SUFFIXES = (
    ".pyc", ".pyo", ".db", ".db-shm", ".db-wal", ".log", ".tmp",
    ".bak", ".lock", ".crt", ".key",
)
ENV_FILE_PREFIXES = ("mcx-trader.env", ".env.live")


def should_skip(path: str) -> bool:
    """True for credential-bearing diagnostics, secrets, or runtime data."""
    rel = path.replace("\\", "/").strip("/")
    if not rel:
        return False
    parts = rel.split("/")
    if parts[0] == "tools":
        return True
    if len(parts) == 1 and (
        rel.startswith(SKIP_ROOT_PREFIXES) or rel in SKIP_ROOT_NAMES
    ):
        return True
    if rel in ENV_FILE_PREFIXES or ("/" not in rel and rel.endswith(".env")):
        return True
    name_skips = {p for p in SKIP_DIRS if "/" not in p}
    if any(part in name_skips for part in parts):
        return True
    if any(rel == p or rel.startswith(p + "/") for p in SKIP_DIRS):
        return True
    return rel.lower().endswith(SKIP_SUFFIXES)
