import hashlib
import os

FILES = [
    "trading_engine.py",
    "persistence/database.py",
    "persistence/manager.py",
    "execution/live/order_watcher.py",
    "dashboard/routes/reversals.py",
    "live/api.py",
    "live/_api_patched.py",
    "config/live_settings.json",
    "strategies/types.py",
    "strategies/instance.py",
]

for rel in FILES:
    p = "/app/" + rel
    if os.path.exists(p):
        h = hashlib.sha256(open(p, "rb").read()).hexdigest()[:16]
        print(f"{rel} {h}")
    else:
        print(f"{rel} MISSING-IN-CONTAINER")