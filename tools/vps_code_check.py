"""Check if container code matches local codebase."""
import paramiko, hashlib, os

env = {}
for line in open("mcx-trader.env"):
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        env[k] = v

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect("200.234.44.93", username="root", password=env["VPS_PASS"], timeout=15)

def run(cmd, timeout=15):
    _, o, e = ssh.exec_command(cmd, timeout=timeout)
    return o.read().decode("utf-8", "replace").strip()

def md5(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def remote_md5(path):
    return run(f"md5sum {path} 2>/dev/null | cut -d' ' -f1")

# Key files to check
files = [
    "execution/price_model.py",
    "execution/live/order_watcher.py",
    "execution/live/engine.py",
    "execution/live/dhan_transport.py",
    "trading_engine.py",
    "strategies/instance.py",
    "live/api.py",
    "live/engine.py",
    "live/run.py",
    "dashboard/routes/overview.py",
    "dashboard/routes/strategies.py",
    "dashboard/routes/positions.py",
    "dashboard/routes/orders.py",
    "dashboard/routes/trades.py",
    "dashboard/routes/pnl.py",
    "dashboard/routes/market_data.py",
    "dashboard/routes/risk.py",
    "dashboard/routes/health.py",
    "dashboard/routes/reconciliation.py",
    "dashboard/routes/alerts.py",
    "dashboard/routes/settings.py",
    "dashboard/routes/audit_log.py",
    "dashboard/routes/indicators.py",
    "dashboard/routes/env_switch.py",
    "dashboard/routes/broker_evidence.py",
    "dashboard/routes/live_ops.py",
    "dashboard/routes/reversals.py",
    "dashboard/routes/replay.py",
    "portfolio/account.py",
    "analytics/routes.py",
    "config/live_settings.json",
]

print(f"{'FILE':<50} {'LOCAL MD5':<12} {'CONTAINER MD5':<12} {'STATUS'}")
print("=" * 90)

mismatches = []
ok_count = 0
missing = 0

for f in files:
    local_path = f
    remote_path = f"/app/{f}"
    
    if not os.path.exists(local_path):
        print(f"{f:<50} {'(not found)':<12} {'':<12} SKIP")
        missing += 1
        continue
    
    local_hash = md5(local_path)
    remote_hash = remote_md5(remote_path)
    
    if local_hash == remote_hash:
        print(f"{f:<50} {local_hash:<12} {remote_hash:<12} OK")
        ok_count += 1
    else:
        print(f"{f:<50} {local_hash:<12} {remote_hash:<12} MISMATCH")
        mismatches.append(f)

print(f"\n{'=' * 90}")
print(f"SUMMARY: {ok_count} OK, {len(mismatches)} MISMATCH, {missing} SKIP")
if mismatches:
    print(f"\nMISMATCHED FILES:")
    for m in mismatches:
        print(f"  - {m}")

# Also check the VPS build context
print(f"\n{'=' * 90}")
print("VPS BUILD CONTEXT CHECK")
print("=" * 90)

for f in ["dashboard/routes/live_ops.py", "dashboard/routes/reversals.py", "dashboard/routes/overview.py", "execution/price_model.py", "config/live_settings.json"]:
    local_hash = md5(f) if os.path.exists(f) else "?"
    remote_hash = remote_md5(f"/home/jadhavdnyaneshwar701/mcx-trader-live/{f}")
    status = "OK" if local_hash == remote_hash else "MISMATCH"
    print(f"  {f:<50} local={local_hash} vps={remote_hash} {status}")

# Check which image is running
print(f"\n{'=' * 90}")
print("RUNNING IMAGE vs LATEST IMAGE")
print("=" * 90)
print("Running:", run("docker inspect mcx-live --format '{{.Config.Image}}'"))
print("Latest:", run("docker images --format '{{.Repository}}:{{.Tag}}' | grep mcx-trader-live:remedy | sort -t'f' -k2 -n | tail -1"))

ssh.close()
