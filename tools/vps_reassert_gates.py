import paramiko, sys, io, json, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
env = {}
for line in open("mcx-trader.env"):
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        env[k] = v
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect("200.234.44.93", username="root", password=env["VPS_PASS"], timeout=15)
def run(cmd, timeout=45):
    _, o, e = ssh.exec_command(cmd, timeout=timeout)
    out = o.read().decode("utf-8", "replace")
    err = e.read().decode("utf-8", "replace")
    return (out or err).encode("ascii", "replace").decode("ascii")
print("=== re-run control start (re-assert in-memory ON, harmless if already ON) ===")
for sid in ["gold_02", "silver_01"]:
    print(run(f"curl -s -X POST http://127.0.0.1:8001/api/strategies/{sid}/control -H 'Content-Type: application/json' -d '{{\"action\": \"start\"}}'"))
print()
print("=== persisted snapshot gates ===")
print(run("docker exec mcx-live cat /app/live/data/db/live_system_state.json | python3 -c \"import sys,json; d=json.load(sys.stdin); print('g02:', json.dumps(d['strategy_gates']['gold_02'])); print('s01:', json.dumps(d['strategy_gates']['silver_01']))\""))
print("=== last gate-blocked events? (should be at 13:15/13:45 only) ===")
print(run("""docker exec mcx-live python3 - <<'PY'
import sqlite3
con = sqlite3.connect('/app/live/data/db/live_trading.db')
try:
    rows = con.execute(\"SELECT rowid, timestamp, event_type, strategy_id, detail FROM events WHERE event_type IN ('strategy_gate_blocked','strategy_control','strategy_gate_changed') ORDER BY rowid DESC LIMIT 12\").fetchall()
    for r in rows:
        print(r[0], r[1], r[2], r[3], str(r[4])[:120])
except Exception as e:
    print('err', e)
PY"""))
ssh.close()
