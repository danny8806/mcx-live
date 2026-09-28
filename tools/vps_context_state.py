import paramiko, sys, io, json
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
print("=== VPS build-context state file? ===")
print(run("ls -la /home/jadhavdnyaneshwar701/mcx-trader-live/live/data/db/ 2>&1 | head; echo ---; cat /home/jadhavdnyaneshwar701/mcx-trader-live/live/data/db/live_system_state.json 2>&1 | python3 -c \"import sys,json; d=json.load(sys.stdin); print('context gates:', json.dumps(d.get('strategy_gates',{}).get('gold_02')))\" 2>&1"))
print("=== VPS dockerignore ===")
print(run("cat /home/jadhavdnyaneshwar701/mcx-trader-live/.dockerignore 2>&1 | grep -iE 'state|db|live'"))
ssh.close()
