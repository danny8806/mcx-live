import paramiko, sys, io
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
print("=== Dockerfile COPY lines ===")
print(run("grep -nE 'COPY|ADD|VOLUME' /home/jadhavdnyaneshwar701/mcx-trader-live/Dockerfile"))
print("=== is live/data/db in image or runtime? ===")
print(run("docker exec mcx-live ls -la /app/live/data/db/live_system_state.json"))
print("=== check if container has the state baked into running gate (via snapshot JSON) ===")
print(run("docker exec mcx-live cat /app/live/data/db/live_system_state.json | python3 -c \"import sys,json; d=json.load(sys.stdin); print(json.dumps(d.get('strategy_gates',{}).get('gold_02')))\""))
ssh.close()
