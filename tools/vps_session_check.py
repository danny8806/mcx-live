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
    try:
        out = o.read().decode("utf-8", "replace").strip()
    except Exception:
        out = ""
    return out.encode("ascii", "replace").decode("ascii")
print("=== recent logs: auth/ws/candle/errors since session open (~09:00) ===")
print(run("docker logs mcx-live --since 65m 2>&1 | grep -iE 'error|invalid|token|auth|401|403|reset|reconnect|renew|websocket|ws |candle|signal|gate' | grep -viE 'HTTP/1.1|GET /favicon|OPTIONS' | tail -30"))
print()
print("=== 15m native closes this session ===")
print(run("docker logs mcx-live --since 65m 2>&1 | grep -iE '15m native closed' | tail -8"))
print()
print("=== master gate / engine_watchdog / daily loss state ===")
print(run("docker logs mcx-live --since 65m 2>&1 | grep -iE 'master_gate|gate.*ON|gate.*OFF|daily_loss|watchdog|resume|new session|market.*open' | grep -viE 'HTTP/1.1' | tail -10"))
ssh.close()
