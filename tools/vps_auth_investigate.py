import paramiko, sys, io, time
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
    out = o.read().decode("utf-8", "replace").strip()
    return out.encode("ascii", "replace").decode("ascii")
print("=== token file in container ===")
print(run("docker exec mcx-live sh -c 'ls -la /app/live/data/db/ 2>&1; echo ---; cat /app/live/data/db/dhan_token.json 2>&1 | head -c 300' 2>&1"))
print("=== env live (dhan vars present?) ===")
print(run("docker exec mcx-live sh -c 'env | grep -E \"DHAN|TRADING|TOTP|CLIENT\" | sed s/=.*/=<SET>/' 2>&1"))
print("=== auth-related log lines ===")
print(run("docker logs mcx-live 2>&1 | grep -iE \"auth|token|dhan|401|400|403|403|rate\" | tail -25"))
print("=== recent container logs tail ===")
print(run("docker logs mcx-live 2>&1 | tail -25"))
ssh.close()
