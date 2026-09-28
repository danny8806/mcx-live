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
print("=== .env.live dhan keys (masked) ===")
print(run("grep -iE 'DHAN|TRADING|TOTP' /home/jadhavdnyaneshwar701/mcx-trader-live/.env.live | sed -E 's/=(.{6}).*/=\\1***/' 2>&1"))
print("=== env in container (masked) ===")
print(run("docker exec mcx-live sh -c 'env | grep -iE \"DHAN|TOTP|TRADING_PIN\" | sed -E \"s/=(.{6}).*/=\\1***/\"' 2>&1"))
print("=== auth/token provider files ===")
print(run("docker exec mcx-live sh -c 'find /app -name \"*.py\" | grep -iE \"auth|token\" | head -20' 2>&1"))
ssh.close()
