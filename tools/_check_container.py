import paramiko
env = {}
for line in open("mcx-trader.env"):
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        env[k] = v
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect("200.234.44.93", username="root", password=env["VPS_PASS"], timeout=15)
cmds = [
    "docker exec mcx-live sh -c 'find /usr/local/lib/python3.11/site-packages -name *.pth'",
    "docker exec mcx-live sh -c 'find / -name sitecustomize.py 2>/dev/null'",
    "docker exec mcx-live sh -c 'ls -la /app | head -30'",
    "docker exec mcx-live python3 /app/config/__init__.py 2>&1 | head -5",
]
for cmd in cmds:
    stdin, stdout, stderr = ssh.exec_command(cmd)
    out = stdout.read().decode("utf-8", errors="replace")
    err = stderr.read().decode("utf-8", errors="replace")
    print("CMD:", cmd)
    if out.strip():
        print("  OUT:", out.strip()[:1500])
    if err.strip():
        print("  ERR:", err.strip()[:500])
    print()
ssh.close()