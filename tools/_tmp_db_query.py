import paramiko, json, sys

env = {}
for line in open("mcx-trader.env"):
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        env[k] = v

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect("200.234.44.93", username="root", password=env["VPS_PASS"], timeout=15)

import os
src = sys.argv[1] if len(sys.argv) > 1 else "tools/recheck_db_query.py"
name = os.path.basename(src)

sftp = ssh.open_sftp()
with sftp.open(f"/tmp/{name}", "w") as f:
    f.write(open(src).read())
sftp.close()

stdin, stdout, stderr = ssh.exec_command(f"docker cp /tmp/{name} mcx-live:/tmp/{name}")
stdout.read()
stdin, stdout, stderr = ssh.exec_command(f"docker exec mcx-live python3 /tmp/{name}")
out = stdout.read().decode("utf-8", errors="replace")
err = stderr.read().decode("utf-8", errors="replace")
print(out)
if err.strip():
    print("STDERR:", err[:500])
ssh.close()
