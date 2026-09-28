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
def run(cmd, timeout=900):
    _, o, e = ssh.exec_command(cmd, timeout=timeout)
    out = o.read().decode("utf-8", "replace").strip()
    return out.encode("ascii", "replace").decode("ascii")
print("=== rebuild remedy-f31 ===")
print(run("cd /home/jadhavdnyaneshwar701/mcx-trader-live && docker build --no-cache -t mcx-trader-live:remedy-f31 . 2>&1 | tail -4", timeout=900))
print("=== recreate container ===")
print(run("docker rm -f mcx-live 2>&1; docker run -d --name mcx-live --restart unless-stopped --env-file /home/jadhavdnyaneshwar701/mcx-trader-live/.env.live -p 8001:8001 mcx-trader-live:remedy-f31 python -m live.run 2>&1", timeout=120))
ssh.close()
