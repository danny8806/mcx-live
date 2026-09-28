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
def run(cmd, timeout=600):
    _, o, e = ssh.exec_command(cmd, timeout=timeout)
    out = o.read().decode("utf-8", "replace").strip()
    err = e.read().decode("utf-8", "replace").strip()
    return (out.encode("ascii","replace").decode("ascii"), err.encode("ascii","replace").decode("ascii"))

print("=== build context config check ===")
out, err = run("cat /home/jadhavdnyaneshwar701/mcx-trader-live/config/live_settings.json | python3 -c \"import sys,json; c=json.load(sys.stdin); print(json.dumps({k:{'enabled':v.get('enabled'),'tf':v.get('fast_timeframe'),'qty':v.get('quantity'),'lots':v.get('lots')} for k,v in c['strategies'].items()}, indent=1))\"")
print(out or err)

print("=== build image (no-cache) ===")
out, err = run("cd /home/jadhavdnyaneshwar701/mcx-trader-live && docker build --no-cache -t mcx-trader-live:remedy-f30 . 2>&1 | tail -20", timeout=900)
print(out or err)

print("=== recreate container ===")
out, err = run("docker rm -f mcx-live 2>&1; docker run -d --name mcx-live --restart unless-stopped --env-file /home/jadhavdnyaneshwar701/mcx-trader-live/.env.live -p 8001:8001 mcx-trader-live:remedy-f30 python -m live.run 2>&1", timeout=120)
print(out or err)
ssh.close()
