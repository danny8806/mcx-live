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
def run(cmd, timeout=60):
    _, o, e = ssh.exec_command(cmd, timeout=timeout)
    out = o.read().decode("utf-8", "replace").strip()
    return out.encode("ascii", "replace").decode("ascii")
sftp = ssh.open_sftp()
sftp.put("config/live_settings.json", "/home/jadhavdnyaneshwar701/mcx-trader-live/config/live_settings.json")
sftp.close()
print("-> build context: done")
run("docker cp /home/jadhavdnyaneshwar701/mcx-trader-live/config/live_settings.json mcx-live:/app/config/live_settings.json")
print("-> container: done")
print("rebuilding image (config baked) ...")
out, e2 = ssh.exec_command("cd /home/jadhavdnyaneshwar701/mcx-trader-live && docker build --no-cache -t mcx-trader-live:remedy-f31 . 2>&1 | tail -5", timeout=900)
import io as _io
print((o.read().decode("utf-8","replace") or e2.read().decode("utf-8","replace")).encode("ascii","replace").decode("ascii").strip())
ssh.close()
