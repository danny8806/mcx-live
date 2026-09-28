import paramiko, io, sys
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
def run(cmd, t=30):
    _, o, e = ssh.exec_command(cmd, timeout=t)
    return (o.read().decode("utf-8", "replace") + e.read().decode("utf-8", "replace")).strip()

print("=== trading_engine.py container hash ===")
print(run("docker exec mcx-live sha256sum /app/trading_engine.py"))
print()
print("=== live_settings.json (full) ===")
print(run("docker exec mcx-live cat /app/config/live_settings.json"))
print()
print("=== container config/env relevant keys ===")
script = r'''
import json, os
d = json.load(open("/app/config/live_settings.json"))
def walk(o, prefix=""):
    if isinstance(o, dict):
        for k, v in o.items():
            kk = "%s.%s" % (prefix, k) if prefix else k
            if isinstance(v, (dict, list)):
                walk(v, kk)
            else:
                print("%s = %r" % (kk, v))
    elif isinstance(o, list):
        print("%s = <list len %d>" % (prefix, len(o)))
walk(d)
print("---- os env relevant ----")
for k in sorted(os.environ):
    kl = k.lower()
    if any(s in kl for s in ("market","limit","fallback","pending","stop","trigger","broker","dhan","exec","order","candle","ws","recon")):
        v = os.environ.get(k)
        if any(sec in k.lower() for sec in ("token","pass","secret","pin","totp")):
            v = "***"
        print("%s = %r" % (k, v))
'''
sf = ssh.open_sftp()
with sf.open("/tmp/cfg_probe.py", "w") as f:
    f.write(script)
sf.close()
print(run("docker cp /tmp/cfg_probe.py mcx-live:/tmp/cfg_probe.py && docker exec mcx-live python3 /tmp/cfg_probe.py"))
ssh.close()