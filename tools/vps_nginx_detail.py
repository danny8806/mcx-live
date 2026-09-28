"""Check nginx sites-enabled and conf.d."""
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

def run(cmd, timeout=15):
    try:
        _, o, e = ssh.exec_command(cmd, timeout=timeout)
        out = o.read().decode("utf-8", "replace").strip()
        err = e.read().decode("utf-8", "replace").strip()
        return out or err or "(empty)"
    except Exception as ex:
        return f"ERROR: {ex}"

print("=== NGINX SITES-ENABLED ===")
print(run("ls -la /etc/nginx/sites-enabled/"))
print()
print(run("cat /etc/nginx/sites-enabled/default 2>/dev/null || echo 'no default'"))
print()

for f in run("ls /etc/nginx/sites-enabled/").split("\n"):
    f = f.strip()
    if f and f != "default":
        print(f"=== {f} ===")
        print(run(f"cat /etc/nginx/sites-enabled/{f}"))
        print()

print("=== NGINX CONF.D ===")
print(run("ls /etc/nginx/conf.d/"))
for f in run("ls /etc/nginx/conf.d/").split("\n"):
    f = f.strip()
    if f.endswith(".conf"):
        print(f"\n=== conf.d/{f} ===")
        print(run(f"cat /etc/nginx/conf.d/{f}"))

print("\n=== CURL TEST: http://200.234.44.93/ ===")
print(run("curl -sI http://200.234.44.93/ 2>/dev/null | head -10"))

print("\n=== CURL TEST: http://200.234.44.93:8001/ ===")
print(run("curl -sI http://200.234.44.93:8001/ 2>/dev/null | head -10"))

print("\n=== CURL TEST: https://deltacapitals.systems/ ===")
print(run("curl -sIk https://deltacapitals.systems/ 2>/dev/null | head -10"))

ssh.close()
