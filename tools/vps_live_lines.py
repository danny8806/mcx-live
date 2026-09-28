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
def run(cmd, t=25):
    _, o, e = ssh.exec_command(cmd, timeout=t)
    return o.read().decode("utf-8", "replace").strip()

print("=== market data LTP ===")
print(run("curl -s http://127.0.0.1:8001/api/market-data 2>/dev/null")[:400])
print("=== live state file raw (strategies node) ===")
print(run('docker exec mcx-live python3 -c "import json; d=json.load(open(chr(47)+chr(97)+chr(112)+chr(112)+chr(47)+chr(108)+chr(105)+chr(118)+chr(101)+chr(47)+chr(100)+chr(97)+chr(116)+chr(97)+chr(47)+chr(100)+chr(98)+chr(47)+chr(108)+chr(105)+chr(118)+chr(101)+chr(95)+chr(115)+chr(121)+chr(115)+chr(116)+chr(101)+chr(109)+chr(95)+chr(115)+chr(116)+chr(97)+chr(116)+chr(101)+chr(46)+chr(106)+chr(115)+chr(111)+chr(110))); print(json.dumps(d.get(chr(115)+chr(116)+chr(114)+chr(97)+chr(116)+chr(101)+chr(103)+chr(105)+chr(101)+chr(115), {})[:1500]))" 2>&1')[:1600])
print("=== htf endpoint ===")
print(run("curl -s http://127.0.0.1:8001/api/htf 2>/dev/null")[:500])
ssh.close()