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
def run_in_container(script, timeout=90):
    sftp = ssh.open_sftp()
    fname = f"/tmp/ins_{int(time.time()*1000)}.py"
    with sftp.open(fname, "w") as f:
        f.write(script)
    sftp.close()
    result = run(f"docker cp {fname} mcx-live:{fname} && docker exec mcx-live python3 {fname} 2>&1", timeout=timeout)
    run(f"docker exec mcx-live rm -f {fname}")
    return result

script = """
import sys, json
sys.path.insert(0, '/app')
import live.api as api
eng = api._engine
print('engine:', eng)
if eng is not None:
    print('strategies dict keys:', list(eng.strategies.keys()) if hasattr(eng, 'strategies') else 'n/a')
    for sid, s in (eng.strategies or {}).items():
        print(f'  {sid}: quantity={getattr(s, \"quantity\", \"?\")} enabled={getattr(s, \"enabled\", \"?\")} state={getattr(s, \"state\", \"?\")}')
    # also check live env strategies
    env = getattr(eng, 'live', None)
    if env is not None and hasattr(env, 'strategies'):
        print('live env strategies:', list(env.strategies.keys()))
        for sid, s in env.strategies.items():
            print(f'  {sid}: quantity={getattr(s, \"quantity\", \"?\")} enabled={getattr(s, \"enabled\", \"?\")}')
    else:
        print('live env strategies: n/a')
else:
    print('engine not initialized')
"""
print(run_in_container(script))
ssh.close()
