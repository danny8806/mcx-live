"""Full live system check."""
import paramiko, json

env = {}
for line in open("mcx-trader.env"):
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        env[k] = v

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect("200.234.44.93", username="root", password=env["VPS_PASS"], timeout=15)

def run(cmd, timeout=30):
    _, o, e = ssh.exec_command(cmd, timeout=timeout)
    return o.read().decode("utf-8", "replace").strip()

print("=" * 60)
print("CONTAINER STATUS")
print("=" * 60)
print(run("docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'"))

print("\n" + "=" * 60)
print("ENGINE HEALTH")
print("=" * 60)
print(run("curl -sk https://deltacapitals.systems/api/health 2>/dev/null | python3 -m json.tool"))

print("\n" + "=" * 60)
print("OVERVIEW")
print("=" * 60)
print(run("curl -sk https://deltacapitals.systems/api/overview 2>/dev/null | python3 -c \"import sys,json; d=json.load(sys.stdin); [print(f'{k}: {v}') for k,v in d.items() if k not in ('strategies','positions','account','failure_states')]\""))

print("\n" + "=" * 60)
print("STRATEGIES (all gates)")
print("=" * 60)
print(run("curl -sk https://deltacapitals.systems/api/strategies 2>/dev/null | python3 -c \"import sys,json; d=json.load(sys.stdin); [print(f'{s[\\\"strategy_id\\\"]}: enabled={s[\\\"enabled\\\"]}, state={s[\\\"state\\\"]}, position={s[\\\"position_side\\\"]}, stop={s[\\\"stop_price\\\"]}') for s in d.get('strategies',[])]\""))

print("\n" + "=" * 60)
print("POSITIONS")
print("=" * 60)
print(run("curl -sk https://deltacapitals.systems/api/positions 2>/dev/null | python3 -m json.tool"))

print("\n" + "=" * 60)
print("OPEN ORDERS")
print("=" * 60)
print(run("curl -sk https://deltacapitals.systems/api/orders 2>/dev/null | python3 -c \"import sys,json; d=json.load(sys.stdin); orders=d.get('orders',[]); print(f'Total: {len(orders)}'); [print(f'  {o[\\\"order_id\\\"][:20]} | {o[\\\"strategy_id\\\"]} | {o[\\\"side\\\"]} | {o[\\\"order_type\\\"]} | state={o[\\\"state\\\"]} | filled={o[\\\"filled_quantity\\\"]}') for o in orders[:10]]\""))

print("\n" + "=" * 60)
print("LIVE ORDERS")
print("=" * 60)
print(run("curl -sk https://deltacapitals.systems/api/live/orders 2>/dev/null | python3 -c \"import sys,json; d=json.load(sys.stdin); orders=d.get('orders',[]); print(f'Total: {len(orders)}'); [print(f'  {o[\\\"order_id\\\"][:20]} | {o[\\\"strategy_id\\\"]} | {o[\\\"side\\\"]} | {o[\\\"order_type\\\"]} | state={o[\\\"state\\\"]} | filled={o[\\\"filled_quantity\\\"]}') for o in orders[:10]]\""))

print("\n" + "=" * 60)
print("LIVE FUNDS")
print("=" * 60)
print(run("curl -sk https://deltacapitals.systems/api/live/funds 2>/dev/null | python3 -m json.tool"))

print("\n" + "=" * 60)
print("LIVE SYNC")
print("=" * 60)
print(run("curl -sk https://deltacapitals.systems/api/live/sync 2>/dev/null | python3 -m json.tool"))

print("\n" + "=" * 60)
print("RISK")
print("=" * 60)
print(run("curl -sk https://deltacapitals.systems/api/risk 2>/dev/null | python3 -m json.tool"))

print("\n" + "=" * 60)
print("RECENT LOGS (last 30)")
print("=" * 60)
print(run("docker logs mcx-live --tail 30 2>&1"))

print("\n" + "=" * 60)
print("GATES CONFIRMATION")
print("=" * 60)
print(run('docker exec mcx-live python3 -c "import json; c=json.load(open(chr(47)+chr(97)+chr(112)+chr(112)+chr(47)+chr(99)+chr(111)+chr(110)+chr(102)+chr(105)+chr(103)+chr(47)+chr(108)+chr(105)+chr(118)+chr(101)+chr(95)+chr(115)+chr(101)+chr(116)+chr(116)+chr(105)+chr(110)+chr(103)+chr(115)+chr(46)+chr(106)+chr(115)+chr(111)+chr(110))); l=c.get(chr(108)+chr(105)+chr(118)+chr(101),{}); s=c.get(chr(115)+chr(116)+chr(114)+chr(97)+chr(116)+chr(101)+chr(103)+chr(105)+chr(101)+chr(115),{}); bs=l.get(chr(98)+chr(114)+chr(111)+chr(107)+chr(101)+chr(114)+chr(95)+chr(115)+chr(108),{}); print(chr(76)+chr(73)+chr(86)+chr(69)+chr(95)+chr(84)+chr(82)+chr(65)+chr(68)+chr(73)+chr(78)+chr(71)+chr(95)+chr(69)+chr(78)+chr(65)+chr(66)+chr(76)+chr(69)+chr(68)+chr(58), l.get(chr(108)+chr(105)+chr(118)+chr(101)+chr(95)+chr(116)+chr(114)+chr(97)+chr(100)+chr(105)+chr(110)+chr(103)+chr(95)+chr(101)+chr(110)+chr(97)+chr(108)+chr(98)+chr(108)+chr(101)+chr(100))); print(chr(71)+chr(65)+chr(84)+chr(69)+chr(58), l.get(chr(103)+chr(97)+chr(116)+chr(101))); print(chr(66)+chr(82)+chr(79)+chr(75)+chr(69)+chr(82)+chr(95)+chr(83)+chr(76)+chr(46)+chr(69)+chr(78)+chr(65)+chr(66)+chr(76)+chr(69)+chr(68)+chr(58), bs.get(chr(101)+chr(110)+chr(97)+chr(98)+chr(108)+chr(101)+chr(100))); [print(k+chr(58), chr(101)+chr(110)+chr(97)+chr(98)+chr(108)+chr(101)+chr(100)+chr(61)+str(v.get(chr(101)+chr(110)+chr(97)+chr(98)+chr(108)+chr(101)+chr(100)))+chr(44)+chr(32)+chr(103)+chr(97)+chr(116)+chr(101)+chr(61)+str(v.get(chr(108)+chr(105)+chr(118)+chr(101)+chr(95)+chr(103)+chr(97)+chr(116)+chr(101)))+chr(44)+chr(32)+chr(101)+chr(110)+chr(116)+chr(114)+chr(121)+chr(61)+str(v.get(chr(101)+chr(110)+chr(116)+chr(114)+chr(121)+chr(95)+chr(101)+chr(110)+chr(97)+chr(98)+chr(108)+chr(101)+chr(100)))+chr(44)+chr(32)+chr(101)+chr(120)+chr(105)+chr(116)+chr(61)+str(v.get(chr(101)+chr(120)+chr(105)+chr(116)+chr(95)+chr(101)+chr(110)+chr(97)+chr(98)+chr(108)+chr(101)+chr(100)))+chr(44)+chr(32)+chr(114)+chr(101)+chr(118)+chr(61)+str(v.get(chr(114)+chr(101)+chr(118)+chr(101)+chr(114)+chr(115)+chr(97)+chr(108)+chr(95)+chr(101)+chr(110)+chr(97)+chr(98)+chr(108)+chr(101)+chr(100)))+chr(44)+chr(32)+chr(115)+chr(108)+chr(61)+str(v.get(chr(115)+chr(108)+chr(95)+chr(101)+chr(110)+chr(97)+chr(98)+chr(108)+chr(101)+chr(100)))) for k,v in s.items()]"'))

ssh.close()
