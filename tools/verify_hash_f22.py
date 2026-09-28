import paramiko, hashlib

files = [
    'strategies/instance.py',
    'strategies/base_dema_strategy.py',
    'trading_engine.py',
    'execution/price_model.py',
    'config/live_settings.json',
]

creds = open('mcx-trader.env').read()
pwd = [l for l in creds.splitlines() if 'PASS' in l.upper()][0].split('=',1)[1].strip().strip('"')

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('200.234.44.93', username='jadhavdnyaneshwar701', password=pwd)
all_match = True
for f in files:
    local_hash = hashlib.sha256(open(f, 'rb').read()).hexdigest()[:16]
    _, stdout, _ = client.exec_command(f'sha256sum /home/jadhavdnyaneshwar701/mcx-trader-live/{f}')
    remote_hash = stdout.read().decode().strip().split()[0][:16]
    match = local_hash == remote_hash
    status = 'MATCH' if match else 'MISMATCH'
    if not match:
        all_match = False
    print(f'{status} {f}: local={local_hash} remote={remote_hash}')
print()
print('ALL MATCH' if all_match else 'MISMATCH DETECTED')
client.close()
