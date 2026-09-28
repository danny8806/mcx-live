#!/usr/bin/env python3
"""Check token renewal mechanism in deployed container."""
import paramiko, time, json

VPS = "200.234.44.93"
USER = "root"
PASS = "Deltacapitals@123"

def ssh(cmd, timeout=30):
    t = paramiko.Transport((VPS, 22))
    t.connect(username=USER, password=PASS)
    ch = t.open_session()
    ch.settimeout(timeout)
    ch.exec_command(cmd)
    out = b""
    while not ch.exit_status_ready():
        if ch.recv_ready(): out += ch.recv(65536)
        time.sleep(0.1)
    while ch.recv_ready(): out += ch.recv(65536)
    ch.close()
    t.close()
    return out.decode(errors="replace")

# 1. Full startup logs
print("=== FULL STARTUP LOGS ===")
r = ssh("docker logs mcx-live 2>&1 | head -50")
print(r.strip()[:1500])

# 2. All auth/token logs
print("\n=== AUTH TOKEN LOGS ===")
r = ssh("docker logs mcx-live 2>&1 | grep -iE 'auth|token|totp|renew|rate'")
print(r.strip()[:1000])

# 3. Token file
print("\n=== TOKEN FILE ===")
r = ssh("docker exec mcx-live cat /app/data/db/dhan_token.json 2>/dev/null || echo NOT_FOUND")
print(r.strip()[:300])

# 4. Find auth modules
print("\n=== AUTH MODULES ===")
r = ssh("docker exec mcx-live find /app -name '*auth*' -o -name '*token*' | grep -v __pycache__ | grep -v node_modules")
print(r.strip()[:500])

# 5. TOTP modules
print("\n=== TOTP MODULES ===")
r = ssh("docker exec mcx-live find /app -name '*totp*' -o -name '*otp*' | grep -v __pycache__")
print(r.strip()[:300])

# 6. Dhan transport auth headers
print("\n=== DHAN TRANSPORT AUTH HEADERS ===")
r = ssh("docker exec mcx-live grep -n 'access.token\|Authorization\|header\|Bearer' /app/execution/live/dhan_transport.py | head -20")
print(r.strip()[:500])

# 7. Token scheduler / renewal
print("\n=== TOKEN SCHEDULER ===")
r = ssh("docker exec mcx-live grep -rn 'scheduler\|token.*renew\|refresh.*token\|token_scheduler\|_schedule' /app/live/ /app/execution/live/ 2>/dev/null | grep -v __pycache__ | head -20")
print(r.strip()[:600])

# 8. Core auth code
print("\n=== CORE AUTH MODULES ===")
r = ssh("docker exec mcx-live grep -rn 'def.*auth\|def.*token\|def.*renew\|def.*totp\|def.*otp' /app/live/ /app/execution/live/ /app/core/ 2>/dev/null | grep -v __pycache__ | head -20")
print(r.strip()[:600])

# 9. Live engine startup
print("\n=== LIVE ENGINE STARTUP ===")
r = ssh("docker exec mcx-live grep -n 'def start\|def startup\|def init\|on_startup\|startup_reconcile\|renew\|token' /app/live/engine.py | head -20")
print(r.strip()[:500])

# 10. Recent health + auth
print("\n=== RECENT CONTAINER LOGS ===")
r = ssh("docker logs mcx-live 2>&1 | tail -30")
print(r.strip()[:800])
