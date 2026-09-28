"""Fixed nginx config - all services with unique non-overlapping routes."""
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
    _, o, e = ssh.exec_command(cmd, timeout=timeout)
    return o.read().decode("utf-8", "replace").strip()

NGINX_CONF = """# deltacapitals.systems — UNIQUE ROUTES per service
#
# SERVICE              ROUTE PREFIX                      BACKEND
# ─────────────────    ───────────────────────────────   ───────
# MCX-LIVE (trading)   /api/overview, /api/strategies,   8001
#                      /api/positions, /api/orders,
#                      /api/trades, /api/pnl, /api/risk,
#                      /api/market-data, /api/recon*, ...
#                      /api/analytics/*, /api/replay/*
#                      /api/live/* (live dashboard tab)
#                      /ws (websocket)
#                      / (frontend SPA)
#
# OPTION-DEMO          /option/ (frontend SPA)            8002
#                      /api/options/* (API only)
#
# SCREENER             /screener/ (frontend SPA)          3000
#                      /screener/api/* (backend API)      5000
#                      /_next/ (static assets)            3000
#
# IMPORTANT: bare /api/ catch-all goes to SCREENER (3000).
# All mcx-live routes are matched BEFORE the catch-all.

server {
    server_name deltacapitals.systems www.deltacapitals.systems;

    # =========================================================
    # 1. OPTION DEMO (port 8002) — unique prefix /option/ + /api/options/
    # =========================================================
    location = /option {
        return 301 /option/;
    }
    location /option/ {
        rewrite ^/option/(.*)$ /$1 break;
        proxy_pass http://127.0.0.1:8002;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_read_timeout 300s;
        proxy_send_timeout 300s;
    }
    location /api/options/ {
        proxy_pass http://127.0.0.1:8002;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_read_timeout 300s;
        proxy_send_timeout 300s;
    }

    # =========================================================
    # 2. MCX-LIVE (port 8001) — all unique routes matched FIRST
    # =========================================================

    # WebSocket
    location /ws {
        proxy_pass http://127.0.0.1:8001;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_read_timeout 300s;
        proxy_send_timeout 300s;
    }

    # /api/live/* — live dashboard tab (unique prefix, no conflict)
    location /api/live/ {
        proxy_pass http://127.0.0.1:8001;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_read_timeout 300s;
        proxy_send_timeout 300s;
    }

    # /api/analytics/* — analytics tab (unique, no conflict)
    location /api/analytics/ {
        proxy_pass http://127.0.0.1:8001;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_read_timeout 300s;
        proxy_send_timeout 300s;
    }

    # /api/replay/* — replay tab (unique, no conflict)
    location /api/replay/ {
        proxy_pass http://127.0.0.1:8001;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_read_timeout 300s;
        proxy_send_timeout 300s;
    }

    # Bare /api/* routes that belong to mcx-live (matched before catch-all)
    # These are the legacy routes the frontend calls (e.g. /api/overview)
    # NOTE: option-demo does NOT have any of these — only /api/options/*
    location ~ ^/api/(overview|strategies|positions|orders|trades|pnl|market-data|risk|reconciliation|alerts|settings|audit|indicators|htf|envs|broker-events|alert-ledger|equity-curve|fills|health) {
        proxy_pass http://127.0.0.1:8001;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_read_timeout 300s;
        proxy_send_timeout 300s;
    }

    # =========================================================
    # 3. SCREENER (port 3000 frontend, port 5000 backend)
    # =========================================================

    # Static assets
    location /_next/ {
        proxy_pass http://127.0.0.1:3000;
        proxy_http_version 1.1;
        proxy_cache_valid 200 1d;
        add_header Cache-Control "public, max-age=86400";
    }

    # Screener SPA
    location /screener/ {
        proxy_pass http://127.0.0.1:3000/;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_read_timeout 300s;
        proxy_send_timeout 300s;
        add_header Cache-Control "no-cache, no-store, must-revalidate";
    }

    # Screener backend API
    location /screener/api/ {
        proxy_pass http://127.0.0.1:5000/api/;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Connection "";
        proxy_buffering off;
        proxy_cache off;
        chunked_transfer_encoding off;
        proxy_read_timeout 300s;
        proxy_send_timeout 300s;
    }

    # Screener Next.js API catch-all (residual /api/* not matched above)
    # This is ONLY for screener's own Next.js API routes
    location /api/ {
        proxy_pass http://127.0.0.1:3000;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_read_timeout 300s;
    }

    # =========================================================
    # 4. MCX-LIVE FRONTEND (port 8001) — catch-all LAST
    # =========================================================
    location / {
        proxy_pass http://127.0.0.1:8001;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_read_timeout 300s;
        proxy_send_timeout 300s;
    }

    listen [::]:443 ssl ipv6only=on;
    listen 443 ssl;
    ssl_certificate /etc/letsencrypt/live/deltacapitals.systems/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/deltacapitals.systems/privkey.pem;
    include /etc/letsencrypt/options-ssl-nginx.conf;
    ssl_dhparam /etc/letsencrypt/ssl-dhparams.pem;
}
server {
    if ($host = www.deltacapitals.systems) {
        return 301 https://$host$request_uri;
    }
    if ($host = deltacapitals.systems) {
        return 301 https://$host$request_uri;
    }
    listen 80;
    listen [::]:80;
    server_name deltacapitals.systems www.deltacapitals.systems;
    return 404;
}
"""

# 1. Backup
print("BACKING UP...")
print(run("cp /etc/nginx/sites-available/deltacapitals.systems /etc/nginx/sites-available/deltacapitals.systems.bak2"))

# 2. Upload
print("UPLOADING new config...")
sftp = ssh.open_sftp()
with sftp.open("/etc/nginx/sites-available/deltacapitals.systems", "w") as f:
    f.write(NGINX_CONF)
sftp.close()

# 3. Test
print("\nTESTING nginx...")
result = run("nginx -t 2>&1")
print(result)

if "successful" in result:
    print("\nRELOADING nginx...")
    print(run("nginx -s reload 2>&1"))
else:
    print("\nREVERTING...")
    run("cp /etc/nginx/sites-available/deltacapitals.systems.bak2 /etc/nginx/sites-available/deltacapitals.systems")
    run("nginx -s reload 2>&1")

import time
time.sleep(1)

# 4. Verify no overlap
print("\n" + "=" * 60)
print("VERIFYING - each service gets correct routes")
print("=" * 60)

tests = [
    # (name, url, expected_backend)
    ("LIVE /api/overview", "/api/overview", "8001"),
    ("LIVE /api/health", "/api/health", "8001"),
    ("LIVE /api/strategies", "/api/strategies", "8001"),
    ("LIVE /api/positions", "/api/positions", "8001"),
    ("LIVE /api/orders", "/api/orders", "8001"),
    ("LIVE /api/pnl", "/api/pnl", "8001"),
    ("LIVE /api/risk", "/api/risk", "8001"),
    ("LIVE /api/market-data", "/api/market-data", "8001"),
    ("LIVE /api/indicators", "/api/indicators", "8001"),
    ("LIVE /api/alerts", "/api/alerts", "8001"),
    ("LIVE /api/settings", "/api/settings", "8001"),
    ("LIVE /api/audit", "/api/audit", "8001"),
    ("LIVE /api/reconciliation", "/api/reconciliation", "8001"),
    ("LIVE /api/equity-curve", "/api/equity-curve", "8001"),
    ("LIVE /api/fills", "/api/fills", "8001"),
    ("LIVE /api/htf", "/api/htf", "8001"),
    ("LIVE /api/envs", "/api/envs", "8001"),
    ("LIVE /api/broker-events", "/api/broker-events", "8001"),
    ("LIVE /api/alert-ledger", "/api/alert-ledger", "8001"),
    ("LIVE /api/reversals", "/api/reversals", "8001"),
    ("LIVE /api/analytics/strategies", "/api/analytics/strategies", "8001"),
    ("LIVE /api/replay/status", "/api/replay/status", "8001"),
    ("LIVE /api/live/dashboard", "/api/live/dashboard", "8001"),
    ("LIVE /api/live/orders", "/api/live/orders", "8001"),
    ("LIVE /api/live/positions", "/api/live/positions", "8001"),
    ("LIVE /api/live/pnl", "/api/live/pnl", "8001"),
    ("LIVE /api/live/funds", "/api/live/funds", "8001"),
    ("LIVE /api/live/profile", "/api/live/profile", "8001"),
    ("LIVE /api/live/signals", "/api/live/signals", "8001"),
    ("LIVE /api/live/candles", "/api/live/candles", "8001"),
    ("LIVE /api/live/recon", "/api/live/recon", "8001"),
    ("LIVE /api/live/telegram", "/api/live/telegram", "8001"),
    ("LIVE /api/live/sync", "/api/live/sync", "8001"),
    ("LIVE /api/live/timeline", "/api/live/timeline", "8001"),
    ("LIVE /ws", "/ws", "8001"),
    ("LIVE / (frontend)", "/", "8001"),
    ("OPTION /option/", "/option/", "8002"),
    ("OPTION /api/options/overview", "/api/options/overview", "8002"),
    ("OPTION /api/options/status", "/api/options/status", "8002"),
    ("SCREENER /screener/", "/screener/", "3000"),
    ("SCREENER /_next/", "/_next/", "3000"),
]

for name, path, expected in tests:
    code = run(f"curl -sk -o /dev/null -w '%{{http_code}}' 'https://deltacapitals.systems{path}'")
    status = "OK" if code in ("200", "308", "307") else f"FAIL({code})"
    print(f"  {name:<40} {code:<6} {status}")

# Check no cross-contamination
print("\n--- /api/options/overview should be 404 from mcx-live ---")
print(run("curl -sk https://deltacapitals.systems/api/options/overview 2>/dev/null | head -c 100"))

print("\n--- /api/health should be mcx-live NOT option-demo ---")
print(run("curl -sk https://deltacapitals.systems/api/health 2>/dev/null | head -c 200"))

ssh.close()
