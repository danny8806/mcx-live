"""Fix nginx config: route mcx-live API to 8001, screener to 3000."""
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

NGINX_CONF = """# deltacapitals.systems:
#   /            -> LIVE trading frontend (port 8001)
#   /api/live/*  -> LIVE trading API (port 8001)
#   /api/overview, strategies, positions, orders, trades, pnl,
#     market-data, risk, reconciliation, alerts, settings, audit,
#     indicators, htf, envs, broker-events, alert-ledger,
#     equity-curve, fills, replay, health, ws -> LIVE (port 8001)
#   /api/options/* -> option selling demo API (port 8002)
#   /option/     -> option selling demo dashboard (port 8002)
#   /screener/   -> AI Pattern Screener (port 3000)
#   /screener/api/ -> Screener backend (port 5000)
#   /_next/      -> Screener static assets (port 3000)
#   /api/* (remainder) -> Screener Next.js API (port 3000)

# --- Shared proxy settings ---
# mcx-live proxy block
# (defined inline per location for clarity)

server {
    server_name deltacapitals.systems www.deltacapitals.systems;

    # =========================================================
    # OPTION DEMO (port 8002)
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
    # MCX-LIVE API (port 8001) — specific routes FIRST
    # =========================================================

    # WebSocket for live dashboard
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

    # All /api/live/* routes -> mcx-live
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

    # Known mcx-live API routes (bare /api/*)
    location ~ ^/api/(overview|strategies|positions|orders|trades|pnl|market-data|risk|reconciliation|alerts|settings|audit|indicators|htf|envs|broker-events|alert-ledger|equity-curve|fills|replay|health) {
        proxy_pass http://127.0.0.1:8001;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_read_timeout 300s;
        proxy_send_timeout 300s;
    }

    # mcx-live fallback health endpoint
    location = /api/health {
        proxy_pass http://127.0.0.1:8001;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # =========================================================
    # SCREENER (port 3000 frontend, port 5000 backend)
    # =========================================================
    location /_next/ {
        proxy_pass http://127.0.0.1:3000;
        proxy_http_version 1.1;
        proxy_cache_valid 200 1d;
        add_header Cache-Control "public, max-age=86400";
    }

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
        add_header Pragma "no-cache";
    }

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
        add_header Cache-Control "no-cache, no-store";
    }

    # Screener Next.js API catch-all (residual /api/* not matched above)
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
    # MCX-LIVE FRONTEND (port 8001) — catch-all LAST
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

# 1. Backup current config
print("BACKING UP current nginx config...")
print(run("cp /etc/nginx/sites-available/deltacapitals.systems /etc/nginx/sites-available/deltacapitals.systems.bak"))

# 2. Upload new config via SFTP
print("UPLOADING new nginx config...")
sftp = ssh.open_sftp()
with sftp.open("/etc/nginx/sites-available/deltacapitals.systems", "w") as f:
    f.write(NGINX_CONF)
sftp.close()
print("  uploaded OK")

# 3. Test nginx config
print("\nTESTING nginx config...")
result = run("nginx -t 2>&1")
print(result)

if "successful" in result:
    # 4. Reload nginx
    print("\nRELOADING nginx...")
    print(run("nginx -s reload 2>&1"))
    print("  nginx reloaded OK")
else:
    print("\n!!! NGINX CONFIG ERROR - reverting !!!")
    run("cp /etc/nginx/sites-available/deltacapitals.systems.bak /etc/nginx/sites-available/deltacapitals.systems")
    run("nginx -s reload 2>&1")

# 5. Verify routing
import time
time.sleep(1)

print("\n" + "=" * 60)
print("VERIFYING ROUTING")
print("=" * 60)

print("\n--- / (should be mcx-live frontend) ---")
print(run("curl -sk https://deltacapitals.systems/ 2>/dev/null | head -c 200"))

print("\n--- /api/overview (should be mcx-live 8001) ---")
print(run("curl -sk https://deltacapitals.systems/api/overview 2>/dev/null | head -c 300"))

print("\n--- /api/health (should be mcx-live 8001) ---")
print(run("curl -sk https://deltacapitals.systems/api/health 2>/dev/null | head -c 200"))

print("\n--- /api/live/orders (should be mcx-live 8001) ---")
print(run("curl -sk https://deltacapitals.systems/api/live/orders 2>/dev/null | head -c 300"))

print("\n--- /api/strategies (should be mcx-live 8001) ---")
print(run("curl -sk https://deltacapitals.systems/api/strategies 2>/dev/null | head -c 300"))

print("\n--- /option/ (should be option-demo 8002) ---")
print(run("curl -sk https://deltacapitals.systems/option/ 2>/dev/null | head -c 200"))

print("\n--- /screener/ (should be screener 3000) ---")
print(run("curl -sk https://deltacapitals.systems/screener/ 2>/dev/null | head -c 200"))

print("\n--- /ws (should connect) ---")
print(run("timeout 2 curl -sk -o /dev/null -w 'HTTP %{http_code}' 'https://deltacapitals.systems/ws' 2>/dev/null || echo 'ws needs browser (expected)'"))

ssh.close()
