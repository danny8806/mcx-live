"""Check all services and their route registrations to find overlaps."""
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

# 1. All containers
print("=== ALL CONTAINERS ===")
print(run("docker ps -a --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}'"))

# 2. Check what the mcx-live backend actually serves (all routes)
print("\n=== MCX-LIVE ROUTES (OpenAPI schema) ===")
print(run("curl -sk https://deltacapitals.systems/openapi.json 2>/dev/null | python3 -c \"import sys,json; d=json.load(sys.stdin); paths=sorted(d.get('paths',{}).keys()); [print(p) for p in paths]\""))

# 3. Check screener API routes
print("\n=== SCREENER API TESTS ===")
for r in ["/screener/", "/screener/api/", "/api/", "/api/auth/", "/api/user/"]:
    code = run(f"curl -sk -o /dev/null -w '%{{http_code}}' 'https://deltacapitals.systems{r}'")
    print(f"  {r:<30} -> {code}")

# 4. Check option demo API
print("\n=== OPTION DEMO API ===")
for r in ["/option/", "/api/options/", "/api/options/health"]:
    code = run(f"curl -sk -o /dev/null -w '%{{http_code}}' 'https://deltacapitals.systems{r}'")
    print(f"  {r:<30} -> {code}")

# 5. Check if there's a paper dashboard or separate service
print("\n=== LOOKING FOR PAPER/DEMO SERVICES ===")
print(run("docker ps -a --format '{{.Names}}' | grep -iE 'paper|demo|option'"))
print(run("docker ps -a --format '{{.Names}}' | grep -v mcx-live | grep -v screener | grep -v option-demo"))

# 6. Check nginx config - current state
print("\n=== CURRENT NGINX CONFIG (key locations) ===")
print(run("grep -E 'location|proxy_pass|server_name' /etc/nginx/sites-available/deltacapitals.systems | head -40"))

# 7. Check what the mcx-live frontend JS actually calls
print("\n=== MCX-LIVE FRONTEND JS API CALLS ===")
print(run("docker exec mcx-live grep -oP '/api/[a-z0-9_/-]+' /app/dashboard-ui/dist/assets/index-6-YdCe5Z.js 2>/dev/null | sort -u"))

# 8. Check mcx-option-demo routes
print("\n=== OPTION DEMO ROUTES (OpenAPI) ===")
print(run("curl -sk http://127.0.0.1:8002/openapi.json 2>/dev/null | python3 -c \"import sys,json; d=json.load(sys.stdin); [print(p) for p in sorted(d.get('paths',{}).keys())]\" 2>/dev/null || echo 'no openapi'"))

# 9. Check screener routes
print("\n=== SCREENER ROUTES ===")
print(run("curl -sk http://127.0.0.1:3000/ 2>/dev/null | head -c 200"))
print(run("curl -sk http://127.0.0.1:5000/ 2>/dev/null | head -c 200"))

# 10. Check the mcx-option-demo frontend for API calls
print("\n=== OPTION DEMO FRONTEND ===")
print(run("curl -sk https://deltacapitals.systems/option/ 2>/dev/null | grep -oP 'fetch\\([\"'\\''/][^)]+' | head -10"))

ssh.close()
