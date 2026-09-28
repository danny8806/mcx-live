"""Check nginx, all containers, and port mappings on VPS."""
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
    try:
        _, o, e = ssh.exec_command(cmd, timeout=timeout)
        out = o.read().decode("utf-8", "replace").strip()
        err = e.read().decode("utf-8", "replace").strip()
        return out or err or "(empty)"
    except Exception as ex:
        return f"ERROR: {ex}"

print("=" * 60)
print("1. ALL CONTAINERS")
print("=" * 60)
print(run("docker ps -a --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}'"))

print("\n" + "=" * 60)
print("2. DOCKER COMPOSE / STACKS")
print("=" * 60)
print(run("docker stack ls 2>/dev/null || echo 'no swarm'"))
print(run("docker compose ls 2>/dev/null || echo 'no compose'"))
print(run("ls /root/docker-compose*.yml /root/*/docker-compose*.yml 2>/dev/null || echo 'no compose files'"))

print("\n" + "=" * 60)
print("3. NGINX CONFIG")
print("=" * 60)
print(run("which nginx 2>/dev/null && nginx -t 2>&1 || echo 'no nginx binary'"))
print(run("cat /etc/nginx/nginx.conf 2>/dev/null | head -80 || echo 'no nginx.conf'"))
print(run("ls /etc/nginx/sites-enabled/ 2>/dev/null || echo 'no sites-enabled'"))
print(run("ls /etc/nginx/conf.d/ 2>/dev/null || echo 'no conf.d'"))

print("\n" + "=" * 60)
print("4. ALL LISTENING PORTS")
print("=" * 60)
print(run("ss -tlnp 2>/dev/null || netstat -tlnp 2>/dev/null"))

print("\n" + "=" * 60)
print("5. MCX-LIVE CONTAINER PORT MAPPING")
print("=" * 60)
print(run("docker inspect mcx-live --format '{{range $k,$v := .NetworkSettings.Ports}}{{$k}} -> {{range $v}}{{.HostIp}}:{{.HostPort}}{{end}}{{println}}{{end}}' 2>/dev/null"))

print("\n" + "=" * 60)
print("6. DOCKER NETWORKS")
print("=" * 60)
print(run("docker network ls --format 'table {{.Name}}\t{{.Driver}}\t{{.Scope}}'"))
print(run("docker network inspect bridge --format '{{range .Containers}}{{.Name}} {{.IPv4Address}}{{println}}{{end}}' 2>/dev/null"))

print("\n" + "=" * 70)
print("7. IPTABLES (port 80/443 redirects)")
print("=" * 70)
print(run("iptables -t nat -L -n 2>/dev/null | head -30 || echo 'no iptables'"))

ssh.close()
