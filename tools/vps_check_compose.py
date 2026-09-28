"""Check and fix docker-compose for mcx-live."""
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

# Check the docker-compose.yml
print("=== DOCKER-COMPOSE.YML ===")
print(run("cat /home/jadhavdnyaneshwar701/mcx-trader-live/docker-compose.yml"))

# Check how the container was originally created (the run command)
print("\n=== CONTAINER HISTORY ===")
print(run("docker inspect mcx-live --format '{{.Config.Image}}'"))
print(run("docker inspect mcx-live --format '{{json .HostConfig.PortBindings}}'"))
print(run("docker inspect mcx-live --format '{{json .HostConfig.Binds}}'"))
print(run("docker inspect mcx-live --format '{{json .Config.Env}}' | python3 -m json.tool"))

# Check the remedy_rebuild script to see how it creates the container
print("\n=== REMEDY_REBUILD CONTAINER CREATION ===")
run_cmd = run("grep -A 20 'docker run' /home/jadhavdnyaneshwar701/mcx-trader-live/tools/remedy_rebuild.py 2>/dev/null || echo 'not found'")
print(run_cmd)

# Check deploy script
print("\n=== DEPLOY SCRIPT ===")
print(run("grep -A 30 'docker run' /home/jadhavdnyaneshwar701/mcx-trader-live/deploy_vps.py 2>/dev/null | head -40"))

ssh.close()
