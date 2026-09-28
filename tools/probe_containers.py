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


def run(cmd):
    i, o, e = ssh.exec_command(cmd, timeout=60)
    return o.read().decode("utf-8", "replace")


print("== docker ps ==")
print(run('docker ps -a --format "{{.Names}}|{{.Image}}|{{.Status}}|{{.Ports}}"'))
print("== inspect mcx-live ==")
print(run(
    'docker inspect mcx-live --format '
    '"img={{.Image}} net={{.HostConfig.NetworkMode}} restart={{.HostConfig.RestartPolicy.Name}} '
    'created={{.Created}} started={{.State.StartedAt}} running={{.State.Running}} '
    'health={{.State.Health.Status}} restarts={{.RestartCount}} cmd={{.Config.Cmd}} id={{.Id}}"'))
print("== images ==")
print(run('docker images --format "{{.Repository}}:{{.Tag}} {{.ID}} {{.CreatedSince}} {{.Size}}"'))
print("== mounts ==")
print(run('docker inspect mcx-live --format "{{range .Mounts}}{{.Source}} -> {{.Destination}} ({{.Type}}){{println}}{{end}}"'))
print("== env live vars (names only) ==")
print(run('docker inspect mcx-live --format "{{range $k,$v := .Config.Env}}{{$k}}{{println}}{{end}}" | sort'))
ssh.close()