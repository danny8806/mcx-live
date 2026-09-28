"""Simple Docker HEALTHCHECK script — no quoting issues."""
import sys

try:
    import urllib.request
    resp = urllib.request.urlopen("http://127.0.0.1:8001/health", timeout=5)
    data = resp.read().decode()
    if resp.status == 200 and '"ok"' in data:
        sys.exit(0)
    sys.exit(1)
except Exception:
    sys.exit(1)
