"""Phase 5: read-only Dhan REST reconciliation inside container using its live token.
Verifies broker_order_id 23826091516004 on the broker side. NO order placement."""
import json, sys, urllib.request, urllib.error, time
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

TOKEN = json.load(open('/app/live/data/db/dhan_token.json'))['access_token']
H = {"access-token": TOKEN, "Content-Type": "application/json"}
BASE = "https://api.dhan.co/v2"

def get(path):
    req = urllib.request.Request(BASE + path, headers=H)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, r.read().decode("utf-8", errors="replace")[:6000]
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", errors="replace")[:3000]
    except Exception as e:
        return None, "ERR %r" % e

for name, path in [
    ("FUNDLIMIT", "/fundlimit"),
    ("POSITIONS (net)", "/positions"),
    ("TRADEBOOK", "/tradebook"),
    ("ORDER by id 23826091516004", "/orders/23826091516004"),
    ("ORDER by id 23826091511504 (yesterday)", "/orders/23826091511504"),
    ("HOLDINGS", "/holdings"),
]:
    print("\n==== %s ====" % name)
    st, body = get(path)
    print("HTTP", st)
    print(body[:3500])