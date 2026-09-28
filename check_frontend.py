#!/usr/bin/env python3
"""Check frontend API calls vs backend endpoints."""
import re, urllib.request, json

# Read frontend API file
with open("dashboard-ui/src/lib/api.ts", "r", encoding="utf-8") as f:
    api_content = f.read()

# Find all API endpoints
endpoints = re.findall(r'["\']/api/([a-zA-Z0-9_/-]+)["\']', api_content)
unique = sorted(set(endpoints))
print("Frontend API endpoints:")
for ep in unique:
    print(f"  /api/{ep}")

# Read DataProvider
with open("dashboard-ui/src/store/DataProvider.tsx", "r", encoding="utf-8") as f:
    dp = f.read()

api_calls = re.findall(r"api\.(\w+)\(", dp)
print(f"\nDataProvider calls: {sorted(set(api_calls))}")

# Check backend
print("\nBackend endpoint check:")
for ep in unique:
    url = f"http://200.234.44.93:8001/api/{ep}"
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            data = json.loads(r.read())
            keys = list(data.keys())[:3] if isinstance(data, dict) else []
            count = data.get("count", "") if isinstance(data, dict) else ""
            print(f"  /api/{ep}: OK keys={keys} count={count}")
    except urllib.error.HTTPError as e:
        print(f"  /api/{ep}: {e.code}")
    except Exception as e:
        print(f"  /api/{ep}: {str(e)[:50]}")
