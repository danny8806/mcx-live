import json, os
p = "live/data/db/live_system_state.json"
if os.path.exists(p):
    d = json.load(open(p))
    print("LOCAL state file exists; strategy_gates:")
    print(json.dumps(d.get("strategy_gates"), indent=1))
else:
    print("NO local live/data/db/live_system_state.json")
print("--- dockerignore? ---")
di = ".dockerignore"
print("exists:", os.path.exists(di))
if os.path.exists(di):
    print(open(di).read())
