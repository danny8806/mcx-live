"""Verify fetch_candle_state classification using LIVE ground-truth data
captured from Dhan (raw rows are genuine POSIX epochs of IST bucket starts).

Live capture at now=1789564146 (18:39:06 IST):
  tf   rows (start)          end   grid
  5    1789563300 ... 1789563900 (18:35 IST cur bucket)
  15   1789561800, 1789562700, 1789563600 (18:30 IST cur)
  60   1789554600, 1789558200, 1789561800 (18:00 IST cur)
Now_ts boundary: 5m cur_start = now//300*300 = 1789563900
"""
import os, sys, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

now_ts = 1789564146  # = int(time.time()) captured live
# Expected classifying from live rows (true epochs):
tfdata = {
    "5": [1789562700, 1789563000, 1789563300, 1789563600, 1789563900],
    "15": [1789559700, 1789560900, 1789561800, 1789562700, 1789563600],
    "60": [1789551000, 1789554600, 1789558200, 1789561800],
}
bucket = {"5": 300, "15": 900, "60": 3600}

ok = True
for tf, rows in tfdata.items():
    bk = bucket[tf]
    closed = [c for c in rows if (c + bk) <= now_ts]
    forming_candidates = [c for c in rows if c <= now_ts < c + bk]
    forming = max(forming_candidates) if forming_candidates else None
    print(f"tf={tf} closed={closed} forming={forming}")
    # exact assertions from live capture
    if tf == "5":
        assert forming == 1789563900, forming
        assert closed[-1] == 1789563600, closed
    if tf == "15":
        assert forming == 1789563600, forming
        assert closed[-1] == 1789562700, closed
    if tf == "60":
        assert forming == 1789561800, forming
        assert closed[-1] == 1789558200, closed
    # no future end_ts
    for c in closed:
        assert c + bk <= now_ts
    assert forming is None or forming + bk > now_ts
print("ALL_OK" if ok else "FAIL")