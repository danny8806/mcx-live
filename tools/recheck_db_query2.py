import sqlite3, json, datetime

IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
db = sqlite3.connect("/app/live/data/db/live_trading.db")
db.row_factory = sqlite3.Row

def ist_ts(v):
    if not v:
        return None
    try:
        return datetime.datetime.fromtimestamp(float(v), tz=IST).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    except Exception:
        return str(v)

print("=== ORDERS (all 5) ===")
for r in db.execute("SELECT * FROM orders ORDER BY id"):
    print(json.dumps(dict(r), default=str))

print()
print("=== EVENTS last 40 (correct schema) ===")
for r in db.execute("SELECT timestamp, event_type, strategy_id, instrument, details, execution_mode FROM events ORDER BY id DESC LIMIT 40"):
    d = {"timestamp": r["timestamp"], "event_type": r["event_type"],
         "strategy_id": r["strategy_id"], "instrument": r["instrument"], "mode": r["execution_mode"]}
    if r["details"]:
        try:
            d["details"] = json.loads(r["details"])
        except Exception:
            d["details"] = r["details"][:300]
    print(json.dumps(d, default=str))

print()
print("=== SIGNALS detail: candle high/low + trigger (last 7) ===")
for r in db.execute("SELECT signal_id, strategy_id, instrument, side, signal_timestamp, high, low, close, trigger_price, stop_price, signal_reason, created_at FROM signals ORDER BY id DESC LIMIT 7"):
    d = dict(r)
    d["signal_ts_IST"] = ist_ts(d.get("signal_timestamp"))
    print(json.dumps(d))

print()
print("=== TRADE_SIGNAL_LINK ===")
try:
    for r in db.execute("SELECT * FROM trade_signal_link ORDER BY id DESC LIMIT 10"):
        print(json.dumps(dict(r), default=str))
except Exception as e:
    print("ERR", e)