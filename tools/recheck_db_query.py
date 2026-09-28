import sqlite3, json, datetime

IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
db = sqlite3.connect("/app/live/data/db/live_trading.db")
db.row_factory = sqlite3.Row

def ts(v):
    if not v:
        return None
    try:
        return datetime.datetime.fromtimestamp(float(v), tz=IST).strftime("%H:%M:%S.%f")[:-3]
    except Exception:
        return str(v)

print("=== SIGNALS (last 15) ===")
for r in db.execute("SELECT signal_id, strategy_id, instrument, side, signal_timestamp, candle_timestamp, created_at FROM signals ORDER BY id DESC LIMIT 15"):
    d = dict(r)
    d["signal_ts_IST"] = ts(d.get("signal_timestamp"))
    d["candle_ts_IST"] = ts(d.get("candle_timestamp"))
    print(json.dumps(d))

print()
print("=== TRADES (last 15) ===")
for r in db.execute("SELECT trade_id, strategy_id, instrument, side, entry_timestamp, entry_price, entry_order_id, entry_signal_id, status FROM trades ORDER BY id DESC LIMIT 15"):
    print(json.dumps(dict(r), default=str))

print()
print("=== EVENTS signal/order (last 30) ===")
for r in db.execute("SELECT event_type, data, created_at FROM events WHERE event_type IN ('signal_generated','order_created','order_submitted','fill_received','signal_persisted','trade_created','entry_escalation','pending_entry_created') ORDER BY id DESC LIMIT 30"):
    d = {"type": r["event_type"], "created_at": r["created_at"]}
    if r["data"]:
        try:
            d["data"] = json.loads(r["data"])
        except Exception:
            d["data"] = r["data"][:200]
    print(json.dumps(d, default=str))

print()
print("=== EVENTS all recent (last 20) ===")
for r in db.execute("SELECT event_type, data, created_at FROM events ORDER BY id DESC LIMIT 20"):
    d = {"type": r["event_type"], "created_at": r["created_at"]}
    if r["data"]:
        try:
            d["data"] = json.loads(r["data"])
        except Exception:
            d["data"] = r["data"][:200]
    print(json.dumps(d, default=str))