"""LIVE read-model routes — the smallest API surface required to represent the
running LIVE Dhan runtime truthfully in the dashboard.

Every endpoint here is READ-ONLY.  It aggregates existing in-memory services
(BrokerSyncService, LiveBrokerPoller, OrderWatcher, data adapter, position
manager, pnl/account engines) and queries existing LIVE tables (orders,
signals, fills_reconciliation, broker_order_mapping, broker_api_events,
events, execution_failure_events).  It never simulates/demoes data and never
duplicates business logic:

* Dhan (broker)            = source of truth for order status, fills,
                             positions and broker P&L.
* engine / strategy layer  = source of truth for signals, trigger decisions,
                             reversal/SL/exit *decisions* and the reference SL.
* LIVE DB                  = durable local audit / lineage.
* This module / dashboard  = visualization + control only.

Security: client ids / tokens are masked before leaving the process.
"""
from __future__ import annotations

import asyncio
import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from fastapi import APIRouter, HTTPException

from dashboard.envs import resolve as _resolve_env

log = logging.getLogger(__name__)
router = APIRouter()

IST = timezone(timedelta(hours=5, minutes=30))

_engine = None
_persistence = None

# ── tiny candle cache so the closed-candle REST probe is not hammered ──
_CANDLE_CACHE_TTL = 8.0
_candle_cache: dict[str, dict] = {}
_candle_cache_lock = threading.Lock()

_STALE_TASK_THRESHOLD_S = 90.0


def init(engine, event_bus, persistence=None):
    global _engine, _persistence
    _engine = engine
    _persistence = persistence


def _ts() -> float:
    return time.time()


def _live_env(env: Optional[str] = None):
    """Resolve the LIVE environment (defaults to the engine's live env)."""
    if _engine is None:
        return None
    if env:
        return _resolve_env(_engine, env)
    return getattr(_engine, "live", None)


def _num(v, default: float = 0.0) -> float:
    try:
        f = float(v)
        return f if f == f and abs(f) != float("inf") else default
    except (TypeError, ValueError):
        return default


def _epoch(v, default: Optional[float] = None) -> Optional[float]:
    """Normalize a REAL epoch, TEXT timestamp, or ISO datetime to a float epoch."""
    if v is None:
        return default
    if isinstance(v, (int, float)):
        f = float(v)
        if f == 0 and default is not None:
            return default
        if f > 1e12:            # millis
            return f / 1000.0
        return f
    s = str(v).strip()
    if not s:
        return default
    # Normalize ISO timezone suffixes (+00:00 / +05:30 / -05:00) to "Z"
    # so existing strptime patterns always match. The DB stores UTC.
    for idx in range(len(s) - 1, 10, -1):
        if s[idx] in ("+", "-") and s[idx - 1].isdigit():
            s = s[:idx] + "Z"
            break
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone.utc).timestamp()
        except ValueError:
            continue
    return default


def _age(ts: Optional[float], now: Optional[float] = None) -> Optional[float]:
    if ts is None:
        return None
    return (now if now is not None else _ts()) - ts


def _mask_client_id(cid) -> str:
    cid = str(cid or "")
    if len(cid) <= 4:
        return cid
    return f"{cid[:3]}{'*' * (len(cid) - 5)}{cid[-2:]}"


def _mask_payload(payload) -> Any:
    """Defensively re-mask credential-ish keys in a stored payload."""
    if isinstance(payload, dict):
        out = {}
        for k, v in payload.items():
            kl = str(k).lower()
            if any(w in kl for w in ("token", "password", "pin", "secret",
                                     "authorization", "client_id")):
                out[k] = ("***" if v is not None else None)
            else:
                out[k] = _mask_payload(v)
        return out
    if isinstance(payload, list):
        return [_mask_payload(x) for x in payload]
    return payload


def _db():
    """Shared read handle on the LIVE DB (creates a transient Database when the
    manager has not yet opened one — reads stay on the shared connection)."""
    if _persistence is None:
        return None
    db = getattr(_persistence, "_db", None)
    if db is not None:
        return db
    try:
        from persistence.database import Database
        return Database(_persistence.db_path)
    except Exception as e:                      # pragma: no cover - defensive
        log.debug("live_ops: no DB handle: %s", e)
        return None


def _q(sql: str, params: tuple = ()) -> list[dict]:
    db = _db()
    if db is None:
        return []
    try:
        return db.query(sql, params) or []
    except Exception as e:                      # pragma: no cover - defensive
        log.debug("live_ops query error: %s", e)
        return []


def _q1(sql: str, params: tuple = ()) -> Optional[dict]:
    rows = _q(sql, params)
    return rows[0] if rows else None


# ═══════════════════════════════════════════════════════════════════════
# 1. Dhan PROFILE  (config + live runtime identity)
# ═══════════════════════════════════════════════════════════════════════

def _get_profile_sync():
    if _engine is None:
        return {"error": "Engine not initialized"}
    cfg = _engine.config
    live_cfg = (cfg.get("live", {}) or {}) if hasattr(cfg, "get") else {}
    dhan_cfg = dict(live_cfg.get("dhan", {}) or {})
    for k, v in ((cfg.get("dhan", {}) or {}).items()):
        dhan_cfg.setdefault(k, v)
    instruments = (cfg.get("instruments", {}) or {}) if hasattr(cfg, "get") else {}
    strategies_cfg = (cfg.get("strategies", {}) or {}) if hasattr(cfg, "get") else {}
    order_ws = live_cfg.get("order_ws", {}) or {}
    watcher_cfg = live_cfg.get("order_watcher", {}) or {}
    env = _live_env()
    execution_models = {
        str(getattr(strategy, "execution_model", ""))
        for strategy in (getattr(env, "strategies", {}) or {}).values()
        if getattr(strategy, "execution_model", None)
    } if env is not None else set()
    execution_model = (next(iter(execution_models)) if len(execution_models) == 1
                       else "mixed" if execution_models else None)
    adapter = getattr(env, "data_adapter", None) if env is not None else None
    data_ws_connected = None
    order_ws_connected = None
    if adapter is not None:
        try:
            data_ws_connected = bool(adapter.connected)
        except Exception:
            data_ws_connected = None
    sync = getattr(env, "sync_service", None) if env is not None else None
    if sync is not None:
        try:
            s = sync.stats() or {}
            order_ws_connected = bool(s.get("ws_connected", False))
        except Exception:
            order_ws_connected = None
    return {
        "execution_mode": "LIVE",
        "broker": live_cfg.get("broker") or "dhan",
        "client_id": _mask_client_id(dhan_cfg.get("client_id", "")),
        "product_type": dhan_cfg.get("product_type") or "MARGIN",
        "execution_model": execution_model,
        "gate": (env.gate_state if env is not None else None) or live_cfg.get("gate"),
        "live_trading_enabled": bool(live_cfg.get("live_trading_enabled", False)),
        "order_ws": {
            "enabled": bool(order_ws.get("enabled", False)),
            "url": order_ws.get("url"),
            "connected": order_ws_connected,
        },
        "data_ws": {
            "enabled": True,
            "url": dhan_cfg.get("ws_url"),
            "connected": data_ws_connected,
        },
        "order_watcher": {
            "market_fallback_enabled": bool(watcher_cfg.get("market_fallback_enabled", False)),
            "market_fallback_timeout_ms": watcher_cfg.get("market_fallback_timeout_ms"),
            "max_order_age_ms": watcher_cfg.get("max_order_age_ms"),
            "verify_interval_ms": watcher_cfg.get("reconcile_verify_interval_ms"),
            "limit_skip_policy": bool((watcher_cfg.get("limit_skip_policy") or {}).get("enabled", False)),
        },
        "instruments": {
            name: {
                "security_id": str((ix or {}).get("security_id", "")),
                "symbol": (ix or {}).get("symbol"),
                "exchange_segment": (ix or {}).get("exchange_segment"),
                "instrument_type": (ix or {}).get("instrument"),
                "multiplier": (ix or {}).get("multiplier"),
                "tick_size": (ix or {}).get("tick_size"),
            }
            for name, ix in instruments.items()
        },
        "strategies": {
            name: {
                "instrument": (sc or {}).get("instrument"),
                "fast_timeframe": (sc or {}).get("fast_timeframe"),
                "htf_timeframe": (sc or {}).get("htf_timeframe"),
                "quantity": (sc or {}).get("quantity"),
                "lots": (sc or {}).get("lots"),
                "live_gate": (sc or {}).get("live_gate"),
                "entry_enabled": bool((sc or {}).get("entry_enabled", True)),
                "exit_enabled": bool((sc or {}).get("exit_enabled", True)),
                "reversal_enabled": bool((sc or {}).get("reversal_enabled", True)),
                "sl_enabled": bool((sc or {}).get("sl_enabled", True)),
            }
            for name, sc in strategies_cfg.items()
        },
        "source": "config+live runtime",
        "timestamp": _ts(),
    }


@router.get("/api/live/profile")
async def get_live_profile():
    return await asyncio.to_thread(_get_profile_sync)


# ═══════════════════════════════════════════════════════════════════════
# 2. FUNDS / MARGIN  (Dhan = source of truth)
# ═══════════════════════════════════════════════════════════════════════

def _funds_data(env) -> tuple[dict, Optional[float]]:
    """Return (funds dict, last_updated_epoch).  Prefers the poller's broker
    cache; falls back to a fresh, read-only broker call."""
    poller = getattr(env, "poller", None) if env is not None else None
    acct = {}
    last_updated = None
    if poller is not None:
        try:
            acct = dict(getattr(poller, "_last_account", None) or {})
            last_updated = (poller.stats().get("last_run", {}) or {}).get("account")
        except Exception:
            acct = {}
    if not acct and env is not None:
        broker = getattr(env, "broker", None)
        if broker is not None and hasattr(broker, "account_status"):
            try:
                acct = dict(broker.account_status() or {})
                last_updated = _ts()
            except Exception:
                acct = {}
    return acct, last_updated


def _get_funds_sync(env=None):
    env = _live_env(env)
    if env is None:
        return {"error": "live environment not available"}
    acct, last_updated = _funds_data(env)
    fields = {k: v for k, v in acct.items() if k not in ("dhan_client_id",)}
    return {
        "source": "DHAN",
        "fields": fields,
        "equity": _num(acct.get("equity")),
        "available_margin": _num(acct.get("available_margin")),
        "used_margin": _num(acct.get("used_margin")),
        "realized_pnl": _num(acct.get("realized_pnl")),
        "unrealized_pnl": _num(acct.get("unrealized_pnl")),
        "net_pnl": _num(acct.get("net_pnl")),
        "dhan_client_id": _mask_client_id(acct.get("dhan_client_id")),
        "last_updated": last_updated,
        "age_seconds": _age(last_updated),
        "fetched_at": _ts(),
    }


@router.get("/api/live/funds")
async def get_live_funds():
    return await asyncio.to_thread(_get_funds_sync)


# ═══════════════════════════════════════════════════════════════════════
# 3. BROKER SYNC / connectivity health
# ═══════════════════════════════════════════════════════════════════════

def _get_sync_sync():
    env = _live_env()
    if env is None:
        return {"error": "live environment not available"}
    now = _ts()
    out = {"error": None, "engine_mode": "LIVE", "generated_at": now}
    sync = getattr(env, "sync_service", None)
    if sync is None:
        out["error"] = "BrokerSyncService not present (no sync worker started)"
        return out
    try:
        stats = dict(sync.stats() or {})
    except Exception as e:
        out["error"] = str(e)
        return out
    try:
        poller_snap = sync.snapshot() or {}
    except Exception:
        poller_snap = {}
    service_snap = poller_snap.get("service") or {}
    poller_intervals = stats.get("intervals", {}) or {}
    last_run = stats.get("last_run", {}) or {}
    stale = {}
    for task in ("orders", "positions", "account", "reconcile"):
        lr = last_run.get(task, 0.0)
        interval = poller_intervals.get(task, 0) or 0
        # Stale when more than 2× interval since last successful cycle.
        threshold = max(interval * 3, _STALE_TASK_THRESHOLD_S)
        age = now - lr if lr else None
        stale[task] = {
            "is_stale": bool(age is None or age > threshold),
            "age_seconds": round(age, 1) if age is not None else None,
            "last_run": lr if lr else None,
        }
    out["sync"] = {
        "service": {
            "running": bool(sync.running),
            "healthy": bool(stats.get("service_healthy", False)),
            "worker_alive": bool(stats.get("worker_alive", False)),
            "ws_enabled": bool(stats.get("ws_enabled", False)),
            "ws_connected": bool(stats.get("ws_connected", False)),
        },
        "stats": stats,
        "stale_tasks": stale,
        "poller": {
            "running": bool(poller_snap.get("running", False)),
            # BrokerSyncService.stats() flattens poller.stats() at the top
            # level; only polling intervals live under ``intervals``.
            "broker_positions_count": int(
                stats.get("broker_positions_count")
                if stats.get("broker_positions_count") is not None
                else (poller_snap.get("stats") or {}).get(
                    "broker_positions_count", 0)
            ),
            "has_broker_account": bool(poller_snap.get("accounts") or {}),
            "last_position_report": list(poller_snap.get("position_mismatches") or []),
        },
        "polling": {
            "orders_s": poller_intervals.get("orders"),
            "positions_s": poller_intervals.get("positions"),
            "account_s": poller_intervals.get("account"),
            "reconcile_s": poller_intervals.get("reconcile"),
        },
        "last_cycle": stats.get("last_cycle", {}),
    }
    adapter = getattr(env, "data_adapter", None)
    data_ws = {"connected": None, "stats": None}
    if adapter is not None:
        try:
            data_ws["connected"] = bool(adapter.connected)
        except Exception:
            data_ws["connected"] = None
        try:
            data_ws["stats"] = adapter.stats        # property -> dict
        except Exception:
            data_ws["stats"] = None
    out["data_ws"] = data_ws
    return out


@router.get("/api/live/sync")
async def get_live_sync():
    return await asyncio.to_thread(_get_sync_sync)


# ═══════════════════════════════════════════════════════════════════════
# 4. CANDLES — last CLOSED (Dhan REST) vs FORMING (LTP snapshot)
# ═══════════════════════════════════════════════════════════════════════

_TF_MINUTES = {"5m": "5", "15m": "15", "30m": "30", "1h": "60"}
# Tolerance for a CLOSED candle's end landing at/before now (REST clock skew).
_CLOSED_TOL = 15.0
# Dhan intraday timestamps are IST wall clock encoded as UTC epoch; +5h30m.
_IST_OFFSET = 5 * 3600 + 30 * 60


def _candles_sync(force: bool = False):
    env = _live_env()
    if env is None:
        return {"error": "live environment not available"}
    adapter = getattr(env, "data_adapter", None)
    cfg = _engine.config if _engine is not None else None
    instruments = (cfg.get("instruments", {}) or {}) if hasattr(cfg, "get") else {}
    strategies_cfg = (cfg.get("strategies", {}) or {}) if hasattr(cfg, "get") else {}
    now = _ts()
    out: dict[str, Any] = {"instruments": {}, "generated_at": now, "errors": []}
    if adapter is None:
        out["errors"].append("no data adapter (REST candle source unavailable)")
        return out

    # per-instrument set of subscribed timeframes
    per_inst: dict[str, list[str]] = {}
    for sc in strategies_cfg.values():
        sc = sc or {}
        inst = sc.get("instrument")
        if not inst:
            continue
        subs = [sc.get("fast_timeframe"), sc.get("mid_timeframe"),
                sc.get("htf_timeframe")]
        per_inst.setdefault(inst, [])
        for tf in subs:
            if tf and tf not in per_inst[inst]:
                per_inst[inst].append(tf)

    for inst_name, inst_cfg in instruments.items():
        ix = inst_cfg or {}
        tf_list = per_inst.get(inst_name) or ["5m", "15m", "1h"]
        ltp_info = None
        if adapter is not None and hasattr(adapter, "_live_ltp"):
            try:
                with adapter._ltp_lock:
                    raw = dict(adapter._live_ltp.get(inst_name, {}) or {})
                if raw.get("ltp"):
                    ltp_info = {
                        "ltp": _num(raw.get("ltp")),
                        "timestamp": _epoch(raw.get("timestamp")),
                        "ltq": raw.get("ltq"),
                    }
            except Exception:
                ltp_info = None
        row: dict[str, Any] = {
            "security_id": str(ix.get("security_id", "")),
            "symbol": ix.get("symbol"),
            "exchange_segment": ix.get("exchange_segment"),
            "instrument_type": ix.get("instrument"),
            "ltp": ltp_info,
            "ltp_age_seconds": _age((ltp_info or {}).get("timestamp"), now),
            "candles": {},
        }
        for tf in tf_list:
            tfm = _TF_MINUTES.get(tf or "")
            if not tfm:
                continue
            key = f"{inst_name}:{tf}"
            bucket_sec = int(tfm) * 60
            cur_start = ((int(now) + _IST_OFFSET) // bucket_sec * bucket_sec) - _IST_OFFSET
            entry: dict[str, Any] = {"timeframe": tf, "closed": None, "forming": None}
            cache = None
            with _candle_cache_lock:
                c = _candle_cache.get(key)
                if c and (now - c.get("fetched_at", 0)) < _CANDLE_CACHE_TTL:
                    cache = c
            if cache is None or force:
                state = {"closed": [], "forming": None, "cur_bucket": cur_start}
                err = None
                try:
                    state = dict(adapter.fetch_candle_state(inst_name, tfm) or {})
                except Exception as e:
                    err = str(e)
                with _candle_cache_lock:
                    _candle_cache[key] = {
                        "fetched_at": now,
                        "state": state,
                        "error": err,
                    }
                cache = _candle_cache[key]
            state = cache.get("state") or {}
            closed_rows = state.get("closed") or []
            forming_row = state.get("forming")
            if cache.get("error"):
                out["errors"].append(f"{key}: {cache['error']}")
            if closed_rows:
                last = closed_rows[-1]
                try:
                    cstart = float(last[0])
                    if cstart + bucket_sec <= now + _CLOSED_TOL:
                        entry["closed"] = {
                            "state": "CLOSED",
                            "start_ts": cstart,
                            "end_ts": cstart + bucket_sec,
                            "open": _num(last[1]),
                            "high": _num(last[2]),
                            "low": _num(last[3]),
                            "close": _num(last[4]),
                            "volume": int(_num(last[5])),
                            "source": "dhan-rest-closed",
                        }
                except Exception:
                    try:
                        entry["closed"] = _closed_from_row(last, inst_name, tf, bucket_sec)
                    except Exception:
                        entry["closed"] = None
            # FORMING = Dhan's broker-confirmed in-progress bar (preferred);
            # fall back to a local LTP snapshot only when the API gives none.
            ltp = (ltp_info or {}).get("ltp")
            if forming_row is not None:
                fstart = float(forming_row[0])
                entry["forming"] = {
                    "state": "FORMING",
                    "start_ts": fstart,
                    "end_ts": fstart + bucket_sec,
                    "open": _num(forming_row[1]),
                    "high": _num(forming_row[2]),
                    "low": _num(forming_row[3]),
                    "close": _num(forming_row[4]),
                    "volume": int(_num(forming_row[5], 0)),
                    "ltp": ltp,
                    "source": "dhan-rest-forming",
                    "note": "Dhan REST forming bar (broker view of the in-progress bucket)",
                }
            elif ltp:
                if entry["closed"]:
                    o = entry["closed"]["close"]
                    entry["forming"] = {
                        "state": "FORMING",
                        "start_ts": cur_start,
                        "end_ts": cur_start + bucket_sec,
                        "open": o,
                        "high": max(o, ltp, float("-inf")),
                        "low": min(o, ltp, float("inf")),
                        "close": ltp,
                        "volume": 0,
                        "ltp": ltp,
                        "source": "local-ltp-snapshot",
                        "note": "LOCAL SNAPSHOT from current LTP; not a broker-confirmed bar",
                    }
                else:
                    entry["forming"] = {
                        "state": "FORMING",
                        "start_ts": cur_start,
                        "end_ts": cur_start + bucket_sec,
                        "open": ltp,
                        "high": ltp,
                        "low": ltp,
                        "close": ltp,
                        "volume": 0,
                        "ltp": ltp,
                        "source": "local-ltp-snapshot",
                        "note": "LOCAL SNAPSHOT from current LTP; no prior closed candle yet",
                    }
            entry["last_updated"] = cache.get("fetched_at")
            row["candles"][tf] = entry
        out["instruments"][inst_name] = row
    return out


def _closed_from_row(row, inst, tf, bucket_sec):
    if len(row) < 6:
        row = list(row) + [0] * (6 - len(row))
    return {
        "state": "CLOSED",
        "start_ts": float(row[0]),
        "end_ts": float(row[0]) + bucket_sec,
        "open": _num(row[1]),
        "high": _num(row[2]),
        "low": _num(row[3]),
        "close": _num(row[4]),
        "volume": int(_num(row[5], 0)),
        "source": "dhan-rest-closed",
    }


@router.get("/api/live/candles")
async def get_live_candles(force: bool = False):
    return await asyncio.to_thread(_candles_sync, force)


# ═══════════════════════════════════════════════════════════════════════
# 5. SIGNALS (LIVE strategy signals from the LIVE DB)
# ═══════════════════════════════════════════════════════════════════════

def _get_signals_sync(limit: int = 30):
    rows = _q(
        "SELECT signal_id, strategy_id, instrument, security_id, timeframe, "
        "side, signal_type, signal_timestamp, candle_timestamp, open, high, "
        "low, close, volume, trigger_price, stop_price, quantity, signal_reason, "
        "execution_mode, created_at FROM signals WHERE execution_mode='LIVE' "
        "ORDER BY id DESC LIMIT ?",
        (int(limit),),
    )
    last_ts = None
    for r in rows:
        ts = _epoch(r.get("signal_timestamp"))
        if ts and (last_ts is None or ts > last_ts):
            last_ts = ts
    return {
        "signals": rows,
        "count": len(rows),
        "latest_at": last_ts,
        "age_seconds": _age(last_ts),
        "generated_at": _ts(),
    }


@router.get("/api/live/signals")
async def get_live_signals(limit: int = 30):
    return await asyncio.to_thread(_get_signals_sync, limit)


# ═══════════════════════════════════════════════════════════════════════
# 6. ORDERS — DB lineage + OrderWatcher broker-confirmed state
# ═══════════════════════════════════════════════════════════════════════

def _watcher_records(env) -> dict[str, dict]:
    watcher = getattr(env, "order_watcher", None) if env is not None else None
    if watcher is None:
        return {}
    try:
        snap = watcher.snapshot() or {}
    except Exception:
        return {}
    return {r.get("internal_order_id"): r for r in (snap.get("orders") or [])}


def _enrich_order_row(row: dict, w: Optional[dict]) -> dict:
    out = dict(row)
    out["broker_order_id"] = row.get("broker_order_id") or (w or {}).get("broker_order_id")
    out["exchange_order_id"] = (w or {}).get("exchange_order_id")
    out["correlation_id"] = row.get("correlation_id") or (w or {}).get("correlation_id")
    out["order_role"] = row.get("order_role") or (w or {}).get("order_role")
    out["trigger_price"] = row.get("trigger_price") or (w or {}).get("trigger_price")
    out["watcher_status"] = (w or {}).get("status")
    out["watcher_previous_status"] = (w or {}).get("previous_status")
    out["remaining_quantity"] = (w or {}).get("remaining_quantity")
    out["watcher_filled_quantity"] = (w or {}).get("filled_quantity")
    out["submitted_at"] = (w or {}).get("submitted_at")
    out["last_event_at"] = (w or {}).get("last_event_at")
    out["last_rest_check_at"] = (w or {}).get("last_rest_check_at")
    out["last_market_check_at"] = (w or {}).get("last_market_check_at")
    out["rest_verified"] = bool((w or {}).get("rest_verified"))
    out["retry_count"] = (w or {}).get("retry_count")
    out["reprice_count"] = (w or {}).get("reprice_count")
    out["last_error"] = (w or {}).get("last_error")
    out["last_error_code"] = (w or {}).get("last_error_code")
    out["classification"] = (w or {}).get("classification")
    out["decision"] = (w or {}).get("decision")
    out["product_type"] = _product_type()
    out["security_id"] = _security_id_for(out.get("instrument"))
    return out


def _product_type() -> str:
    if _engine is None:
        return "MARGIN"
    cfg = _engine.config
    try:
        pt = (cfg.get("live", {}) or {}).get("dhan", {}).get("product_type")
        if not pt:
            pt = (cfg.get("dhan", {}) or {}).get("product_type")
        return pt or "MARGIN"
    except Exception:
        return "MARGIN"


def _security_id_for(instrument: Optional[str]) -> Optional[str]:
    if not instrument or _engine is None:
        return None
    try:
        ix = (_engine.config.get("instruments", {}) or {}).get(instrument) or {}
        return str(ix.get("security_id") or "")
    except Exception:
        return None


def _get_orders_sync(limit: int = 40, instrument: Optional[str] = None,
                     strategy: Optional[str] = None):
    env = _live_env()
    sql = ("SELECT order_id, trade_id, pending_order_id, signal_id, "
           "broker_order_id, strategy_id, instrument, side, quantity, order_type, "
           "price, planned_entry_price, planned_sl, planned_order_type, "
           "order_intent, state, filled_quantity, average_fill_price, "
           "execution_mode, created_at, updated_at, order_role, trigger_price, "
           "protected_order_id, correlation_id FROM orders WHERE execution_mode='LIVE'")
    params: list = []
    if instrument:
        sql += " AND instrument=?"
        params.append(str(instrument).upper())
    if strategy:
        sql += " AND strategy_id=?"
        params.append(str(strategy))
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(int(limit))
    rows = _q(sql, tuple(params))
    recs = _watcher_records(env)
    enriched = [_enrich_order_row(r, recs.get(r.get("order_id"))) for r in rows]
    last_ts = None
    for e in enriched:
        for k in ("last_event_at", "submitted_at"):
            ts = _epoch(e.get(k))
            if ts and (last_ts is None or ts > last_ts):
                last_ts = ts
        ts = _epoch(e.get("updated_at"))
        if ts and (last_ts is None or ts > last_ts):
            last_ts = ts
    return {
        "orders": enriched,
        "count": len(enriched),
        "watcher_tracking_active": bool(recs),
        "latest_at": last_ts,
        "age_seconds": _age(last_ts),
        "generated_at": _ts(),
    }


@router.get("/api/live/orders")
async def get_live_orders(limit: int = 40, instrument: Optional[str] = None,
                          strategy: Optional[str] = None):
    return await asyncio.to_thread(_get_orders_sync, limit, instrument, strategy)


@router.get("/api/live/order/{order_id}")
async def get_live_order(order_id: str):
    return await asyncio.to_thread(_get_order_sync, order_id)


def _get_order_sync(order_id: str):
    env = _live_env()
    row = _q1("SELECT * FROM orders WHERE order_id=? AND execution_mode='LIVE'",
              (order_id,))
    if row is None:
        raise HTTPException(status_code=404,
                            detail=f"order '{order_id}' not found in LIVE DB")
    recs = _watcher_records(env)
    w = recs.get(order_id)
    enriched = _enrich_order_row(row, w)
    broker_id = enriched.get("broker_order_id")
    corr = enriched.get("correlation_id")
    ev_rows = []
    if broker_id or corr:
        sql = ("SELECT action, endpoint, http_method, request_timestamp, "
               "response_timestamp, http_status, error_type, error_code, "
               "error_message, broker_order_id, exchange_order_id, correlation_id, "
               "request_payload, response_payload FROM broker_api_events "
               "WHERE (? IS NOT NULL AND broker_order_id=?) OR "
               "(? IS NOT NULL AND correlation_id=?) "
               "ORDER BY request_timestamp DESC LIMIT 60")
        ev_rows = _q(sql, (broker_id, broker_id, corr, corr))
    for e in ev_rows:
        try:
            e["request_payload"] = _mask_payload(json_loads(e.get("request_payload")))
        except Exception:
            pass
        try:
            e["response_payload"] = _mask_payload(json_loads(e.get("response_payload")))
        except Exception:
            pass
    return {
        "order": enriched,
        "broker_events": ev_rows,
        "timeline": _timeline_sync(order_id=order_id, trade_id=row.get("trade_id"),
                                   broker_order_id=broker_id),
        "generated_at": _ts(),
    }


def json_loads(v):
    import json
    if v is None:
        return None
    try:
        return json.loads(v) if isinstance(v, str) else v
    except Exception:
        return v


# ═══════════════════════════════════════════════════════════════════════
# 7. ORDER TIMELINE  (LOCAL decisions + BROKER responses, merged + sorted)
# ═══════════════════════════════════════════════════════════════════════

def _timeline_sync(order_id: Optional[str] = None, trade_id: Optional[str] = None,
                   broker_order_id: Optional[str] = None, limit: int = 200):
    out: list[dict] = []
    ids = [x for x in (order_id, trade_id, broker_order_id) if x]
    if not ids:
        return out
    for i in ids:
        # generic events (LOCAL decisions / engine notifications)
        for r in _q("SELECT timestamp, event_type, strategy_id, instrument, details "
                    "FROM events WHERE details LIKE ? ORDER BY id DESC LIMIT 80",
                    (f"%{i}%",)):
            ts = _epoch(r.get("timestamp"))
            out.append({
                "at": ts, "source": "LOCAL", "kind": r.get("event_type") or "event",
                "strategy_id": r.get("strategy_id"),
                "instrument": r.get("instrument"),
                "detail": str(r.get("details") or "")[:400],
            })
        # lifecycle trade events (LOCAL)
        for r in _q("SELECT event_type, strategy_id, instrument, timestamp, payload_json "
                    "FROM trade_events WHERE trade_id=? ORDER BY sequence_no DESC LIMIT 60",
                    (trade_id or "",)):
            ts = _epoch(r.get("timestamp"))
            out.append({
                "at": ts, "source": "LOCAL", "kind": r.get("event_type") or "trade_event",
                "strategy_id": r.get("strategy_id"),
                "instrument": r.get("instrument"),
                "detail": str(r.get("payload_json") or "")[:400],
            })
        # execution-failure / protection events (LOCAL + BROKER errors)
        for r in _q("SELECT event_type, strategy_id, trade_id, order_id, "
                    "broker_order_id, error, action, final_state, created_at "
                    "FROM execution_failure_events "
                    "WHERE order_id=? OR trade_id=? OR broker_order_id=? "
                    "ORDER BY id DESC LIMIT 40",
                    (i, i, i)):
            ts = _epoch(r.get("created_at"))
            out.append({
                "at": ts, "source": "ALERT", "kind": r.get("event_type") or "exec_failure",
                "strategy_id": r.get("strategy_id"),
                "instrument": None,
                "detail": f"{r.get('action') or ''} → {r.get('final_state') or ''}: "
                          f"{r.get('error') or ''}"[:400],
            })
        # broker API evidence (BROKER)
        for r in _q("SELECT action, endpoint, http_method, request_timestamp, "
                    "response_timestamp, http_status, error_code, error_message, "
                    "broker_order_id, exchange_order_id FROM broker_api_events "
                    "WHERE broker_order_id=? OR correlation_id=? "
                    "ORDER BY request_timestamp DESC LIMIT 80",
                    (i, i)):
            ts = _epoch(r.get("request_timestamp"))
            resp_ts = _epoch(r.get("response_timestamp"))
            detail = f"{r.get('endpoint') or ''} → HTTP {r.get('http_status') or '?'}"
            if r.get("error_message"):
                detail += f" error={r.get('error_message')}"
            elif r.get("error_code"):
                detail += f" code={r.get('error_code')}"
            out.append({
                "at": ts, "source": "BROKER", "kind": r.get("action") or "dhan",
                "strategy_id": None,
                "instrument": None,
                "broker_order_id": r.get("broker_order_id"),
                "exchange_order_id": r.get("exchange_order_id"),
                "response_at": resp_ts,
                "detail": detail[:400],
            })
    out = [o for o in out if o.get("at") is not None]
    out.sort(key=lambda o: o["at"], reverse=True)
    return out[:limit]


@router.get("/api/live/timeline")
async def get_live_timeline(order_id: Optional[str] = None,
                            trade_id: Optional[str] = None,
                            broker_order_id: Optional[str] = None,
                            limit: int = 200):
    return await asyncio.to_thread(_timeline_sync, order_id, trade_id,
                                   broker_order_id, limit)


# ═══════════════════════════════════════════════════════════════════════
# 8. POSITIONS — LOCAL book vs DHAN broker, per strategy/instrument
# ═══════════════════════════════════════════════════════════════════════

def _get_positions_sync():
    env = _live_env()
    if env is None:
        return {"error": "live environment not available"}
    now = _ts()
    poller = getattr(env, "poller", None)
    dhan: list[dict] = []
    mismatches: list[dict] = []
    dhan_updated = None
    if poller is not None:
        try:
            snap = poller.snapshot() or {}
            dhan = list(snap.get("positions") or [])
            mismatches = list(snap.get("position_mismatches") or [])
            dhan_updated = (poller.stats().get("last_run", {}) or {}).get("positions")
        except Exception:
            dhan = []
            mismatches = []
    local_by_instrument: dict[str, list[dict]] = {}
    pm = getattr(env, "position_manager", None)
    if pm is not None:
        try:
            psnap = pm.snapshot() or {}
            for pos in (psnap.get("open_positions") or {}).values():
                pos = pos or {}
                instrument = str(pos.get("instrument") or "")
                quantity = int(pos.get("quantity") or 0)
                if instrument and quantity > 0 and pos.get("is_open", True):
                    local_by_instrument.setdefault(instrument, []).append(pos)
        except Exception:
            pass
    # Dhan reports net exposure per instrument. DhanRestTransport annotates
    # that same net row with each configured strategy id, so strategy-scoped
    # comparison invents a MISSING_LOCAL row for every non-owner strategy.
    # Deduplicate repeated net exposures at instrument level, like the core
    # poller, while retaining the raw rows below for diagnostics.
    dhan_by_instrument: dict[str, list[dict]] = {}
    for p in dhan:
        instrument = str(p.get("instrument") or "")
        quantity = int(p.get("quantity") or 0)
        if instrument and quantity > 0:
            dhan_by_instrument.setdefault(instrument, []).append(p)
    keys = set(local_by_instrument) | set(dhan_by_instrument)
    rows = []
    for inst in sorted(keys):
        local_rows = local_by_instrument.get(inst, [])
        broker_rows = dhan_by_instrument.get(inst, [])
        local_signed = sum(
            int(pos.get("quantity") or 0)
            * (1 if str(pos.get("side") or "").upper() in ("LONG", "BUY") else -1)
            for pos in local_rows
        )
        local_side = "LONG" if local_signed > 0 else "SHORT" if local_signed < 0 else None
        local_qty = abs(local_signed)

        broker_nets = {
            int(p.get("quantity") or 0)
            * (1 if str(p.get("side") or "").upper() in ("LONG", "BUY") else -1)
            for p in broker_rows
        }
        broker_conflict = len(broker_nets) > 1
        broker_signed = next(iter(broker_nets)) if len(broker_nets) == 1 else None
        # Repeated copies of the same instrument-level Dhan row are one
        # broker exposure. Use the first representative to avoid multiplying
        # its average price and P&L by the number of configured strategies.
        dp = broker_rows[0] if broker_rows and not broker_conflict else None
        d_side = ("LONG" if broker_signed > 0 else "SHORT" if broker_signed < 0
                  else None) if broker_signed is not None else None
        d_qty = abs(broker_signed) if broker_signed is not None else 0
        if broker_conflict:
            status = "MISMATCH"
        elif broker_rows and not local_rows and d_qty > 0:
            status = "MISSING_LOCAL"
        elif local_rows and not broker_rows and local_qty > 0:
            status = "MISSING_DHAN"
        elif broker_signed == local_signed:
            status = "MATCHED"
        else:
            status = "MISMATCH"
        owner_ids = sorted({str(p.get("strategy_id") or "")
                            for p in local_rows if p.get("strategy_id")})
        local_side_rows = ([p for p in local_rows
                            if ((str(p.get("side") or "").upper()
                                 in ("LONG", "BUY")) == (local_side == "LONG"))]
                           if local_side is not None else [])
        local_qty_weight = sum(int(p.get("quantity") or 0) for p in local_side_rows)
        local_avg = (
            sum(_num(p.get("average_entry_price") or p.get("average_entry"))
                * int(p.get("quantity") or 0) for p in local_side_rows)
            / local_qty_weight
            if local_qty_weight else None
        )
        local_unrealized = sum(_num(p.get("unrealized_pnl")) for p in local_rows)
        stop_states = {str(p.get("sl_state") or "") for p in local_rows}
        rows.append({
            "strategy_id": owner_ids[0] if len(owner_ids) == 1 else None,
            "local_owners": owner_ids,
            "instrument": inst,
            "status": status,
            "delta_qty": d_qty - local_qty if not broker_conflict else None,
            "local": ({
                "side": local_side,
                "quantity": local_qty,
                "average_entry_price": local_avg,
                "unrealized_pnl": local_unrealized,
                "sl_state": next(iter(stop_states)) if len(stop_states) == 1 else "MULTIPLE",
            } if local_rows else None),
            "dhan": {
                "side": d_side,
                "quantity": d_qty,
                "average_entry_price": (dp or {}).get("average_entry_price"),
                "realized_profit": _num((dp or {}).get("realized_profit")),
                "unrealized_profit": _num((dp or {}).get("unrealized_profit")),
                "ltp": (dp or {}).get("ltp"),
                "mapped_strategy_rows": len(broker_rows),
                "conflicting_rows": len(broker_rows) if broker_conflict else 0,
            } if broker_rows else None,
        })
    return {
        "positions": rows,
        "counts": {
            "MATCHED": sum(1 for r in rows if r["status"] == "MATCHED"),
            "MISMATCH": sum(1 for r in rows if r["status"] == "MISMATCH"),
            "MISSING_LOCAL": sum(1 for r in rows if r["status"] == "MISSING_LOCAL"),
            "MISSING_DHAN": sum(1 for r in rows if r["status"] == "MISSING_DHAN"),
        },
        "dhan_raw": dhan,
        "broker_position_mismatch_report": mismatches,
        "dhan_last_updated": dhan_updated,
        "dhan_position_age_seconds": _age(dhan_updated, now),
        "generated_at": now,
    }


@router.get("/api/live/positions")
async def get_live_positions():
    return await asyncio.to_thread(_get_positions_sync)


# ═══════════════════════════════════════════════════════════════════════
# 9. P&L — DHAN broker vs LOCAL book (difference surfaced, never hidden)
# ═══════════════════════════════════════════════════════════════════════

def _get_pnl_sync():
    env = _live_env()
    if env is None:
        return {"error": "live environment not available"}
    acct, last_updated = _funds_data(env)
    d_real = _num(acct.get("realized_pnl"))
    d_unr = _num(acct.get("unrealized_pnl"))
    l_real = 0.0
    l_unr = 0.0
    for pnl in (env.pnl_engines or {}).values():
        try:
            l_real += _num(pnl.snapshot().get("realized_net"))
        except Exception:
            pass
    pm = getattr(env, "position_manager", None)
    if pm is not None:
        try:
            for pos in (pm.snapshot().get("open_positions") or {}).values():
                pos = pos or {}
                if int(pos.get("quantity") or 0) > 0:
                    l_unr += _num(pos.get("unrealized_pnl"))
        except Exception:
            pass
    return {
        "dhan": {
            "realized_pnl": d_real,
            "unrealized_pnl": d_unr,
            "net_pnl": d_real + d_unr,
            "source": "DHAN (fundlimit+positions)",
            "last_updated": last_updated,
            "age_seconds": _age(last_updated),
        },
        "local": {
            "realized_pnl": round(l_real, 2),
            "unrealized_pnl": round(l_unr, 2),
            "net_pnl": round(l_real + l_unr, 2),
            "source": "engine pnl engines + local position book",
            "generated_at": _ts(),
        },
        "difference": {
            "realized_pnl": round(d_real - l_real, 2),
            "unrealized_pnl": round(d_unr - l_unr, 2),
            "net_pnl": round((d_real + d_unr) - (l_real + l_unr), 2),
            "note": "positive = broker ahead of local book (charges/timing expected)",
        },
    }


@router.get("/api/live/pnl")
async def get_live_pnl():
    return await asyncio.to_thread(_get_pnl_sync)


# ═══════════════════════════════════════════════════════════════════════
# 10. RECONCILIATION — LOCAL vs DHAN per broker order (DB ledger)
# ═══════════════════════════════════════════════════════════════════════

def _get_recon_sync():
    fr = _q("SELECT * FROM fill_reconciliation WHERE execution_mode='LIVE' "
            "ORDER BY updated_at DESC, broker_order_id DESC LIMIT 200")
    bom = {str(b.get("broker_order_id") or ""): b
           for b in _q("SELECT * FROM broker_order_mapping WHERE execution_mode='LIVE'")}
    orders_by_broker = {}
    for o in _q("SELECT order_id, strategy_id, instrument, side, state, broker_order_id "
                "FROM orders WHERE execution_mode='LIVE' AND broker_order_id IS NOT NULL"):
        k = str(o.get("broker_order_id") or "")
        if k:
            orders_by_broker[k] = o
    rows = []
    for r in fr:
        bid = r.get("broker_order_id")
        mapping = bom.get((bid or "")) or {}
        order = orders_by_broker.get((bid or "")) or {}
        gap = int(r.get("gap_qty") or 0)
        bc = int(r.get("broker_cumulative_qty") or 0)
        lc = int(r.get("local_cumulative_qty") or 0)
        if gap > 0:
            status = "MISMATCH"
        elif str(r.get("status")) == "divergence":
            status = "MISMATCH"
        elif bc == 0 and lc == 0:
            status = "PENDING"
        else:
            status = "MATCHED"
        rows.append({
            "broker_order_id": bid,
            "order_id": r.get("order_id") or order.get("order_id"),
            "strategy_id": r.get("strategy_id") or order.get("strategy_id")
                           or mapping.get("strategy_id"),
            "instrument": r.get("instrument") or order.get("instrument"),
            "side": r.get("side") or order.get("side"),
            "local_order_state": order.get("state"),
            "broker_cumulative_qty": bc,
            "local_cumulative_qty": lc,
            "gap_qty": gap,
            "status": status,
            "broker_average_price": r.get("broker_average_price"),
            "last_broker_fill_id": r.get("last_broker_fill_id"),
            "updated_at": r.get("updated_at"),
        })
    # Broker-mapped orders that never reached the fill ledger -> PENDING.
    for bid, m in bom.items():
        if any(r["broker_order_id"] == bid for r in rows):
            continue
        if (bid or "").startswith("MCX-") and m.get("order_id"):
            rows.append({
                "broker_order_id": bid,
                "order_id": m.get("order_id"),
                "strategy_id": m.get("strategy_id"),
                "instrument": m.get("instrument"),
                "side": None,
                "local_order_state": None,
                "broker_cumulative_qty": 0,
                "local_cumulative_qty": 0,
                "gap_qty": 0,
                "status": "PENDING",
                "broker_average_price": None,
                "last_broker_fill_id": None,
                "updated_at": None,
            })
    counts = {
        "MATCHED": sum(1 for r in rows if r["status"] == "MATCHED"),
        "MISMATCH": sum(1 for r in rows if r["status"] == "MISMATCH"),
        "PENDING": sum(1 for r in rows if r["status"] == "PENDING"),
        "MISSING_LOCAL": 0,
        "MISSING_BROKER": 0,
    }
    return {
        "reconciliation": rows,
        "summary": counts,
        "total": len(rows),
        "generated_at": _ts(),
    }


@router.get("/api/live/recon")
async def get_live_recon():
    return await asyncio.to_thread(_get_recon_sync)


# ═══════════════════════════════════════════════════════════════════════
# 11. TELEGRAM notifier
# ═══════════════════════════════════════════════════════════════════════

def _get_telegram_sync():
    if _engine is None:
        return {"error": "Engine not initialized"}
    stats = {}
    try:
        stats = dict(_engine.telegram.get_stats() or {})
    except Exception:
        stats = {}
    cfg = _engine.config
    try:
        enabled = bool(cfg.get("telegram", {}).get("enabled", False))
        chat_id = cfg.get("telegram", {}).get("chat_id", "")
        has_token = bool(cfg.get("telegram", {}).get("bot_token", ""))
    except Exception:
        enabled = False
        chat_id = ""
        has_token = False
    return {
        "enabled": enabled,
        "chat_id": chat_id,
        "bot_token_configured": has_token,
        "stats": stats,
        "generated_at": _ts(),
    }


@router.get("/api/live/telegram")
async def get_live_telegram():
    return await asyncio.to_thread(_get_telegram_sync)


# ═══════════════════════════════════════════════════════════════════════
# AGGREGATE — one payload for the consolidated LIVE panel page
# ═══════════════════════════════════════════════════════════════════════

def _global_timeline_sync(limit: int = 50) -> list[dict]:
    """Global recent event feed across all strategies/orders (no filter)."""
    out: list[dict] = []
    for r in _q("SELECT timestamp, event_type, strategy_id, instrument, details "
                "FROM events ORDER BY id DESC LIMIT 30"):
        ts = _epoch(r.get("timestamp"))
        if ts is None:
            continue
        out.append({
            "at": ts, "source": "LOCAL", "kind": r.get("event_type") or "event",
            "strategy_id": r.get("strategy_id"),
            "instrument": r.get("instrument"),
            "detail": str(r.get("details") or "")[:300],
        })
    for r in _q("SELECT event_type, strategy_id, instrument, timestamp, payload_json "
                "FROM trade_events ORDER BY sequence_no DESC LIMIT 20"):
        ts = _epoch(r.get("timestamp"))
        if ts is None:
            continue
        out.append({
            "at": ts, "source": "LOCAL", "kind": r.get("event_type") or "trade_event",
            "strategy_id": r.get("strategy_id"),
            "instrument": r.get("instrument"),
            "detail": str(r.get("payload_json") or "")[:300],
        })
    for r in _q("SELECT event_type, strategy_id, trade_id, order_id, "
                "error, action, final_state, created_at "
                "FROM execution_failure_events ORDER BY id DESC LIMIT 20"):
        ts = _epoch(r.get("created_at"))
        if ts is None:
            continue
        out.append({
            "at": ts, "source": "ALERT", "kind": r.get("event_type") or "exec_failure",
            "strategy_id": r.get("strategy_id"),
            "detail": f"{r.get('action') or ''} -> {r.get('final_state') or ''}: "
                      f"{r.get('error') or ''}"[:300],
        })
    for r in _q("SELECT action, endpoint, http_status, error_message, "
                "broker_order_id, request_timestamp "
                "FROM broker_api_events ORDER BY request_timestamp DESC LIMIT 20"):
        ts = _epoch(r.get("request_timestamp"))
        if ts is None:
            continue
        detail = f"{r.get('endpoint') or ''} -> HTTP {r.get('http_status') or '?'}"
        if r.get("error_message"):
            detail += f" error={r.get('error_message')}"
        out.append({
            "at": ts, "source": "BROKER", "kind": r.get("action") or "dhan",
            "detail": detail[:300],
        })
    out.sort(key=lambda o: o["at"], reverse=True)
    return out[:limit]


def _get_dashboard_sync():
    env = _live_env()
    if env is None:
        return {"error": "live environment not available", "execution_mode": None}
    sync = _get_sync_sync()
    return {
        "execution_mode": "LIVE",
        "generated_at": _ts(),
        "profile": _get_profile_sync(),
        "funds": _get_funds_sync(),
        "sync": sync,
        "candles": _candles_sync(),
        "signals": _get_signals_sync(30),
        "orders": _get_orders_sync(40),
        "positions": _get_positions_sync(),
        "pnl": _get_pnl_sync(),
        "recon": _get_recon_sync(),
        "telegram": _get_telegram_sync(),
        "timeline": _global_timeline_sync(50),
    }


@router.get("/api/live/dashboard")
async def get_live_dashboard():
    return await asyncio.to_thread(_get_dashboard_sync)
