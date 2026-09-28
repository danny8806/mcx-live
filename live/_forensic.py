"""FORENSIC single-shot test-signal endpoint (REAL Dhan order pipeline test).

Explicit safe mechanism created for the REAL-DHAN forensic test:
* ARMED only while the marker file ``/app/live/data/db/forensic_test.armed``
  exists (created manually; deleted after the one test).
* Consumed exactly ONCE; every later call returns 403.
* Runs inside the RUNNING live process using its own engine, broker, token,
  persistence and poller — identical to production candle->signal->order flow.
* Refuses to submit whenever: gate OFF, safe mode active, market not in
  trading-allowed state, margin NOT provably insufficient at Dhan, or the
  local LIVE risk book would reject first (so the order can never be an
  unintentional live fill).  A real Dhan rejection is the expected PASS.
"""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path

from fastapi import APIRouter, Body, HTTPException

from strategies.types import Signal, SignalType

_log = logging.getLogger("live.forensic")

AMRED = "/app/live/data/db/forensic_test.armed"
_consumed = {"used": False}

router = APIRouter(prefix="/api/live/_forensic", tags=["forensic"])


def _mask(v) -> str:
    s = str(v or "")
    return s[:4] + "****" + s[-3:] if len(s) > 6 else "***"


def _safe_funds(d: dict) -> dict:
    out = {}
    for k, v in (d or {}).items():
        if any(t in k.lower() for t in ("margin", "avail", "balance")):
            out[k] = v
    out["client_id"] = _mask((d or {}).get("clientId") or (d or {}).get("dhanClientId"))
    return out


@router.get("/status")
def status():
    import live.api as api
    engine = api._engine
    env = getattr(engine, "live", None) if engine is not None else None
    return {
        "armed": Path(AMRED).exists(),
        "consumed": _consumed["used"],
        "engine_up": engine is not None,
        "trading_allowed": bool(env is not None and env.market_status.is_trading_allowed),
        "gate_enabled": bool(env is not None and env.gate_enabled),
        "safe_mode": bool(env is not None and env.safe_mode is not None and env.safe_mode.is_active),
    }


@router.get("/probe")
def probe(promote: bool = False):
    import live.api as api
    engine = api._engine
    env = getattr(engine, "live", None) if engine is not None else None
    if env is None:
        raise HTTPException(503, "live engine not running")
    if promote:
        _log.error("[FORENSIC] calling production _maybe_enable_trading() once")
        try:
            engine._maybe_enable_trading()
        except Exception as e:
            _log.error("[FORENSIC] _maybe_enable_trading error: %s", e)
    ms = env.market_status
    import time as _t
    out = {
        "trading_allowed": bool(ms.is_trading_allowed),
        "has_live_market_data": bool(ms.has_live_market_data),
        "rest_fresh_age_s": round(_t.time() - (ms._rest_last_tick_time or 0), 1),
        "state": str(getattr(ms, "state", None)),
        "engine_status": str(getattr(ms, "engine_status", None)),
        "data_status": str(getattr(ms, "data_status", None)),
        "safe_mode": bool(env.safe_mode is not None and env.safe_mode.is_active),
        "gate_enabled": bool(env.gate_enabled),
    }
    _log.error("[FORENSIC] PROBE: %s", json.dumps(out, default=str))
    return out


@router.post("/order")
def order(req: dict = Body(default={})):
    import live.api as api
    if not Path(AMRED).exists():
        raise HTTPException(403, "DISARMED: forensic_marker absent; no order submitted")
    if _consumed["used"]:
        raise HTTPException(403, "DISARMED: forensic endpoint already consumed once")
    engine = api._engine
    if engine is None:
        raise HTTPException(503, "live engine not running")
    env = engine.live
    sid = req.get("strategy_id") or "gold_01"
    inst = req.get("instrument") or "GOLDM"
    strat = env.strategies.get(sid)
    if strat is None:
        raise HTTPException(404, f"unknown strategy {sid}")
    broker = getattr(env, "broker", None)
    if broker is None:
        raise HTTPException(503, "no live broker")

    if not env.gate_enabled:
        raise HTTPException(409, "master gate OFF — refusing to submit")
    if env.safe_mode is not None and env.safe_mode.is_active:
        raise HTTPException(409, "safe mode active — refusing to submit")
    if not env.market_status.is_trading_allowed:
        raise HTTPException(409, "market not in trading-allowed state — refusing to submit")

    quote = broker.circuit_quote(inst) or {}
    ltp = quote.get("ltp")
    ws_ltp = None
    adapter = getattr(env, "data_adapter", None) or getattr(env, "adapter", None)
    ws_cache = getattr(adapter, "_live_ltp", {}) if adapter is not None else {}
    wsc = ws_cache.get(inst)
    if isinstance(wsc, dict) and wsc.get("ltp"):
        ws_ltp = float(wsc["ltp"])
    ltp = float(ws_ltp) if ws_ltp else ltp
    if not ltp:
        raise HTTPException(
            503,
            f"no live LTP — cannot prove margin "
            f"(rest_quote={quote} ws_cache_keys={list(ws_cache.keys())})",
        )
    px = round(float(ltp))
    stop = round(float(ltp) * 0.99)
    qty = 1
    required = float(engine._calculate_margin(inst, px, qty))

    acct = env.account_engines.get(sid)
    book_avail = float(getattr(acct, "available_margin", 0.0) or 0.0) if acct is not None else 0.0
    funds = broker.account_status() or {}
    real_avail = float(funds.get("availableMargin")
                       or funds.get("availabelMargin")
                       or funds.get("available") or 0.0)

    _log.error("[FORENSIC] PRE-LOG: instrument=%s ltp=%s px=%s required_margin=%s "
               "real_available=%s book_available=%s",
               inst, ltp, px, required, real_avail, book_avail)
    if not (required > real_avail):
        raise HTTPException(409, "REAL margin could cover the order — refusing (must be provably insufficient)")
    if not (required <= book_avail):
        raise HTTPException(409, "LOCAL risk book would reject BEFORE Dhan — order would never reach the broker")

    sig = Signal(
        signal_type=SignalType.LONG,
        instrument=inst,
        strategy_id=sid,
        timestamp=time.time(),
        trigger_price=px,
        stop_price=stop,
        quantity=qty,
        side="LONG",
        metadata={
            "pending": True,
            "triggered": True,
            "entry_price": px,
            "fill_price": px,
            "executed": True,
            "signal_source": "forensic-real-dhan-pipeline-test",
            "forensic_test": True,
            "reason": "deliberate-insufficient-margin-test",
            "base_ltp": ltp,
        },
    )
    _consumed["used"] = True
    _log.error("[FORENSIC] SUBMITTING ONE REAL DHAN ORDER signal=%s px=%s stop=%s qty=%s",
               sig.signal_id, px, stop, qty)
    with engine._lock:
        engine._process_signal(sig, "live")

    rt = env.runtimes.require(sid)
    ex = rt.order_manager.execution_engine
    mine = [o for o in ex._orders.values()
            if getattr(o, "strategy_id", None) == sid
            and getattr(o, "instrument", None) == inst]
    last = mine[-1] if mine else None
    out = {
        "signal_id": sig.signal_id,
        "quote": {**{k: v for k, v in quote.items() if k != "raw"}, "ws_ltp": ws_ltp},
        "required_margin": required,
        "real_available": real_avail,
        "local_book_available": book_avail,
        "order": ({
            "order_id": last.order_id,
            "state": last.state.value if hasattr(last.state, "value") else str(last.state),
            "reason": getattr(last, "reason", None),
            "broker_order_id": getattr(last, "_broker_order_id", None),
            "correlation_id": getattr(last, "correlation_id", None),
        } if last is not None else None),
    }
    _log.error("[FORENSIC] ORDER RESULT: %s", json.dumps(out, default=str))
    return out


@router.get("/verify")
def verify():
    import live.api as api
    engine = api._engine
    env = getattr(engine, "live", None) if engine is not None else None
    if env is None:
        raise HTTPException(503, "live engine not running")
    broker = getattr(env, "broker", None)
    out = {"funds": {}, "orderbook": [], "tradebook": [], "positions": []}
    if broker is not None:
        out["funds"] = _safe_funds(broker.account_status() or {})
        ob = broker.day_order_book() or []
        out["orderbook"] = ob if isinstance(ob, list) else [ob]
        tb = broker.tradebook() or []
        out["tradebook"] = tb if isinstance(tb, list) else [tb]
        pss = broker.positions() or []
        out["positions"] = pss if isinstance(pss, list) else [pss]
    return out