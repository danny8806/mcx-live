"""Dhan REST completed-candle classification + warmup forensics (MCX).

Direct market-data contract:
  - Dhan REST historical OHLC is the authoritative completed-candle source.
  - ``end_time`` of a request is a fetch boundary, never a market-close rule.
  - The returned dataset determines the latest available completed candle
    (weekend/weekday restart alike - the latest valid trading candle wins).
  - A candle is completed only when its end instant has elapsed; a forming
    candle must never enter a completed indicator stream.

The session check is START-based and valid for every native timeframe:
  a 5m/15m candle starting at 23:25/23:15 (ends 23:30) is kept, the 23:00 1H
  candle (ends 00:00) is kept, and any candle whose start is at/after the
  session close (e.g. Dhan's post-close 23:30 echo) is rejected.
"""

from __future__ import annotations

import math
import time as _time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

IST = timezone(timedelta(hours=5, minutes=30))

TF_MINUTES = {"5": 5, "15": 15, "60": 60}

REASON_FORMING = "FORMING_CANDLE"
REASON_OUTSIDE = "OUTSIDE_SUPPORTED_SESSION"
REASON_BAD_TS = "INVALID_TIMESTAMP"
REASON_BAD_OHLC = "INVALID_OHLC"


def tf_minutes(tf_id: str) -> int:
    return TF_MINUTES.get(str(tf_id), 5)


def parse_hhmm(value: str) -> int:
    h, m = (value or "23:30").split(":")
    return int(h) * 60 + int(m)


def ist_wall_minutes(open_ts: float) -> int:
    dt = datetime.fromtimestamp(float(open_ts), tz=IST)
    return dt.hour * 60 + dt.minute


def effective_end_ts(
    open_ts: float,
    tf_id: str,
    session_close: str = "23:30",
) -> float:
    """Grid end capped by the session-close instant of the candle's own day.

    A native 1H candle starting at 23:00 has grid end 00:00 next day even
    though its actual market data finishes at the session close (23:30).
    Completion is judged against the effective end so the final 1H candle of
    an evening or weekend restart is the latest available and genuinely
    final data (req-7 23:00 1H preserved; no arbitrary open>=23:30 reject).
    """
    dt = datetime.fromtimestamp(float(open_ts), tz=IST)
    h, m = (session_close or "23:30").split(":")
    close_instant = int(dt.replace(
        hour=int(h), minute=int(m), second=0, microsecond=0).timestamp())
    grid_end = open_ts + tf_minutes(tf_id) * 60
    return min(grid_end, close_instant) if close_instant < grid_end else grid_end


def classify(
    candle: list,
    tf_id: str,
    now_epoch: Optional[int] = None,
    session_open: str = "09:00",
    session_close: str = "23:30",
) -> tuple[bool, Optional[str]]:
    """Return (accepted, reason). Accepted == genuinely completed data."""
    if now_epoch is None:
        now_epoch = int(_time.time())
    try:
        open_ts = float(candle[0])
    except (TypeError, ValueError):
        return False, REASON_BAD_TS
    if not math.isfinite(open_ts):
        return False, REASON_BAD_TS
    if effective_end_ts(open_ts, tf_id, session_close) > now_epoch:
        return False, REASON_FORMING
    for v in candle[1:5]:
        if v is None or not math.isfinite(float(v)):
            return False, REASON_BAD_OHLC
    wall = ist_wall_minutes(open_ts)
    if wall < parse_hhmm(session_open):
        return False, REASON_OUTSIDE
    if wall >= parse_hhmm(session_close):
        return False, REASON_OUTSIDE
    return True, None


def filter_completed(
    candles: list,
    tf_id: str,
    now_epoch: Optional[int] = None,
    session_open: str = "09:00",
    session_close: str = "23:30",
) -> tuple[list, list]:
    """Split Dhan candles into (accepted, [(reason, candle), ...])."""
    accepted: list = []
    rejected: list = []
    for c in candles:
        ok, reason = classify(c, tf_id, now_epoch, session_open, session_close)
        if ok:
            accepted.append(c)
        else:
            rejected.append((reason, c))
    return accepted, rejected


def latest_completed(
    candles: list,
    tf_id: str,
    now_epoch: Optional[int] = None,
    session_open: str = "09:00",
    session_close: str = "23:30",
) -> Optional[list]:
    accepted, _ = filter_completed(
        candles, tf_id, now_epoch, session_open, session_close)
    if not accepted:
        return None
    return max(accepted, key=lambda c: float(c[0]))


def iso_ist(open_ts: float) -> str:
    return datetime.fromtimestamp(float(open_ts), tz=IST).isoformat(timespec="seconds")