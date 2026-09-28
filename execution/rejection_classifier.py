"""Deterministic broker-rejection / failure classification (retry matrix).

Maps a broker error (HTTP status, Dhan error code, message fragment or an
order-status observation) into ONE category with an explicit retry policy::

    NONE           nothing was submitted; nothing to reconcile; no retry.
    RETRYABLE      transport is degraded (timeout / 429 / 5xx / disconnected);
                   the order MAY still be in flight at the broker -> the
                   caller must RESOLVE first (correlation / order book) and
                   only retry placement once the state is known settled.
    VALIDATION     deterministic input rejection (bad price/qty/type): the
                   order can NEVER be placed; same-input retry is pointless.
    RISK           ambiguous / unknown order state: never retry, escalate to
                   reconciliation-style handling.

The classifier is intentionally pure and testable: no I/O, no side effects.
``classify_error`` is the single entry point used by the poller and engine.
"""
from __future__ import annotations

import re
from typing import Optional

# ── categories ────────────────────────────────────────────────────────────
NONE = "NONE"
RETRYABLE = "RETRYABLE"
VALIDATION = "VALIDATION"
RISK = "RISK"

_CATEGORY_MAX_RETRIES = {
    NONE: 0,
    RETRYABLE: 3,
    VALIDATION: 0,
    RISK: 0,
}

# Transport-level failures (HTTP / socket / auth) whose only safe recovery is
# "resolve the broker's truth before doing anything else".
_RETRYABLE_HTTP = {429, 500, 502, 503, 504}
_RETRYABLE_HTTP_REASONS = ("timeout", "timed out", "connection", "reset",
                           "unavailable", "unreachable", "rate", "throttl",
                           "too many", "network", "ssl", "keepalive")

# Deterministic input rejections — the order never reached the exchange; the
# same payload would be rejected again.  Dhan uses DH-9xx error codes.
_VALIDATION_PREFIXES = ("DH-901", "DH-902", "DH-903", "DH-904", "DH-905",
                        "DH-906", "DH-907", "DH-908", "DH-914", "DH-915")
_VALIDATION_REASONS = ("invalid", "not found", "incorrect", "missing",
                       "required", "mismatch", "unsupported", "exceeds",
                       "limit", "lot", "quantity", "price", "trigger",
                       "segment", "symbol", "security")

# Dhan order-status values that are themselves terminal + action-defining.
_TERMINAL = ("rejected", "cancelled", "canceled", "expired")


def _category_max_retries(category: str) -> int:
    return _CATEGORY_MAX_RETRIES.get(category, 0)


def classify_error(
    *,
    http_status: Optional[int] = None,
    error_code: Optional[str] = None,
    message: Optional[str] = None,
    order_status: Optional[str] = None,
) -> dict:
    """Classify one failure signal into the retry matrix.

    All inputs are optional; the classifier is monotone (a RISK signal wins
    over everything, then VALIDATION, then RETRYABLE).  Returns a dict with
    ``category``, ``retryable``, ``max_retries``, ``reason`` and ``level``.
    """
    reason = ""
    category = NONE

    status = (str(order_status or "") or "").strip().lower()
    code = (str(error_code or "") or "").strip().upper()
    msg = (str(message or "") or "").strip()

    # 1. A known terminal broker status is authoritative for the ORDER state,
    #    independent of transport failures.
    if status in _TERMINAL:
        category = VALIDATION if status == "rejected" else NONE
        reason = f"broker_terminal_status={status}"
        if status == "rejected":
            return {"category": category, "retryable": False,
                    "max_retries": 0, "reason": reason, "level": "alert"}
        return {"category": category, "retryable": False,
                "max_retries": 0, "reason": reason, "level": "info"}

    # 2. Deterministic Dhan DH-9xx input rejections.
    hit = next((p for p in _VALIDATION_PREFIXES if code.startswith(p)), None)
    if hit is not None:
        return {"category": VALIDATION, "retryable": False,
                "max_retries": 0, "reason": f"dhan_error_code={code}",
                "level": "alert"}

    # 3. Message heuristics (validation before transport).
    low = msg.lower()
    if any(k in low for k in _VALIDATION_REASONS):
        # "connection limit" is ambiguous; only treat as validation when a
        # DH-/OMS code is present, otherwise fall through to transport.
        if code or "dh-" in low or "oms" in low or "reject" in low:
            return {"category": VALIDATION, "retryable": False,
                    "max_retries": 0, "reason": f"validation_message={msg[:80]}",
                    "level": "alert"}

    # 4. Transport-layer failures.
    if http_status in _RETRYABLE_HTTP:
        return {"category": RETRYABLE, "retryable": True,
                "max_retries": _category_max_retries(RETRYABLE),
                "reason": f"http_{http_status}", "level": "warn"}
    if any(k in low for k in _RETRYABLE_HTTP_REASONS):
        if code:
            return {"category": VALIDATION, "retryable": False,
                    "max_retries": 0,
                    "reason": f"code_with_transport_msg={code}:{msg[:60]}",
                    "level": "alert"}
        return {"category": RETRYABLE, "retryable": True,
                "max_retries": _category_max_retries(RETRYABLE),
                "reason": f"transport={msg[:80]}", "level": "warn"}

    # 5. No usable signal — never blind-retry: the safe default is RESOLVE.
    return {"category": RISK, "retryable": False,
            "max_retries": 0,
            "reason": "unknown_failure_requires_reconciliation", "level": "risk"}


def classify_order_result(result: dict) -> dict:
    """Classify a broker placement / cancel / modify result dict.

    Reads ``{status, http_status, error_code, reason}`` and returns the same
    shape as :func:`classify_error`.
    """
    return classify_error(
        http_status=result.get("http_status"),
        error_code=result.get("error_code") or result.get("code"),
        message=result.get("reason") or result.get("message"),
        order_status=result.get("status") or result.get("raw_status"),
    )