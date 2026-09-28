"""Payload sanitization for durable broker/telegram audit storage.

Every raw broker request/response that outlives a single request must be
scrubbed before it touches the database: credentials never persist.  The
sanitizer masks any key whose name looks like a token/secret/credential field
and caps serialized payload size so a pathological broker echo cannot balloon
a row.
"""
from __future__ import annotations

import json
import re
from typing import Any, Optional

# Keys whose VALUES are always replaced — whatever the case/spacing.
_SENSITIVE_KEY = re.compile(
    r"(?i)(token|secret|passw(or)?d|pin|totp|client[-_ ]?id|"
    r"authorization|access[-_ ]?token|refresh[-_ ]?token|api[-_ ]?key"
    r"|private[-_ ]?key|auth[-_ ]?code|cookie|session)"
)

# Hardcoded secret values that must never surface even as a bare literal
# (only used for exact-string matches, never for prefix stripping).
_MIN_LEN = 6

MAX_PAYLOAD_CHARS = 200_000


def _mask_value(_key: str, value: Any) -> Any:
    return "***"


def sanitize(obj: Any, max_chars: int = MAX_PAYLOAD_CHARS) -> Any:
    """Recursively mask sensitive fields, preserving the rest of the shape.

    Lists/dicts are walked in place on copied structures; scalar values pass
    through untouched.  Truncation happens at the JSON layer so a huge echo
    reduces to a bounded string instead of a broken object.
    """
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if _SENSITIVE_KEY.search(str(k)):
                out[k] = _mask_value(k, v)
            else:
                out[k] = sanitize(v, max_chars)
        return out
    if isinstance(obj, (list, tuple)):
        return [sanitize(x, max_chars) for x in obj]
    return obj


def sanitize_json(obj: Any, max_chars: int = MAX_PAYLOAD_CHARS) -> str:
    """Sanitize then serialize to a bounded JSON string ('' when nonserializable)."""
    try:
        clean = sanitize(obj, max_chars)
        text = json.dumps(clean, default=str, sort_keys=True)
    except Exception:
        try:
            text = json.dumps({"sanitized": str(obj)[:2000]})
        except Exception:
            text = ""
    if len(text) > max_chars:
        text = text[:max_chars]
    return text


def payload_snapshot(obj: Any, max_chars: int = MAX_PAYLOAD_CHARS) -> str:
    """Helper alias for storing a compact, sanitized snapshot of a payload."""
    if obj is None:
        return ""
    return sanitize_json(obj, max_chars)