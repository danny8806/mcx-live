"""Shared HTTP contract handling for API error envelopes."""
from __future__ import annotations

import json

from starlette.responses import JSONResponse, Response


def status_for_error(error: str) -> int:
    """Choose an HTTP status for the existing ``{"error": ...}`` envelope."""
    message = str(error or "").strip().lower()
    if any(token in message for token in (
            "not found", "outside the exclusive strategy runtime")):
        return 404
    if any(token in message for token in (
            "not initialized", "unavailable", "not ready")):
        return 503
    return 500


async def normalize_api_error_status(request, call_next):
    """Prevent error-shaped API JSON from being returned as HTTP 200.

    Some legacy read models return a JSON ``error`` field instead of raising
    an HTTPException. Preserve that payload for existing clients while also
    setting a meaningful HTTP status and a standard ``detail`` field.
    """
    response = await call_next(request)
    if (not request.url.path.startswith("/api/")
            or response.status_code != 200
            or "application/json" not in response.headers.get("content-type", "")):
        return response

    body = getattr(response, "body", None)
    if body is None:
        body = b"".join([chunk async for chunk in response.body_iterator])
    try:
        payload = json.loads(body)
    except (TypeError, ValueError, UnicodeDecodeError):
        return Response(body, status_code=response.status_code,
                        headers=dict(response.headers),
                        background=response.background)

    error = payload.get("error") if isinstance(payload, dict) else None
    if not isinstance(error, str) or not error.strip():
        return Response(body, status_code=response.status_code,
                        headers=dict(response.headers),
                        background=response.background)

    payload.setdefault("detail", error)
    headers = dict(response.headers)
    headers.pop("content-length", None)
    return JSONResponse(payload, status_code=status_for_error(error),
                        headers=headers, background=response.background)
