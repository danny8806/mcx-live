"""Single-SPA serving helper with per-service API/WS namespace injection.

The SPA is built once (``dashboard-ui/dist``).  Each backend serves the same
dist but injects a tiny config script into ``index.html`` that sets
``window.APP_API_BASE`` and ``window.APP_WS_BASE``, so a namespaced deployment
(e.g. ``/live/`` UI backed by ``/api/live`` + ``/live/ws``) reuses the exact
same bundle.  With no base set the SPA behaves exactly as before (same-origin
``/api/*`` and ``/ws``).
"""
from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

from fastapi import HTTPException
from fastapi.responses import FileResponse, Response


@lru_cache(maxsize=16)
def configured_index(dist: str, api_base: str, ws_base: str) -> str:
    index = Path(dist) / "index.html"
    html = index.read_text(encoding="utf-8")
    script = (
        "<script>"
        f"window.APP_API_BASE={json.dumps(api_base)};"
        f"window.APP_WS_BASE={json.dumps(ws_base)};"
        "</script>"
    )
    if "</head>" in html:
        return html.replace("</head>", script + "</head>", 1)
    return script + html


def configured_index_response(dist: str, api_base: str = "", ws_base: str = "") -> Response:
    return Response(
        content=configured_index(str(dist), api_base, ws_base),
        media_type="text/html",
    )


def include_frontend(app, dist: str, api_base: str = "", ws_base: str = "") -> None:
    """Add the SPA catch-all (with injected namespace) to ``app``.

    Call AFTER all API/WS routes so the catch-all does not shadow them; API/WS
    requests that fall through return 404.  Callers that already mount
    ``/assets`` keep doing so — this helper only handles the catch-all.
    """
    frontend_dist = Path(dist)
    if not frontend_dist.exists():
        return

    @app.get("/{full_path:path}", include_in_schema=False)
    async def serve_configured_frontend(full_path: str):
        if full_path.startswith("api/") or full_path.startswith("ws"):
            raise HTTPException(status_code=404)
        file_path = frontend_dist / full_path
        if file_path.is_file():
            return FileResponse(str(file_path))
        return configured_index_response(frontend_dist, api_base, ws_base)