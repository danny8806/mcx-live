import re
from pathlib import Path

from live.api import create_live_app


def test_frontend_api_roots_are_served_by_live_backend():
    workspace = Path(__file__).resolve().parents[1]
    frontend_api = (workspace / "dashboard-ui/src/lib/api.ts").read_text(
        encoding="utf-8")
    frontend_roots = set(re.findall(r"/api/[a-zA-Z0-9_/-]+", frontend_api))
    backend_paths = {
        route.path for route in create_live_app().routes
        if getattr(route, "path", None)
    }

    unmatched = sorted(
        root for root in frontend_roots
        if not any(path == root or path.startswith(root + "/")
                   or root.startswith(path + "/")
                   for path in backend_paths)
    )
    assert not unmatched, f"frontend paths missing from backend: {unmatched}"
