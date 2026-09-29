from pathlib import Path

from scripts.deployment_context import should_skip


def test_vps_diagnostics_never_sync_to_deployment_host_or_image():
    for path in (
        "p6_deploy.py", "p6_dhan.py", "live_verify.py", "vps_phase6.py",
        "verify_vps.py", "check_frontend.py", "tools/remedy_deploy.py",
    ):
        assert should_skip(path), f"diagnostic artifact entered deploy sync: {path}"

    assert not should_skip("live/api.py")
    assert not should_skip("execution/live/engine.py")
    assert not should_skip("config/live_settings.json")


def test_dockerignore_excludes_root_diagnostic_artifacts():
    root = Path(__file__).resolve().parents[1]
    rules = set((root / ".dockerignore").read_text().splitlines())
    for rule in (
        "/tools/", "/scripts/", "/p6_*.py", "/live_verify*.py", "/vps_*.py",
        "/verify_*.py", "/check_*.py", "/deploy_restart.py",
    ):
        assert rule in rules
