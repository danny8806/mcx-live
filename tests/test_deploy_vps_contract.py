import json

import pytest

from deploy_vps import (
    api_error_probe_is_valid,
    frontend_probe_is_valid,
    health_payload_is_ready,
    parse_http_probe,
    require_success,
)


def test_http_probe_parser_uses_explicit_status_and_content_type_markers():
    body = json.dumps({"status": "ok", "engine": True,
                       "persistence": True, "live_only": True})
    output = f"{body}\n__HTTP_STATUS__200\n__CONTENT_TYPE__application/json"

    parsed_body, status, media_type = parse_http_probe(output)

    assert parsed_body == body
    assert status == 200
    assert media_type == "application/json"


def test_readiness_rejects_http_200_html_and_partial_engine_startup():
    assert not health_payload_is_ready(
        "/api/health", "<html>Next fallback</html>", 200, "text/html")
    assert not health_payload_is_ready(
        "/api/health",
        '{"status":"ok","engine":true,"persistence":false,"live_only":true}',
        200, "application/json")


def test_readiness_requires_all_service_checks():
    body = json.dumps({
        "status": "ok",
        "checks": {"engine": True, "persistence": True, "live_only": True},
    })
    assert health_payload_is_ready("/health", body, 200, "application/json")
    assert not health_payload_is_ready("/health", body, 502, "application/json")
    assert not health_payload_is_ready("/health", body, 200, "text/html")


def test_required_deploy_command_failure_is_not_treated_as_success():
    with pytest.raises(RuntimeError, match="Docker build failed"):
        require_success(("partial output", "builder error", 1),
                        "Docker build")


def test_proxy_html_fallback_is_rejected_for_api_error_routes():
    next_html = (
        '<!DOCTYPE html><html><head><title>Not Found</title></head></html>'
        '\n__HTTP_STATUS__404\n__CONTENT_TYPE__text/html; charset=utf-8')
    api_json = (
        '{"detail":"environment not found"}'
        '\n__HTTP_STATUS__404\n__CONTENT_TYPE__application/json')

    assert not api_error_probe_is_valid(next_html, 0)
    assert api_error_probe_is_valid(api_json, 0)


def test_frontend_probe_requires_the_actual_dashboard_root_and_bundle():
    valid = (
        '<html><div id="root"></div><script src="/assets/app.js"></script></html>'
        '\n__HTTP_STATUS__200\n__CONTENT_TYPE__text/html')
    wrong_site = (
        '<html><div id="other"></div></html>'
        '\n__HTTP_STATUS__200\n__CONTENT_TYPE__text/html')

    assert frontend_probe_is_valid(valid, 0)
    assert not frontend_probe_is_valid(wrong_site, 0)
