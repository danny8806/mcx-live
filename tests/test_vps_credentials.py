import pytest

from vps_credentials import load_vps_password


def test_process_environment_takes_precedence_over_local_seed(tmp_path):
    seed = tmp_path / "mcx-trader.env"
    seed.write_text("VPS_PASS=seed-password\n", encoding="utf-8")

    assert load_vps_password(seed, {"VPS_PASS": "environment-password"}) == "environment-password"


def test_ignored_local_seed_is_supported_without_printing_credentials(tmp_path):
    seed = tmp_path / "mcx-trader.env"
    seed.write_text("OTHER=value\nVPS_PASS='seed-password'\n", encoding="utf-8")

    assert load_vps_password(seed, {}) == "seed-password"


def test_missing_credential_fails_closed(tmp_path):
    with pytest.raises(RuntimeError, match="VPS_PASS is required"):
        load_vps_password(tmp_path / "missing.env", {})
