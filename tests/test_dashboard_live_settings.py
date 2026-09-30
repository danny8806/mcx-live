import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from dashboard.routes import settings


class FakeConfig:
    def __init__(self, data):
        self._config = data

    def get(self, key, default=None):
        value = self._config
        for part in key.split("."):
            if not isinstance(value, dict) or part not in value:
                return default
            value = value[part]
        return value


class FakeEngine:
    def __init__(self, config, positions=None, orders=None):
        self.config = FakeConfig(config)
        self.strategies = {
            "gold_01": SimpleNamespace(quantity=1, pending_entry=None),
        }
        self.position_manager = SimpleNamespace(
            get_positions_by_strategy=lambda _sid: positions() if callable(positions) else (positions or [])
        )
        self.execution_engine = SimpleNamespace(_orders=orders)
        self._gate = {
            "live_gate": "OFF", "entry_enabled": False,
            "exit_enabled": True, "reversal_enabled": False,
            "sl_enabled": True, "close_only": True,
        }
        self.gate_changes = []

    def strategy_gate(self, _sid):
        return dict(self._gate)

    def set_strategy_gate(self, _sid, **fields):
        self._gate.update(fields)
        self.gate_changes.append(dict(fields))
        return self.strategy_gate(_sid)

    def snapshot(self, _name):
        return {}

    def notify_settings_refreshed(self):
        pass


def configured_engine(tmp_path: Path, *, positions=None, orders=None):
    raw_path = tmp_path / "raw.json"
    resolved_path = tmp_path / "resolved.json"
    data = {
        "strategies": {
            "gold_01": {
                "quantity": 1, "live_gate": "OFF", "entry_enabled": False,
                "exit_enabled": True, "reversal_enabled": False,
                "sl_enabled": True, "close_only": True,
            }
        }
    }
    raw_path.write_text(json.dumps(data), encoding="utf-8")
    resolved_path.write_text(json.dumps(data), encoding="utf-8")
    engine = FakeEngine(data, positions=positions, orders=orders or {})
    settings.init(engine, None, config_path=raw_path,
                  resolved_config_path=resolved_path)
    return engine, raw_path, resolved_path


def test_settings_write_persists_and_applies_quantity_and_gate(tmp_path):
    engine, raw_path, resolved_path = configured_engine(tmp_path)

    result = settings._save_strategy_settings_sync(
        "gold_01",
        {"quantity": 2, "live_gate": "ON", "entry_enabled": True,
         "close_only": False},
    )

    assert result["status"] == "saved"
    assert engine.strategies["gold_01"].quantity == 2
    assert engine._gate["live_gate"] == "ON"
    assert engine._gate["entry_enabled"] is True
    for path in (raw_path, resolved_path):
        saved = json.loads(path.read_text(encoding="utf-8"))["strategies"]["gold_01"]
        assert saved["quantity"] == 2
        assert saved["live_gate"] == "ON"


@pytest.mark.parametrize("positions", [
    lambda: [SimpleNamespace(is_open=True)],
    lambda: (_ for _ in ()).throw(RuntimeError("position store unavailable")),
])
def test_quantity_change_is_blocked_when_open_or_position_state_unknown(
    tmp_path, positions
):
    configured_engine(tmp_path, positions=positions)
    with pytest.raises(HTTPException) as exc:
        settings._save_strategy_settings_sync(
            "gold_01", {"quantity": 2})
    assert exc.value.status_code == 409


def test_quantity_change_is_blocked_when_order_state_unknown(tmp_path):
    configured_engine(tmp_path, orders=None)
    settings._engine.execution_engine._orders = None
    with pytest.raises(HTTPException) as exc:
        settings._save_strategy_settings_sync(
            "gold_01", {"quantity": 2})
    assert exc.value.status_code == 409


@pytest.mark.parametrize("body", [
    {"quantity": 0}, {"quantity": True}, {"quantity": 1.5},
    {"live_gate": "MAYBE"}, {"sl_enabled": "false"},
    {"unsupported": True},
])
def test_invalid_settings_are_rejected(tmp_path, body):
    configured_engine(tmp_path)
    with pytest.raises(HTTPException) as exc:
        settings._save_strategy_settings_sync(
            "gold_01", body)
    assert exc.value.status_code == 422

