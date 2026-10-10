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


class FakeSuperTrend:
    def __init__(self):
        self.atr_period = 10
        self.multiplier = 3.0
        self.source_name = "hl2"
        self.atr_method = "sma_tr"

    def configure(self, *, atr_period, multiplier, source, atr_method):
        self.atr_period = atr_period
        self.multiplier = multiplier
        self.source_name = source
        self.atr_method = atr_method

    def snapshot(self):
        return {
            "atr_period": self.atr_period,
            "multiplier": self.multiplier,
            "source": self.source_name,
            "atr_method": self.atr_method,
        }


class FakeEngine:
    def __init__(self, config, positions=None, orders=None):
        self.config = FakeConfig(config)
        self.strategies = {
            "gold_02": SimpleNamespace(quantity=1, pending_entry=None,
                                       pending_exit_trigger=None,
                                       strategy_type="supertrend",
                                       supertrend=FakeSuperTrend()),
        }
        self.position_manager = SimpleNamespace(
            get_positions_by_strategy=lambda _sid: positions() if callable(positions) else (positions or [])
        )
        self.execution_engine = SimpleNamespace(_orders=orders)
        self._gate = {
            "live_gate": "OFF", "entry_enabled": False,
            "exit_enabled": True, "reversal_enabled": False,
            "close_only": True,
        }
        self.gate_changes = []
        self.telegram = None

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


class FakeEventBus:
    def __init__(self):
        self.events = []

    def publish(self, event_type, data):
        self.events.append((event_type, data))


class FakeTelegram:
    def __init__(self):
        self.configuration_changes = []

    def on_configuration_change(self, data):
        self.configuration_changes.append(data)


def configured_engine(tmp_path: Path, *, positions=None, orders=None,
                      event_bus=None):
    raw_path = tmp_path / "raw.json"
    resolved_path = tmp_path / "resolved.json"
    data = {
        "strategies": {
            "gold_02": {
                "quantity": 1, "live_gate": "OFF", "entry_enabled": False,
                "exit_enabled": True, "reversal_enabled": False,
                "supertrend": {}, "close_only": True,
            }
        }
    }
    raw_path.write_text(json.dumps(data), encoding="utf-8")
    resolved_path.write_text(json.dumps(data), encoding="utf-8")
    engine = FakeEngine(data, positions=positions, orders=orders or {})
    engine.telegram = FakeTelegram()
    settings.init(engine, event_bus, config_path=raw_path,
                  resolved_config_path=resolved_path)
    return engine, raw_path, resolved_path


def test_settings_write_persists_and_applies_quantity_and_gate(tmp_path):
    event_bus = FakeEventBus()
    engine, raw_path, resolved_path = configured_engine(
        tmp_path, event_bus=event_bus)

    result = settings._save_strategy_settings_sync(
        "gold_02",
        {"quantity": 2, "live_gate": "ON", "entry_enabled": True,
         "close_only": False},
    )

    assert result["status"] == "saved"
    assert engine.strategies["gold_02"].quantity == 2
    assert engine._gate["live_gate"] == "ON"
    assert engine._gate["entry_enabled"] is True
    for path in (raw_path, resolved_path):
        saved = json.loads(path.read_text(encoding="utf-8"))["strategies"]["gold_02"]
        assert saved["quantity"] == 2
        assert saved["live_gate"] == "ON"
    assert event_bus.events[0][0] == "settings_changed"
    assert event_bus.events[0][1]["changes"]["quantity"] == {
        "from": 1, "to": 2}
    assert engine.telegram.configuration_changes == [event_bus.events[0][1]]


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
            "gold_02", {"quantity": 2})
    assert exc.value.status_code == 409


def test_quantity_change_is_blocked_when_order_state_unknown(tmp_path):
    configured_engine(tmp_path, orders=None)
    settings._engine.execution_engine._orders = None
    with pytest.raises(HTTPException) as exc:
        settings._save_strategy_settings_sync(
            "gold_02", {"quantity": 2})
    assert exc.value.status_code == 409


def test_open_supertrend_position_can_save_entry_gate(tmp_path):
    engine, raw_path, _ = configured_engine(
        tmp_path, positions=lambda: [SimpleNamespace(is_open=True)])
    result = settings._save_strategy_settings_sync(
        "gold_02", {"live_gate": "CLOSE_ONLY", "entry_enabled": False,
                    "reversal_enabled": False, "close_only": True})

    assert result["status"] == "saved"
    saved = json.loads(raw_path.read_text(encoding="utf-8"))["strategies"]["gold_02"]
    assert "sl_enabled" not in saved
    assert engine._gate["exit_enabled"] is True


@pytest.mark.parametrize("body", [
    {"quantity": 0}, {"quantity": True}, {"quantity": 1.5},
    {"live_gate": "MAYBE"}, {"obsolete_stop_gate": False},
    {"unsupported": True},
])
def test_invalid_settings_are_rejected(tmp_path, body):
    configured_engine(tmp_path)
    with pytest.raises(HTTPException) as exc:
        settings._save_strategy_settings_sync(
            "gold_02", body)
    assert exc.value.status_code == 422


def test_every_exposed_gate_mode_and_switch_combination_applies(tmp_path):
    engine, _raw, _resolved = configured_engine(tmp_path)
    modes = ("ON", "OFF", "CLOSE_ONLY", "EMERGENCY_STOP", "LOCKED")
    for index, mode in enumerate(modes):
        controls = {
            "live_gate": mode,
            "entry_enabled": bool(index & 1),
            "exit_enabled": bool(index & 2),
            "reversal_enabled": bool(index & 4),
            "close_only": bool(index & 8),
        }
        result = settings._save_strategy_settings_sync("gold_02", controls)
        assert result["gate"] == controls
        assert engine._gate == controls


def test_all_supertrend_inputs_and_quantity_apply_while_flat(tmp_path):
    engine, raw_path, resolved_path = configured_engine(tmp_path)
    engine.config._config["strategies"]["gold_02"]["supertrend"] = {
        "atr_period": 10, "multiplier": 3.0,
        "source": "hl2", "atr_method": "sma_tr",
    }
    for path in (raw_path, resolved_path):
        config = json.loads(path.read_text(encoding="utf-8"))
        config["strategies"]["gold_02"]["supertrend"] = {
            "atr_period": 10, "multiplier": 3.0,
            "source": "hl2", "atr_method": "sma_tr",
        }
        path.write_text(json.dumps(config), encoding="utf-8")

    sources = ("open", "high", "low", "close", "hl2", "hlc3", "ohlc4")
    atr_methods = ("sma_tr", "wilder")
    for index, source in enumerate(sources):
        for atr_method in atr_methods:
            period = 1 if index == 0 else 500 if index == len(sources) - 1 else 10 + index
            result = settings._save_strategy_settings_sync(
                "gold_02", {
                    "quantity": 100 if index == len(sources) - 1 else 1,
                    "supertrend": {
                        "atr_period": period,
                        "multiplier": 0.1 + index,
                        "source": source,
                        "atr_method": atr_method,
                    },
                })
            assert result["quantity"] == (100 if index == len(sources) - 1 else 1)
            assert result["supertrend"]["atr_period"] == period
            assert result["supertrend"]["source"] == source
            assert result["supertrend"]["atr_method"] == atr_method
    assert engine.strategies["gold_02"].supertrend.atr_period == 500
    assert engine.strategies["gold_02"].quantity == 100


def configured_live_master_gate(tmp_path: Path, *, gate="OFF",
                                live_enabled=True, broker_positions=None,
                                open_positions=None, orders=None,
                                strategies=None):
    raw_path = tmp_path / "raw-live.json"
    resolved_path = tmp_path / "resolved-live.json"
    data = {"live": {"gate": gate,
                     "live_trading_enabled": live_enabled},
            "strategies": {}}
    raw_path.write_text(json.dumps(data), encoding="utf-8")
    resolved_path.write_text(json.dumps(data), encoding="utf-8")
    engine = FakeEngine(data)
    broker = SimpleNamespace(
        gate_enabled=gate == "ON",
        positions=lambda: broker_positions or [],
    )
    env = SimpleNamespace(
        is_live=True, gate_state=gate, gate_enabled=gate == "ON",
        broker=broker,
        position_manager=SimpleNamespace(open_positions=open_positions or []),
        strategies=strategies or {},
        execution_engine=SimpleNamespace(_orders=orders or {}, broker=broker),
        persistence=None,
    )
    engine.live = env
    engine._envs = {"live": env}
    engine._lock = __import__("threading").RLock()
    settings.init(engine, None, config_path=raw_path,
                  resolved_config_path=resolved_path)
    return engine, env, raw_path, resolved_path


def test_master_gate_on_updates_runtime_and_both_config_files(tmp_path):
    event_bus = FakeEventBus()
    engine, env, raw_path, resolved_path = configured_live_master_gate(tmp_path)
    engine.telegram = FakeTelegram()
    settings._event_bus = event_bus

    result = settings._save_live_gate_sync({"gate": "ON"})

    assert result["status"] == "saved"
    assert result["broker_gate_enabled"] is True
    assert env.gate_enabled is True
    assert env.broker.gate_enabled is True
    for path in (raw_path, resolved_path):
        assert json.loads(path.read_text(encoding="utf-8"))["live"]["gate"] == "ON"
    assert event_bus.events[0][0] == "settings_changed"
    assert event_bus.events[0][1]["changes"]["gate"] == {
        "from": "OFF", "to": "ON"}
    assert engine.telegram.configuration_changes == [event_bus.events[0][1]]


def test_master_gate_off_requires_broker_flat_and_no_active_orders(tmp_path):
    _engine, env, raw_path, _resolved = configured_live_master_gate(
        tmp_path, gate="ON", broker_positions=[{"net_quantity": 1}])

    with pytest.raises(HTTPException) as exc:
        settings._save_live_gate_sync({"gate": "OFF"})

    assert exc.value.status_code == 409
    assert env.gate_enabled is True
    assert json.loads(raw_path.read_text(encoding="utf-8"))["live"]["gate"] == "ON"

    active_dir = tmp_path / "active-order"
    active_dir.mkdir()
    _engine, env, _raw, _resolved = configured_live_master_gate(
        active_dir, gate="ON",
        orders={"order-1": SimpleNamespace(state="submitted")})

    with pytest.raises(HTTPException) as exc:
        settings._save_live_gate_sync({"gate": "OFF"})
    assert exc.value.status_code == 409
    assert env.gate_enabled is True


def test_master_gate_on_requires_live_trading_enabled(tmp_path):
    _engine, env, raw_path, _resolved = configured_live_master_gate(
        tmp_path, live_enabled=False)

    with pytest.raises(HTTPException) as exc:
        settings._save_live_gate_sync({"gate": "ON"})

    assert exc.value.status_code == 409
    assert env.gate_enabled is False
    assert json.loads(raw_path.read_text(encoding="utf-8"))["live"]["gate"] == "OFF"


@pytest.mark.parametrize("kwargs", [
    {"open_positions": [SimpleNamespace(is_open=True)]},
    {"broker_positions": RuntimeError("Dhan unavailable")},
])
def test_master_gate_off_blocks_position_or_unavailable_broker_state(tmp_path, kwargs):
    _engine, env, raw_path, _resolved = configured_live_master_gate(
        tmp_path, gate="ON", **kwargs)
    if isinstance(kwargs.get("broker_positions"), Exception):
        env.broker.positions = lambda: (_ for _ in ()).throw(
            RuntimeError("Dhan unavailable"))

    with pytest.raises(HTTPException) as exc:
        settings._save_live_gate_sync({"gate": "OFF"})

    assert exc.value.status_code in {409, 503}
    assert env.gate_enabled is True
    assert json.loads(raw_path.read_text(encoding="utf-8"))["live"]["gate"] == "ON"


def test_master_gate_off_blocks_pending_strategy_trigger(tmp_path):
    strategy = SimpleNamespace(pending_entry=object(), pending_exit_trigger=None)
    _engine, env, _raw, _resolved = configured_live_master_gate(
        tmp_path, gate="ON", strategies={"gold_02": strategy})

    with pytest.raises(HTTPException) as exc:
        settings._save_live_gate_sync({"gate": "OFF"})

    assert exc.value.status_code == 409
    assert env.gate_enabled is True


def test_master_gate_rejects_invalid_or_extra_fields(tmp_path):
    configured_live_master_gate(tmp_path)
    for body in ({"gate": "CLOSE_ONLY"}, {"gate": "ON", "enabled": True}):
        with pytest.raises(HTTPException) as exc:
            settings._save_live_gate_sync(body)
        assert exc.value.status_code == 422

